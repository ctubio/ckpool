/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include "config.h"

#include <sys/socket.h>
#include <jansson.h>
#include <string.h>
#include <unistd.h>

#include "ckpool.h"
#include "libckpool.h"
#include "generator.h"
#include "bitcoin.h"
#include "stratifier.h"
#include "uthash.h"
#include "utlist.h"

struct notify_instance {
	/* Hash table data */
	UT_hash_handle hh;
	int id;

	char prevhash[68];
	char *jobid;
	char *coinbase1;
	char *coinbase2;
	int merkles;
	char merklehash[16][68];
	char nbit[12];
	char ntime[12];
	char bbversion[12];
	bool clean;

	time_t notify_time;
};

typedef struct notify_instance notify_instance_t;

/* Per proxied pool instance data */
struct proxy_instance {
	ckpool_t *ckp;
	connsock_t *cs;

	const char *auth;
	const char *pass;

	char *enonce1;
	char *enonce1bin;
	int nonce1len;
	char *sessionid;
	int nonce2len;

	tv_t last_message;

	double diff;
	int absolute_shares;
	int diff_shares;
	tv_t last_share;

	int id; /* Message id for sending stratum messages */
	bool no_sessionid; /* Doesn't support session id resume on subscribe */
	bool no_params; /* Doesn't want any parameters on subscribe */

	bool notified; /* Received new template for work */
	bool diffed; /* Received new diff */

	pthread_mutex_t notify_lock;
	notify_instance_t *notify_instances;
	notify_instance_t *current_notify;
	int notify_id;

	pthread_t pth_precv;
	pthread_t pth_psend;
	pthread_mutex_t psend_lock;
	pthread_cond_t psend_cond;

	stratum_msg_t *psends;
};

typedef struct proxy_instance proxy_instance_t;


static int gen_loop(proc_instance_t *pi, connsock_t *cs)
{
	unixsock_t *us = &pi->us;
	ckpool_t *ckp = pi->ckp;
	int sockd, ret = 0;
	char *buf = NULL;
	gbtbase_t gbt;
	char hash[68];

	memset(&gbt, 0, sizeof(gbt));
retry:
	sockd = accept(us->sockd, NULL, NULL);
	if (sockd < 0) {
		if (interrupted())
			goto retry;
		LOGERR("Failed to accept on generator socket");
		ret = 1;
		goto out;
	}

	dealloc(buf);
	buf = recv_unix_msg(sockd);
	if (!buf) {
		LOGWARNING("Failed to get message in gen_loop");
		close(sockd);
		goto retry;
	}
	LOGDEBUG("Generator received request: %s", buf);
	if (!strncasecmp(buf, "shutdown", 8)) {
		ret = 0;
		goto out;
	}
	if (!strncasecmp(buf, "getbase", 7)) {
		if (!gen_gbtbase(cs, &gbt)) {
			LOGWARNING("Failed to get block template from %s:%s",
				   cs->url, cs->port);
			send_unix_msg(sockd, "Failed");
		} else {
			char *s = json_dumps(gbt.json, 0);

			send_unix_msg(sockd, s);
			free(s);
			clear_gbtbase(&gbt);
		}
	} else if (!strncasecmp(buf, "getbest", 7)) {
		if (!get_bestblockhash(cs, hash)) {
			LOGWARNING("No best block hash support from %s:%s",
				   cs->url, cs->port);
			send_unix_msg(sockd, "Failed");
		} else {
			send_unix_msg(sockd, hash);
		}
	} else if (!strncasecmp(buf, "getlast", 7)) {
		int height = get_blockcount(cs);

		if (height == -1)
			send_unix_msg(sockd,  "Failed");
		else {
			LOGDEBUG("Height: %d", height);
			if (!get_blockhash(cs, height, hash))
				send_unix_msg(sockd, "Failed");
			else {
				send_unix_msg(sockd, hash);
				LOGDEBUG("Hash: %s", hash);
			}
		}
	} else if (!strncasecmp(buf, "submitblock:", 12)) {
		LOGNOTICE("Submitting block data!");
		if (submit_block(cs, buf + 12))
			send_proc(ckp->stratifier, "update");
		/* FIXME Add logging of block solves */
	} else if (!strncasecmp(buf, "ping", 4)) {
		LOGDEBUG("Generator received ping request");
		send_unix_msg(sockd, "pong");
	}
	close(sockd);
	goto retry;

out:
	dealloc(buf);
	return ret;
}

static int server_mode(ckpool_t *ckp, proc_instance_t *pi, connsock_t *cs,
		       const char *auth, const char *pass)
{
	char *userpass = NULL;
	gbtbase_t gbt;
	int ret = 1;

	memset(&gbt, 0, sizeof(gbt));

	userpass = strdup(auth);
	realloc_strcat(&userpass, ":");
	realloc_strcat(&userpass, pass);
	cs->auth = http_base64(userpass);
	if (!cs->auth) {
		LOGWARNING("Failed to create base64 auth from %s", userpass);
		goto out;
	}

	cs->fd = connect_socket(cs->url, cs->port);
	if (cs->fd < 0) {
		LOGWARNING("FATAL: Failed to connect socket to %s:%s !", cs->url, cs->port);
		goto out;
	}
	keep_sockalive(cs->fd);
	/* Test we can connect, authorise and get a block template */
	if (!gen_gbtbase(cs, &gbt)) {
		LOGWARNING("FATAL: Failed to get test block template from %s:%s auth %s !",
			   cs->url, cs->port, userpass);
		goto out;
	}
	clear_gbtbase(&gbt);
	if (!validate_address(cs, ckp->btcaddress)) {
		LOGWARNING("FATAL: Invalid btcaddress: %s !", ckp->btcaddress);
		goto out;
	}
	ret = gen_loop(pi, cs);
out:
	close(cs->fd);
	dealloc(userpass);
	return ret;
}

static bool send_json_msg(connsock_t *cs, json_t *json_msg)
{
	int len, sent;
	char *s;

	s = json_dumps(json_msg, JSON_ESCAPE_SLASH);
	realloc_strcat(&s, "\n");
	len = strlen(s);
	sent = write_socket(cs->fd, s, len);
	dealloc(s);
	if (sent != len) {
		LOGWARNING("Failed to send %d bytes in send_json_msg", len);
		return false;
	}
	return true;
}

static bool connect_proxy(connsock_t *cs)
{
	cs->fd = connect_socket(cs->url, cs->port);
	if (cs->fd < 0) {
		LOGWARNING("Failed to connect socket to %s:%s in connect_proxy",
			   cs->url, cs->port);
		return false;
	}
	keep_sockalive(cs->fd);
	return true;
}

/* Decode a string that should have a json message and return just the contents
 * of the result key or NULL. */
static json_t *json_result(json_t *val)
{
	json_t *res_val = NULL, *err_val;

	res_val = json_object_get(val, "result");
	if (json_is_null(res_val))
		res_val = NULL;
	if (!res_val) {
		char *ss;

		err_val = json_object_get(val, "error");
		if (err_val)
			ss = json_dumps(err_val, 0);
		else
			ss = strdup("(unknown reason)");

		LOGWARNING("JSON-RPC decode failed: %s", ss);
		free(ss);
	}
	return res_val;
}

/* Parse a string and return the json value it contains, if any, and the
 * result in res_val. Return NULL if no result key is found. */
static json_t *json_msg_result(char *msg, json_t **res_val)
{
	json_error_t err;
	json_t *val;

	*res_val = NULL;
	val = json_loads(msg, 0, &err);
	if (!val) {
		LOGWARNING("Json decode failed(%d): %s", err.line, err.text);
		goto out;
	}
	*res_val = json_result(val);
	if (!*res_val) {
		LOGWARNING("No json result found");
		json_decref(val);
		val = NULL;
	}

out:
	return val;
}

/* For some reason notify is buried at various different array depths so use
 * a reentrant function to try and find it. */
static json_t *find_notify(json_t *val)
{
	int arr_size, i;
	json_t *ret;
	const char *entry;

	if (!json_is_array(val))
		return NULL;
	arr_size = json_array_size(val);
	entry = json_string_value(json_array_get(val, 0));
	if (entry && !strncasecmp(entry, "mining.notify", 13))
		return val;
	for (i = 0; i < arr_size; i++) {
		json_t *arr_val;

		arr_val = json_array_get(val, i);
		ret = find_notify(arr_val);
		if (ret)
			break;
	}
	return ret;
}

static bool parse_subscribe(connsock_t *cs, proxy_instance_t *proxi)
{
	json_t *val = NULL, *res_val, *notify_val, *tmp;
	const char *string;
	bool ret = false;
	int size;

	size = read_socket_line(cs, 5);
	if (size < 1) {
		LOGWARNING("Failed to receive line in parse_subscribe");
		goto out;
	}
	val = json_msg_result(cs->buf, &res_val);
	if (!val) {
		LOGWARNING("Failed to get a json result in parse_subscribe, got: %s", cs->buf);
		goto out;
	}
	if (!json_is_array(res_val)) {
		LOGWARNING("Result in parse_subscribe not an array");
		goto out;
	}
	size = json_array_size(res_val);
	if (size < 3) {
		LOGWARNING("Result in parse_subscribe array too small");
		goto out;
	}
	notify_val = find_notify(res_val);
	if (!notify_val) {
		LOGWARNING("Failed to find notify in parse_subscribe");
		goto out;
	}
	if (!proxi->no_params && !proxi->no_sessionid && json_array_size(notify_val) > 1) {
		/* Copy the session id if one exists. */
		string = json_string_value(json_array_get(notify_val, 1));
		if (string)
			proxi->sessionid = strdup(string);
	}
	tmp = json_array_get(res_val, 1);
	if (!tmp || !json_is_string(tmp)) {
		LOGWARNING("Failed to parse enonce1 in parse_subscribe");
		goto out;
	}
	string = json_string_value(tmp);
	if (strlen(string) < 1) {
		LOGWARNING("Invalid string length for enonce1 in parse_subscribe");
		goto out;
	}
	proxi->enonce1 = strdup(string);
	proxi->nonce1len = strlen(proxi->enonce1) / 2;
	if (proxi->nonce1len > 15) {
		LOGWARNING("Nonce1 too long at %d", proxi->nonce1len);
		goto out;
	}
	proxi->enonce1bin = ckalloc(proxi->nonce1len);
	hex2bin(proxi->enonce1bin, proxi->enonce1, proxi->nonce1len);
	tmp = json_array_get(res_val, 2);
	if (!tmp || !json_is_integer(tmp)) {
		LOGWARNING("Failed to parse nonce2len in parse_subscribe");
		goto out;
	}
	size = json_integer_value(tmp);
	if (size < 1 || size > 8) {
		LOGWARNING("Invalid nonce2len %d in parse_subscribe", size);
		goto out;
	}
	if (size < 4) {
		LOGWARNING("Nonce2 length %d too small to be able to proxy", size);
		goto out;
	}
	proxi->nonce2len = size;

	LOGINFO("Found notify with enonce %s nonce2len %d", proxi->enonce1,
		   proxi->nonce2len);
	ret = true;

out:
	if (val)
		json_decref(val);
	return ret;
}

static bool subscribe_stratum(connsock_t *cs, proxy_instance_t *proxi)
{
	json_t *req;
	bool ret = false;

retry:
	/* Attempt to reconnect if the pool supports resuming */
	if (proxi->sessionid) {
		req = json_pack("{s:i,s:s,s:[s,s]}",
				"id", proxi->id++,
				"method", "mining.subscribe",
				"params", "ckproxy", proxi->sessionid);
	/* Then attempt to connect with just the client description */
	} else if (!proxi->no_params) {
		req = json_pack("{s:i,s:s,s:[s]}",
				"id", proxi->id++,
				"method", "mining.subscribe",
				"params", "ckproxy");
	/* Then try without any parameters */
	} else {
		req = json_pack("{s:i,s:s,s:[]}",
				"id", proxi->id++,
				"method", "mining.subscribe",
				"params");
	}
	ret = send_json_msg(cs, req);
	json_decref(req);
	if (!ret) {
		LOGWARNING("Failed to send message in subscribe_stratum");
		close(cs->fd);
		goto out;
	}
	ret = parse_subscribe(cs, proxi);
	if (ret)
		goto out;

	close(cs->fd);
	if (proxi->no_params) {
		LOGWARNING("Failed all subscription options in subscribe_stratum");
		goto out;
	}
	if (proxi->sessionid) {
		LOGNOTICE("Failed sessionid reconnect in subscribe_stratum, retrying without");
		proxi->no_sessionid = true;
		dealloc(proxi->sessionid);
	} else {
		LOGNOTICE("Failed connecting with parameters in subscribe_stratum, retrying without");
		proxi->no_params = true;
	}
	ret = connect_proxy(cs);
	if (!ret) {
		LOGWARNING("Failed to reconnect in subscribe_stratum");
		goto out;
	}
	goto retry;

out:
	return ret;
}

#define parse_reconnect(a, b) true
#define send_version(a, b) true
#define show_message(a, b) true

static bool parse_notify(proxy_instance_t *proxi, json_t *val)
{
	const char *prev_hash, *bbversion, *nbit, *ntime;
	char *job_id, *coinbase1, *coinbase2;
	bool clean, ret = false;
	notify_instance_t *ni;
	int merkles, i;
	json_t *arr;

	arr = json_array_get(val, 4);
	if (!arr || !json_is_array(arr))
		goto out;

	merkles = json_array_size(arr);
	job_id = json_array_string(val, 0);
	prev_hash = __json_array_string(val, 1);
	coinbase1 = json_array_string(val, 2);
	coinbase2 = json_array_string(val, 3);
	bbversion = __json_array_string(val, 5);
	nbit = __json_array_string(val, 6);
	ntime = __json_array_string(val, 7);
	clean = json_is_true(json_array_get(val, 8));
	if (!job_id || !prev_hash || !coinbase1 || !coinbase2 || !bbversion || !nbit || !ntime) {
		if (job_id)
			free(job_id);
		if (coinbase1)
			free(coinbase1);
		if (coinbase2)
			free(coinbase2);
		goto out;
	}

	LOGDEBUG("New notify");
	ni = ckzalloc(sizeof(notify_instance_t));
	ni->jobid = job_id;
	LOGDEBUG("Job ID %s", job_id);
	ni->coinbase1 = coinbase1;
	LOGDEBUG("Coinbase1 %s", coinbase1);
	ni->coinbase2 = coinbase2;
	LOGDEBUG("Coinbase2 %s", coinbase2);
	memcpy(ni->prevhash, prev_hash, 65);
	LOGDEBUG("Prevhash %s", prev_hash);
	memcpy(ni->bbversion, bbversion, 9);
	LOGDEBUG("BBVersion %s", bbversion);
	memcpy(ni->nbit, nbit, 9);
	LOGDEBUG("Nbit %s", nbit);
	memcpy(ni->ntime, ntime, 9);
	LOGDEBUG("Ntime %s", ntime);
	ni->clean = clean;
	LOGDEBUG("Clean %s", clean ? "true" : "false");
	LOGDEBUG("Merkles %d", merkles);
	for (i = 0; i < merkles; i++) {
		const char *merkle = __json_array_string(arr, i);

		LOGDEBUG("Merkle %d %s", i, merkle);
		memcpy(&ni->merklehash[i][0], merkle, 65);
	}
	ni->merkles = merkles;
	ret = true;
	ni->notify_time = time(NULL);

	mutex_lock(&proxi->notify_lock);
	ni->id = proxi->notify_id++;
	HASH_ADD_INT(proxi->notify_instances, id, ni);
	proxi->current_notify = ni;
	mutex_unlock(&proxi->notify_lock);

out:
	return ret;
}

static bool parse_diff(proxy_instance_t *proxi, json_t *val)
{
	double diff = json_number_value(json_array_get(val, 0));

	if (diff == 0 || diff == proxi->diff)
		return true;
	proxi->diff = diff;
	proxi->diffed = true;
	return true;
}

static bool parse_method(proxy_instance_t *proxi, const char *msg)
{
	json_t *val = NULL, *method, *err_val, *params;
	json_error_t err;
	bool ret = false;
	const char *buf;

	val = json_loads(msg, 0, &err);
	if (!val) {
		LOGWARNING("JSON decode failed(%d): %s", err.line, err.text);
		goto out;
	}

	method = json_object_get(val, "method");
	if (!method) {
		LOGDEBUG("Failed to find method in json for parse_method");
		goto out;
	}
	err_val = json_object_get(val, "error");
	params = json_object_get(val, "params");

	if (err_val && !json_is_null(err_val)) {
		char *ss;

		if (err_val)
			ss = json_dumps(err_val, 0);
		else
			ss = strdup("(unknown reason)");

		LOGINFO("JSON-RPC method decode failed: %s", ss);
		free(ss);
		goto out;
	}

	if (!json_is_string(method)) {
		LOGINFO("Method is not string in parse_method");
		goto out;
	}
	buf = json_string_value(method);
	if (!buf || strlen(buf) < 1) {
		LOGINFO("Invalid string for method in parse_method");
		goto out;
	}

	if (!strncasecmp(buf, "mining.notify", 13)) {
		if (parse_notify(proxi, params))
			proxi->notified = ret = true;
		else
			proxi->notified = ret = false;
		goto out;
	}

	if (!strncasecmp(buf, "mining.set_difficulty", 21)) {
		ret = parse_diff(proxi, params);
		goto out;
	}

	if (!strncasecmp(buf, "client.reconnect", 16)) {
		ret = parse_reconnect(proxi, params);
		goto out;
	}

	if (!strncasecmp(buf, "client.get_version", 18)) {
		ret =  send_version(proxi, val);
		goto out;
	}

	if (!strncasecmp(buf, "client.show_message", 19)) {
		ret = show_message(proxi, params);
		goto out;
	}
out:
	if (val)
		json_decref(val);
	return ret;
}

static bool auth_stratum(connsock_t *cs, proxy_instance_t *proxi)
{
	json_t *val = NULL, *res_val, *req;
	bool ret;

	req = json_pack("{s:i,s:s,s:[s,s]}",
			"id", proxi->id++,
			"method", "mining.authorize",
			"params", proxi->auth, proxi->pass);
	ret = send_json_msg(cs, req);
	json_decref(req);
	if (!ret) {
		LOGWARNING("Failed to send message in auth_stratum");
		close(cs->fd);
		goto out;
	}

	/* Read and parse any extra methods sent. Anything left in the buffer
	 * should be the response to our auth request. */
	do {
		int size;

		size = read_socket_line(cs, 5);
		if (size < 1) {
			LOGWARNING("Failed to receive line in auth_stratum");
			ret = false;
			goto out;
		}
		ret = parse_method(proxi, cs->buf);
	} while (ret);

	val = json_msg_result(cs->buf, &res_val);
	if (!val) {
		LOGWARNING("Failed to get a json result in auth_stratum, got: %s", cs->buf);
		goto out;
	}

	ret = json_is_true(res_val);
	if (!ret) {
		LOGWARNING("Failed to authorise in auth_stratum");
		goto out;
	}
	LOGINFO("Auth success in auth_stratum");
out:
	if (val)
		json_decref(val);
	return ret;
}

static void send_subscribe(proxy_instance_t *proxi, int sockd)
{
	json_t *json_msg;
	char *msg;

	json_msg = json_pack("{sssi}", "enonce1", proxi->enonce1,
			     "nonce2len", proxi->nonce2len);
	msg = json_dumps(json_msg, 0);
	json_decref(json_msg);
	send_unix_msg(sockd, msg);
	free(msg);
	close(sockd);
}

static void send_notify(proxy_instance_t *proxi, int sockd)
{
	json_t *json_msg, *merkle_arr;
	notify_instance_t *ni;
	char *msg;
	int i;

	merkle_arr = json_array();

	mutex_lock(&proxi->notify_lock);
	ni = proxi->current_notify;
	for (i = 0; i < ni->merkles; i++)
		json_array_append(merkle_arr, json_string(&ni->merklehash[i][0]));
	/* Use our own jobid instead of the server's one for easy lookup */
	json_msg = json_pack("{sisssssssosssssssb}",
			     "jobid", ni->id, "prevhash", ni->prevhash,
			     "coinbase1", ni->coinbase1, "coinbase2", ni->coinbase2,
			     "merklehash", merkle_arr, "bbversion", ni->bbversion,
			     "nbit", ni->nbit, "ntime", ni->ntime,
			     "clean", ni->clean);
	mutex_unlock(&proxi->notify_lock);

	msg = json_dumps(json_msg, 0);
	json_decref(json_msg);
	send_unix_msg(sockd, msg);
	free(msg);
	close(sockd);
}

static void send_diff(proxy_instance_t *proxi, int sockd)
{
	json_t *json_msg;
	char *msg;

	json_msg = json_pack("{sf}", "diff", proxi->diff);
	msg = json_dumps(json_msg, 0);
	json_decref(json_msg);
	send_unix_msg(sockd, msg);
	free(msg);
	close(sockd);
}

static void submit_share(proxy_instance_t *proxi, json_t *val)
{
	stratum_msg_t *msg;

	msg = ckzalloc(sizeof(stratum_msg_t));
	msg->json_msg = val;

	mutex_lock(&proxi->psend_lock);
	DL_APPEND(proxi->psends, msg);
	pthread_cond_signal(&proxi->psend_cond);
	mutex_unlock(&proxi->psend_lock);
}

static int proxy_loop(proc_instance_t *pi, connsock_t *cs, proxy_instance_t *proxi)
{
	unixsock_t *us = &pi->us;
	ckpool_t *ckp = pi->ckp;
	int sockd, ret = 0;
	char *buf = NULL;

	/* We're not subscribed and authorised so tell the stratifier to
	 * retrieve the first subscription. */
	send_proc(ckp->stratifier, "subscribe");
	send_proc(ckp->stratifier, "notify");
	proxi->notified = false;

retry:
	sockd = accept(us->sockd, NULL, NULL);
	if (sockd < 0) {
		if (interrupted())
			goto retry;
		LOGERR("Failed to accept on proxy socket");
		ret = 1;
		goto out;
	}
	dealloc(buf);
	buf = recv_unix_msg(sockd);
	if (!buf) {
		LOGWARNING("Failed to get message in proxy_loop");
		close(sockd);
		goto retry;
	}
	LOGDEBUG("Proxy received request: %s", buf);
	if (!strncasecmp(buf, "shutdown", 8)) {
		ret = 0;
		goto out;
	} else if (!strncasecmp(buf, "getsubscribe", 12)) {
		send_subscribe(proxi, sockd);
	} else if (!strncasecmp(buf, "getnotify", 9)) {
		send_notify(proxi, sockd);
	} else if (!strncasecmp(buf, "getdiff", 7)) {
		send_diff(proxi, sockd);
	} else if (!strncasecmp(buf, "ping", 4)) {
		LOGDEBUG("Proxy received ping request");
		send_unix_msg(sockd, "pong");
	} else {
		/* Anything remaining should be share submissions */
		json_t *val = json_loads(buf, 0, NULL);

		if (!val)
			LOGWARNING("Received unrecognised message: %s", buf);
		else
			submit_share(proxi, val);
	}
	close(sockd);
	goto retry;
out:
	close(sockd);
	return ret;
}

static void reconnect_stratum(connsock_t *cs, proxy_instance_t *proxi)
{
	bool ret = true;

	do {
		if (!ret)
			sleep(5);
		close(cs->fd);
		ret = connect_proxy(cs);
		if (!ret)
			continue;
		ret = subscribe_stratum(cs, proxi);
		if (!ret)
			continue;
		ret = auth_stratum(cs, proxi);
	} while (!ret);
}

static void clear_notify(notify_instance_t *ni)
{
	free(ni->jobid);
	free(ni->coinbase1);
	free(ni->coinbase2);
}

static void *proxy_recv(void *arg)
{
	proxy_instance_t *proxi = (proxy_instance_t *)arg;
	connsock_t *cs = proxi->cs;
	ckpool_t *ckp = proxi->ckp;

	rename_proc("proxyrecv");

	while (42) {
		notify_instance_t *ni, *tmp;
		time_t now;
		int ret;

		now = time(NULL);

		mutex_lock(&proxi->notify_lock);
		HASH_ITER(hh, proxi->notify_instances, ni, tmp) {
			if (HASH_COUNT(proxi->notify_instances) < 3)
				break;
			/* Age old notifications older than 10 mins old */
			if (ni->notify_time < now - 600) {
				HASH_DEL(proxi->notify_instances, ni);
				clear_notify(ni);
			}
		}
		mutex_unlock(&proxi->notify_lock);

		ret = read_socket_line(cs, 120);
		if (ret < 1) {
			LOGWARNING("Failed to read_socket_line in proxy_recv, attempting reconnect");
			reconnect_stratum(cs, proxi);
			continue;
		}
		if (parse_method(proxi, cs->buf)) {
			if (proxi->notified) {
				send_proc(ckp->stratifier, "notify");
				proxi->notified = false;
			}
			if (proxi->diffed) {
				send_proc(ckp->stratifier, "diff");
				proxi->diffed = false;
			}
			continue;
		}
		LOGWARNING("Unhandled stratum message: %s", cs->buf);
	}
	return NULL;
}

/* For processing and sending shares */
static void *proxy_send(void *arg)
{
	proxy_instance_t *proxi = (proxy_instance_t *)arg;

	rename_proc("proxysend");

	while (42) {
		stratum_msg_t *msg;
		char *buf;

		mutex_lock(&proxi->psend_lock);
		if (!proxi->psends)
			pthread_cond_wait(&proxi->psend_cond, &proxi->psend_lock);
		msg = proxi->psends;
		if (likely(msg))
			DL_DELETE(proxi->psends, msg);
		mutex_unlock(&proxi->psend_lock);

		if (unlikely(!msg))
			continue;

		buf = json_dumps(msg->json_msg, 0);
		LOGDEBUG("Proxysend received: %s", buf);

		json_decref(msg->json_msg);
		free(msg);
	}
	return NULL;
}

static int proxy_mode(ckpool_t *ckp, proc_instance_t *pi, connsock_t *cs,
		      const char *auth, const char *pass)
{
	proxy_instance_t proxi;
	int ret = 1;

	memset(&proxi, 0, sizeof(proxi));
	proxi.ckp = ckp;
	proxi.cs = cs;

	if (!connect_proxy(cs)) {
		LOGWARNING("FATAL: Failed to connect to %s:%s in proxy_mode!",
			   cs->url, cs->port);
		goto out;
	}

	/* Test we can connect, authorise and get stratum information */
	if (!subscribe_stratum(cs, &proxi)) {
		LOGWARNING("FATAL: Failed initial subscribe to %s:%s !",
			   cs->url, cs->port);
		goto out;
	}

	proxi.auth = auth;
	proxi.pass = pass;
	if (!auth_stratum(cs, &proxi)) {
		LOGWARNING("FATAL: Failed initial authorise to %s:%s with %s:%s !",
			   cs->url, cs->port, auth, pass);
		goto out;
	}

	mutex_init(&proxi.notify_lock);
	create_pthread(&proxi.pth_precv, proxy_recv, &proxi);
	mutex_init(&proxi.psend_lock);
	cond_init(&proxi.psend_cond);
	create_pthread(&proxi.pth_psend, proxy_send, &proxi);

	ret = proxy_loop(pi, cs, &proxi);

	/* Return from the proxy loop means we have received a shutdown
	 * request */
	pthread_cancel(proxi.pth_precv);
	pthread_cancel(proxi.pth_psend);
	join_pthread(proxi.pth_precv);
	join_pthread(proxi.pth_psend);
out:
	close(cs->fd);
	free(proxi.enonce1);
	free(proxi.enonce1bin);
	free(proxi.sessionid);

	return ret;
}


/* FIXME: Hard wired to just use config 0 for now */
int generator(proc_instance_t *pi)
{
	char *url, *auth, *pass, *userpass = NULL;
	ckpool_t *ckp = pi->ckp;
	connsock_t cs;
	int ret = 1;

	memset(&cs, 0, sizeof(cs));

	if (!ckp->proxy) {
		url = ckp->btcdurl[0];
		auth = ckp->btcdauth[0];
		pass = ckp->btcdpass[0];
	} else {
		url = ckp->proxyurl[0];
		auth = ckp->proxyauth[0];
		pass = ckp->proxypass[0];
	}
	if (!extract_sockaddr(url, &cs.url, &cs.port)) {
		LOGWARNING("Failed to extract address from %s", url);
		goto out;
	}

	if (!ckp->proxy)
		ret = server_mode(ckp, pi, &cs, auth, pass);
	else
		ret = proxy_mode(ckp, pi, &cs, auth, pass);
out:
	/* Clean up here */
	dealloc(cs.url);
	dealloc(cs.port);
	dealloc(userpass);

	LOGINFO("%s generator exiting with return code %d", ckp->name, ret);
	if (ret) {
		send_proc(&ckp->main, "shutdown");
		sleep(1);
	}
	exit(ret);
}
