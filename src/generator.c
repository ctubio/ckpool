/*
 * Copyright 2014-2015 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include "config.h"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <jansson.h>
#include <string.h>
#include <unistd.h>

#include "ckpool.h"
#include "libckpool.h"
#include "generator.h"
#include "bitcoin.h"
#include "uthash.h"
#include "utlist.h"
#include "api.h"

struct notify_instance {
	/* Hash table data */
	UT_hash_handle hh;
	int id;

	char prevhash[68];
	json_t *jobid;
	char *coinbase1;
	char *coinbase2;
	int coinb1len;
	int merkles;
	char merklehash[16][68];
	char nbit[12];
	char ntime[12];
	char bbversion[12];
	bool clean;

	time_t notify_time;
};

typedef struct notify_instance notify_instance_t;

struct share_msg {
	UT_hash_handle hh;
	int64_t id; // Our own id for submitting upstream

	int client_id;
	time_t submit_time;
};

typedef struct share_msg share_msg_t;

struct stratum_msg {
	struct stratum_msg *next;
	struct stratum_msg *prev;

	json_t *json_msg;
	int client_id;
};

typedef struct stratum_msg stratum_msg_t;

struct pass_msg {
	connsock_t *cs;
	char *msg;
};

typedef struct pass_msg pass_msg_t;

typedef struct proxy_instance proxy_instance_t;

/* Per proxied pool instance data */
struct proxy_instance {
	UT_hash_handle hh; /* Proxy list */
	UT_hash_handle sh; /* Subproxy list */
	proxy_instance_t *next; /* For dead proxy list */
	proxy_instance_t *prev; /* For dead proxy list */

	ckpool_t *ckp;
	connsock_t cs;
	bool passthrough;
	int id; /* Proxy server id*/
	int subid; /* Subproxy id */

	char *url;
	char *auth;
	char *pass;

	char *enonce1;
	char *enonce1bin;
	int nonce1len;
	int nonce2len;

	tv_t last_message;

	double diff;
	tv_t last_share;

	bool no_params; /* Doesn't want any parameters on subscribe */

	bool notified; /* Has this proxy received any notifies yet */
	bool disabled; /* Subproxy no longer to be used */
	bool reconnect; /* We need to drop and reconnect */
	bool reconnecting; /* Testing of parent in progress */
	int64_t recruit; /* No of recruiting requests in progress */
	bool alive;

	mutex_t notify_lock;
	notify_instance_t *notify_instances;

	pthread_t pth_precv;
	pthread_t pth_psend;
	mutex_t psend_lock;
	pthread_cond_t psend_cond;

	stratum_msg_t *psends;

	mutex_t share_lock;
	share_msg_t *shares;
	int64_t share_id;

	ckmsgq_t *passsends;	// passthrough sends

	char_entry_t *recvd_lines; /* Linked list of unprocessed messages */

	time_t reconnect_time;

	int epfd; /* Epoll fd used by the parent proxy */

	mutex_t proxy_lock; /* Lock protecting hashlist of proxies */
	proxy_instance_t *parent; /* Parent proxy of subproxies */
	proxy_instance_t *subproxies; /* Hashlist of subproxies of this proxy */
	int64_t clients_per_proxy; /* Max number of clients of this proxy */
	int subproxy_count; /* Number of subproxies */
};

/* Private data for the generator */
struct generator_data {
	ckpool_t *ckp;
	mutex_t lock; /* Lock protecting linked lists */
	proxy_instance_t *proxies; /* Hash list of all proxies */
	proxy_instance_t *proxy; /* Current proxy */
	proxy_instance_t *dead_proxies; /* Disabled proxies */
	int proxy_notify_id;	// Globally increasing notify id
	ckmsgq_t *srvchk;	// Server check message queue
};

typedef struct generator_data gdata_t;

static bool server_alive(ckpool_t *ckp, server_instance_t *si, bool pinging)
{
	char *userpass = NULL;
	bool ret = false;
	connsock_t *cs;
	gbtbase_t *gbt;

	cs = &si->cs;
	/* Has this server already been reconnected? */
	if (cs->fd > 0)
		return true;
	if (!extract_sockaddr(si->url, &cs->url, &cs->port)) {
		LOGWARNING("Failed to extract address from %s", si->url);
		return ret;
	}
	userpass = strdup(si->auth);
	realloc_strcat(&userpass, ":");
	realloc_strcat(&userpass, si->pass);
	cs->auth = http_base64(userpass);
	dealloc(userpass);
	if (!cs->auth) {
		LOGWARNING("Failed to create base64 auth from %s", userpass);
		return ret;
	}

	cs->fd = connect_socket(cs->url, cs->port);
	if (cs->fd < 0) {
		if (!pinging)
			LOGWARNING("Failed to connect socket to %s:%s !", cs->url, cs->port);
		return ret;
	}

	/* Test we can connect, authorise and get a block template */
	gbt = ckzalloc(sizeof(gbtbase_t));
	si->data = gbt;
	if (!gen_gbtbase(cs, gbt)) {
		if (!pinging) {
			LOGINFO("Failed to get test block template from %s:%s!",
				cs->url, cs->port);
		}
		goto out;
	}
	clear_gbtbase(gbt);
	if (!validate_address(cs, ckp->btcaddress)) {
		LOGWARNING("Invalid btcaddress: %s !", ckp->btcaddress);
		goto out;
	}
	ret = true;
out:
	if (!ret) {
		/* Close and invalidate the file handle */
		Close(cs->fd);
	} else
		keep_sockalive(cs->fd);
	return ret;
}

/* Find the highest priority server alive and return it */
static server_instance_t *live_server(ckpool_t *ckp)
{
	server_instance_t *alive = NULL;
	connsock_t *cs;
	int i;

	LOGDEBUG("Attempting to connect to bitcoind");
retry:
	if (!ping_main(ckp))
		goto out;

	for (i = 0; i < ckp->btcds; i++) {
		server_instance_t *si = ckp->servers[i];

		if (server_alive(ckp, si, false)) {
			alive = si;
			break;
		}
	}
	if (!alive) {
		LOGWARNING("CRITICAL: No bitcoinds active!");
		sleep(5);
		goto retry;
	}
	cs = &alive->cs;
	LOGINFO("Connected to live server %s:%s", cs->url, cs->port);
out:
	send_proc(ckp->connector, alive ? "accept" : "reject");
	return alive;
}

static void kill_server(server_instance_t *si)
{
	connsock_t *cs;

	if (!si) // This shouldn't happen
		return;

	LOGNOTICE("Killing server");
	cs = &si->cs;
	Close(cs->fd);
	empty_buffer(cs);
	dealloc(cs->url);
	dealloc(cs->port);
	dealloc(cs->auth);
	dealloc(si->data);
}

static int gen_loop(proc_instance_t *pi)
{
	int sockd = -1, ret = 0, selret;
	server_instance_t *si = NULL;
	bool reconnecting = false;
	unixsock_t *us = &pi->us;
	ckpool_t *ckp = pi->ckp;
	gdata_t *gdata = ckp->data;
	bool started = false;
	char *buf = NULL;
	connsock_t *cs;
	gbtbase_t *gbt;
	char hash[68];

reconnect:
	Close(sockd);
	if (si) {
		kill_server(si);
		reconnecting = true;
	}
	si = live_server(ckp);
	if (!si)
		goto out;

	gbt = si->data;
	cs = &si->cs;
	if (reconnecting) {
		LOGWARNING("Failed over to bitcoind: %s:%s", cs->url, cs->port);
		reconnecting = false;
	}

retry:
	Close(sockd);
	ckmsgq_add(gdata->srvchk, si);

	do {
		selret = wait_read_select(us->sockd, 5);
		if (!selret && !ping_main(ckp)) {
			LOGEMERG("Generator failed to ping main process, exiting");
			ret = 1;
			goto out;
		}
	} while (selret < 1);

	if (unlikely(cs->fd < 0)) {
		LOGWARNING("Bitcoind socket invalidated, will attempt failover");
		goto reconnect;
	}

	sockd = accept(us->sockd, NULL, NULL);
	if (sockd < 0) {
		LOGEMERG("Failed to accept on generator socket");
		ret = 1;
		goto out;
	}

	dealloc(buf);
	buf = recv_unix_msg(sockd);
	if (!buf) {
		LOGWARNING("Failed to get message in gen_loop");
		goto retry;
	}
	LOGDEBUG("Generator received request: %s", buf);
	if (cmdmatch(buf, "shutdown")) {
		ret = 0;
		goto out;
	}
	if (cmdmatch(buf, "getbase")) {
		if (!gen_gbtbase(cs, gbt)) {
			LOGWARNING("Failed to get block template from %s:%s",
				   cs->url, cs->port);
			send_unix_msg(sockd, "Failed");
			goto reconnect;
		} else {
			char *s = json_dumps(gbt->json, JSON_NO_UTF8);

			send_unix_msg(sockd, s);
			free(s);
			clear_gbtbase(gbt);
		}
	} else if (cmdmatch(buf, "getbest")) {
		if (si->notify)
			send_unix_msg(sockd, "notify");
		else if (!get_bestblockhash(cs, hash)) {
			LOGINFO("No best block hash support from %s:%s",
				cs->url, cs->port);
			send_unix_msg(sockd, "failed");
		} else {
			if (unlikely(!started)) {
				started = true;
				LOGWARNING("%s generator ready", ckp->name);
			}
			send_unix_msg(sockd, hash);
		}
	} else if (cmdmatch(buf, "getlast")) {
		int height;

		if (si->notify)
			send_unix_msg(sockd, "notify");
		else if ((height = get_blockcount(cs)) == -1) {
			send_unix_msg(sockd,  "failed");
			goto reconnect;
		} else {
			LOGDEBUG("Height: %d", height);
			if (!get_blockhash(cs, height, hash)) {
				send_unix_msg(sockd, "failed");
				goto reconnect;
			} else {
				if (unlikely(!started)) {
					started = true;
					LOGWARNING("%s generator ready", ckp->name);
				}

				send_unix_msg(sockd, hash);
				LOGDEBUG("Hash: %s", hash);
			}
		}
	} else if (cmdmatch(buf, "submitblock:")) {
		char blockmsg[80];
		bool ret;

		LOGNOTICE("Submitting block data!");
		ret = submit_block(cs, buf + 12 + 64 + 1);
		memset(buf + 12 + 64, 0, 1);
		sprintf(blockmsg, "%sblock:%s", ret ? "" : "no", buf + 12);
		send_proc(ckp->stratifier, blockmsg);
	} else if (cmdmatch(buf, "checkaddr:")) {
		if (validate_address(cs, buf + 10))
			send_unix_msg(sockd, "true");
		else
			send_unix_msg(sockd, "false");
	} else if (cmdmatch(buf, "reconnect")) {
		goto reconnect;
	} else if (cmdmatch(buf, "loglevel")) {
		sscanf(buf, "loglevel=%d", &ckp->loglevel);
	} else if (cmdmatch(buf, "ping")) {
		LOGDEBUG("Generator received ping request");
		send_unix_msg(sockd, "pong");
	}
	goto retry;

out:
	kill_server(si);
	dealloc(buf);
	return ret;
}

static bool send_json_msg(connsock_t *cs, const json_t *json_msg)
{
	int len, sent;
	char *s;

	s = json_dumps(json_msg, JSON_ESCAPE_SLASH | JSON_EOL);
	LOGDEBUG("Sending json msg: %s", s);
	len = strlen(s);
	sent = write_socket(cs->fd, s, len);
	dealloc(s);
	if (sent != len) {
		LOGNOTICE("Failed to send %d bytes sent %d in send_json_msg", len, sent);
		return false;
	}
	return true;
}

static bool connect_proxy(connsock_t *cs)
{
	cs->fd = connect_socket(cs->url, cs->port);
	if (cs->fd < 0) {
		LOGINFO("Failed to connect socket to %s:%s in connect_proxy",
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
	/* (null) is a valid result while no value is an error, so mask out
	 * (null) and only handle lack of result */
	if (json_is_null(res_val))
		res_val = NULL;
	else if (!res_val) {
		char *ss;

		err_val = json_object_get(val, "error");
		if (err_val)
			ss = json_dumps(err_val, 0);
		else
			ss = strdup("(unknown reason)");

		LOGNOTICE("JSON-RPC decode of json_result failed: %s", ss);
		free(ss);
	}
	return res_val;
}

/* Return the error value if one exists */
static json_t *json_errval(json_t *val)
{
	json_t *err_val = json_object_get(val, "error");

	return err_val;
}

/* Parse a string and return the json value it contains, if any, and the
 * result in res_val. Return NULL if no result key is found. */
static json_t *json_msg_result(char *msg, json_t **res_val, json_t **err_val)
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
	*err_val = json_errval(val);

out:
	return val;
}

/* For some reason notify is buried at various different array depths so use
 * a reentrant function to try and find it. */
static json_t *find_notify(json_t *val)
{
	int arr_size, i;
	json_t *ret = NULL;
	const char *entry;

	if (!json_is_array(val))
		return NULL;
	arr_size = json_array_size(val);
	entry = json_string_value(json_array_get(val, 0));
	if (cmdmatch(entry, "mining.notify"))
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

/* Get stored line in the proxy linked list of messages if any exist or NULL */
static char *cached_proxy_line(proxy_instance_t *proxi)
{
	char *buf = NULL;

	if (proxi->recvd_lines) {
		char_entry_t *char_t = proxi->recvd_lines;

		DL_DELETE(proxi->recvd_lines, char_t);
		buf = char_t->buf;
		free(char_t);
	}
	return buf;
}

/* Get next line in the proxy linked list of messages or a new line from the
 * connsock if there are none. */
static char *next_proxy_line(connsock_t *cs, proxy_instance_t *proxi)
{
	char *buf = cached_proxy_line(proxi);

	if (!buf && read_socket_line(cs, 5) > 0)
		buf = strdup(cs->buf);
	return buf;
}

/* For appending a line to the proxy recv list */
static void append_proxy_line(proxy_instance_t *proxi, const char *buf)
{
	char_entry_t *char_t = ckalloc(sizeof(char_entry_t));
	char_t->buf = strdup(buf);
	DL_APPEND(proxi->recvd_lines, char_t);
}

/* Get a new line from the connsock and return a copy of it */
static char *new_proxy_line(connsock_t *cs)
{
	char *buf = NULL;

	if (read_socket_line(cs, 5) < 1)
		goto out;
	buf = strdup(cs->buf);
out:
	return buf;
}

static inline bool parent_proxy(proxy_instance_t *proxy)
{
	return (proxy->parent == proxy);
}

static void recruit_subproxies(proxy_instance_t *proxi, const int recruits);

static bool parse_subscribe(connsock_t *cs, proxy_instance_t *proxi)
{
	json_t *val = NULL, *res_val, *notify_val, *tmp;
	bool parsed, ret = false;
	proxy_instance_t *parent;
	int retries = 0, size;
	const char *string;
	char *buf, *old;

retry:
	parsed = true;
	if (!(buf = new_proxy_line(cs))) {
		LOGNOTICE("Proxy %d:%d %s failed to receive line in parse_subscribe",
			   proxi->id, proxi->subid, proxi->url);
		goto out;
	}
	LOGDEBUG("parse_subscribe received %s", buf);
	/* Ignore err_val here stored in &tmp */
	val = json_msg_result(buf, &res_val, &tmp);
	if (!val || !res_val) {
		LOGINFO("Failed to get a json result in parse_subscribe, got: %s", buf);
		parsed = false;
	}
	if (!json_is_array(res_val)) {
		LOGINFO("Result in parse_subscribe not an array");
		parsed = false;
	}
	size = json_array_size(res_val);
	if (size < 3) {
		LOGINFO("Result in parse_subscribe array too small");
		parsed = false;
	}
	notify_val = find_notify(res_val);
	if (!notify_val) {
		LOGINFO("Failed to find notify in parse_subscribe");
		parsed = false;
	}
	if (!parsed) {
		if (++retries < 3) {
			/* We don't want this response so put it on the proxy
			 * recvd list to be parsed later */
			append_proxy_line(proxi, buf);
			buf = NULL;
			goto retry;
		}
		LOGNOTICE("Proxy %d:%d %s failed to parse subscribe response in parse_subscribe",
			  proxi->id, proxi->subid, proxi->url);
		goto out;
	}

	tmp = json_array_get(res_val, 1);
	if (!tmp || !json_is_string(tmp)) {
		LOGWARNING("Failed to parse enonce1 in parse_subscribe");
		goto out;
	}
	string = json_string_value(tmp);
	old = proxi->enonce1;
	proxi->enonce1 = strdup(string);
	free(old);
	proxi->nonce1len = strlen(proxi->enonce1) / 2;
	if (proxi->nonce1len > 15) {
		LOGWARNING("Nonce1 too long at %d", proxi->nonce1len);
		goto out;
	}
	old = proxi->enonce1bin;
	proxi->enonce1bin = ckalloc(proxi->nonce1len);
	free(old);
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
	if (size < 3) {
		if (!proxi->subid) {
			LOGWARNING("Proxy %d %s Nonce2 length %d too small for fast miners",
				   proxi->id, proxi->url, size);
		} else {
			LOGNOTICE("Proxy %d:%d Nonce2 length %d too small for fast miners",
				   proxi->id, proxi->subid, size);
		}
	}
	proxi->nonce2len = size;
	proxi->clients_per_proxy = 1ll << ((size - 3) * 8);
	parent = proxi->parent;

	mutex_lock(&parent->proxy_lock);
	parent->recruit -= proxi->clients_per_proxy;
	if (parent->recruit < 0)
		parent->recruit = 0;
	mutex_unlock(&parent->proxy_lock);

	LOGNOTICE("Found notify for new proxy %d:%d with enonce %s nonce2len %d", proxi->id,
		proxi->subid, proxi->enonce1, proxi->nonce2len);
	ret = true;

out:
	if (val)
		json_decref(val);
	free(buf);
	return ret;
}

static bool subscribe_stratum(connsock_t *cs, proxy_instance_t *proxi)
{
	bool ret = false;
	json_t *req;

retry:
	/* Attempt to connect with the client description */
	if (!proxi->no_params) {
		JSON_CPACK(req, "{s:i,s:s,s:[s]}",
				"id", 0,
				"method", "mining.subscribe",
				"params", PACKAGE"/"VERSION);
	/* Then try without any parameters */
	} else {
		JSON_CPACK(req, "{s:i,s:s,s:[]}",
				"id", 0,
				"method", "mining.subscribe",
				"params");
	}
	ret = send_json_msg(cs, req);
	json_decref(req);
	if (!ret) {
		LOGNOTICE("Proxy %d:%d %s failed to send message in subscribe_stratum",
			   proxi->id, proxi->subid, proxi->url);
		goto out;
	}
	ret = parse_subscribe(cs, proxi);
	if (ret)
		goto out;

	if (proxi->no_params) {
		LOGNOTICE("Proxy %d:%d %s failed all subscription options in subscribe_stratum",
			   proxi->id, proxi->subid, proxi->url);
		goto out;
	}
	LOGINFO("Proxy %d:%d %s failed connecting with parameters in subscribe_stratum, retrying without",
		proxi->id, proxi->subid, proxi->url);
	proxi->no_params = true;
	ret = connect_proxy(cs);
	if (!ret) {
		LOGNOTICE("Proxy %d:%d %s failed to reconnect in subscribe_stratum",
			   proxi->id, proxi->subid, proxi->url);
		goto out;
	}
	goto retry;

out:
	if (!ret && cs->fd > 0) {
		epoll_ctl(proxi->epfd, EPOLL_CTL_DEL, cs->fd, NULL);
		Close(cs->fd);
	}
	return ret;
}

static bool passthrough_stratum(connsock_t *cs, proxy_instance_t *proxi)
{
	json_t *req, *val = NULL, *res_val, *err_val;
	bool ret = false;

	JSON_CPACK(req, "{s:s,s:[s]}",
			"method", "mining.passthrough",
			"params", PACKAGE"/"VERSION);
	ret = send_json_msg(cs, req);
	json_decref(req);
	if (!ret) {
		LOGWARNING("Failed to send message in passthrough_stratum");
		goto out;
	}
	if (read_socket_line(cs, 5) < 1) {
		LOGWARNING("Failed to receive line in passthrough_stratum");
		goto out;
	}
	/* Ignore err_val here since we should always get a result from an
	 * upstream passthrough server */
	val = json_msg_result(cs->buf, &res_val, &err_val);
	if (!val || !res_val) {
		LOGWARNING("Failed to get a json result in passthrough_stratum, got: %s",
			   cs->buf);
		goto out;
	}
	ret = json_is_true(res_val);
	if (!ret) {
		LOGWARNING("Denied passthrough for stratum");
		goto out;
	}
	proxi->passthrough = true;
out:
	if (val)
		json_decref(val);
	if (!ret)
		Close(cs->fd);
	return ret;
}

static void send_notify(ckpool_t *ckp, proxy_instance_t *proxi, notify_instance_t *ni);

static void reconnect_generator(const ckpool_t *ckp)
{
	send_proc(ckp->generator, "reconnect");
}

static bool parse_notify(ckpool_t *ckp, proxy_instance_t *proxi, json_t *val)
{
	const char *prev_hash, *bbversion, *nbit, *ntime;
	proxy_instance_t *proxy = proxi->parent;
	gdata_t *gdata = proxi->ckp->data;
	char *coinbase1, *coinbase2;
	const char *jobidbuf;
	bool clean, ret = false;
	notify_instance_t *ni;
	json_t *arr, *job_id;
	int merkles, i;

	arr = json_array_get(val, 4);
	if (!arr || !json_is_array(arr))
		goto out;

	merkles = json_array_size(arr);
	job_id = json_copy(json_array_get(val, 0));
	prev_hash = __json_array_string(val, 1);
	coinbase1 = json_array_string(val, 2);
	coinbase2 = json_array_string(val, 3);
	bbversion = __json_array_string(val, 5);
	nbit = __json_array_string(val, 6);
	ntime = __json_array_string(val, 7);
	clean = json_is_true(json_array_get(val, 8));
	if (!job_id || !prev_hash || !coinbase1 || !coinbase2 || !bbversion || !nbit || !ntime) {
		if (job_id)
			json_decref(job_id);
		if (coinbase1)
			free(coinbase1);
		if (coinbase2)
			free(coinbase2);
		goto out;
	}

	LOGDEBUG("Received new notify from proxy %d:%d", proxi->id, proxi->subid);
	ni = ckzalloc(sizeof(notify_instance_t));
	ni->jobid = job_id;
	jobidbuf = json_string_value(job_id);
	LOGDEBUG("JobID %s", jobidbuf);
	ni->coinbase1 = coinbase1;
	LOGDEBUG("Coinbase1 %s", coinbase1);
	ni->coinb1len = strlen(coinbase1) / 2;
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

	/* Add the notify instance to the parent proxy list, not the subproxy */
	mutex_lock(&proxy->notify_lock);
	ni->id = gdata->proxy_notify_id++;
	HASH_ADD_INT(proxy->notify_instances, id, ni);
	mutex_unlock(&proxy->notify_lock);

	send_notify(ckp, proxi, ni);
	/* We have all the ingredients necessary to switch to this proxy if
	 * it's the best one so reassess now. */
	if (unlikely(parent_proxy(proxi) && !proxi->notified)) {
		proxi->notified = true;
		reconnect_generator(ckp);
	}
out:
	return ret;
}

static bool parse_diff(proxy_instance_t *proxi, json_t *val)
{
	double diff = json_number_value(json_array_get(val, 0));

	if (diff == 0 || diff == proxi->diff)
		return true;
	proxi->diff = diff;
	return true;
}

static bool send_version(proxy_instance_t *proxi, json_t *val)
{
	json_t *json_msg, *id_val = json_object_dup(val, "id");
	bool ret;

	JSON_CPACK(json_msg, "{sossso}", "id", id_val, "result", PACKAGE"/"VERSION,
			     "error", json_null());
	ret = send_json_msg(&proxi->cs, json_msg);
	json_decref(json_msg);
	return ret;
}

static bool show_message(json_t *val)
{
	const char *msg;

	if (!json_is_array(val))
		return false;
	msg = json_string_value(json_array_get(val, 0));
	if (!msg)
		return false;
	LOGNOTICE("Pool message: %s", msg);
	return true;
}

static bool send_pong(proxy_instance_t *proxi, json_t *val)
{
	json_t *json_msg, *id_val = json_object_dup(val, "id");
	bool ret;

	JSON_CPACK(json_msg, "{sossso}", "id", id_val, "result", "pong",
			     "error", json_null());
	ret = send_json_msg(&proxi->cs, json_msg);
	json_decref(json_msg);
	return ret;
}

static void prepare_proxy(proxy_instance_t *proxi);

/* Creates a duplicate instance or proxi to be used as a subproxy, ignoring
 * fields we don't use in the subproxy. */
static proxy_instance_t *create_subproxy(gdata_t *gdata, proxy_instance_t *proxi, const char *url)
{
	proxy_instance_t *subproxy;

	mutex_lock(&gdata->lock);
	if (gdata->dead_proxies) {
		/* Recycle an old proxy instance if one exists */
		subproxy = gdata->dead_proxies;
		DL_DELETE(gdata->dead_proxies, subproxy);
		subproxy->disabled = false;
	} else {
		subproxy = ckzalloc(sizeof(proxy_instance_t));
		mutex_init(&subproxy->share_lock);
	}
	mutex_unlock(&gdata->lock);

	subproxy->cs.ckp = subproxy->ckp = proxi->ckp;

	mutex_lock(&proxi->proxy_lock);
	subproxy->subid = ++proxi->subproxy_count;
	mutex_unlock(&proxi->proxy_lock);

	subproxy->id = proxi->id;
	subproxy->url = strdup(url);
	subproxy->auth = strdup(proxi->auth);
	subproxy->pass = strdup(proxi->pass);
	subproxy->parent = proxi;
	subproxy->epfd = proxi->epfd;
	return subproxy;
}

static void add_subproxy(proxy_instance_t *proxi, proxy_instance_t *subproxy)
{
	mutex_lock(&proxi->proxy_lock);
	HASH_ADD(sh, proxi->subproxies, subid, sizeof(int), subproxy);
	mutex_unlock(&proxi->proxy_lock);
}

static proxy_instance_t *__subproxy_by_id(proxy_instance_t *proxy, const int subid)
{
	proxy_instance_t *subproxy;

	HASH_FIND(sh, proxy->subproxies, &subid, sizeof(int), subproxy);
	return subproxy;
}

/* Add to the dead list to be recycled if possible */
static void store_proxy(gdata_t *gdata, proxy_instance_t *proxy)
{
	LOGINFO("Recycling data from proxy %d:%d", proxy->id, proxy->subid);

	mutex_lock(&gdata->lock);
	dealloc(proxy->url);
	dealloc(proxy->auth);
	dealloc(proxy->pass);
	DL_APPEND(gdata->dead_proxies, proxy);
	mutex_unlock(&gdata->lock);
}

static void send_stratifier_deadproxy(ckpool_t *ckp, const int id, const int subid)
{
	char buf[256];

	sprintf(buf, "deadproxy=%d:%d", id, subid);
	send_proc(ckp->stratifier, buf);
}

/* Remove the subproxy from the proxi list and put it on the dead list.
 * Further use of the subproxy pointer may point to a new proxy but will not
 * dereference */
static void disable_subproxy(gdata_t *gdata, proxy_instance_t *proxi, proxy_instance_t *subproxy)
{
	subproxy->alive = false;
	send_stratifier_deadproxy(gdata->ckp, subproxy->id, subproxy->subid);
	if (subproxy->cs.fd > 0) {
		epoll_ctl(proxi->epfd, EPOLL_CTL_DEL, subproxy->cs.fd, NULL);
		Close(subproxy->cs.fd);
	}
	if (parent_proxy(subproxy))
		return;

	mutex_lock(&proxi->proxy_lock);
	subproxy->disabled = true;
	/* Make sure subproxy is still in the list */
	subproxy = __subproxy_by_id(proxi, subproxy->subid);
	if (likely(subproxy))
		HASH_DELETE(sh, proxi->subproxies, subproxy);
	mutex_unlock(&proxi->proxy_lock);

	if (subproxy)
		store_proxy(gdata, subproxy);
}

static bool parse_reconnect(proxy_instance_t *proxy, json_t *val)
{
	bool sameurl = false, ret = false;
	ckpool_t *ckp = proxy->ckp;
	gdata_t *gdata = ckp->data;
	proxy_instance_t *parent;
	const char *new_url;
	int new_port;
	char *url;

	new_url = json_string_value(json_array_get(val, 0));
	new_port = json_integer_value(json_array_get(val, 1));
	/* See if we have an invalid entry listing port as a string instead of
	 * integer and handle that. */
	if (!new_port) {
		const char *newport_string = json_string_value(json_array_get(val, 1));

		if (newport_string)
			sscanf(newport_string, "%d", &new_port);
	}
	if (new_url && strlen(new_url) && new_port) {
		char *dot_pool, *dot_reconnect;
		int len;

		dot_pool = strchr(proxy->url, '.');
		if (!dot_pool) {
			LOGWARNING("Denied stratum reconnect request from server without domain %s",
				   proxy->url);
			goto out;
		}
		dot_reconnect = strchr(new_url, '.');
		if (!dot_reconnect) {
			LOGWARNING("Denied stratum reconnect request to url without domain %s",
				   new_url);
			goto out;
		}
		len = strlen(dot_reconnect);
		if (strncmp(dot_pool, dot_reconnect, len)) {
			LOGWARNING("Denied stratum reconnect request from %s to non-matching domain %s",
				   proxy->url, new_url);
			goto out;
		}
		ASPRINTF(&url, "%s:%d", new_url, new_port);
	} else {
		url = strdup(proxy->url);
		sameurl = true;
	}
	LOGINFO("Processing reconnect request to %s", url);

	ret = true;
	parent = proxy->parent;
	disable_subproxy(gdata, parent, proxy);
	if (parent != proxy) {
		/* If this is a subproxy we only need to create a new one if
		 * the url has changed. Otherwise automated recruiting will
		 * take care of creating one if needed. */
		if (!sameurl)
			create_subproxy(gdata, parent, url);
		goto out;
	}

	proxy->reconnect = true;
	LOGWARNING("Proxy %d:%s reconnect issue to %s, dropping existing connection",
		   proxy->id, proxy->url, url);
	if (!sameurl) {
		char *oldurl = proxy->url;

		proxy->url = url;
		free(oldurl);
	}
out:
	return ret;
}

static void send_diff(ckpool_t *ckp, proxy_instance_t *proxi)
{
	proxy_instance_t *proxy = proxi->parent;
	json_t *json_msg;
	char *msg, *buf;

	/* Not set yet */
	if (!proxi->diff)
		return;

	JSON_CPACK(json_msg, "{sIsisf}",
		   "proxy", proxy->id,
		   "subproxy", proxi->subid,
		   "diff", proxi->diff);
	msg = json_dumps(json_msg, JSON_NO_UTF8);
	json_decref(json_msg);
	ASPRINTF(&buf, "diff=%s", msg);
	free(msg);
	send_proc(ckp->stratifier, buf);
	free(buf);
}

static void send_notify(ckpool_t *ckp, proxy_instance_t *proxi, notify_instance_t *ni)
{
	proxy_instance_t *proxy = proxi->parent;
	json_t *json_msg, *merkle_arr;
	char *msg, *buf;
	int i;

	merkle_arr = json_array();

	for (i = 0; i < ni->merkles; i++)
		json_array_append_new(merkle_arr, json_string(&ni->merklehash[i][0]));
	/* Use our own jobid instead of the server's one for easy lookup */
	JSON_CPACK(json_msg, "{sIsisisssisssssosssssssb}",
			     "proxy", proxy->id, "subproxy", proxi->subid,
			     "jobid", ni->id, "prevhash", ni->prevhash, "coinb1len", ni->coinb1len,
			     "coinbase1", ni->coinbase1, "coinbase2", ni->coinbase2,
			     "merklehash", merkle_arr, "bbversion", ni->bbversion,
			     "nbit", ni->nbit, "ntime", ni->ntime,
			     "clean", ni->clean);

	msg = json_dumps(json_msg, JSON_NO_UTF8);
	json_decref(json_msg);
	ASPRINTF(&buf, "notify=%s", msg);
	free(msg);
	send_proc(ckp->stratifier, buf);
	free(buf);

	/* Send diff now as stratifier will not accept diff till it has a
	 * valid workbase */
	send_diff(ckp, proxi);
}

static bool parse_method(ckpool_t *ckp, proxy_instance_t *proxi, const char *msg)
{
	json_t *val = NULL, *method, *err_val, *params;
	json_error_t err;
	bool ret = false;
	const char *buf;

	memset(&err, 0, sizeof(err));
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

	LOGDEBUG("Proxy %d:%d received method %s", proxi->id, proxi->subid, buf);
	if (cmdmatch(buf, "mining.notify")) {
		ret = parse_notify(ckp, proxi, params);
		goto out;
	}

	if (cmdmatch(buf, "mining.set_difficulty")) {
		ret = parse_diff(proxi, params);
		if (likely(ret))
			send_diff(ckp, proxi);
		goto out;
	}

	if (cmdmatch(buf, "client.reconnect")) {
		ret = parse_reconnect(proxi, params);
		goto out;
	}

	if (cmdmatch(buf, "client.get_version")) {
		ret =  send_version(proxi, val);
		goto out;
	}

	if (cmdmatch(buf, "client.show_message")) {
		ret = show_message(params);
		goto out;
	}

	if (cmdmatch(buf, "mining.ping")) {
		ret = send_pong(proxi, val);
		goto out;
	}
out:
	if (val)
		json_decref(val);
	return ret;
}

static bool auth_stratum(ckpool_t *ckp, connsock_t *cs, proxy_instance_t *proxi)
{
	json_t *val = NULL, *res_val, *req, *err_val;
	char *buf = NULL;
	bool ret;

	JSON_CPACK(req, "{s:i,s:s,s:[s,s]}",
			"id", 42,
			"method", "mining.authorize",
			"params", proxi->auth, proxi->pass);
	ret = send_json_msg(cs, req);
	json_decref(req);
	if (!ret) {
		LOGNOTICE("Proxy %d:%d %s failed to send message in auth_stratum",
			  proxi->id, proxi->subid, proxi->url);
		if (cs->fd > 0) {
			epoll_ctl(proxi->epfd, EPOLL_CTL_DEL, cs->fd, NULL);
			Close(cs->fd);
		}
		goto out;
	}

	/* Read and parse any extra methods sent. Anything left in the buffer
	 * should be the response to our auth request. */
	do {
		free(buf);
		buf = next_proxy_line(cs, proxi);
		if (!buf) {
			LOGNOTICE("Proxy %d:%d %s failed to receive line in auth_stratum",
				  proxi->id, proxi->subid, proxi->url);
			ret = false;
			goto out;
		}
		ret = parse_method(ckp, proxi, buf);
	} while (ret);

	val = json_msg_result(buf, &res_val, &err_val);
	if (!val) {
		LOGWARNING("Proxy %d:%d %s failed to get a json result in auth_stratum, got: %s",
			   proxi->id, proxi->subid, proxi->url, buf);
		goto out;
	}

	if (err_val && !json_is_null(err_val)) {
		LOGWARNING("Proxy %d:%d %s failed to authorise in auth_stratum due to err_val, got: %s",
			   proxi->id, proxi->subid, proxi->url, buf);
		goto out;
	}
	if (res_val) {
		ret = json_is_true(res_val);
		if (!ret) {
			LOGWARNING("Proxy %d:%d %s failed to authorise in auth_stratum, got: %s",
				   proxi->id, proxi->subid, proxi->url, buf);
			goto out;
		}
	} else {
		/* No result and no error but successful val means auth success */
		ret = true;
	}
	LOGINFO("Proxy %d:%d %s auth success in auth_stratum", proxi->id, proxi->subid, proxi->url);
out:
	if (val)
		json_decref(val);
	if (ret) {
		/* Now parse any cached responses so there are none in the
		 * queue and they can be managed one at a time from now on. */
		while(42) {
			dealloc(buf);
			buf = cached_proxy_line(proxi);
			if (!buf)
				break;
			parse_method(ckp, proxi, buf);
		};
	}
	return ret;
}

static proxy_instance_t *proxy_by_id(gdata_t *gdata, const int64_t id)
{
	proxy_instance_t *proxi;

	mutex_lock(&gdata->lock);
	HASH_FIND_I64(gdata->proxies, &id, proxi);
	mutex_unlock(&gdata->lock);

	return proxi;
}

static void send_subscribe(ckpool_t *ckp, proxy_instance_t *proxi)
{
	json_t *json_msg;
	char *msg, *buf;

	JSON_CPACK(json_msg, "{sIsisssi}",
			     "proxy", proxi->id,
			     "subproxy", proxi->subid,
			     "enonce1", proxi->enonce1,
			     "nonce2len", proxi->nonce2len);
	msg = json_dumps(json_msg, JSON_NO_UTF8);
	json_decref(json_msg);
	ASPRINTF(&buf, "subscribe=%s", msg);
	free(msg);
	send_proc(ckp->stratifier, buf);
	free(buf);
}

static proxy_instance_t *subproxy_by_id(proxy_instance_t *proxy, const int subid)
{
	proxy_instance_t *subproxy;

	mutex_lock(&proxy->proxy_lock);
	subproxy = __subproxy_by_id(proxy, subid);
	if (subproxy && subproxy->disabled)
		subproxy = NULL;
	mutex_unlock(&proxy->proxy_lock);

	return subproxy;
}

static void drop_proxy(gdata_t *gdata, const char *buf)
{
	proxy_instance_t *proxy, *subproxy;
	int id = -1, subid = -1;

	sscanf(buf, "dropproxy=%d:%d", &id, &subid);
	if (unlikely(!subid)) {
		LOGWARNING("Generator asked to drop parent proxy %d", id);
		return;
	}
	proxy = proxy_by_id(gdata, id);
	if (unlikely(!proxy)) {
		LOGINFO("Generator asked to drop subproxy from non-existent parent %d", id);
		return;
	}
	subproxy = subproxy_by_id(proxy, subid);
	if (!subproxy) {
		LOGINFO("Generator asked to drop non-existent subproxy %d:%d", id, subid);
		return;
	}
	LOGNOTICE("Generator asked to drop proxy %d:%d", id, subid);
	disable_subproxy(gdata, proxy, subproxy);
}

static void stratifier_reconnect_client(ckpool_t *ckp, const int64_t id)
{
	char buf[256];

	sprintf(buf, "reconnclient=%"PRId64, id);
	send_proc(ckp->stratifier, buf);
}

static void submit_share(gdata_t *gdata, json_t *val)
{
	proxy_instance_t *proxy, *proxi;
	ckpool_t *ckp = gdata->ckp;
	bool success = false;
	stratum_msg_t *msg;
	share_msg_t *share;
	int64_t client_id;
	int id, subid;

	/* Get the client id so we can tell the stratifier to drop it if the
	 * proxy it's bound to is not functional */
	if (unlikely(!json_get_int64(&client_id, val, "client_id"))) {
		LOGWARNING("Got no client_id in share");
		goto out;
	}
	if (unlikely(!json_get_int(&id, val, "proxy"))) {
		LOGWARNING("Got no proxy in share");
		goto out;
	}
	if (unlikely(!json_get_int(&subid, val, "subproxy"))) {
		LOGWARNING("Got no subproxy in share");
		goto out;
	}
	proxy = proxy_by_id(gdata, id);
	if (unlikely(!proxy)) {
		LOGNOTICE("Client %"PRId64" sending shares to non existent proxy %d, dropping",
			  client_id, id);
		stratifier_reconnect_client(ckp, client_id);
		goto out;
	}
	proxi = subproxy_by_id(proxy, subid);
	if (unlikely(!proxi)) {
		LOGNOTICE("Client %"PRId64" sending shares to non existent subproxy %d:%d, dropping",
			  client_id, id, subid);
		stratifier_reconnect_client(ckp, client_id);
		goto out;
	}
	if (!proxi->alive) {
		LOGNOTICE("Client %"PRId64" sending shares to dead subproxy %d:%d, dropping",
			  client_id, id, subid);
		stratifier_reconnect_client(ckp, client_id);
		goto out;
	}

	success = true;
	msg = ckzalloc(sizeof(stratum_msg_t));
	share = ckzalloc(sizeof(share_msg_t));
	share->submit_time = time(NULL);
	share->client_id = client_id;
	msg->json_msg = val;

	/* Add new share entry to the share hashtable */
	mutex_lock(&proxi->share_lock);
	share->id = proxi->share_id++;
	HASH_ADD_I64(proxi->shares, id, share);
	mutex_unlock(&proxi->share_lock);

	json_object_set_nocheck(val, "id", json_integer(share->id));

	/* Add the new message to the psend list */
	mutex_lock(&proxy->psend_lock);
	DL_APPEND(proxy->psends, msg);
	pthread_cond_signal(&proxy->psend_cond);
	mutex_unlock(&proxy->psend_lock);

out:
	if (!success)
		json_decref(val);
}

static void clear_notify(notify_instance_t *ni)
{
	if (ni->jobid)
		json_decref(ni->jobid);
	free(ni->coinbase1);
	free(ni->coinbase2);
	free(ni);
}

/* FIXME: Return something useful to the stratifier based on this result */
static bool parse_share(proxy_instance_t *proxi, const char *buf)
{
	json_t *val = NULL, *idval;
	share_msg_t *share;
	bool ret = false;
	int64_t id;

	val = json_loads(buf, 0, NULL);
	if (!val) {
		LOGINFO("Failed to parse json msg: %s", buf);
		goto out;
	}
	idval = json_object_get(val, "id");
	if (!idval) {
		LOGINFO("Failed to find id in json msg: %s", buf);
		goto out;
	}
	id = json_integer_value(idval);

	mutex_lock(&proxi->share_lock);
	HASH_FIND_I64(proxi->shares, &id, share);
	if (share)
		HASH_DEL(proxi->shares, share);
	mutex_unlock(&proxi->share_lock);

	/* We set response to true even if we don't find the matching share,
	 * so long as we recognised it as a share response */
	ret = true;
	if (!share) {
		LOGINFO("Proxy %d:%d failed to find matching share to result: %s",
			proxi->id, proxi->subid, buf);
		goto out;
	}
	LOGINFO("Proxy %d:%d share result %s from client %d", proxi->id, proxi->subid,
		buf, share->client_id);
	free(share);
out:
	if (val)
		json_decref(val);
	return ret;
}

/* For processing and sending shares. proxy refers to parent proxy here */
static void *proxy_send(void *arg)
{
	proxy_instance_t *proxy = (proxy_instance_t *)arg;
	connsock_t *cs = &proxy->cs;
	ckpool_t *ckp = cs->ckp;
	gdata_t *gdata = ckp->data;
	stratum_msg_t *msg = NULL;

	rename_proc("proxysend");

	while (42) {
		proxy_instance_t *subproxy;
		int proxyid = 0, subid = 0;
		int64_t client_id = 0, id;
		notify_instance_t *ni;
		json_t *jobid = NULL;
		bool ret = true;
		json_t *val;
		tv_t now;
		ts_t abs;

		if (unlikely(proxy->reconnect)) {
			LOGINFO("Shutting down proxy_send thread for proxy %d to reconnect",
				proxy->id);
			break;
		}

		tv_time(&now);
		tv_to_ts(&abs, &now);
		abs.tv_sec++;

		if (unlikely(msg)) {
			json_decref(msg->json_msg);
			free(msg);
		}

		mutex_lock(&proxy->psend_lock);
		if (!proxy->psends)
			cond_timedwait(&proxy->psend_cond, &proxy->psend_lock, &abs);
		msg = proxy->psends;
		if (likely(msg))
			DL_DELETE(proxy->psends, msg);
		mutex_unlock(&proxy->psend_lock);

		if (!msg)
			continue;

		if (unlikely(!json_get_int(&subid, msg->json_msg, "subproxy"))) {
			LOGWARNING("Failed to find subproxy in proxy_send msg");
			continue;
		}
		if (unlikely(!json_get_int64(&id, msg->json_msg, "jobid"))) {
			LOGWARNING("Failed to find jobid in proxy_send msg");
			continue;
		}
		if (unlikely(!json_get_int(&proxyid, msg->json_msg, "proxy"))) {
			LOGWARNING("Failed to find proxy in proxy_send msg");
			continue;
		}
		if (unlikely(!json_get_int64(&client_id, msg->json_msg, "client_id"))) {
			LOGWARNING("Failed to find client_id in proxy_send msg");
			continue;
		}
		if (unlikely(proxyid != proxy->id)) {
			LOGWARNING("Proxysend for proxy %d got message for proxy %d!",
				   proxy->id, proxyid);
		}

		mutex_lock(&proxy->notify_lock);
		HASH_FIND_INT(proxy->notify_instances, &id, ni);
		if (ni)
			jobid = json_copy(ni->jobid);
		mutex_unlock(&proxy->notify_lock);

		subproxy = subproxy_by_id(proxy, subid);
		if (subproxy)
			cs = &subproxy->cs;
		if (jobid && subproxy) {
			JSON_CPACK(val, "{s[soooo]soss}", "params", proxy->auth, jobid,
					json_object_dup(msg->json_msg, "nonce2"),
					json_object_dup(msg->json_msg, "ntime"),
					json_object_dup(msg->json_msg, "nonce"),
					"id", json_object_dup(msg->json_msg, "id"),
					"method", "mining.submit");
			ret = send_json_msg(cs, val);
			json_decref(val);
		} else if (!jobid) {
			stratifier_reconnect_client(ckp, client_id);
			LOGNOTICE("Proxy %d:%s failed to find matching jobid for %sknown subproxy in proxysend",
				  proxy->id, proxy->url, subproxy ? "" : "un");
		} else {
			stratifier_reconnect_client(ckp, client_id);
			LOGNOTICE("Failed to find subproxy %d:%d to send message to",
				  proxy->id, subid);
		}
		if (!ret && subproxy) {
			LOGNOTICE("Proxy %d:%d %s failed to send msg in proxy_send, dropping to reconnect",
				  proxy->id, subid, proxy->url);
			disable_subproxy(gdata, proxy, subproxy);
		}
	}
	return NULL;
}

static void passthrough_send(ckpool_t __maybe_unused *ckp, pass_msg_t *pm)
{
	int len, sent;

	LOGDEBUG("Sending upstream json msg: %s", pm->msg);
	len = strlen(pm->msg);
	sent = write_socket(pm->cs->fd, pm->msg, len);
	if (sent != len) {
		/* FIXME: Do something about this? */
		LOGWARNING("Failed to passthrough %d bytes of message %s", len, pm->msg);
	}
	free(pm->msg);
	free(pm);
}

static void passthrough_add_send(proxy_instance_t *proxi, const char *msg)
{
	pass_msg_t *pm = ckzalloc(sizeof(pass_msg_t));

	pm->cs = &proxi->cs;
	ASPRINTF(&pm->msg, "%s\n", msg);
	ckmsgq_add(proxi->passsends, pm);
}

static bool proxy_alive(ckpool_t *ckp, proxy_instance_t *proxi, connsock_t *cs,
			bool pinging, int epfd)
{
	struct epoll_event event;
	bool ret = false;

	/* Has this proxy already been reconnected? */
	if (cs->fd > 0)
		return true;
	if (!extract_sockaddr(proxi->url, &cs->url, &cs->port)) {
		LOGWARNING("Failed to extract address from %s", proxi->url);
		goto out;
	}
	if (!connect_proxy(cs)) {
		if (!pinging) {
			LOGINFO("Failed to connect to %s:%s in proxy_mode!",
				cs->url, cs->port);
		}
		goto out;
	}
	if (ckp->passthrough) {
		if (!passthrough_stratum(cs, proxi)) {
			LOGWARNING("Failed initial passthrough to %s:%s !",
				   cs->url, cs->port);
			goto out;
		}
		ret = true;
		goto out;
	}
	/* Test we can connect, authorise and get stratum information */
	if (!subscribe_stratum(cs, proxi)) {
		if (!pinging) {
			LOGWARNING("Failed initial subscribe to %s:%s !",
				   cs->url, cs->port);
		}
		goto out;
	}
	if (!ckp->passthrough)
		send_subscribe(ckp, proxi);
	if (!auth_stratum(ckp, cs, proxi)) {
		if (!pinging) {
			LOGWARNING("Failed initial authorise to %s:%s with %s:%s !",
				   cs->url, cs->port, proxi->auth, proxi->pass);
		}
		goto out;
	}
	ret = true;
out:
	if (!ret) {
		send_stratifier_deadproxy(ckp, proxi->id, proxi->subid);
		/* Close and invalidate the file handle */
		if (cs->fd > 0) {
			epoll_ctl(proxi->epfd, EPOLL_CTL_DEL, cs->fd, NULL);
			Close(cs->fd);
		}
		if (parent_proxy(proxi))
			reconnect_generator(ckp);
	} else {
		keep_sockalive(cs->fd);
		event.events = EPOLLIN;
		event.data.ptr = proxi;
		/* Add this connsock_t to the epoll list */
		if (unlikely(epoll_ctl(epfd, EPOLL_CTL_ADD, cs->fd, &event) == -1)) {
			LOGERR("Failed to add fd %d to epfd %d to epoll_ctl in proxy_alive",
			       cs->fd, epfd);
			return false;
		}
	}
	proxi->alive = ret;
	return ret;
}

static void *proxy_recruit(void *arg)
{
	proxy_instance_t *proxy, *parent = (proxy_instance_t *)arg;
	ckpool_t *ckp = parent->ckp;
	gdata_t *gdata = ckp->data;
	bool recruit, alive;

	pthread_detach(pthread_self());

retry:
	recruit = false;
	proxy = create_subproxy(gdata, parent, parent->url);
	alive = proxy_alive(ckp, proxy, &proxy->cs, false, parent->epfd);
	if (!alive) {
		LOGNOTICE("Subproxy failed proxy_alive testing");
		store_proxy(gdata, proxy);
	} else
		add_subproxy(parent, proxy);

	mutex_lock(&parent->proxy_lock);
	if (alive && parent->recruit > 0)
		recruit = true;
	else /* Reset so the next request will try again */
		parent->recruit = 0;
	mutex_unlock(&parent->proxy_lock);

	if (recruit)
		goto retry;

	return NULL;
}

static void recruit_subproxies(proxy_instance_t *proxi, const int recruits)
{
	bool recruit = false;
	pthread_t pth;

	mutex_lock(&proxi->proxy_lock);
	if (!proxi->recruit)
		recruit = true;
	if (proxi->recruit < recruits)
		proxi->recruit = recruits;
	mutex_unlock(&proxi->proxy_lock);

	if (recruit)
		create_pthread(&pth, proxy_recruit, proxi);
}

/* Queue up to the requested amount */
static void recruit_subproxy(proxy_instance_t *proxi, const char *buf)
{
	int recruits = 1;

	sscanf(buf, "recruit=%d", &recruits);
	recruit_subproxies(proxi, recruits);
}

static void *proxy_reconnect(void *arg)
{
	proxy_instance_t *proxy = (proxy_instance_t *)arg;
	connsock_t *cs = &proxy->cs;
	ckpool_t *ckp = proxy->ckp;

	pthread_detach(pthread_self());
	proxy_alive(ckp, proxy, cs, true, proxy->epfd);
	proxy->reconnecting = false;
	return NULL;
}

/* For reconnecting the parent proxy instance async */
static void reconnect_proxy(proxy_instance_t *proxi)
{
	pthread_t pth;

	if (proxi->reconnecting)
		return;
	proxi->reconnecting = true;
	create_pthread(&pth, proxy_reconnect, proxi);
}

/* For receiving messages from an upstream pool to pass downstream. Responsible
 * for setting up the connection and testing pool is live. */
static void *passthrough_recv(void *arg)
{
	proxy_instance_t *proxi = (proxy_instance_t *)arg;
	connsock_t *cs = &proxi->cs;
	ckpool_t *ckp = proxi->ckp;
	struct epoll_event event;
	bool alive;
	int epfd;

	rename_proc("passrecv");

	proxi->epfd = epfd = epoll_create1(EPOLL_CLOEXEC);
	if (epfd < 0){
		LOGEMERG("FATAL: Failed to create epoll in passrecv");
		return NULL;
	}

	if (proxy_alive(ckp, proxi, cs, false, epfd)) {
		reconnect_generator(ckp);
		LOGWARNING("Proxy %d:%s connection established", proxi->id, proxi->url);
	}
	alive = proxi->alive;

	while (42) {
		int ret;

		while (!proxy_alive(ckp, proxi, cs, true, epfd)) {
			if (alive) {
				alive = false;
				reconnect_generator(ckp);
			}
			sleep(5);
		}
		if (!alive)
			reconnect_generator(ckp);

		/* Make sure we receive a line within 90 seconds */
		ret = epoll_wait(epfd, &event, 1, 90000);
		if (likely(ret > 0))
			ret = read_socket_line(cs, 60);
		if (ret < 1) {
			LOGWARNING("Proxy %d:%s failed to read_socket_line in proxy_recv, attempting reconnect",
				   proxi->id, proxi->url);
			alive = proxi->alive = false;
			reconnect_generator(ckp);
			continue;
		}
		/* Simply forward the message on, as is, to the connector to
		 * process. Possibly parse parameters sent by upstream pool
		 * here */
		send_proc(ckp->connector, cs->buf);
	}
	return NULL;
}

static proxy_instance_t *current_proxy(gdata_t *gdata)
{
	proxy_instance_t *ret;

	mutex_lock(&gdata->lock);
	ret = gdata->proxy;
	mutex_unlock(&gdata->lock);

	return ret;
}

static bool subproxies_alive(proxy_instance_t *proxy)
{
	proxy_instance_t *subproxy, *tmp;
	bool ret = false;

	mutex_lock(&proxy->proxy_lock);
	HASH_ITER(sh, proxy->subproxies, subproxy, tmp) {
		if (subproxy->alive) {
			ret = true;
			break;
		}
	}
	mutex_unlock(&proxy->proxy_lock);

	return ret;
}

/* For receiving messages from the upstream proxy, also responsible for setting
 * up the connection and testing it's alive. */
static void *proxy_recv(void *arg)
{
	proxy_instance_t *proxi = (proxy_instance_t *)arg;
	proxy_instance_t *subproxy, *tmp;
	connsock_t *cs = &proxi->cs;
	ckpool_t *ckp = proxi->ckp;
	gdata_t *gdata = ckp->data;
	struct epoll_event event;
	bool alive;
	int epfd;

	rename_proc("proxyrecv");

	proxi->epfd = epfd = epoll_create1(EPOLL_CLOEXEC);
	if (epfd < 0){
		LOGEMERG("FATAL: Failed to create epoll in proxyrecv");
		return NULL;
	}

	if (proxy_alive(ckp, proxi, cs, false, epfd)) {
		LOGWARNING("Proxy %d:%s connection established", proxi->id, proxi->url);
	}
	alive = proxi->alive;

	while (42) {
		share_msg_t *share, *tmpshare;
		notify_instance_t *ni, *tmp;
		time_t now;
		int ret;

		subproxy = proxi;
		if (!proxi->alive) {
			reconnect_proxy(proxi);
			while (!subproxies_alive(proxi)) {
				reconnect_proxy(proxi);
				if (alive) {
					LOGWARNING("Proxy %d:%s failed, attempting reconnect",
						   proxi->id, proxi->url);
					alive = false;
				}
				sleep(5);
				proxi->reconnect_time = time(NULL);
			}
		}
		/* Wait 30 seconds before declaring this upstream pool alive
		 * to prevent switching to unstable pools. */
		if (!alive && (!current_proxy(gdata) || time(NULL) - proxi->reconnect_time > 30)) {
			reconnect_generator(ckp);
			LOGWARNING("Proxy %d:%s recovered", proxi->id, proxi->url);
			proxi->reconnect_time = 0;
			alive = true;
		}

		now = time(NULL);

		/* Age old notifications older than 10 mins old */
		mutex_lock(&proxi->notify_lock);
		HASH_ITER(hh, proxi->notify_instances, ni, tmp) {
			if (HASH_COUNT(proxi->notify_instances) < 3)
				break;
			if (ni->notify_time < now - 600) {
				HASH_DEL(proxi->notify_instances, ni);
				clear_notify(ni);
			}
		}
		mutex_unlock(&proxi->notify_lock);

		/* Similary with shares older than 2 mins without response */
		mutex_lock(&proxi->share_lock);
		HASH_ITER(hh, proxi->shares, share, tmpshare) {
			if (share->submit_time < now - 120) {
				HASH_DEL(proxi->shares, share);
			}
		}
		mutex_unlock(&proxi->share_lock);

		/* If we don't get an update within 10 minutes the upstream pool
		 * has likely stopped responding. */
		ret = epoll_wait(epfd, &event, 1, 600000);
		if (likely(ret > 0)) {
			subproxy = event.data.ptr;
			cs = &subproxy->cs;
			if (event.events & EPOLLHUP)
				ret = -1;
			else
				ret = read_socket_line(cs, 5);
		}
		if (ret < 1) {
			LOGNOTICE("Proxy %d:%d %s failed to epoll/read_socket_line in proxy_recv",
				  proxi->id, subproxy->subid, subproxy->url);
			disable_subproxy(gdata, proxi, subproxy);
			continue;
		}
		do {
			/* subproxy may have been recycled here if it is not a
			 * parent and reconnect was issued */
			if (parse_method(ckp, subproxy, cs->buf))
				continue;
			/* If it's not a method it should be a share result */
			if (!parse_share(subproxy, cs->buf))
				LOGNOTICE("Proxy %d:%d unhandled stratum message: %s",
					  subproxy->id, subproxy->subid, cs->buf);
		} while ((ret = read_socket_line(cs, 0)) > 0);
	}

	HASH_ITER(sh, proxi->subproxies, subproxy, tmp) {
		subproxy->disabled = true;
		send_stratifier_deadproxy(ckp, subproxy->id, subproxy->subid);
		if (subproxy->cs.fd > 0) {
			epoll_ctl(epfd, EPOLL_CTL_DEL, subproxy->cs.fd, NULL);
			Close(subproxy->cs.fd);
		}
		HASH_DELETE(sh, proxi->subproxies, subproxy);
	}
	mutex_unlock(&proxi->proxy_lock);

	return NULL;
}

static void prepare_proxy(proxy_instance_t *proxi)
{
	proxi->parent = proxi;
	mutex_init(&proxi->proxy_lock);
	add_subproxy(proxi, proxi);
	mutex_init(&proxi->psend_lock);
	cond_init(&proxi->psend_cond);
	create_pthread(&proxi->pth_psend, proxy_send, proxi);
	create_pthread(&proxi->pth_precv, proxy_recv, proxi);
}

static proxy_instance_t *wait_best_proxy(ckpool_t *ckp, gdata_t *gdata)
{
	proxy_instance_t *ret = NULL, *proxi, *tmp;

	while (42) {
		if (!ping_main(ckp))
			break;

		mutex_lock(&gdata->lock);
		HASH_ITER(hh, gdata->proxies, proxi, tmp) {
			if (proxi->disabled)
				continue;
			if (proxi->alive || subproxies_alive(proxi)) {
				if (!ret || proxi->id < ret->id)
					ret = proxi;
			}
		}
		gdata->proxy = ret;
		mutex_unlock(&gdata->lock);

		if (ret)
			break;
		send_proc(ckp->connector, "reject");
		sleep(1);
	}
	send_proc(ckp->connector, ret ? "accept" : "reject");
	return ret;
}

static void send_list(gdata_t *gdata, const int sockd)
{
	proxy_instance_t *proxy, *tmp;
	json_t *val, *array_val;

	array_val = json_array();

	mutex_lock(&gdata->lock);
	HASH_ITER(hh, gdata->proxies, proxy, tmp) {
		JSON_CPACK(val, "{si,ss,ss,sf,sb,sb,sb,si}",
			"id", proxy->id,
			"auth", proxy->auth, "pass", proxy->pass,
			"diff", proxy->diff, "notified", proxy->notified,
			"disabled", proxy->disabled, "alive", proxy->alive,
			"subproxies", proxy->subproxy_count);
		if (proxy->enonce1) {
			json_set_string(val, "enonce1", proxy->enonce1);
			json_set_int(val, "nonce1len", proxy->nonce1len);
			json_set_int(val, "nonce2len", proxy->nonce2len);
		}
		json_array_append_new(array_val, val);
	}
	mutex_unlock(&gdata->lock);

	JSON_CPACK(val, "{so}", "proxies", array_val);
	send_api_response(val, sockd);
}

static void send_sublist(gdata_t *gdata, const int sockd, const char *buf)
{
	proxy_instance_t *proxy, *subproxy, *tmp;
	json_t *val = NULL, *array_val;
	json_error_t err_val;
	int64_t id;

	array_val = json_array();

	val = json_loads(buf, 0, &err_val);
	if (unlikely(!val)) {
		val = json_encode_errormsg(&err_val);
		goto out;
	}
	if (unlikely(!json_get_int64(&id, val, "id"))) {
		val = json_errormsg("Failed to get ID in send_sublist JSON: %s", buf);
		goto out;
	}
	proxy = proxy_by_id(gdata, id);
	if (unlikely(!proxy)) {
		val = json_errormsg("Failed to find proxy %"PRId64" in send_sublist", id);
		goto out;
	}

	mutex_lock(&gdata->lock);
	HASH_ITER(sh, proxy->subproxies, subproxy, tmp) {
		JSON_CPACK(val, "{si,ss,ss,sf,sb,sb,sb}",
			"subid", subproxy->id,
			"auth", subproxy->auth, "pass", subproxy->pass,
			"diff", subproxy->diff, "notified", subproxy->notified,
			"disabled", subproxy->disabled, "alive", subproxy->alive);
		if (subproxy->enonce1) {
			json_set_string(val, "enonce1", subproxy->enonce1);
			json_set_int(val, "nonce1len", subproxy->nonce1len);
			json_set_int(val, "nonce2len", subproxy->nonce2len);
		}
		json_array_append_new(array_val, val);
	}
	mutex_unlock(&gdata->lock);

	JSON_CPACK(val, "{so}", "subproxies", array_val);
out:
	send_api_response(val, sockd);
}

static proxy_instance_t *__add_proxy(ckpool_t *ckp, gdata_t *gdata, const int num);

static void parse_addproxy(ckpool_t *ckp, gdata_t *gdata, const int sockd, const char *buf)
{
	char *url = NULL, *auth = NULL, *pass = NULL;
	proxy_instance_t *proxy;
	json_error_t err_val;
	json_t *val = NULL;
	int id;

	val = json_loads(buf, 0, &err_val);
	if (unlikely(!val)) {
		val = json_encode_errormsg(&err_val);
		goto out;
	}
	json_get_string(&url, val, "url");
	json_get_string(&auth, val, "auth");
	json_get_string(&pass, val, "pass");
	json_decref(val);
	if (unlikely(!url || !auth || !pass)) {
		val = json_errormsg("Failed to decode url/auth/pass in addproxy %s", buf);
		goto out;
	}

	mutex_lock(&gdata->lock);
	id = ckp->proxies++;
	ckp->proxyurl = realloc(ckp->proxyurl, sizeof(char **) * ckp->proxies);
	ckp->proxyauth = realloc(ckp->proxyauth, sizeof(char **) * ckp->proxies);
	ckp->proxypass = realloc(ckp->proxypass, sizeof(char **) * ckp->proxies);
	ckp->proxyurl[id] = strdup(url);
	ckp->proxyauth[id] = strdup(auth);
	ckp->proxypass[id] = strdup(pass);
	proxy = __add_proxy(ckp, gdata, id);
	mutex_unlock(&gdata->lock);

	prepare_proxy(proxy);
	JSON_CPACK(val, "{sI,ss,ss,ss}",
		   "id", proxy->id, "url", url, "auth", auth, "pass", pass);
out:
	send_api_response(val, sockd);
}

static int proxy_loop(proc_instance_t *pi)
{
	proxy_instance_t *proxi = NULL, *cproxy;
	int sockd = -1, ret = 0, selret;
	unixsock_t *us = &pi->us;
	ckpool_t *ckp = pi->ckp;
	gdata_t *gdata = ckp->data;
	char *buf = NULL;

reconnect:
	Close(sockd);
	/* This does not necessarily mean we reconnect, but a change has
	 * occurred and we need to reexamine the proxies. */
	cproxy = wait_best_proxy(ckp, gdata);
	if (!cproxy)
		goto out;
	if (proxi != cproxy) {
		proxi = cproxy;
		if (!ckp->passthrough) {
			LOGWARNING("Successfully connected to proxy %d %s as proxy",
				   proxi->id, proxi->url);
			dealloc(buf);
			ASPRINTF(&buf, "proxy=%d", proxi->id);
			send_proc(ckp->stratifier, buf);
		}
	}
retry:
	Close(sockd);
	do {
		selret = wait_read_select(us->sockd, 5);
		if (!selret && !ping_main(ckp)) {
			LOGEMERG("Generator failed to ping main process, exiting");
			ret = 1;
			goto out;
		}
	} while (selret < 1);

	sockd = accept(us->sockd, NULL, NULL);
	if (sockd < 0) {
		LOGEMERG("Failed to accept on proxy socket");
		ret = 1;
		goto out;
	}
	dealloc(buf);
	buf = recv_unix_msg(sockd);
	if (!buf) {
		LOGWARNING("Failed to get message in proxy_loop");
		goto retry;
	}
	LOGDEBUG("Proxy received request: %s", buf);
	if (likely(buf[0] == '{')) {
		if (ckp->passthrough)
			passthrough_add_send(proxi, buf);
		else {
			/* Anything remaining should be share submissions */
			json_t *val = json_loads(buf, 0, NULL);

			if (unlikely(!val))
				LOGWARNING("Generator received invalid json message: %s", buf);
			else
				submit_share(gdata, val);
		}
	} else if (cmdmatch(buf, "list")) {
		send_list(gdata, sockd);
	} else if (cmdmatch(buf, "sublist")) {
		send_sublist(gdata, sockd, buf + 8);
	} else if (cmdmatch(buf, "addproxy")) {
		parse_addproxy(ckp, gdata, sockd, buf + 9);
	} else if (cmdmatch(buf, "shutdown")) {
		ret = 0;
		goto out;
	} else if (cmdmatch(buf, "reconnect")) {
		goto reconnect;
	} else if (cmdmatch(buf, "submitblock:")) {
		LOGNOTICE("Submitting likely block solve share to upstream pool");
	} else if (cmdmatch(buf, "loglevel")) {
		sscanf(buf, "loglevel=%d", &ckp->loglevel);
	} else if (cmdmatch(buf, "ping")) {
		LOGDEBUG("Proxy received ping request");
		send_unix_msg(sockd, "pong");
	} else if (cmdmatch(buf, "recruit")) {
		recruit_subproxy(proxi, buf);
	} else if (cmdmatch(buf, "dropproxy")) {
		drop_proxy(gdata, buf);
	} else {
		LOGWARNING("Generator received unrecognised message: %s", buf);
	}
	goto retry;
out:
	Close(sockd);
	return ret;
}

static int server_mode(ckpool_t *ckp, proc_instance_t *pi)
{
	server_instance_t *si;
	int i, ret;

	ckp->servers = ckalloc(sizeof(server_instance_t *) * ckp->btcds);
	for (i = 0; i < ckp->btcds; i++) {
		ckp->servers[i] = ckzalloc(sizeof(server_instance_t));
		si = ckp->servers[i];
		si->url = ckp->btcdurl[i];
		si->auth = ckp->btcdauth[i];
		si->pass = ckp->btcdpass[i];
		si->notify = ckp->btcdnotify[i];
	}

	ret = gen_loop(pi);

	for (i = 0; i < ckp->btcds; i++) {
		si = ckp->servers[i];
		kill_server(si);
		dealloc(si);
	}
	dealloc(ckp->servers);
	return ret;
}

static proxy_instance_t *__add_proxy(ckpool_t *ckp, gdata_t *gdata, const int id)
{
	proxy_instance_t *proxy;

	proxy = ckzalloc(sizeof(proxy_instance_t));
	proxy->id = id;
	proxy->url = strdup(ckp->proxyurl[id]);
	proxy->auth = strdup(ckp->proxyauth[id]);
	proxy->pass = strdup(ckp->proxypass[id]);
	proxy->ckp = ckp;
	proxy->cs.ckp = ckp;
	mutex_init(&proxy->notify_lock);
	mutex_init(&proxy->share_lock);
	HASH_ADD_INT(gdata->proxies, id, proxy);
	return proxy;
}

static int proxy_mode(ckpool_t *ckp, proc_instance_t *pi)
{
	gdata_t *gdata = ckp->data;
	proxy_instance_t *proxy;
	int i, ret;

	mutex_init(&gdata->lock);

	/* Create all our proxy structures and pointers */
	for (i = 0; i < ckp->proxies; i++) {
		proxy = __add_proxy(ckp, gdata, i);
		if (ckp->passthrough) {
			create_pthread(&proxy->pth_precv, passthrough_recv, proxy);
			proxy->passsends = create_ckmsgq(ckp, "passsend", &passthrough_send);
		} else {
			prepare_proxy(proxy);
		}
	}

	LOGWARNING("%s generator ready", ckp->name);

	ret = proxy_loop(pi);

	mutex_lock(&gdata->lock);
	for (i = 0; i < ckp->proxies; i++) {
		continue; // FIXME: Find proxies
		Close(proxy->cs.fd);
		free(proxy->enonce1);
		free(proxy->enonce1bin);
		pthread_cancel(proxy->pth_psend);
		pthread_cancel(proxy->pth_precv);
		join_pthread(proxy->pth_psend);
		join_pthread(proxy->pth_precv);
		dealloc(proxy->url);
		dealloc(proxy->auth);
		dealloc(proxy->pass);
	}
	mutex_unlock(&gdata->lock);

	return ret;
}

/* Tell the watchdog what the current server instance is and decide if we
 * should check to see if the higher priority servers are alive and fallback */
static void server_watchdog(ckpool_t *ckp, server_instance_t *cursi)
{
	static time_t last_t = 0;
	bool alive = false;
	time_t now_t;
	int i;

	/* Rate limit to checking only once every 5 seconds */
	now_t = time(NULL);
	if (now_t <= last_t + 5)
		return;

	last_t = now_t;

	/* Is this the highest priority server already? */
	if (!cursi->id)
		return;

	for (i = 0; i < ckp->btcds; i++) {
		server_instance_t *si  = ckp->servers[i];

		/* Have we reached the current server? */
		if (si == cursi)
			return;

		alive = server_alive(ckp, si, true);
		if (alive)
			break;
	}
	if (alive)
		reconnect_generator(ckp);
}

int generator(proc_instance_t *pi)
{
	ckpool_t *ckp = pi->ckp;
	gdata_t *gdata;
	int ret;

	LOGWARNING("%s generator starting", ckp->name);
	gdata = ckzalloc(sizeof(gdata_t));
	ckp->data = gdata;
	gdata->ckp = ckp;
	if (ckp->proxy) {
		char *buf = NULL;

		/* Wait for the stratifier to be ready for us */
		do {
			if (!ping_main(ckp)) {
				ret = 1;
				goto out;
			}
			cksleep_ms(10);
			buf = send_recv_proc(ckp->stratifier, "ping");
		} while (!buf);
		dealloc(buf);
		ret = proxy_mode(ckp, pi);
	} else {
		gdata->srvchk = create_ckmsgq(ckp, "srvchk", &server_watchdog);
		ret = server_mode(ckp, pi);
	}
out:
	dealloc(ckp->data);
	return process_exit(ckp, pi, ret);
}
