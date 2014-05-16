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

/* Per proxied pool instance data */
struct proxy_instance {
	char *auth;
	char *pass;

	char *enonce1;
	char *enonce1bin;
	char *sessionid;

	tv_t last_message;

	double diff;
	int absolute_shares;
	int diff_shares;
	tv_t last_share;

	int id; /* Message id for sending stratum messages */
	bool no_sessionid; /* Doesn't support session id resume on subscribe */
	bool no_params; /* Doesn't want any parameters on subscribe */
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

static bool parse_subscribe(connsock_t *cs, proxy_instance_t *proxi)
{
	bool ret = false;
	int size;

	size = read_socket_line(cs);
	if (size < 1) {
		LOGWARNING("Failed to receive line in parse_subscribe");
		goto out;
	}
	LOGWARNING("Got message: %s", cs->buf);
	ret = true;

out:
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

static bool auth_stratum(connsock_t *cs, proxy_instance_t *proxi, const char *auth,
			 const char *pass)
{
	json_t *req;
	bool ret;

	req = json_pack("{s:i,s:s,s:[s,s]}",
			"id", proxi->id++,
			"method", "mining.authorize",
			"params", auth, pass);
	ret = send_json_msg(cs, req);
	json_decref(req);
	if (!ret) {
		LOGWARNING("Failed to send message in auth_stratum");
		close(cs->fd);
		goto out;
	}
out:
	return ret;
}

static int proxy_loop(proc_instance_t *pi, connsock_t *cs)
{
	return 0;
}

static int proxy_mode(ckpool_t *ckp, proc_instance_t *pi, connsock_t *cs,
		      const char *auth, const char *pass)
{
	proxy_instance_t proxi;
	int ret;

	memset(&proxi, 0, sizeof(proxi));
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

	if (!auth_stratum(cs, &proxi, auth, pass)) {
		LOGWARNING("FATAL: Failed initial authorise to %s:%s with %s:%s !",
			   cs->url, cs->port, auth, pass);
		goto out;
	}

	ret = proxy_loop(pi, cs);
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
