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
#include <string.h>
#include <unistd.h>

#include "ckpool.h"
#include "libckpool.h"
#include "generator.h"
#include "bitcoin.h"

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
			send_proc(&ckp->stratifier, "update");
		/* FIXME Add logging of block solves */
	}
	close(sockd);
	goto retry;

out:
	dealloc(buf);
	return ret;
}

int generator(proc_instance_t *pi)
{
	ckpool_t *ckp = pi->ckp;
	char *userpass = NULL;
	gbtbase_t gbt;
	connsock_t cs;
	int ret = 1;

	memset(&cs, 0, sizeof(cs));
	memset(&gbt, 0, sizeof(gbt));

	if (!extract_sockaddr(ckp->btcdurl, &cs.url, &cs.port)) {
		LOGWARNING("Failed to extract address from %s", ckp->btcdurl);
		goto out;
	}
	userpass = strdup(ckp->btcdauth);
	realloc_strcat(&userpass, ":");
	realloc_strcat(&userpass, ckp->btcdpass);
	cs.auth = http_base64(userpass);
	if (!cs.auth) {
		LOGWARNING("Failed to create base64 auth from %s", userpass);
		goto out;
	}
	dealloc(userpass);
	cs.fd = connect_socket(cs.url, cs.port);
	if (cs.fd < 0) {
		LOGWARNING("Failed to connect socket to %s:%s", cs.url, cs.port);
		goto out;
	}
	keep_sockalive(cs.fd);

	/* Test we can connect, authorise and get a block template */
	if (!gen_gbtbase(&cs, &gbt)) {
		LOGWARNING("Failed to get test block template from %s:%s auth %s",
			   cs.url, cs.port, userpass);
		goto out;
	}
	clear_gbtbase(&gbt);
	if (!validate_address(&cs, ckp->btcaddress)) {
		LOGWARNING("Invalid btcaddress: %s, unable to start!", ckp->btcaddress);
		goto out;
	}

	ret = gen_loop(pi, &cs);
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
