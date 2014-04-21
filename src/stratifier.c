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
#include "bitcoin.h"

static void update_base(ckpool_t *ckp)
{
	char *path = ckp->generator.us.path, *buf;
	int genfd;

	genfd = open_unix_client(ckp->generator.us.path);
	if (genfd < 0) {
		LOGWARNING("Failed to open generator socket %s", path);
		return;
	}
	if (!send_unix_msg(genfd, "getbase")) {
		LOGWARNING("Failed to send getbase to generator socket");
		close(genfd);
		return;
	}
	buf = recv_unix_msg(genfd);
	/* Do something with this buffer here */
	dealloc(buf);
}

static int strat_loop(ckpool_t *ckp, proc_instance_t *pi)
{
	int sockd, ret = 0, selret;
	unixsock_t *us = &pi->us;
	char *buf = NULL;
	fd_set readfds;
	tv_t timeout;

reset:
	timeout.tv_sec = 60;
retry:
	FD_ZERO(&readfds);
	FD_SET(us->sockd, &readfds);
	selret = select(us->sockd + 1, &readfds, NULL, NULL, &timeout);
	if (selret < 0) {
		if (interrupted())
			goto retry;
		LOGERR("Select failed in strat_loop");
		ret = 1;
		goto out;
	}
	if (!selret) {
		LOGDEBUG("60s elapsed in strat_loop, updating gbt base");
		update_base(ckp);
		goto reset;
	}
	sockd = accept(us->sockd, NULL, NULL);
	if (sockd < 0) {
		if (interrupted())
			goto retry;
		LOGERR("Failed to accept on stratifier socket");
		ret = 1;
		goto out;
	}

	dealloc(buf);
	buf = recv_unix_msg(sockd);
	if (!buf) {
		LOGWARNING("Failed to get message in strat_loop");
		close(sockd);
		goto retry;
	}
	LOGDEBUG("Stratifier received request: %s", buf);
	if (!strncasecmp(buf, "shutdown", 8))
		goto out;
	if (!strncasecmp(buf, "update", 6)) {
		update_base(ckp);
		goto reset;
	}

out:
	dealloc(buf);
	return ret;
}

int stratifier(proc_instance_t *pi)
{
	ckpool_t *ckp = pi->ckp;
	int ret = 0;

	strat_loop(ckp, pi);
	return ret;
}
