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

struct workbase {
	/* GBT/shared variables */
	char target[68];
	double diff;
	uint32_t version;
	uint32_t curtime;
	char prevhash[68];
	char ntime[12];
	char bbversion[12];
	char nbit[12];
	int coinbasevalue;
	int height;
	int transactions;
	char *txn_data;
	int merkles;
	char merklehash[16][68];

	/* Work variables */
};

typedef struct workbase workbase_t;

/* No error checking with these, make sure we know they're valid already! */
static inline void json_strcpy(char *buf, json_t *val, const char *key)
{
	strcpy(buf, json_string_value(json_object_get(val, key)));
}

static inline void json_dblcpy(double *dbl, json_t *val, const char *key)
{
	*dbl = json_real_value(json_object_get(val, key));
}

static inline void json_uintcpy(uint32_t *u32, json_t *val, const char *key)
{
	*u32 = (uint32_t)json_integer_value(json_object_get(val, key));
}

static inline void json_intcpy(int *i, json_t *val, const char *key)
{
	*i = json_integer_value(json_object_get(val, key));
}

static inline void json_strdup(char **buf, json_t *val, const char *key)
{
	*buf = strdup(json_string_value(json_object_get(val, key)));
}

/* This function assumes it will only receive a valid json gbt base template
 * since checking should have been done earlier, and creates the base template
 * for generating work templates. */
static void update_base(ckpool_t *ckp)
{
	workbase_t wb;
	json_t *val;
	char *buf;

	memset(&wb, 0, sizeof(wb));
	buf = send_recv_proc(&ckp->generator, "getbase");
	if (unlikely(!buf)) {
		LOGWARNING("Failed to get base from generator in update_base");
		return;
	}
	val = json_loads(buf, 0, NULL);
	dealloc(buf);

	json_strcpy(wb.target, val, "target");
	json_dblcpy(&wb.diff, val, "diff");
	json_uintcpy(&wb.version, val, "version");
	json_uintcpy(&wb.curtime, val, "curtime");
	json_strcpy(wb.prevhash, val, "prevhash");
	json_strcpy(wb.ntime, val, "ntime");
	json_strcpy(wb.bbversion, val, "bbversion");
	json_strcpy(wb.nbit, val, "nbit");
	json_intcpy(&wb.coinbasevalue, val, "coinbasevalue");
	json_intcpy(&wb.height, val, "height");
	json_intcpy(&wb.transactions, val, "transactions");
	if (wb.transactions)
		json_strdup(&wb.txn_data, val, "txn_data");
	json_intcpy(&wb.merkles, val, "merkles");
	if (wb.merkles) {
		json_t *arr;
		int i;

		arr = json_object_get(val, "merklehash");
		for (i = 0; i < wb.merkles; i++)
			strcpy(&wb.merklehash[i][0], json_string_value(json_array_get(arr, i)));
	}
	json_decref(val);
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
