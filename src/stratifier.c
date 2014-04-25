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
#include "uthash.h"
#include "utlist.h"

static const char *workpadding = "000000800000000000000000000000000000000000000000000000000000000000000000000000000000000080020000";

static const char *scriptsig_header = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff";
static uchar scriptsig_header_bin[41];

static char pubkeytxnbin[25];
static char pubkeytxn[52];

static uint64_t enonce1_64;

struct workbase {
	/* Hash table data */
	UT_hash_handle hh;
	int id;

	time_t gentime;

	/* GBT/shared variables */
	char target[68];
	double diff;
	uint32_t version;
	uint32_t curtime;
	char prevhash[68];
	char ntime[12];
	char bbversion[12];
	char nbit[12];
	uint64_t coinbasevalue;
	int height;
	char *flags;
	int transactions;
	char *txn_data;
	int merkles;
	char merklehash[16][68];

	/* Template variables, lengths are binary lengths! */
	char coinb1[256]; // coinbase1
	uchar coinb1bin[128];
	int coinb1len; // length of above

	char enonce1const[32]; // extranonce1 section that is constant
	uchar enonce1constbin[16];
	int enonce1constlen; // length of above - usually zero unless proxying
	int enonce1varlen; // length of unique extranonce1 string for each worker - usually 8

	char enonce2const[16]; // extranonce2 section that is constant
	uchar enonce2constbin[8];
	int enonce2constlen; // length of above - usually zero unless proxying
	int enonce2varlen; // length of space left for extranonce2 - usually 8

	char coinb2[128]; // coinbase2
	uchar coinb2bin[64];
	int coinb2len; // length of above

	/* Cached header binary */
	char headerbin[112];
};

typedef struct workbase workbase_t;

/* For protecting the hashtable data */
static cklock_t workbase_lock;

/* For the hashtable of all workbases */
static workbase_t *workbases;
static int workbase_id;
static char lasthash[68];

struct stratum_msg {
	struct stratum_msg *next;
	struct stratum_msg *prev;

	json_t *json_msg;
	int client_id;
};

typedef struct stratum_msg stratum_msg_t;

/* For protecting the stratum msg data */
static pthread_mutex_t stratum_recv_lock;
static pthread_mutex_t stratum_send_lock;

/* For signalling the threads to wake up and do work */
static pthread_cond_t stratum_recv_cond;
static pthread_cond_t stratum_send_cond;

/* For the linked list of all queued messages */
static stratum_msg_t *stratum_recvs;
static stratum_msg_t *stratum_sends;

/* Per client stratum instance, to be further expanded */
struct stratum_instance {
	UT_hash_handle hh;
	int id;
	char enonce1[20];
	double diff;
	bool authorised;
	char *packagever;
};

typedef struct stratum_instance stratum_instance_t;

static stratum_instance_t *stratum_instances;

static cklock_t instance_lock;

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

static inline void json_uint64cpy(uint64_t *u64, json_t *val, const char *key)
{
	*u64 = (uint64_t)json_integer_value(json_object_get(val, key));
}
static inline void json_intcpy(int *i, json_t *val, const char *key)
{
	*i = json_integer_value(json_object_get(val, key));
}

static inline void json_strdup(char **buf, json_t *val, const char *key)
{
	*buf = strdup(json_string_value(json_object_get(val, key)));
}

static void generate_coinbase(ckpool_t *ckp, workbase_t *wb)
{
	char header[228];
	int len, ofs = 0;
	uint64_t *u64;
	tv_t now;

	/* Strings in wb should have been zero memset prior. Generate binary
	 * templates first, then convert to hex */
	memcpy(wb->coinb1bin, scriptsig_header_bin, 41);
	ofs += 41; // Fixed header length;

	ofs++; // Script length is filled in at the end @wb->coinb1bin[41];

	/* Put block height at start of template */
	ofs += ser_number(wb->coinb1bin + ofs, wb->height);

	/* Followed by flag */
	len = strlen(wb->flags) / 2;
	wb->coinb1bin[ofs++] = len;
	hex2bin(wb->coinb1bin + ofs, wb->flags, len);
	ofs += len;

	/* Followed by timestamp */
	tv_time(&now);
	ofs += ser_number(wb->coinb1bin + ofs, now.tv_sec);

	wb->coinb1len = ofs;

	len = wb->coinb1len - 41;

	/* Leave enonce1/2varlen constant at 8 bytes for bitcoind sources */
	wb->enonce1varlen = 8;
	len += wb->enonce1varlen;
	wb->enonce2varlen = 8;
	len += wb->enonce2varlen;

	memcpy(wb->coinb2bin, "\x0a\x2f\x63\x6b\x70\x6f\x6f\x6c\x34\x32\x2f", 11);
	wb->coinb2len = 11;
	if (ckp->btcsig) {
		int siglen = strlen(ckp->btcsig);

		LOGDEBUG("Len %d sig %s", siglen, ckp->btcsig);
		if (siglen) {
			wb->coinb2bin[wb->coinb2len++] = siglen;
			memcpy(wb->coinb2bin + wb->coinb2len, ckp->btcsig, siglen);
			wb->coinb2len += siglen;
		}
	}
	len += wb->coinb2len;

	wb->coinb1bin[41] = len; /* Set the length now */
	__bin2hex(wb->coinb1, wb->coinb1bin, wb->coinb1len);
	LOGDEBUG("Coinb1: %s", wb->coinb1);
	/* Coinbase 1 complete */

	memcpy(wb->coinb2bin + wb->coinb2len, "\xff\xff\xff\xff", 4);
	wb->coinb2len += 4;

	wb->coinb2bin[wb->coinb2len++] = 1;
	u64 = (uint64_t *)&wb->coinb2bin[wb->coinb2len];
	*u64 = htole64(wb->coinbasevalue);
	wb->coinb2len += 8;

	wb->coinb2bin[wb->coinb2len++] = 25;

	memcpy(wb->coinb2bin + wb->coinb2len, pubkeytxnbin, 25);
	wb->coinb2len += 25;

	wb->coinb2len += 4; // Blank lock

	__bin2hex(wb->coinb2, wb->coinb2bin, wb->coinb2len);
	LOGDEBUG("Coinb2: %s", wb->coinb2);
	/* Coinbase 2 complete */

	snprintf(header, 225, "%08x%s%s%s%s%s%s",
		 wb->version, wb->prevhash,
		 "0000000000000000000000000000000000000000000000000000000000000000",
		 wb->ntime, wb->nbit,
		 "00000000", /* nonce */
		 workpadding);
	LOGDEBUG("Header: %s", header);
	hex2bin(wb->headerbin, header, 112);
}

/* This function assumes it will only receive a valid json gbt base template
 * since checking should have been done earlier, and creates the base template
 * for generating work templates. */
static void update_base(ckpool_t *ckp)
{
	workbase_t *wb, *tmp, *tmpa;
	bool new_block = false;
	json_t *val;
	char *buf;

	wb = ckzalloc(sizeof(workbase_t));
	buf = send_recv_proc(&ckp->generator, "getbase");
	if (unlikely(!buf)) {
		LOGWARNING("Failed to get base from generator in update_base");
		return;
	}
	val = json_loads(buf, 0, NULL);
	dealloc(buf);

	json_strcpy(wb->target, val, "target");
	json_dblcpy(&wb->diff, val, "diff");
	json_uintcpy(&wb->version, val, "version");
	json_uintcpy(&wb->curtime, val, "curtime");
	json_strcpy(wb->prevhash, val, "prevhash");
	json_strcpy(wb->ntime, val, "ntime");
	json_strcpy(wb->bbversion, val, "bbversion");
	json_strcpy(wb->nbit, val, "nbit");
	json_uint64cpy(&wb->coinbasevalue, val, "coinbasevalue");
	json_intcpy(&wb->height, val, "height");
	json_strdup(&wb->flags, val, "flags");
	json_intcpy(&wb->transactions, val, "transactions");
	if (wb->transactions)
		json_strdup(&wb->txn_data, val, "txn_data");
	json_intcpy(&wb->merkles, val, "merkles");
	if (wb->merkles) {
		json_t *arr;
		int i;

		arr = json_object_get(val, "merklehash");
		for (i = 0; i < wb->merkles; i++)
			strcpy(&wb->merklehash[i][0], json_string_value(json_array_get(arr, i)));
	}
	json_decref(val);
	generate_coinbase(ckp, wb);
	wb->gentime = time(NULL);

	ck_wlock(&workbase_lock);
	if (strncmp(wb->prevhash, lasthash, 64)) {
		new_block = true;
		memcpy(lasthash, wb->prevhash, 65);
	}
	wb->id = workbase_id++;
	HASH_ITER(hh, workbases, tmp, tmpa) {
		/*  Age old workbases older than 10 minutes old */
		if (tmp->gentime < wb->gentime - 600)
			HASH_DEL(workbases, tmp);
	}
	HASH_ADD_INT(workbases, id, wb);
	ck_wunlock(&workbase_lock);
}

/* Enter with instance_lock held */
static stratum_instance_t *__instance_by_id(int id)
{
	stratum_instance_t *instance;

	HASH_FIND_INT(stratum_instances, &id, instance);
	return instance;
}

/* Enter with write instance_lock held */
static stratum_instance_t *__stratum_add_instance(int id)
{
	stratum_instance_t *instance = ckzalloc(sizeof(stratum_instance_t));

	sprintf(instance->enonce1, "%016lx", enonce1_64++);
	instance->id = id;
	instance->diff = 1.0;
	LOGDEBUG("Added instance %d with enonce1 %s", id, instance->enonce1);
	HASH_ADD_INT(stratum_instances, id, instance);
	return instance;
}

static void stratum_add_recvd(json_t *val)
{
	stratum_msg_t *msg;

	msg = ckzalloc(sizeof(stratum_msg_t));
	msg->json_msg = val;

	mutex_lock(&stratum_recv_lock);
	LL_APPEND(stratum_recvs, msg);
	pthread_cond_signal(&stratum_recv_cond);
	mutex_unlock(&stratum_recv_lock);
}

static void stratum_add_send(json_t *val, int client_id)
{
	stratum_msg_t *msg;

	msg = ckzalloc(sizeof(stratum_msg_t));
	msg->json_msg = val;
	msg->client_id = client_id;

	mutex_lock(&stratum_send_lock);
	LL_APPEND(stratum_sends, msg);
	pthread_cond_signal(&stratum_send_cond);
	mutex_unlock(&stratum_send_lock);
}

static int strat_loop(ckpool_t *ckp, proc_instance_t *pi)
{
	int sockd, ret = 0, selret;
	unixsock_t *us = &pi->us;
	char *buf = NULL;
	fd_set readfds;
	tv_t timeout;

reset:
	timeout.tv_sec = ckp->update_interval;
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
		LOGDEBUG("%ds elapsed in strat_loop, updating gbt base", ckp->update_interval);
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
	else if (!strncasecmp(buf, "update", 6)) {
		update_base(ckp);
		goto reset;
	} else {
		json_t *val = json_loads(buf, 0, NULL);

		if (!val) {
			LOGDEBUG("Received unrecognised message: %s", buf);
		}  else
			stratum_add_recvd(val);
		goto retry;
	}

out:
	dealloc(buf);
	return ret;
}

static void *blockupdate(void *arg)
{
	ckpool_t *ckp = (ckpool_t *)arg;
	char *buf = NULL, hash[68];
	char request[8];

	rename_proc("blockupdate");
	buf = send_recv_proc(&ckp->generator, "getbest");
	if (buf && strncasecmp(buf, "Failed", 6))
		sprintf(request, "getbest");
	else
		sprintf(request, "getlast");

	memset(hash, 0, 68);
	while (42) {
		dealloc(buf);
		buf = send_recv_proc(&ckp->generator, request);
		if (buf && strcmp(buf, hash) && strncasecmp(buf, "Failed", 6)) {
			strcpy(hash, buf);
			LOGINFO("Detected hash change to %s", hash);
			send_proc(&ckp->stratifier, "update");
		} else
			cksleep_ms(ckp->blockpoll);
	}
	return NULL;
}

static json_t *gen_json_result(int client_id, json_t *method, json_t *params)
{
	return json_string("Empty");
}

static void parse_instance_msg(int client_id, json_t *msg)
{
	json_t *result_val = NULL, *err_val = NULL, *id_val = NULL;
	json_t *method, *msg_id, *params;
	json_t *json_msg;

	json_msg = json_object();
	id_val = json_object_get(msg, "id");
	if (unlikely(!id_val)) {
		err_val = json_string("-1:id not found");
		goto out;
	}
	if (unlikely(!json_is_integer(id_val))) {
		err_val = json_string("-1:id is not integer");
		goto out;
	}
	method = json_object_get(msg, "method");
	if (unlikely(!method)) {
		err_val = json_string("-3:method not found");
		goto out;
	}
	if (unlikely(!json_is_string(method))) {
		err_val = json_string("-1:method is not string");
		goto out;
	}
	params = json_object_get(msg, "params");
	if (unlikely(!params)) {
		err_val = json_string("-1:params not found");
		goto out;
	}
	err_val = json_null();
	result_val = gen_json_result(client_id, method, params);
out:
	json_object_set_nocheck(json_msg, "id", id_val);
	json_object_set_nocheck(json_msg, "error", err_val);
	json_object_set_nocheck(json_msg, "result", result_val);

	stratum_add_send(json_msg, client_id);
}

static void *stratum_receiver(void *arg)
{
	ckpool_t *ckp = (ckpool_t *)arg;
	stratum_msg_t *msg;

	rename_proc("receiver");

	while (42) {
		stratum_instance_t *instance;

		/* Pop the head off the list if it exists or wait for a conditional
		* signal telling us there is work */
		mutex_lock(&stratum_recv_lock);
		if (!stratum_recvs)
			pthread_cond_wait(&stratum_recv_cond, &stratum_recv_lock);
		msg = stratum_recvs;
		if (likely(msg))
			LL_DELETE(stratum_recvs, msg);
		mutex_unlock(&stratum_recv_lock);

		if (unlikely(!msg))
			continue;

		msg->client_id = json_integer_value(json_object_get(msg->json_msg, "client_id"));
		json_object_del(msg->json_msg, "client_id");

		/* Parse the message here */
		ck_ilock(&instance_lock);
		instance = __instance_by_id(msg->client_id);
		if (!instance) {
			/* client_id instance doesn't exist yet, create one */
			ck_ulock(&instance_lock);
			instance = __stratum_add_instance(msg->client_id);
			ck_dwilock(&instance_lock);
		}
		ck_uilock(&instance_lock);

		parse_instance_msg(msg->client_id, msg->json_msg);

		json_decref(msg->json_msg);
		free(msg);
	}

	return NULL;
}

static void *stratum_sender(void *arg)
{
	ckpool_t *ckp = (ckpool_t *)arg;

	rename_proc("sender");

	while (42) {
		stratum_msg_t *msg;
		char *s;

		mutex_lock(&stratum_send_lock);
		if (!stratum_sends)
			pthread_cond_wait(&stratum_send_cond, &stratum_send_lock);
		msg = stratum_sends;
		if (likely(msg))
			LL_DELETE(stratum_sends, msg);
		mutex_unlock(&stratum_send_lock);

		if (unlikely(!msg))
			continue;

		/* Add client_id to the json message and send it to the
		 * connector process to be delivered */
		json_object_set_new_nocheck(msg->json_msg, "client_id", json_integer(msg->client_id));
		s = json_dumps(msg->json_msg, 0);
		send_proc(&ckp->connector, s);
		free(s);

		json_decref(msg->json_msg);
		free(msg);
	}

	return NULL;
}

int stratifier(proc_instance_t *pi)
{
	pthread_t pth_blockupdate, pth_stratum_receiver, pth_stratum_sender;
	ckpool_t *ckp = pi->ckp;
	int ret = 0;

	/* Store this for use elsewhere */
	hex2bin(scriptsig_header_bin, scriptsig_header, 41);
	address_to_pubkeytxn(pubkeytxnbin, ckp->btcaddress);
	__bin2hex(pubkeytxn, pubkeytxnbin, 25);

	cklock_init(&instance_lock);

	mutex_init(&stratum_recv_lock);
	cond_init(&stratum_recv_cond);
	create_pthread(&pth_stratum_receiver, stratum_receiver, ckp);

	mutex_init(&stratum_send_lock);
	cond_init(&stratum_send_cond);
	create_pthread(&pth_stratum_sender, stratum_sender, ckp);

	cklock_init(&workbase_lock);
	create_pthread(&pth_blockupdate, blockupdate, ckp);

	strat_loop(ckp, pi);

	return ret;
}
