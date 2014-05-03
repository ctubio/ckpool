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
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <math.h>
#include <string.h>
#include <unistd.h>

#include "ckpool.h"
#include "libckpool.h"
#include "bitcoin.h"
#include "uthash.h"
#include "utlist.h"
#include "sha2.h"

static const char *workpadding = "000000800000000000000000000000000000000000000000000000000000000000000000000000000000000080020000";

static const char *scriptsig_header = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff";
static uchar scriptsig_header_bin[41];

static char pubkeytxnbin[25];
static char pubkeytxn[52];

/* Add unaccounted shares when they arrive, remove them with each update of
 * rolling stats. */
struct pool_stats {
	tv_t start_time;
	ts_t last_update;

	int live_clients;
	int dead_clients;
	int reused_clients;
	int reusable_clients;

	/* Absolute shares stats */
	int64_t unaccounted_shares;
	int64_t accounted_shares;

	/* Shares per second for 1/5/15/60 minute rolling averages */
	double sps1;
	double sps5;
	double sps15;
	double sps60;

	/* Diff shares stats */
	int64_t unaccounted_diff_shares;
	int64_t accounted_diff_shares;

	/* Diff shares per second for 1/5/15... minute rolling averages */
	double dsps1;
	double dsps5;
	double dsps15;
	double dsps60;
	double dsps360;
	double dsps1440;
};

typedef struct pool_stats pool_stats_t;

static pool_stats_t stats;

static pthread_mutex_t stats_lock;

static uint64_t enonce1_64;

struct workbase {
	/* Hash table data */
	UT_hash_handle hh;
	uint64_t id;
	char idstring[20];

	time_t gentime;

	/* GBT/shared variables */
	char target[68];
	double diff;
	uint32_t version;
	uint32_t curtime;
	char prevhash[68];
	char ntime[12];
	uint32_t ntime32;
	char bbversion[12];
	char nbit[12];
	uint64_t coinbasevalue;
	int height;
	char *flags;
	int transactions;
	char *txn_data;
	int merkles;
	char merklehash[16][68];
	char merklebin[16][32];
	json_t *merkle_array;

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

	char *logdir;
};

typedef struct workbase workbase_t;

/* For protecting the hashtable data */
static cklock_t workbase_lock;

/* For the hashtable of all workbases */
static workbase_t *workbases;
static workbase_t *current_workbase;
static uint64_t workbase_id;
static uint64_t blockchange_id;
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
	uint64_t enonce1_64;

	int diff; /* Current diff */
	int old_diff; /* Previous diff */
	uint64_t diff_change_job_id; /* Last job_id we changed diff */
	double dsps5; /* Diff shares per second, 5 minute rolling average */
	tv_t ldc; /* Last diff change */
	int ssdc; /* Shares since diff change */
	tv_t first_share;
	tv_t last_share;
	int absolute_shares;
	int diff_shares;

	bool authorised;

	char *useragent;
	char *workername;
	int user_id;
};

typedef struct stratum_instance stratum_instance_t;

/* Stratum_instances hashlist is stored by id, whereas disconnected_instances
 * is sorted by enonce1_64. */
static stratum_instance_t *stratum_instances;
static stratum_instance_t *disconnected_instances;

static cklock_t instance_lock;

struct share {
	UT_hash_handle hh;
	uchar hash[32];
	uint64_t workbase_id;
};

typedef struct share share_t;

static share_t *shares;

static cklock_t share_lock;

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

static inline void json_set_string(json_t *val, const char *key, const char *str)
{
	json_object_set_nocheck(val, key, json_string(str));
}

static inline void json_set_int(json_t *val, const char *key, int integer)
{
	json_object_set_new_nocheck(val, key, json_integer(integer));
}

static inline void json_set_double(json_t *val, const char *key, double real)
{
	json_object_set_new_nocheck(val, key, json_real(real));
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

	/* Leave enonce1/2varlen constant at 8 bytes for bitcoind sources */
	wb->enonce1varlen = 8;
	wb->enonce2varlen = 8;
	wb->coinb1bin[ofs++] = wb->enonce1varlen + wb->enonce2varlen;

	wb->coinb1len = ofs;

	len = wb->coinb1len - 41;

	len += wb->enonce1varlen;
	len += wb->enonce2varlen;

	memcpy(wb->coinb2bin, "\x0a\x63\x6b\x70\x6f\x6f\x6c", 7);
	wb->coinb2len = 7;
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

	wb->coinb1bin[41] = len - 1; /* Set the length now */
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

	snprintf(header, 225, "%s%s%s%s%s%s%s",
		 wb->bbversion, wb->prevhash,
		 "0000000000000000000000000000000000000000000000000000000000000000",
		 wb->ntime, wb->nbit,
		 "00000000", /* nonce */
		 workpadding);
	LOGDEBUG("Header: %s", header);
	hex2bin(wb->headerbin, header, 112);
}

static void stratum_broadcast_update(bool clean);

static void clear_workbase(workbase_t *wb)
{
	free(wb->flags);
	free(wb->txn_data);
	free(wb->logdir);
	json_decref(wb->merkle_array);
	free(wb);
}

static void purge_share_hashtable(uint64_t wb_id)
{
	share_t *share, *tmp;
	int purged = 0;

	ck_wlock(&share_lock);
	HASH_ITER(hh, shares, share, tmp) {
		if (share->workbase_id < wb_id) {
			HASH_DEL(shares, share);
			free(share);
			purged++;
		}
	}
	ck_wunlock(&share_lock);

	if (purged)
		LOGINFO("Cleared %d shares from share hashtable", purged);
}

/* This function assumes it will only receive a valid json gbt base template
 * since checking should have been done earlier, and creates the base template
 * for generating work templates. */
static void update_base(ckpool_t *ckp)
{
	workbase_t *wb, *tmp, *tmpa;
	bool new_block = false;
	int len, ret;
	json_t *val;
	char *buf;

	buf = send_recv_proc(&ckp->generator, "getbase");
	if (unlikely(!buf)) {
		LOGWARNING("Failed to get base from generator in update_base");
		return;
	}
	if (unlikely(!strncasecmp(buf, "failed", 6))) {
		LOGWARNING("Generator returned failure in update_base");
		return;
	}

	wb = ckzalloc(sizeof(workbase_t));
	val = json_loads(buf, 0, NULL);
	dealloc(buf);

	json_strcpy(wb->target, val, "target");
	json_dblcpy(&wb->diff, val, "diff");
	json_uintcpy(&wb->version, val, "version");
	json_uintcpy(&wb->curtime, val, "curtime");
	json_strcpy(wb->prevhash, val, "prevhash");
	json_strcpy(wb->ntime, val, "ntime");
	sscanf(wb->ntime, "%x", &wb->ntime32);
	json_strcpy(wb->bbversion, val, "bbversion");
	json_strcpy(wb->nbit, val, "nbit");
	json_uint64cpy(&wb->coinbasevalue, val, "coinbasevalue");
	json_intcpy(&wb->height, val, "height");
	json_strdup(&wb->flags, val, "flags");
	json_intcpy(&wb->transactions, val, "transactions");
	if (wb->transactions)
		json_strdup(&wb->txn_data, val, "txn_data");
	json_intcpy(&wb->merkles, val, "merkles");
	wb->merkle_array = json_array();
	if (wb->merkles) {
		json_t *arr;
		int i;

		arr = json_object_get(val, "merklehash");
		for (i = 0; i < wb->merkles; i++) {
			strcpy(&wb->merklehash[i][0], json_string_value(json_array_get(arr, i)));
			hex2bin(&wb->merklebin[i][0], &wb->merklehash[i][0], 32);
			json_array_append(wb->merkle_array, json_string(&wb->merklehash[i][0]));
		}
	}
	json_decref(val);
	generate_coinbase(ckp, wb);
	wb->gentime = time(NULL);

	len = strlen(ckp->logdir) + 8 + 1 + 16;
	wb->logdir = ckalloc(len);

	ck_wlock(&workbase_lock);
	wb->id = workbase_id++;

	if (strncmp(wb->prevhash, lasthash, 64)) {
		new_block = true;
		memcpy(lasthash, wb->prevhash, 65);
		blockchange_id = wb->id;
	}
	if (new_block) {
		sprintf(wb->logdir, "%s%08x/", ckp->logdir, wb->height);
		ret = mkdir(wb->logdir, 0700);
		if (unlikely(ret && errno != EEXIST))
			quit(1, "Failed to create log directory %s", wb->logdir);
	}
	sprintf(wb->idstring, "%016lx", wb->id);
	/* Do not store the trailing slash for the subdir */
	sprintf(wb->logdir, "%s%08x/%s", ckp->logdir, wb->height, wb->idstring);
	ret = mkdir(wb->logdir, 0700);
	if (unlikely(ret && errno != EEXIST))
		quit(1, "Failed to create log directory %s", wb->logdir);
	HASH_ITER(hh, workbases, tmp, tmpa) {
		if (HASH_COUNT(workbases) < 3)
			break;
		/*  Age old workbases older than 10 minutes old */
		if (tmp->gentime < wb->gentime - 600) {
			HASH_DEL(workbases, tmp);
			clear_workbase(tmp);
		}
	}
	HASH_ADD_INT(workbases, id, wb);
	current_workbase = wb;
	ck_wunlock(&workbase_lock);

	if (new_block)
		purge_share_hashtable(wb->id);

	stratum_broadcast_update(new_block);
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

	stats.live_clients++;
	instance->id = id;
	instance->diff = instance->old_diff = global_ckp->startdiff;
	tv_time(&instance->ldc);
	LOGINFO("Added instance %d", id);
	HASH_ADD_INT(stratum_instances, id, instance);
	return instance;
}

static bool disconnected_sessionid_exists(const char *sessionid, int id)
{
	bool connected_exists = false, ret = false;
	stratum_instance_t *instance, *tmp;
	uint64_t session64;

	if (!sessionid)
		goto out;
	if (strlen(sessionid) != 16)
		goto out;
	/* Number is in BE but we don't swap either of them */
	hex2bin(&session64, sessionid, 8);

	ck_rlock(&instance_lock);
	HASH_ITER(hh, stratum_instances, instance, tmp) {
		if (instance->id == id)
			continue;
		if (instance->enonce1_64 == session64) {
			/* Only allow one connected instance per enonce1 */
			connected_exists = true;
			break;
		}
	}
	if (connected_exists)
		goto out_unlock;
	instance = NULL;
	HASH_FIND(hh, disconnected_instances, &session64, sizeof(uint64_t), instance);
	if (instance)
		ret = true;
out_unlock:
	ck_runlock(&instance_lock);
out:
	return ret;
}

static void stratum_add_recvd(json_t *val)
{
	stratum_msg_t *msg;

	msg = ckzalloc(sizeof(stratum_msg_t));
	msg->json_msg = val;

	mutex_lock(&stratum_recv_lock);
	DL_APPEND(stratum_recvs, msg);
	pthread_cond_signal(&stratum_recv_cond);
	mutex_unlock(&stratum_recv_lock);
}

/* For creating a list of sends without locking that can then be concatenated
 * to the stratum_sends list. Minimises locking and avoids taking recursive
 * locks. */
static void stratum_broadcast(json_t *val)
{
	stratum_instance_t *instance, *tmp;
	stratum_msg_t *bulk_send = NULL;

	if (unlikely(!val)) {
		LOGERR("Sent null json to stratum_broadcast");
		return;
	}

	ck_rlock(&instance_lock);
	HASH_ITER(hh, stratum_instances, instance, tmp) {
		stratum_msg_t *msg;

		if (!instance->authorised)
			continue;
		msg = ckzalloc(sizeof(stratum_msg_t));
		msg->json_msg = json_deep_copy(val);
		msg->client_id = instance->id;
		DL_APPEND(bulk_send, msg);
	}
	ck_runlock(&instance_lock);

	json_decref(val);

	if (!bulk_send)
		return;

	mutex_lock(&stratum_send_lock);
	if (stratum_sends)
		DL_CONCAT(stratum_sends, bulk_send);
	else
		stratum_sends = bulk_send;
	pthread_cond_signal(&stratum_send_cond);
	mutex_unlock(&stratum_send_lock);
}

static void stratum_add_send(json_t *val, int client_id)
{
	stratum_msg_t *msg;

	msg = ckzalloc(sizeof(stratum_msg_t));
	msg->json_msg = val;
	msg->client_id = client_id;

	mutex_lock(&stratum_send_lock);
	DL_APPEND(stratum_sends, msg);
	pthread_cond_signal(&stratum_send_cond);
	mutex_unlock(&stratum_send_lock);
}

static void drop_client(int id)
{
	stratum_instance_t *client = NULL;

	ck_ilock(&instance_lock);
	client = __instance_by_id(id);
	if (client) {
		stratum_instance_t *old_client = NULL;

		stats.live_clients--;
		stats.dead_clients++;

		ck_ulock(&instance_lock);
		HASH_DEL(stratum_instances, client);
		HASH_FIND(hh, disconnected_instances, &client->enonce1_64, sizeof(uint64_t), old_client);
		/* Only keep around one copy of the old client */
		if (!old_client) {
			stats.reusable_clients++;
			HASH_ADD(hh, disconnected_instances, enonce1_64, sizeof(uint64_t), client);
		} else
			free(client);
		ck_dwilock(&instance_lock);
	}
	ck_uilock(&instance_lock);
}

static int stratum_loop(ckpool_t *ckp, proc_instance_t *pi)
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
		LOGERR("Select failed in strat_loop, killing stratifier!");
		sleep(5);
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
		LOGERR("Failed to accept on stratifier socket, retrying in 5s");
		sleep(5);
		goto retry;
	}

	dealloc(buf);
	buf = recv_unix_msg(sockd);
	close(sockd);
	if (!buf) {
		LOGWARNING("Failed to get message in strat_loop");
		goto retry;
	}
	LOGDEBUG("Stratifier received request: %s", buf);
	if (!strncasecmp(buf, "shutdown", 8)) {
		ret = 0;
		goto out;
	} else if (!strncasecmp(buf, "update", 6)) {
		update_base(ckp);
		goto reset;
	} else if (!strncasecmp(buf, "dropclient", 10)) {
		int client_id;

		ret = sscanf(buf, "dropclient=%d", &client_id);
		if (ret < 0)
			LOGDEBUG("Failed to parse dropclient command: %s", buf);
		else
			drop_client(client_id);
		goto retry;
	} else {
		json_t *val = json_loads(buf, 0, NULL);

		if (!val) {
			LOGWARNING("Received unrecognised message: %s", buf);
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
			LOGNOTICE("Block hash changed to %s", hash);
			send_proc(&ckp->stratifier, "update");
		} else
			cksleep_ms(ckp->blockpoll);
	}
	return NULL;
}

/* Extranonce1 must be set here */
static json_t *parse_subscribe(int client_id, json_t *params_val)
{
	stratum_instance_t *client = NULL;
	bool old_match = false;
	int arr_size;
	json_t *ret;
	int n2len;

	if (unlikely(!json_is_array(params_val)))
		return json_string("params not an array");

	ck_rlock(&instance_lock);
	client = __instance_by_id(client_id);
	ck_runlock(&instance_lock);

	if (unlikely(!client)) {
		LOGERR("Failed to find client id %d in hashtable!", client_id);
		return NULL;
	}

	arr_size = json_array_size(params_val);
	if (arr_size > 0) {
		const char *buf;

		buf = json_string_value(json_array_get(params_val, 0));
		if (buf && strlen(buf))
			client->useragent = strdup(buf);
		if (arr_size > 1) {
			/* This would be the session id for reconnect */
			buf = json_string_value(json_array_get(params_val, 1));
			LOGDEBUG("Found old session id %s", buf);
			/* Add matching here */
			if (disconnected_sessionid_exists(buf, client_id)) {
				hex2bin(&client->enonce1_64, buf, 8);
				strcpy(client->enonce1, buf);
				old_match = true;
				stats.reused_clients++;
			}
		}
	}
	if (!old_match) {
		/* Create a new extranonce1 based on non-endian swapped
		 * uint64_t pointer */
		ck_wlock(&instance_lock);
		client->enonce1_64 = enonce1_64++;
		ck_wunlock(&instance_lock);

		__bin2hex(client->enonce1, &client->enonce1_64, 8);
		LOGINFO("Set new subscription %d to new enonce1 %s", client->id,
			client->enonce1);
	} else {
		LOGINFO("Set new subscription %d to old matched enonce1 %s", client->id,
			 client->enonce1);
	}

	ck_rlock(&workbase_lock);
	if (likely(workbases))
		n2len = workbases->enonce2varlen;
	else
		n2len = 8;
	ret = json_pack("[[[s,s]],s,i]", "mining.notify", client->enonce1, client->enonce1,
			n2len);
	ck_runlock(&workbase_lock);

	return ret;
}

static int authorise_user(const char __maybe_unused *workername)
{
	/* Talk to database here and return user_id or -1 if invalid */
	return 1;
}

static json_t *parse_authorize(stratum_instance_t *client, json_t *params_val, json_t **err_val)
{
	int arr_size, user_id;
	bool ret = false;
	const char *buf;

	if (unlikely(!json_is_array(params_val))) {
		*err_val = json_string("params not an array");
		goto out;
	}
	arr_size = json_array_size(params_val);
	if (unlikely(arr_size < 1)) {
		*err_val = json_string("params missing array entries");
		goto out;
	}
	buf = json_string_value(json_array_get(params_val, 0));
	if (!buf) {
		*err_val = json_string("Invalid workername parameter");
		goto out;
	}
	if (!strlen(buf)) {
		*err_val = json_string("Empty workername parameter");
		goto out;
	}
	user_id = authorise_user(buf);
	if (user_id < 0) {
		*err_val = json_string("User not found");
		goto out;
	}
	LOGNOTICE("Authorised client %d as user %s", client->id, buf);
	client->workername = strdup(buf);
	client->user_id = user_id;
	client->authorised = true;
	ret = true;
out:
	return json_boolean(ret);
}

static void stratum_send_diff(stratum_instance_t *client)
{
	json_t *json_msg;

	json_msg = json_pack("{s[i]soss}", "params", client->diff, "id", json_null(),
			     "method", "mining.set_difficulty");
	stratum_add_send(json_msg, client->id);
}

static void stratum_send_message(stratum_instance_t *client, const char *msg)
{
	json_t *json_msg;

	json_msg = json_pack("{sosss[s]}", "id", json_null(), "method", "client.show_message",
			     "params", msg);
	stratum_add_send(json_msg, client->id);
}

static double time_bias(double tdiff, double period)
{
	return 1.0 - 1.0 / exp(tdiff / period);
}

static void add_submit(stratum_instance_t *client, int diff, bool valid)
{
	double tdiff, bdiff, dsps, drr, network_diff, bias;
	uint64_t next_blockid;
	int optimal;
	tv_t now_t;

	tv_time(&now_t);
	tdiff = tvdiff(&now_t, &client->last_share);
	copy_tv(&client->last_share, &now_t);
	client->ssdc++;
	decay_time(&client->dsps5, diff, tdiff, 300);
	bdiff = tvdiff(&now_t, &client->first_share);
	bias = time_bias(bdiff, 300);
	tdiff = tvdiff(&now_t, &client->ldc);

	if (valid) {
		mutex_lock(&stats_lock);
		stats.unaccounted_shares++;
		stats.unaccounted_diff_shares += diff;
		mutex_unlock(&stats_lock);
	}

	/* Check the difficulty every 240 seconds or as many shares as we
	 * should have had in that time, whichever comes first. */
	if (client->ssdc < 72 && tdiff < 240)
		return;

	if (diff != client->diff)
		return;

	/* Diff rate ratio */
	dsps = client->dsps5 / bias;
	drr = dsps / (double)client->diff;

	/* Optimal rate product is 0.3, allow some hysteresis. */
	if (drr > 0.2 && drr < 0.4) {
		/* FIXME For now show the hashrate every ~1m when the diff is
		 * stable */
		if (!(client->ssdc % 18)) {
			LOGNOTICE("Client %d worker %s hashrate %.1fGH/s", client->id,
				  client->workername, dsps * 4.294967296);
		}
		return;
	}

	optimal = round(dsps * 3.33);
	if (optimal <= global_ckp->mindiff) {
		if (client->diff == global_ckp->mindiff)
			return;
		optimal = global_ckp->mindiff;
	}

	ck_rlock(&workbase_lock);
	next_blockid = workbase_id + 1;
	network_diff = current_workbase->diff;
	ck_runlock(&workbase_lock);

	if (optimal > network_diff) {
		/* Intentionally round down here */
		optimal = network_diff;
		if (client->diff == optimal)
			return;
	}
	/* We have the effect of a change pending */
	if (client->diff_change_job_id >= next_blockid)
		return;

	client->ssdc = 0;

	LOGINFO("Client %d biased dsps %.2f dsps %.2f drr %.2f adjust diff from %d to: %d ",
		client->id, dsps, client->dsps5, drr, client->diff, optimal);

	copy_tv(&client->ldc, &now_t);
	client->diff_change_job_id = next_blockid;
	client->old_diff = client->diff;
	client->diff = optimal;
	stratum_send_diff(client);
}

static void add_submit_success(stratum_instance_t *client, const char *logdir,
			       const char *idstring, const char *nonce2,
			       const uint32_t ntime, const int diff,
			       double sdiff, const char *hash)
{
	char *fname, *s;
	json_t *val;
	FILE *fp;
	int len;

	if (unlikely(!client->absolute_shares++))
		tv_time(&client->first_share);
	client->diff_shares += diff;
	add_submit(client, diff, true);
	len = strlen(logdir) + strlen(client->workername) + 12;
	fname = alloca(len);

	/* First write to the user's sharelog */
	sprintf(fname, "%s/%s.sharelog", logdir, client->workername);
	fp = fopen(fname, "a");
	if (unlikely(!fp < 0)) {
		LOGERR("Failed to fopen %s", fname);
		return;
	}

	val = json_object();
	json_set_string(val, "wbid", idstring);
	json_set_string(val, "nonce2", nonce2);
	json_set_int(val, "ntime", ntime);
	json_set_int(val, "diff", diff);
	json_set_double(val, "sdiff", sdiff);
	json_set_string(val, "hash", hash);
	s = json_dumps(val, 0);
	len = strlen(s);
	len = fprintf(fp, "%s\n", s);
	free(s);
	fclose(fp);
	if (unlikely(len < 0))
		LOGERR("Failed to fwrite to %s", fname);

	/* Now write to the pool's sharelog, adding workername to json */
	sprintf(fname, "%s.sharelog", logdir);
	fp = fopen(fname, "a");
	if (unlikely(!fp < 0)) {
		LOGERR("Failed to fopen %s", fname);
		return;
	}
	json_set_string(val, "worker", client->workername);
	s = json_dumps(val, 0);
	len = strlen(s);
	len = fprintf(fp, "%s\n", s);
	free(s);
	fclose(fp);
	if (unlikely(len < 0))
		LOGERR("Failed to fwrite to %s", fname);

	json_decref(val);
}

static void add_submit_fail(stratum_instance_t *client, int diff, bool share)
{
	/* FIXME Do something else here? */
	if (share)
		add_submit(client, diff, false);
}

/* We should already be holding the workbase_lock */
static void test_blocksolve(workbase_t *wb, const uchar *data, double diff, const char *coinbase,
			    int cblen)
{
	int transactions = wb->transactions + 1;
	char *gbt_block, varint[12];
	char hexcoinbase[512];

	/* Submit anything over 95% of the diff in case of rounding errors */
	if (diff < current_workbase->diff * 0.95)
		return;

	gbt_block = ckalloc(512);
	sprintf(gbt_block, "submitblock:");
	__bin2hex(gbt_block + 12, data, 80);
	if (transactions < 0xfd) {
		uint8_t val8 = transactions;

		__bin2hex(varint, (const unsigned char *)&val8, 1);
	} else if (transactions <= 0xffff) {
		uint16_t val16 = htole16(transactions);

		strcat(gbt_block, "fd");
		__bin2hex(varint, (const unsigned char *)&val16, 2);
	} else {
		uint32_t val32 = htole32(transactions);

		strcat(gbt_block, "fe");
		__bin2hex(varint, (const unsigned char *)&val32, 4);
	}
	strcat(gbt_block, varint);
	__bin2hex(hexcoinbase, coinbase, cblen);
	strcat(gbt_block, hexcoinbase);
	if (wb->transactions)
		realloc_strcat(&gbt_block, wb->txn_data);
	send_proc(&global_ckp->generator, gbt_block);
	free(gbt_block);
}

static double submission_diff(stratum_instance_t *client, workbase_t *wb, const char *nonce2,
			      uint32_t ntime32, const char *nonce, uchar *hash)
{
	unsigned char merkle_root[32], merkle_sha[64];
	uint32_t *data32, *swap32, nonce32;
	char coinbase[256], data[80];
	uchar swap[80], hash1[32];
	int cblen = 0, i;
	double ret;

	memcpy(coinbase, wb->coinb1bin, wb->coinb1len);
	cblen += wb->coinb1len;
	memcpy(coinbase + cblen, &client->enonce1_64, wb->enonce1varlen);
	cblen += wb->enonce1varlen;
	hex2bin(coinbase + cblen, nonce2, wb->enonce2varlen);
	cblen += wb->enonce2varlen;
	memcpy(coinbase + cblen, wb->coinb2bin, wb->coinb2len);
	cblen += wb->coinb2len;

	gen_hash((uchar *)coinbase, merkle_root, cblen);
	memcpy(merkle_sha, merkle_root, 32);
	for (i = 0; i < wb->merkles; i++) {
		memcpy(merkle_sha + 32, &wb->merklebin[i], 32);
		gen_hash(merkle_sha, merkle_root, 64);
		memcpy(merkle_sha, merkle_root, 32);
	}
	data32 = (uint32_t *)merkle_sha;
	swap32 = (uint32_t *)merkle_root;
	flip_32(swap32, data32);

	/* Copy the cached header binary and insert the merkle root */
	memcpy(data, wb->headerbin, 80);
	memcpy(data + 36, merkle_root, 32);

	/* Insert the nonce value into the data */
	sscanf(nonce, "%x", &nonce32);
	data32 = (uint32_t *)(data + 64 + 12);
	*data32 = htobe32(nonce32);

	/* Insert the ntime value into the data */
	data32 = (uint32_t *)(data + 68);
	*data32 = htobe32(ntime32);

	/* Hash the share */
	data32 = (uint32_t *)data;
	swap32 = (uint32_t *)swap;
	flip_80(swap32, data32);
	sha256(swap, 80, hash1);
	sha256(hash1, 32, hash);

	/* Calculate the diff of the share here */
	ret = diff_from_target(hash);

	/* Test we haven't solved a block regardless of share status */
	test_blocksolve(wb, swap, ret, coinbase, cblen);

	/* FIXME: Log share here */
	return ret;
}

static bool new_share(const uchar *hash, uint64_t  wb_id)
{
	share_t *share, *match = NULL;
	bool ret = false;

	ck_ilock(&share_lock);
	HASH_FIND(hh, shares, hash, 32, match);
	if (match)
		goto out_unlock;
	share = ckzalloc(sizeof(share_t));
	memcpy(share->hash, hash, 32);
	share->workbase_id = wb_id;
	ck_ulock(&share_lock);
	HASH_ADD(hh, shares, hash, 32, share);
	ck_dwilock(&share_lock);
	ret = true;
out_unlock:
	ck_uilock(&share_lock);

	return ret;
}

static json_t *parse_submit(stratum_instance_t *client, json_t *json_msg,
			    json_t *params_val, json_t **err_val)
{
	const char *user, *job_id, *nonce2, *ntime, *nonce;
	char hexhash[68], sharehash[32], *logdir;
	bool share = false, ret = false;
	uint64_t wb_id = 0, id;
	double sdiff = -1;
	char idstring[20];
	uint32_t ntime32;
	workbase_t *wb;
	uchar hash[32];
	int diff;

	if (unlikely(!json_is_array(params_val))) {
		*err_val = json_string("params not an array");
		goto out;
	}
	if (unlikely(json_array_size(params_val) != 5)) {
		*err_val = json_string("Invalid array size");
		goto out;
	}
	user = json_string_value(json_array_get(params_val, 0));
	if (unlikely(!user || !strlen(user))) {
		*err_val = json_string("No username");
		goto out;
	}
	job_id = json_string_value(json_array_get(params_val, 1));
	if (unlikely(!job_id || !strlen(job_id))) {
		*err_val = json_string("No job_id");
		goto out;
	}
	nonce2 = json_string_value(json_array_get(params_val, 2));
	if (unlikely(!nonce2 || !strlen(nonce2))) {
		*err_val = json_string("No nonce2");
		goto out;
	}
	ntime = json_string_value(json_array_get(params_val, 3));
	if (unlikely(!ntime || !strlen(ntime))) {
		*err_val = json_string("No ntime");
		goto out;
	}
	nonce = json_string_value(json_array_get(params_val, 4));
	if (unlikely(!nonce || !strlen(nonce))) {
		*err_val = json_string("No nonce");
		goto out;
	}
	if (strcmp(user, client->workername)) {
		*err_val = json_string("Worker mismatch");
		goto out;
	}
	sscanf(job_id, "%lx", &id);
	sscanf(ntime, "%x", &ntime32);

	/* Decide whether this submit request should count towards share diff
	 * management or not. */
	share = true;

	ck_rlock(&workbase_lock);
	HASH_FIND_INT(workbases, &id, wb);
	if (unlikely(!wb)) {
		json_object_set_nocheck(json_msg, "reject-reason", json_string("Invalid JobID"));
		goto out_unlock;
	}
	if (id < blockchange_id) {
		json_object_set_nocheck(json_msg, "reject-reason", json_string("Stale"));
		goto out_unlock;
	}
	if ((int)strlen(nonce2) != wb->enonce2varlen * 2) {
		*err_val = json_string("Invalid nonce2 length");
		goto out_unlock;
	}
	/* Ntime cannot be less, but allow forward ntime rolling up to max */
	if (ntime32 < wb->ntime32 || ntime32 > wb->ntime32 + 7000) {
		json_object_set_nocheck(json_msg, "reject-reason", json_string("Ntime out of range"));
		goto out_unlock;
	}
	sdiff = submission_diff(client, wb, nonce2, ntime32, nonce, hash);
	wb_id = wb->id;
	strcpy(idstring, wb->idstring);
	logdir = strdupa(wb->logdir);
out_unlock:
	ck_runlock(&workbase_lock);

	/* Accept the lower of new and old diffs until the next update */
	if (id < client->diff_change_job_id && client->old_diff < client->diff)
		diff = client->old_diff;
	else
		diff = client->diff;
	if (sdiff >= diff) {
		bswap_256(sharehash, hash);
		__bin2hex(hexhash, sharehash, 32);
		if (new_share(hash, wb_id)) {
			LOGINFO("Accepted client %d share diff %.1f/%d: %s", client->id, sdiff, diff, hexhash);
			add_submit_success(client, logdir, idstring, nonce2, ntime32, diff, sdiff, hexhash);
			ret = true;
		} else {
			add_submit_fail(client, diff, share);
			json_object_set_nocheck(json_msg, "reject-reason", json_string("Duplicate"));
			LOGINFO("Rejected client %d dupe diff %.1f/%d: %s", client->id, sdiff, diff, hexhash);
		}
	} else {
		add_submit_fail(client, diff, share);
		if (sdiff >= 0) {
			bswap_256(sharehash, hash);
			__bin2hex(hexhash, sharehash, 32);
			LOGINFO("Rejected client %d high diff %.1f/%d: %s", client->id, sdiff, diff, hexhash);
			json_object_set_nocheck(json_msg, "reject-reason", json_string("Above target"));
		} else
			LOGINFO("Rejected client %d invalid share", client->id);
	}
out:
	return json_boolean(ret);
}

/* We should have already determined all the values passed to this are valid
 * by now. Set update if we should also send the latest stratum parameters */
static json_t *gen_json_result(int client_id, json_t *json_msg, json_t *method_val,
			       json_t *params_val, json_t **err_val, bool *update)
{
	stratum_instance_t *client = NULL;
	const char *method;
	json_t *ret = NULL;

	method = json_string_value(method_val);
	if (!strncasecmp(method, "mining.subscribe", 16)) {
		*update = true;
		ret = parse_subscribe(client_id, params_val);
		goto out;
	}

	ck_rlock(&instance_lock);
	client = __instance_by_id(client_id);
	ck_runlock(&instance_lock);

	if (unlikely(!client)) {
		LOGINFO("Failed to find client id %d in hashtable!", client_id);
		goto out;
	}

	if (!strncasecmp(method, "mining.auth", 11)) {
		ret = parse_authorize(client, params_val, err_val);
		if (ret)
			stratum_send_message(client, "Authorised, welcome to ckpool!");
		goto out;
	}

	/* We should only accept authorised requests from here on */
	if (!client->authorised) {
		ret = json_string("Unauthorised");
		goto out;
	}

	if (!strncasecmp(method, "mining.submit", 13)) {
		ret = parse_submit(client, json_msg, params_val, err_val);
		goto out;
	}

out:
	return ret;
}

/* Must enter with workbase_lock held */
static json_t *__stratum_notify(bool clean)
{
	json_t *val;

	val = json_pack("{s:[ssssosssb],s:o,s:s}",
			"params",
			current_workbase->idstring,
			current_workbase->prevhash,
			current_workbase->coinb1,
			current_workbase->coinb2,
			json_copy(current_workbase->merkle_array),
			current_workbase->bbversion,
			current_workbase->nbit,
			current_workbase->ntime,
			clean,
			"id", json_null(),
			"method", "mining.notify");
	return val;
}

static void stratum_broadcast_update(bool clean)
{
	json_t *json_msg;

	ck_rlock(&workbase_lock);
	json_msg = __stratum_notify(clean);
	ck_runlock(&workbase_lock);

	stratum_broadcast(json_msg);
}

/* For sending a single stratum template update */
static void stratum_send_update(int client_id, bool clean)
{
	json_t *json_msg;

	ck_rlock(&workbase_lock);
	json_msg = __stratum_notify(clean);
	ck_runlock(&workbase_lock);

	stratum_add_send(json_msg, client_id);
}

static void parse_instance_msg(int client_id, json_t *msg)
{
	json_t *result_val = NULL, *err_val = NULL, *id_val = NULL;
	json_t *method, *params;
	bool update = false;
	json_t *json_msg;

	json_msg = json_object();
	id_val = json_object_get(msg, "id");
	if (unlikely(!id_val)) {
		err_val = json_string("-1:id not found");
		goto out;
	}
#if 0
	/* Random broken clients send something not an integer so just use the
	 * json object, whatever it is. */
	if (unlikely(!json_is_integer(id_val))) {
		err_val = json_string("-1:id is not integer");
		goto out;
	}
#endif
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
	result_val = gen_json_result(client_id, json_msg, method, params,
				     &err_val, &update);
	if (!err_val)
		err_val = json_null();
out:
	json_object_set_nocheck(json_msg, "id", id_val);
	json_object_set_nocheck(json_msg, "error", err_val);
	json_object_set_nocheck(json_msg, "result", result_val);

	stratum_add_send(json_msg, client_id);
	if (update) {
		stratum_instance_t *client;

		stratum_send_update(client_id, true);

		ck_rlock(&instance_lock);
		client = __instance_by_id(client_id);
		ck_runlock(&instance_lock);

		if (likely(client))
			stratum_send_diff(client);
	}
}

static void *stratum_receiver(void *arg)
{
	ckpool_t __maybe_unused *ckp = (ckpool_t *)arg;
	stratum_msg_t *msg;

	rename_proc("sreceiver");

	while (42) {
		stratum_instance_t *instance;

		/* Pop the head off the list if it exists or wait for a conditional
		* signal telling us there is work */
		mutex_lock(&stratum_recv_lock);
		if (!stratum_recvs)
			pthread_cond_wait(&stratum_recv_cond, &stratum_recv_lock);
		msg = stratum_recvs;
		if (likely(msg))
			DL_DELETE(stratum_recvs, msg);
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

	rename_proc("ssender");

	while (42) {
		stratum_msg_t *msg;
		char *s;

		mutex_lock(&stratum_send_lock);
		if (!stratum_sends)
			pthread_cond_wait(&stratum_send_cond, &stratum_send_lock);
		msg = stratum_sends;
		if (likely(msg))
			DL_DELETE(stratum_sends, msg);
		mutex_unlock(&stratum_send_lock);

		if (unlikely(!msg))
			continue;

		if (unlikely(!msg->json_msg)) {
			LOGERR("Sent null json msg to stratum_sender");
			free(msg);
			continue;
		}

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

static void *statsupdate(void *arg)
{
	ckpool_t __maybe_unused *ckp = (ckpool_t *)arg;

	rename_proc("statsupdate");

	tv_time(&stats.start_time);
	cksleep_prepare_r(&stats.last_update);

	while (42) {
		char suffix1[16], suffix5[16], suffix15[16], suffix60[16];
		char suffix360[16], suffix1440[16];
		const double nonces = 4294967296;
		double sps1, sps5, sps15, sps60;
		double ghs, tdiff, bias;
		tv_t diff;
		int i;

		tv_time(&diff);
		timersub(&diff, &stats.start_time, &diff);
		tdiff = diff.tv_sec + (double)diff.tv_usec / 1000000;

		bias = time_bias(tdiff, 60);
		ghs = stats.dsps1 * nonces / bias;
		suffix_string(ghs, suffix1, 16, 0);
		sps1 = stats.sps1 / bias;

		bias = time_bias(tdiff, 300);
		ghs = stats.dsps5 * nonces / bias;
		suffix_string(ghs, suffix5, 16, 0);
		sps5 = stats.sps5 / bias;

		bias = time_bias(tdiff, 900);
		ghs = stats.dsps15 * nonces / bias;
		suffix_string(ghs, suffix15, 16, 0);
		sps15 = stats.sps15 / bias;

		bias = time_bias(tdiff, 3600);
		ghs = stats.dsps60 * nonces / bias;
		suffix_string(ghs, suffix60, 16, 0);
		sps60 = stats.sps60 / bias;

		bias = time_bias(tdiff, 21600);
		ghs = stats.dsps360 * nonces / bias;
		suffix_string(ghs, suffix360, 16, 0);

		bias = time_bias(tdiff, 86400);
		ghs = stats.dsps1440 * nonces / bias;
		suffix_string(ghs, suffix1440, 16, 0);

		LOGNOTICE("Pool runtime: %lus  Live clients: %d  Dead clients: %d  "
			  "Reusable clients: %d  Reused clients: %d",
			  diff.tv_sec, stats.live_clients, stats.dead_clients,
			  stats.reusable_clients, stats.reused_clients);
		LOGNOTICE("Pool hashrate: (1m):%s  (5m):%s  (15m):%s  (1h):%s  "
			  "(6h):%s  (1d):%s",
			  suffix1, suffix5, suffix15, suffix60, suffix360, suffix1440);
		LOGNOTICE("Pool shares difftotal: %ld  Absolute per second: (1m):%.1f  (5m):%.1f  (15m):%.1f  (1h):%.1f",
			  stats.accounted_shares, sps1, sps5, sps15, sps60);

		/* Update stats 4 times per minute for smooth values, displaying
		 * status every minute. */
		for (i = 0; i < 4; i++) {
			cksleep_ms_r(&stats.last_update, 15000);
			cksleep_prepare_r(&stats.last_update);

			mutex_lock(&stats_lock);
			stats.accounted_shares += stats.unaccounted_shares;
			stats.accounted_diff_shares += stats.unaccounted_diff_shares;

			decay_time(&stats.sps1, stats.unaccounted_shares, 15, 60);
			decay_time(&stats.sps5, stats.unaccounted_shares, 15, 300);
			decay_time(&stats.sps15, stats.unaccounted_shares, 15, 900);
			decay_time(&stats.sps60, stats.unaccounted_shares, 15, 3600);

			decay_time(&stats.dsps1, stats.unaccounted_diff_shares, 15, 60);
			decay_time(&stats.dsps5, stats.unaccounted_diff_shares, 15, 300);
			decay_time(&stats.dsps15, stats.unaccounted_diff_shares, 15, 900);
			decay_time(&stats.dsps60, stats.unaccounted_diff_shares, 15, 3600);
			decay_time(&stats.dsps360, stats.unaccounted_diff_shares, 15, 21600);
			decay_time(&stats.dsps1440, stats.unaccounted_diff_shares, 15, 86400);

			stats.unaccounted_shares = stats.unaccounted_diff_shares = 0;
			mutex_unlock(&stats_lock);
		}
	}

	return NULL;
}

int stratifier(proc_instance_t *pi)
{
	pthread_t pth_blockupdate, pth_stratum_receiver, pth_stratum_sender;
	pthread_t pth_statsupdate;
	ckpool_t *ckp = pi->ckp;
	int ret;

	/* Store this for use elsewhere */
	hex2bin(scriptsig_header_bin, scriptsig_header, 41);
	address_to_pubkeytxn(pubkeytxnbin, ckp->btcaddress);
	__bin2hex(pubkeytxn, pubkeytxnbin, 25);

	/* Set the initial id to time as high bits so as to not send the same
	 * id on restarts */
	blockchange_id = workbase_id = ((int64_t)time(NULL)) << 32;

	cklock_init(&instance_lock);

	mutex_init(&stratum_recv_lock);
	cond_init(&stratum_recv_cond);
	create_pthread(&pth_stratum_receiver, stratum_receiver, ckp);

	mutex_init(&stratum_send_lock);
	cond_init(&stratum_send_cond);
	create_pthread(&pth_stratum_sender, stratum_sender, ckp);

	cklock_init(&workbase_lock);
	create_pthread(&pth_blockupdate, blockupdate, ckp);

	mutex_init(&stats_lock);
	create_pthread(&pth_statsupdate, statsupdate, ckp);

	cklock_init(&share_lock);

	ret = stratum_loop(ckp, pi);
	LOGINFO("%s stratifier exiting with return code %d", ckp->name, ret);
	if (ret) {
		send_proc(&ckp->main, "shutdown");
		sleep(1);
	}

	exit(ret);
}
