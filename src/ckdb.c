/*
 * Copyright 2003-2014 Andrew Smith
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include "config.h"

#include <sys/ioctl.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <jansson.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <regex.h>
#include <libpq-fe.h>

#include "ckpool.h"
#include "libckpool.h"

#include "klist.h"
#include "ktree.h"

static char *db_user;
static char *db_pass;

// size limit on the command string
#define ID_SIZ 31

#define TXT_BIG 256
#define TXT_MED 128
#define TXT_SML 64
#define TXT_FLAG 1

#define FLDSEP 0x02

// Ensure long long and int64_t are both 8 bytes (and thus also the same)
#define ASSERT1(condition) __maybe_unused static char sizeof_longlong_must_be_8[(condition)?1:-1]
#define ASSERT2(condition) __maybe_unused static char sizeof_int64_t_must_be_8[(condition)?1:-1]
ASSERT1(sizeof(long long) == 8);
ASSERT2(sizeof(int64_t) == 8);

#define PGLOG(__LOG, __str, __rescode, __conn) do { \
		char *__ptr, *__buf = strdup(PQerrorMessage(__conn)); \
		__ptr = __buf + strlen(__buf) - 1; \
		while (__ptr >= __buf && (*__ptr == '\n' || *__ptr == '\r')) \
			*(__ptr--) = '\0'; \
		while (--__ptr >= __buf) \
			if (*__ptr == '\n' || *__ptr == '\r' || *__ptr == '\t') \
				*__ptr = ' '; \
		__LOG("%s(): %s failed (%d) '%s'", __func__, \
			__str, (int)rescode, __buf); \
		free(__buf); \
	} while (0)

#define PGLOGERR(_str, _rescode, _conn) PGLOG(LOGERR, _str, _rescode, _conn)

/* N.B. STRNCPY() truncates, whereas txt_to_str() aborts ckdb if src > trg
 * If copying data from the DB, code should always use txt_to_str() since
 * data should never be lost/truncated if it came from the DB -
 * that simply implies a code bug or a database change that must be fixed */
#define STRNCPY(trg, src) do { \
		strncpy((char *)(trg), (char *)(src), sizeof(trg)); \
		trg[sizeof(trg) - 1] = '\0'; \
	} while (0)

#define STRNCPYSIZ(trg, src, siz) do { \
		strncpy((char *)(trg), (char *)(src), siz); \
		trg[siz - 1] = '\0'; \
	} while (0)

#define APPEND_REALLOC(_dst, _dstoff, _dstsiz, _src) do { \
		size_t _srclen = strlen(_src); \
		if ((_dstoff) + _srclen >= (_dstsiz)) { \
			_dstsiz += 1024; \
			_dst = realloc(_dst, _dstsiz); \
			if (!(_dst)) \
				quithere(1, "realloc (%d) OOM", (int)_dstsiz); \
		} \
		strcpy((_dst)+(_dstoff), _src); \
		_dstoff += _srclen; \
	} while(0)

enum data_type {
	TYPE_STR,
	TYPE_BIGINT,
	TYPE_INT,
	TYPE_TV,
	TYPE_BLOB
};

#define TXT_TO_STR(__nam, __fld, __data) txt_to_str(__nam, __fld, (__data), sizeof(__data))
#define TXT_TO_BIGINT(__nam, __fld, __data) txt_to_bigint(__nam, __fld, &(__data), sizeof(__data))
#define TXT_TO_INT(__nam, __fld, __data) txt_to_int(__nam, __fld, &(__data), sizeof(__data))
#define TXT_TO_TV(__nam, __fld, __data) txt_to_tv(__nam, __fld, &(__data), sizeof(__data))
#define TXT_TO_BLOB(__nam, __fld, __data) txt_to_blob(__nam, __fld, __data)

#define PQ_GET_FLD(__res, __row, __name, __fld, __ok) do { \
		int __col = PQfnumber(__res, __name); \
		if (__col == -1) { \
			LOGERR("%s(): Unknown field '%s' row %d", __func__, __name, __row); \
			__ok = false; \
		} else \
			__fld = PQgetvalue(__res, __row, __col); \
	} while (0)

// HISTORY FIELDS
#define HISTORYDATECONTROL ",createdate,createby,createcode,createinet,expirydate"
#define HISTORYDATECOUNT 5

#define HISTORYDATEFLDS(_res, _row, _data, _ok) do { \
		char *_fld; \
		PQ_GET_FLD(_res, _row, "createdate", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_TV("createdate", _fld, (_data)->createdate); \
		PQ_GET_FLD(_res, _row, "createby", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_STR("createby", _fld, (_data)->createby); \
		PQ_GET_FLD(_res, _row, "createcode", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_STR("createcode", _fld, (_data)->createcode); \
		PQ_GET_FLD(_res, _row, "createinet", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_STR("createinet", _fld, (_data)->createinet); \
		PQ_GET_FLD(_res, _row, "expirydate", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_TV("expirydate", _fld, (_data)->expirydate); \
	} while (0)

#define HISTORYDATECONTROLFIELDS \
	tv_t createdate; \
	char createby[TXT_SML+1]; \
	char createcode[TXT_MED+1]; \
	char createinet[TXT_MED+1]; \
	tv_t expirydate

#define HISTORYDATEPARAMS(_params, _his_pos, _row) do { \
		_params[_his_pos++] = tv_to_buf(&(_row->createdate), NULL, 0); \
		_params[_his_pos++] = str_to_buf(_row->createby, NULL, 0); \
		_params[_his_pos++] = str_to_buf(_row->createcode, NULL, 0); \
		_params[_his_pos++] = str_to_buf(_row->createinet, NULL, 0); \
		_params[_his_pos++] = tv_to_buf(&(_row->expirydate), NULL, 0); \
	} while (0)

// 6-Jun-6666 06:06:06+00
#define DEFAULT_EXPIRY 148204965966L
// 1-Jun-6666 00:00:00+00
#define COMPARE_EXPIRY 148204512000L

static const tv_t default_expiry = { DEFAULT_EXPIRY, 0L };

#define HISTORYDATEINIT(_row, _by, _code, _inet) do { \
		setnow(&(_row->createdate)); \
		STRNCPY(_row->createby, _by); \
		STRNCPY(_row->createcode, _code); \
		STRNCPY(_row->createinet, _inet); \
		_row->expirydate.tv_sec = default_expiry.tv_sec; \
		_row->expirydate.tv_usec = default_expiry.tv_usec; \
	} while (0)

// MODIFY FIELDS
#define MODIFYDATECONTROL ",createdate,createby,createcode,createinet" \
			  ",modifydate,modifyby,modifycode,modifyinet"
#define MODIFYDATECOUNT 8

#define MODIFYDATEFLDS(_res, _row, _data, _ok) do { \
		char *_fld; \
		PQ_GET_FLD(_res, _row, "createdate", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_TV("createdate", _fld, (_data)->createdate); \
		PQ_GET_FLD(_res, _row, "createby", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_STR("createby", _fld, (_data)->createby); \
		PQ_GET_FLD(_res, _row, "createcode", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_STR("createcode", _fld, (_data)->createcode); \
		PQ_GET_FLD(_res, _row, "createinet", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_STR("createinet", _fld, (_data)->createinet); \
		PQ_GET_FLD(_res, _row, "modifydate", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_TV("modifydate", _fld, (_data)->modifydate); \
		PQ_GET_FLD(_res, _row, "modifyby", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_STR("modifyby", _fld, (_data)->modifyby); \
		PQ_GET_FLD(_res, _row, "modifycode", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_STR("modifycode", _fld, (_data)->modifycode); \
		PQ_GET_FLD(_res, _row, "modifyinet", _fld, _ok); \
		if (!_ok) \
			break; \
		TXT_TO_STR("modifyinet", _fld, (_data)->modifyinet); \
	} while (0)

#define MODIFYDATECONTROLFIELDS \
	tv_t createdate; \
	char createby[TXT_SML+1]; \
	char createcode[TXT_MED+1]; \
	char createinet[TXT_MED+1]; \
	tv_t modifydate; \
	char modifyby[TXT_SML+1]; \
	char modifycode[TXT_MED+1]; \
	char modifyinet[TXT_MED+1]

#define MODIFYDATEPARAMS(_params, _mod_pos, _row) do { \
		_params[_mod_pos++] = tv_to_buf(&(_row->createdate), NULL, 0); \
		_params[_mod_pos++] = str_to_buf(_row->createby, NULL, 0); \
		_params[_mod_pos++] = str_to_buf(_row->createcode, NULL, 0); \
		_params[_mod_pos++] = str_to_buf(_row->createinet, NULL, 0); \
		_params[_mod_pos++] = tv_to_buf(&(_row->modifydate), NULL, 0); \
		_params[_mod_pos++] = str_to_buf(_row->modifyby, NULL, 0); \
		_params[_mod_pos++] = str_to_buf(_row->modifycode, NULL, 0); \
		_params[_mod_pos++] = str_to_buf(_row->modifyinet, NULL, 0); \
	} while (0)

#define MODIFYDATEINIT(_row, _by, _code, _inet) do { \
		setnow(&(_row->createdate)); \
		STRNCPY(_row->createby, _by); \
		STRNCPY(_row->createcode, _code); \
		STRNCPY(_row->createinet, _inet); \
		_row->modifydate.tv_sec = 0; \
		_row->modifydate.tv_usec = 0; \
		_row->modifyby[0] = '\0'; \
		_row->modifycode[0] = '\0'; \
		_row->modifyinet[0] = '\0'; \
	} while (0)

// For easy parameter constant strings
#define PQPARAM1  "$1"
#define PQPARAM2  "$1,$2"
#define PQPARAM3  "$1,$2,$3"
#define PQPARAM4  "$1,$2,$3,$4"
#define PQPARAM5  "$1,$2,$3,$4,$5"
#define PQPARAM6  "$1,$2,$3,$4,$5,$6"
#define PQPARAM7  "$1,$2,$3,$4,$5,$6,$7"
#define PQPARAM8  "$1,$2,$3,$4,$5,$6,$7,$8"
#define PQPARAM9  "$1,$2,$3,$4,$5,$6,$7,$8,$9"
#define PQPARAM10 "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10"
#define PQPARAM11 "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11"
#define PQPARAM12 "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12"
#define PQPARAM13 "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13"
#define PQPARAM14 "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14"
#define PQPARAM15 "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15"
#define PQPARAM16 "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16"
#define PQPARAM17 "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17"

#define PARCHK(_par, _params) do { \
		if (_par != (int)(sizeof(_params)/sizeof(_params[0]))) { \
			quithere(1, "params[] usage (%d) != size (%d)", \
					_par, (int)(sizeof(_params)/sizeof(_params[0]))); \
		} \
	} while (0)


static const char *userpatt = "^[!-~]*$"; // no spaces
static const char *mailpatt = "^[A-Za-z0-9_-][A-Za-z0-9_\\.-]*@[A-Za-z0-9][A-Za-z0-9\\.]*[A-Za-z0-9]$";
static const char *idpatt = "^[_A-Za-z][_A-Za-z0-9]*$";
static const char *intpatt = "^[0-9][0-9]*$";
static const char *hashpatt = "^[A-Fa-f0-9]*$";

// TRANSFER
#define NAME_SIZE 63
#define VALUE_SIZE 1023
typedef struct transfer {
	char name[NAME_SIZE+1];
	char value[VALUE_SIZE+1];
} TRANSFER;

#define ALLOC_TRANSFER 1024
#define LIMIT_TRANSFER 0
#define DATA_TRANSFER(_item) ((TRANSFER *)(_item->data))

static K_TREE *transfer_root;
static K_LIST *transfer_list;
static K_STORE *transfer_store;

// USERS
typedef struct users {
	int64_t userid;
	char username[TXT_BIG+1];
	char emailaddress[TXT_BIG+1];
	tv_t joineddate;
	char passwordhash[TXT_BIG+1];
	char secondaryuserid[TXT_SML+1];
	HISTORYDATECONTROLFIELDS;
} USERS;

#define ALLOC_USERS 1024
#define LIMIT_USERS 0
#define DATA_USERS(_item) ((USERS *)(_item->data))

static K_TREE *users_root;
static K_LIST *users_list;
static K_STORE *users_store;

// WORKERS
typedef struct workers {
	int64_t workerid;
	int64_t userid;
	char workername[TXT_SML+1];
	int32_t difficultydefault;
	char idlenotificationenabled[TXT_FLAG+1];
	int32_t idlenotificationtime;
	HISTORYDATECONTROLFIELDS;
} WORKERS;

#define ALLOC_WORKERS 1024
#define LIMIT_WORKERS 0
#define DATA_WORKERS(_item) ((WORKERS *)(_item->data))

static K_TREE *workers_root;
static K_LIST *workers_list;
static K_STORE *workers_store;

/* unused yet
// PAYMENTADDRESSES
typedef struct paymentaddresses {
	int64_t paymentaddressid;
	int64_t userid;
	char payaddress[TXT_BIG+1];
	int32_t payratio;
	HISTORYDATECONTROLFIELDS;
} PAYMENTADDRESSES;

#define ALLOC_PAYMENTADDRESSES 1024
#define LIMIT_PAYMENTADDRESSES 0
#define DATA_PAYMENTADDRESSES(_item) ((PAYMENTADDRESSES *)(_item->data))

static K_TREE *paymentaddresses_root;
static K_LIST *paymentaddresses_list;
static K_STORE *paymentaddresses_store;
*/

// PAYMENTS
typedef struct payments {
	int64_t paymentid;
	int64_t userid;
	tv_t paydate;
	char payaddress[TXT_BIG+1];
	char originaltxn[TXT_BIG+1];
	int64_t amount;
	char committxn[TXT_BIG+1];
	char commitblockhash[TXT_BIG+1];
	HISTORYDATECONTROLFIELDS;
} PAYMENTS;

#define ALLOC_PAYMENTS 1024
#define LIMIT_PAYMENTS 0
#define DATA_PAYMENTS(_item) ((PAYMENTS *)(_item->data))

static K_TREE *payments_root;
static K_LIST *payments_list;
static K_STORE *payments_store;

/* unused yet
// ACCOUNTBALANCE
typedef struct accountbalance {
	int64_t userid;
	int64_t confirmedpaid;
	int64_t confirmedunpaid;
	int64_t pendingconfirm;
	int32_t heightupdate;
	HISTORYDATECONTROLFIELDS;
} ACCOUNTBALANCE;

#define ALLOC_ACCOUNTBALANCE 1024
#define LIMIT_ACCOUNTBALANCE 0
#define DATA_ACCOUNTBALANCE(_item) ((ACCOUNTBALANCE *)(_item->data))

static K_TREE *accountbalance_root;
static K_LIST *accountbalance_list;
static K_STORE *accountbalance_store;
*/

// IDCONTROL
typedef struct idcontrol {
	char idname[TXT_SML+1];
	int64_t lastid;
	MODIFYDATECONTROLFIELDS;
} IDCONTROL;

#define ALLOC_IDCONTROL 16
#define LIMIT_IDCONTROL 0
#define DATA_IDCONTROL(_item) ((IDCONTROL *)(_item->data))

// These are only used for db access - not stored in memory
//static K_TREE *idcontrol_root;
static K_LIST *idcontrol_list;
static K_STORE *idcontrol_store;

/* unused yet
// OPTIONCONTROL
typedef struct optioncontrol {
	char optionname[TXT_SML+1];
	char optionvalue[TXT_MED+1];
	tv_t activationdate;
	int32_t activationheight;
	HISTORYDATECONTROLFIELDS;
} OPTIONCONTROL;

#define ALLOC_OPTIONCONTROL 64
#define LIMIT_OPTIONCONTROL 0
#define DATA_OPTIONCONTROL(_item) ((OPTIONCONTROL *)(_item->data))

static K_TREE *optioncontrol_root;
static K_LIST *optioncontrol_list;
static K_STORE *optioncontrol_store;

// TODO: aging/discarding workinfo,shares
// WORKINFO id.workinfo.json={...}
typedef struct workinfo {
	int64_t workinfoid;
	char poolinstance[TXT_BIG+1];
	char *transactiontree;
	char prevhash[TXT_BIG+1];
	char coinbase1[TXT_BIG+1];
	char coinbase2[TXT_BIG+1];
	char version[TXT_SML+1];
	char bits[TXT_SML+1];
	char ntime[TXT_SML+1];
	HISTORYDATECONTROLFIELDS;
} WORKINFO;

// ~10 hrs
#define ALLOC_WORKINFO 1400
#define LIMIT_WORKINFO 0
#define DATA_WORKINFO(_item) ((WORKINFO *)(_item->data))

static K_TREE *workinfo_root;
static K_LIST *workinfo_list;
static K_STORE *workinfo_store;

// SHARES id.sharelog.json={...}
typedef struct shares {
	int64_t workinfoid;
	int64_t userid;
	char workername[TXT_SML+1];
	int32_t clientid;
	char enonce1[TXT_SML+1];
	char nonce2[TXT_BIG+1];
	char nonce[TXT_SML+1];
	char secondaryuserid[TXT_SML+1];
	HISTORYDATECONTROLFIELDS;
} SHARES;

#define ALLOC_SHARES 10000
#define LIMIT_SHARES 0
#define DATA_SHARES(_item) ((SHARES *)(_item->data))

static K_TREE *shares_root;
static K_LIST *shares_list;
static K_STORE *shares_store;

// SHARESUMMARY
typedef struct sharesummary {
	int64_t userid;
	char workername[TXT_SML+1];
	int64_t workinfoid;
	int64_t diff_acc;
	int64_t diff_sta;
	int64_t diff_dup;
	int64_t diff_low;
	int64_t diff_rej;
	int64_t share_acc;
	int64_t share_sta;
	int64_t share_dup;
	int64_t share_low;
	int64_t share_rej;
	tv_t first_share;
	tv_t last_share;
	char complete[TXT_FLAG+1];
	MODIFYDATECONTROLFIELDS;
} SHARESUMMARY;

#define ALLOC_SHARESUMMARY 10000
#define LIMIT_SHARESUMMARY 0
#define DATA_SHARESUMMARY(_item) ((SHARESUMMARY *)(_item->data))

static K_TREE *sharesummary_root;
static K_LIST *sharesummary_list;
static K_STORE *sharesummary_store;

// BLOCKSUMMARY
typedef struct blocksummary {
	int32_t height;
	char blockhash[TXT_BIG+1];
	int64_t userid;
	char workername[TXT_SML+1];
	int64_t diff_acc;
	int64_t diff_sta;
	int64_t diff_dup;
	int64_t diff_low;
	int64_t diff_rej;
	int64_t share_acc;
	int64_t share_sta;
	int64_t share_dup;
	int64_t share_low;
	int64_t share_rej;
	tv_t first_share;
	tv_t last_share;
	char complete[TXT_FLAG+1];
	MODIFYDATECONTROLFIELDS;
} BLOCKSUMMARY;

#define ALLOC_BLOCKSUMMARY 10000
#define LIMIT_BLOCKSUMMARY 0
#define DATA_BLOCKSUMMARY(_item) ((BLOCKSUMMARY *)(_item->data))

static K_TREE *blocksummary_root;
static K_LIST *blocksummary_list;
static K_STORE *blocksummary_store;

// BLOCKS
typedef struct blocks {
	int32_t height;
	char blockhash[TXT_BIG+1];
	int64_t workinfoid;
	int64_t userid;
	char workername[TXT_SML+1];
	int32_t clientid;
	char enonce1[TXT_SML+1];
	char nonce2[TXT_BIG+1];
	char nonce[TXT_SML+1];
	char confirmed[TXT_FLAG+1];
	HISTORYDATECONTROLFIELDS;
} BLOCKS;

#define ALLOC_BLOCKS 10000
#define LIMIT_BLOCKS 0
#define DATA_BLOCKS ((BLOCKS *)(_item->data))

static K_TREE *blocks_root;
static K_LIST *blocks_list;
static K_STORE *blocks_store;

// MININGPAYOUTS
typedef struct miningpayouts {
	int64_t miningpayoutid;
	int64_t userid;
	int32_t height;
	char blockhash[TXT_BIG+1];
	int64_t amount;
	HISTORYDATECONTROLFIELDS;
} MININGPAYOUTS;

#define ALLOC_MININGPAYOUTS 1000
#define LIMIT_MININGPAYOUTS 0
#define DATA_MININGPAYOUTS(_item) ((MININGPAYOUTS *)(_item->data))

static K_TREE *miningpayouts_root;
static K_LIST *miningpayouts_list;
static K_STORE *miningpayouts_store;

// EVENTLOG
typedef struct eventlog {
	int64_t eventlogid;
	char eventlogcode[TXT_SML+1];
	char *eventlogdescription;
	HISTORYDATECONTROLFIELDS;
} EVENTLOG;

#define ALLOC_EVENTLOG 100
#define LIMIT_EVENTLOG 0
#define DATA_EVENTLOG(_item) ((EVENTLOG *)(_item->data))

static K_TREE *eventlog_root;
static K_LIST *eventlog_list;
static K_STORE *eventlog_store;

// AUTHS
typedef struct auths {
	int64_t authid;
	int64_t userid;
	char workername[TXT_SML+1];
	int32_t clientid;
	char enonce1[TXT_SML+1];
	char useragent[TXT_BIG+1];
	HISTORYDATECONTROLFIELDS;
} AUTHS;

#define ALLOC_AUTHS 1000
#define LIMIT_AUTHS 0
#define DATA_AUTHS(_item) ((AUTHS *)(_item->data))

static K_TREE *auths_root;
static K_LIST *auths_list;
static K_STORE *auths_store;

// POOLSTATS
// TODO: not in DB yet - design incomplete
// poll pool(s) every 10min?
typedef struct poolstats {
	char poolinstance[TXT_BIG+1];
	int32_t users;
	int32_t workers;
	int64_t hashrate; // ... etc
	int64_t hashrate5m;
	int64_t hashrate1hr;
	int64_t hashrate24hr;
	HISTORYDATECONTROLFIELDS;
} POOLSTATS;

#define ALLOC_POOLSTATS 10000
#define LIMIT_POOLSTATS 0
#define DATA_POOLSTATS(_item) ((POOLSTATS *)(_item->data))

static K_TREE *poolstats_root;
static K_LIST *poolstats_list;
static K_STORE *poolstats_store;
*/

static void setnow(tv_t *now)
{
	ts_t spec;
	spec.tv_sec = 0;
	spec.tv_nsec = 0;
	clock_gettime(CLOCK_REALTIME, &spec);
	now->tv_sec = spec.tv_sec;
	now->tv_usec = spec.tv_nsec / 1000;
}

static void txt_to_data(enum data_type typ, char *nam, char *fld, void *data, size_t siz)
{
	char *tmp;

	switch (typ) {
		case TYPE_STR:
			// A database field being bigger than local storage is a fatal error
			if (siz < (strlen(fld)+1)) {
				quithere(1, "Field %s structure size %d is smaller than db %d",
						nam, (int)siz, (int)strlen(fld)+1);
			}
			strcpy((char *)data, fld);
			break;
		case TYPE_BIGINT:
			if (siz != sizeof(int64_t)) {
				quithere(1, "Field %s bigint incorrect structure size %d - should be %d",
						nam, (int)siz, (int)sizeof(int64_t));
			}
			*((long long *)data) = atoll(fld);
			break;
		case TYPE_INT:
			if (siz != sizeof(int32_t)) {
				quithere(1, "Field %s int incorrect structure size %d - should be %d",
						nam, (int)siz, (int)sizeof(int32_t));
			}
			*((int32_t *)data) = atoi(fld);
			break;
		case TYPE_TV:
			if (siz != sizeof(tv_t)) {
				quithere(1, "Field %s timeval incorrect structure size %d - should be %d",
						nam, (int)siz, (int)sizeof(tv_t));
			}
			unsigned int yyyy, mm, dd, HH, MM, SS, uS = 0, tz;
			struct tm tm;
			time_t tim;
			int n;
			n = sscanf(fld, "%u-%u-%u %u:%u:%u+%u",
					&yyyy, &mm, &dd, &HH, &MM, &SS, &tz);
			if (n != 7) {
				// allow uS in case data was created elsewhere
				n = sscanf(fld, "%u-%u-%u %u:%u:%u.%u+%u",
						&yyyy, &mm, &dd, &HH, &MM, &SS, &uS, &tz);
				if (n != 8) {
					quithere(1, "Field %s timeval unhandled date '%s' (%d)",
						 nam, fld, n);
				}
			}
			tm.tm_sec = (int)SS;
			tm.tm_min = (int)MM;
			tm.tm_hour = (int)HH;
			tm.tm_mday = (int)dd;
			tm.tm_mon = (int)mm - 1;
			tm.tm_year = (int)yyyy - 1900;
			tm.tm_isdst = -1;
			tim = mktime(&tm);
			// Fix TZ offsets errors
			if (tim > COMPARE_EXPIRY) {
				((tv_t *)data)->tv_sec = default_expiry.tv_sec;
				((tv_t *)data)->tv_usec = default_expiry.tv_usec;
			} else {
				((tv_t *)data)->tv_sec = tim;
				((tv_t *)data)->tv_usec = uS;
			}
			break;
		case TYPE_BLOB:
			tmp = strdup(fld);
			if (!tmp)
				quithere(1, "Field %s (%d) OOM", nam, (int)strlen(fld));
			*((char **)data) = tmp;
			break;
		default:
			quithere(1, "Unknown field %s (%d) to convert", nam, (int)typ);
			break;
	}
}

// N.B. STRNCPY* macros truncate, whereas this aborts ckdb if src > trg
static void txt_to_str(char *nam, char *fld, char data[], size_t siz)
{
	txt_to_data(TYPE_STR, nam, fld, (void *)data, siz);
}

static void txt_to_bigint(char *nam, char *fld, int64_t *data, size_t siz)
{
	txt_to_data(TYPE_BIGINT, nam, fld, (void *)data, siz);
}

static void txt_to_int(char *nam, char *fld, int32_t *data, size_t siz)
{
	txt_to_data(TYPE_INT, nam, fld, (void *)data, siz);
}

static void txt_to_tv(char *nam, char *fld, tv_t *data, size_t siz)
{
	txt_to_data(TYPE_TV, nam, fld, (void *)data, siz);
}

/* unused yet
static void txt_to_blob(char *nam, char *fld, char *data)
{
	txt_to_data(TYPE_BLOB, nam, fld, (void *)(&data), 0);
}
*/

static char *data_to_buf(enum data_type typ, void *data, char *buf, size_t siz)
{
	struct tm tm;
	char *buf2;

	if (!buf) {
		switch (typ) {
			case TYPE_STR:
			case TYPE_BLOB:
				siz = strlen((char *)data) + 1;
				break;
			case TYPE_BIGINT:
			case TYPE_INT:
			case TYPE_TV:
				siz = 64; // More than big enough
				break;
			default:
				quithere(1, "Unknown field (%d) to convert", (int)typ);
				break;
		}

		buf = malloc(siz);
		if (!buf)
			quithere(1, "OOM (%d)", (int)siz);
	}

	switch (typ) {
		case TYPE_STR:
		case TYPE_BLOB:
			snprintf(buf, siz, "%s", (char *)data);
			break;
		case TYPE_BIGINT:
			snprintf(buf, siz, "%"PRId64, *((uint64_t *)data));
			break;
		case TYPE_INT:
			snprintf(buf, siz, "%"PRId32, *((uint32_t *)data));
			break;
		case TYPE_TV:
			buf2 = malloc(siz);
			if (!buf2)
				quithere(1, "OOM (%d)", (int)siz);
			localtime_r(&(((struct timeval *)data)->tv_sec), &tm);
			strftime(buf2, siz, "%Y-%m-%d %H:%M:%S", &tm);
			snprintf(buf, siz, "%s.%06ld", buf2,
					   (((struct timeval *)data)->tv_usec));
			free(buf2);
			break;
	}

	return buf;
}

static char *str_to_buf(char data[], char *buf, size_t siz)
{
	return data_to_buf(TYPE_STR, (void *)data, buf, siz);
}

static char *bigint_to_buf(int64_t data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_BIGINT, (void *)(&data), buf, siz);
}

/* unused yet
static char *int_to_buf(int32_t data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_INT, (void *)(&data), buf, siz);
}
*/

static char *tv_to_buf(tv_t *data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_TV, (void *)data, buf, siz);
}

/* unused yet
static char *blob_to_buf(char *data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_BLOB, (void *)data, buf, siz);
}
*/

static PGconn *dbconnect()
{
	char conninfo[128];
	PGconn *conn;

	snprintf(conninfo, sizeof(conninfo), "host=127.0.0.1 dbname=ckdb user=%s", db_user);

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
		quithere(1, "ERR: Failed to connect to db '%s'", PQerrorMessage(conn));

	return conn;
}

static int64_t nextid(PGconn *conn, char *idname, int64_t increment, char *by, char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res;
	char qry[1024];
	char *params[5];
	int par;
	int64_t lastid;
	char *field;
	tv_t now;
	bool ok;
	int n;

	lastid = 0;

	snprintf(qry, sizeof(qry), "select lastid from idcontrol "
				   "where idname='%s' for update",
				   idname);

	res = PQexec(conn, qry);
	rescode = PQresultStatus(res);
	if (rescode != PGRES_TUPLES_OK) {
		PGLOGERR("Select", rescode, conn);
		goto cleanup;
	}

	n = PQnfields(res);
	if (n != 1) {
		LOGERR("%s(): Invalid field count - should be %d, but is %d",
			__func__, 1, n);
		goto cleanup;
	}

	n = PQntuples(res);
	if (n < 1) {
		LOGERR("%s(): No matching idname='%s'", __func__, idname);
		goto cleanup;
	}

	ok = true;
	PQ_GET_FLD(res, 0, "lastid", field, ok);
	if (!ok)
		goto cleanup;
	TXT_TO_BIGINT("lastid", field, lastid);

	PQclear(res);

	lastid += increment;
	snprintf(qry, sizeof(qry), "update idcontrol set "
				   "lastid=$1, modifydate=$2, modifyby=$3, "
				   "modifycode=$4, modifyinet=$5 "
				   "where idname='%s'", 
				   idname);

	setnow(&now);

	par = 0;
	params[par++] = bigint_to_buf(lastid, NULL, 0);
	params[par++] = tv_to_buf(&now, NULL, 0);
	params[par++] = str_to_buf(by, NULL, 0);
	params[par++] = str_to_buf(code, NULL, 0);
	params[par++] = str_to_buf(inet, NULL, 0);
	PARCHK(par, params);

	res = PQexecParams(conn, qry, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (rescode != PGRES_COMMAND_OK) {
		PGLOGERR("Update", rescode, conn);
		lastid = 0;
	}

	for (n = 0; n < par; n++)
		free(params[n]);
cleanup:
	PQclear(res);
	return lastid;
}

// order by username asc,expirydate desc
static double users_cmp(K_ITEM *a, K_ITEM *b)
{
	double c = strcmp(DATA_USERS(a)->username, DATA_USERS(b)->username);
	if (c == 0.0)
		c = tvdiff(&(DATA_USERS(b)->expirydate), &(DATA_USERS(a)->expirydate));
	return c;
}

static bool users_add(PGconn *conn, char *username, char *emailaddress, char *passwordhash, char *by, char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n;
	USERS *row;
	char *ins;
	char tohash[64];
	uint64_t hash;
	__maybe_unused uint64_t tmp;
	bool ok = false;
	char *params[11];
	int par;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(users_list);
	item = k_unlink_head(users_list);
	K_WUNLOCK(users_list);

	row = DATA_USERS(item);

	row->userid = nextid(conn, "userid", (int64_t)(666 + (rand() % 334)), by, code, inet);
	if (row->userid == 0)
		goto unitem;

	STRNCPY(row->username, username);
	STRNCPY(row->emailaddress, emailaddress);
	STRNCPY(row->passwordhash, passwordhash);

	snprintf(tohash, sizeof(tohash), "%s&#%s", username, emailaddress);
	HASH_BER(tohash, strlen(tohash), 1, hash, tmp);
	__bin2hex(row->secondaryuserid, (void *)(&hash), sizeof(hash));

	HISTORYDATEINIT(row, by, code, inet);

	// copy createdate
	row->joineddate.tv_sec = row->createdate.tv_sec;
	row->joineddate.tv_usec = row->createdate.tv_usec;

	par = 0;
	params[par++] = bigint_to_buf(row->userid, NULL, 0);
	params[par++] = str_to_buf(row->username, NULL, 0);
	params[par++] = str_to_buf(row->emailaddress, NULL, 0);
	params[par++] = tv_to_buf(&(row->joineddate), NULL, 0);
	params[par++] = str_to_buf(row->passwordhash, NULL, 0);
	params[par++] = str_to_buf(row->secondaryuserid, NULL, 0);
	HISTORYDATEPARAMS(params, par, row);
	PARCHK(par, params);

	ins = "insert into users "
		"(userid,username,emailaddress,joineddate,passwordhash,"
		"secondaryuserid"
		HISTORYDATECONTROL ") values (" PQPARAM11 ")";

	res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (rescode != PGRES_COMMAND_OK) {
		PGLOGERR("Insert", rescode, conn);
		goto unparam;
	}

	ok = true;
unparam:
	PQclear(res);
	for (n = 0; n < par; n++)
		free(params[n]);
unitem:
	K_WLOCK(users_list);
	if (!ok)
		k_add_head(users_list, item);
	else {
		users_root = add_to_ktree(users_root, item, users_cmp);
		k_add_head(users_store, item);
	}
	K_WUNLOCK(users_list);

	return ok;
}

static bool users_fill(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n, i;
	USERS *row;
	char *field;
	char *sel;
	int fields = 6;
	bool ok;

	LOGDEBUG("%s(): select", __func__);

	sel = "select "
		"userid,username,emailaddress,joineddate,passwordhash,"
		"secondaryuserid"
		HISTORYDATECONTROL
		" from users";
	res = PQexec(conn, sel);
	rescode = PQresultStatus(res);
	if (rescode != PGRES_TUPLES_OK) {
		PGLOGERR("Select", rescode, conn);
		PQclear(res);
		return false;
	}

	n = PQnfields(res);
	if (n != (fields + HISTORYDATECOUNT)) {
		LOGERR("%s(): Invalid field count - should be %d, but is %d",
			__func__, fields + HISTORYDATECOUNT, n);
		PQclear(res);
		return false;
	}

	n = PQntuples(res);
	LOGDEBUG("%s(): tree build count %d", __func__, n);
	ok = true;
	K_WLOCK(users_list);
	for (i = 0; i < n; i++) {
		item = k_unlink_head(users_list);
		row = DATA_USERS(item);

		PQ_GET_FLD(res, i, "userid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("userid", field, row->userid);

		PQ_GET_FLD(res, i, "username", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("username", field, row->username);

		PQ_GET_FLD(res, i, "emailaddress", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("emailaddress", field, row->emailaddress);

		PQ_GET_FLD(res, i, "joineddate", field, ok);
		if (!ok)
			break;
		TXT_TO_TV("joineddate", field, row->joineddate);

		PQ_GET_FLD(res, i, "passwordhash", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("passwordhash", field, row->passwordhash);

		PQ_GET_FLD(res, i, "secondaryuserid", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("secondaryuserid", field, row->secondaryuserid);

		HISTORYDATEFLDS(res, i, row, ok);
		if (!ok)
			break;

		users_root = add_to_ktree(users_root, item, users_cmp);
		k_add_head(users_store, item);
	}
	if (!ok)
		k_add_head(users_list, item);

	K_WUNLOCK(users_list);
	PQclear(res);

	if (ok)
		LOGDEBUG("%s(): built", __func__);

	return true;
}

void users_reload()
{
	PGconn *conn = dbconnect();

	K_WLOCK(users_list);
	users_root = free_ktree(users_root, NULL);
	k_list_transfer_to_head(users_store, users_list);
	K_WUNLOCK(users_list);

	users_fill(conn);

	PQfinish(conn);
}

// order by userid asc,workername asc,expirydate desc
static double workers_cmp(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_WORKERS(a)->userid) - (double)(DATA_WORKERS(b)->userid);
	if (c == 0.0) {
		c = strcmp(DATA_WORKERS(a)->workername, DATA_WORKERS(b)->workername);
		if (c == 0.0)
			c = tvdiff(&(DATA_WORKERS(b)->expirydate), &(DATA_WORKERS(a)->expirydate));
	}
	return c;
}

static bool workers_fill(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n, i;
	WORKERS *row;
	char *field;
	char *sel;
	int fields = 6;
	bool ok;

	LOGDEBUG("%s(): select", __func__);

	sel = "select "
		"userid,workername,difficultydefault,"
		"idlenotificationenabled,idlenotificationtime"
		HISTORYDATECONTROL
		",workerid from workers";
	res = PQexec(conn, sel);
	rescode = PQresultStatus(res);
	if (rescode != PGRES_TUPLES_OK) {
		PGLOGERR("Select", rescode, conn);
		PQclear(res);
		return false;
	}

	n = PQnfields(res);
	if (n != (fields + HISTORYDATECOUNT)) {
		LOGERR("%s(): Invalid field count - should be %d, but is %d",
			__func__, fields + HISTORYDATECOUNT, n);
		PQclear(res);
		return false;
	}

	n = PQntuples(res);
	LOGDEBUG("%s(): tree build count %d", __func__, n);
	ok = true;
	K_WLOCK(workers_list);
	for (i = 0; i < n; i++) {
		item = k_unlink_head(workers_list);
		row = DATA_WORKERS(item);

		PQ_GET_FLD(res, i, "userid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("userid", field, row->userid);

		PQ_GET_FLD(res, i, "workername", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("workername", field, row->workername);

		PQ_GET_FLD(res, i, "difficultydefault", field, ok);
		if (!ok)
			break;
		TXT_TO_INT("difficultydefault", field, row->difficultydefault);

		PQ_GET_FLD(res, i, "idlenotificationenabled", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("idlenotificationenabled", field, row->idlenotificationenabled);

		PQ_GET_FLD(res, i, "idlenotificationtime", field, ok);
		if (!ok)
			break;
		TXT_TO_INT("idlenotificationtime", field, row->idlenotificationtime);

		HISTORYDATEFLDS(res, i, row, ok);
		if (!ok)
			break;

		PQ_GET_FLD(res, i, "workerid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("workerid", field, row->workerid);

		workers_root = add_to_ktree(workers_root, item, workers_cmp);
		k_add_head(workers_store, item);
	}
	if (!ok)
		k_add_head(workers_list, item);

	K_WUNLOCK(workers_list);
	PQclear(res);

	if (ok)
		LOGDEBUG("%s(): built", __func__);

	return true;
}

void workers_reload()
{
	PGconn *conn = dbconnect();

	K_WLOCK(workers_list);
	workers_root = free_ktree(workers_root, NULL);
	k_list_transfer_to_head(workers_store, workers_list);
	K_WUNLOCK(workers_list);

	workers_fill(conn);

	PQfinish(conn);
}

// order by userid asc,paydate asc,payaddress asc
static double payments_cmp(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_PAYMENTS(a)->userid) - (double)(DATA_PAYMENTS(b)->userid);
	if (c == 0.0) {
		c = tvdiff(&(DATA_PAYMENTS(a)->paydate), &(DATA_PAYMENTS(b)->paydate));
		if (c == 0.0)
			c = strcmp(DATA_PAYMENTS(a)->payaddress, DATA_PAYMENTS(b)->payaddress);
	}
	return c;
}

static bool payments_fill(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n, i;
	PAYMENTS *row;
	char *params[1];
	int par;
	char *field;
	char *sel;
	int fields = 8;
	bool ok;

	LOGDEBUG("%s(): select", __func__);

	// TODO: handle selecting a subset, eg 20 per web page
	sel = "select "
		"userid,paydate,payaddress,originaltxn,amount,committxn,commitblockhash"
		HISTORYDATECONTROL
		",paymentid from payments where expirydate=$1";
	par = 0;
	params[par++] = tv_to_buf((tv_t *)(&default_expiry), NULL, 0);
	PARCHK(par, params);
	res = PQexecParams(conn, sel, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (rescode != PGRES_TUPLES_OK) {
		PGLOGERR("Select", rescode, conn);
		PQclear(res);
		return false;
	}

	n = PQnfields(res);
	if (n != (fields + HISTORYDATECOUNT)) {
		LOGERR("%s(): Invalid field count - should be %d, but is %d",
			__func__, fields + HISTORYDATECOUNT, n);
		PQclear(res);
		return false;
	}

	n = PQntuples(res);
	LOGDEBUG("%s(): tree build count %d", __func__, n);
	ok = true;
	K_WLOCK(payments_list);
	for (i = 0; i < n; i++) {
		item = k_unlink_head(payments_list);
		row = DATA_PAYMENTS(item);

		PQ_GET_FLD(res, i, "userid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("userid", field, row->userid);

		PQ_GET_FLD(res, i, "paydate", field, ok);
		if (!ok)
			break;
		TXT_TO_TV("paydate", field, row->paydate);

		PQ_GET_FLD(res, i, "payaddress", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("payaddress", field, row->payaddress);

		PQ_GET_FLD(res, i, "originaltxn", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("originaltxn", field, row->originaltxn);

		PQ_GET_FLD(res, i, "amount", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("amount", field, row->amount);

		PQ_GET_FLD(res, i, "committxn", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("committxn", field, row->committxn);

		PQ_GET_FLD(res, i, "commitblockhash", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("commitblockhash", field, row->commitblockhash);

		HISTORYDATEFLDS(res, i, row, ok);
		if (!ok)
			break;

		PQ_GET_FLD(res, i, "paymentid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("paymentid", field, row->paymentid);

		payments_root = add_to_ktree(payments_root, item, payments_cmp);
		k_add_head(payments_store, item);
	}
	if (!ok)
		k_add_head(payments_list, item);

	K_WUNLOCK(payments_list);
	PQclear(res);

	if (ok)
		LOGDEBUG("%s(): built", __func__);

	return true;
}

void payments_reload()
{
	PGconn *conn = dbconnect();

	K_WLOCK(payments_list);
	payments_root = free_ktree(payments_root, NULL);
	k_list_transfer_to_head(payments_store, payments_list);
	K_WUNLOCK(payments_list);

	payments_fill(conn);

	PQfinish(conn);
}

static void getdata()
{
	PGconn *conn = dbconnect();

	users_fill(conn);
	workers_fill(conn);
	payments_fill(conn);

	PQfinish(conn);
}

static PGconn *dbquit(PGconn *conn)
{
	if (conn != NULL)
		PQfinish(conn);
	return NULL;
}

/* Open the file in path, check if there is a pid in there that still exists
 * and if not, write the pid into that file. */
static bool write_pid(ckpool_t *ckp, const char *path, pid_t pid)
{
	struct stat statbuf;
	FILE *fp;
	int ret;

	if (!stat(path, &statbuf)) {
		int oldpid;

		LOGWARNING("File %s exists", path);
		fp = fopen(path, "r");
		if (!fp) {
			LOGEMERG("Failed to open file %s", path);
			return false;
		}
		ret = fscanf(fp, "%d", &oldpid);
		fclose(fp);
		if (ret == 1 && !(kill(oldpid, 0))) {
			if (!ckp->killold) {
				LOGEMERG("Process %s pid %d still exists, start ckpool with -k if you wish to kill it",
					 path, oldpid);
				return false;
			}
			if (kill(oldpid, 9)) {
				LOGEMERG("Unable to kill old process %s pid %d", path, oldpid);
				return false;
			}
			LOGWARNING("Killing off old process %s pid %d", path, oldpid);
		}
	}
	fp = fopen(path, "w");
	if (!fp) {
		LOGERR("Failed to open file %s", path);
		return false;
	}
	fprintf(fp, "%d", pid);
	fclose(fp);

	return true;
}

static void create_process_unixsock(proc_instance_t *pi)
{
	unixsock_t *us = &pi->us;

	us->path = strdup(pi->ckp->socket_dir);
	realloc_strcat(&us->path, pi->sockname);
	LOGDEBUG("Opening %s", us->path);
	us->sockd = open_unix_server(us->path);
	if (unlikely(us->sockd < 0))
		quit(1, "Failed to open %s socket", pi->sockname);
}

static void write_namepid(proc_instance_t *pi)
{
	char s[256];

	pi->pid = getpid();
	sprintf(s, "%s%s.pid", pi->ckp->socket_dir, pi->processname);
	if (!write_pid(pi->ckp, s, pi->pid))
		quit(1, "Failed to write %s pid %d", pi->processname, pi->pid);
}

static void rm_namepid(proc_instance_t *pi)
{
	char s[256];

	sprintf(s, "%s%s.pid", pi->ckp->socket_dir, pi->processname);
	unlink(s);

}

static void clean_up(ckpool_t *ckp)
{
	rm_namepid(&ckp->main);
	dealloc(ckp->socket_dir);
	fclose(ckp->logfp);
}

static void setup_data()
{
	transfer_list = k_new_list("Transfer", sizeof(TRANSFER), ALLOC_TRANSFER, LIMIT_TRANSFER, true);
	transfer_store = k_new_store(transfer_list);
	transfer_root = new_ktree();

	users_list = k_new_list("Users", sizeof(USERS), ALLOC_USERS, LIMIT_USERS, true);
	users_store = k_new_store(users_list);
	users_root = new_ktree();

	workers_list = k_new_list("Workers", sizeof(WORKERS), ALLOC_WORKERS, LIMIT_WORKERS, true);
	workers_store = k_new_store(workers_list);
	workers_root = new_ktree();

	payments_list = k_new_list("Payments", sizeof(PAYMENTS), ALLOC_PAYMENTS, LIMIT_PAYMENTS, true);
	payments_store = k_new_store(payments_list);
	payments_root = new_ktree();

	idcontrol_list = k_new_list("IDControl", sizeof(IDCONTROL), ALLOC_IDCONTROL, LIMIT_IDCONTROL, true);
	idcontrol_store = k_new_store(idcontrol_list);

	getdata();
}

static double transfer_cmp(K_ITEM *a, K_ITEM *b)
{
	double c = strcmp(DATA_TRANSFER(a)->name, DATA_TRANSFER(b)->name);
	return c;
}

static K_ITEM *find_transfer(char *name)
{
	TRANSFER transfer;
	K_TREE_CTX ctx;
	K_ITEM look;

	STRNCPY(transfer.name, name);
	look.data = (void *)(&transfer);
	return find_in_ktree(transfer_root, &look, transfer_cmp, &ctx);
}

static K_ITEM *require_name(char *name, int len, char *patt, char *reply, size_t siz)
{
	K_ITEM *item;
	char *value;
	regex_t re;
	int ret;

	item = find_transfer(name);
	if (!item) {
		snprintf(reply, siz, "failed.missing %s", name);
		return NULL;
	}

	value = DATA_TRANSFER(item)->value;
	if (!*value || (int)strlen(value) < len) {
		snprintf(reply, siz, "failed.short %s", name);
		return NULL;
	}

	if (patt) {
		if (regcomp(&re, patt, REG_NOSUB) != 0) {
			snprintf(reply, siz, "failed.REC %s", name);
			return NULL;
		}

		ret = regexec(&re, value, (size_t)0, NULL, 0);
		regfree(&re);

		if (ret != 0) {
			snprintf(reply, siz, "failed.invalid %s", name);
			return NULL;
		}
	}

	return item;
}

static char *cmd_adduser(char *id, char *by, char *code, char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);

	K_ITEM *i_username, *i_emailaddress, *i_passwordhash;
	PGconn *conn;
	bool ok;

	i_username = require_name("username", 3, (char *)userpatt, reply, siz);
	if (!i_username)
		return strdup(reply);

	i_emailaddress = require_name("emailaddress", 7, (char *)mailpatt, reply, siz);
	if (!i_emailaddress)
		return strdup(reply);

	i_passwordhash = require_name("passwordhash", 64, (char *)hashpatt, reply, siz);
	if (!i_passwordhash)
		return strdup(reply);

	conn = dbconnect();
	ok = users_add(conn, DATA_TRANSFER(i_username)->value,
				DATA_TRANSFER(i_emailaddress)->value,
				DATA_TRANSFER(i_passwordhash)->value,
				by, code, inet);
	PQfinish(conn);

	if (!ok) {
		STRNCPY(reply, "failed.DBE");
		return strdup(reply);
	}

	LOGDEBUG("%s.added.%s", id, DATA_TRANSFER(i_username)->value);
	snprintf(reply, siz, "added.%s", DATA_TRANSFER(i_username)->value);

	return strdup(reply);
}

static char *cmd_chkpass(char *id, __maybe_unused char *by, __maybe_unused char *code, __maybe_unused char *inet)
{
	K_ITEM *i_username, *i_passwordhash, *look, *find;
	K_TREE_CTX ctx[1];
	USERS *row;
	char reply[1024] = "";
	size_t siz = sizeof(reply);
	bool ok;

	i_username = require_name("username", 3, (char *)userpatt, reply, siz);
	if (!i_username)
		return strdup(reply);

	i_passwordhash = require_name("passwordhash", 64, (char *)hashpatt, reply, siz);
	if (!i_passwordhash)
		return strdup(reply);

	K_WLOCK(users_list);
	look = k_unlink_head(users_list);
	K_WUNLOCK(users_list);

	row = DATA_USERS(look);

	STRNCPY(row->username, DATA_TRANSFER(i_username)->value);
	row->expirydate.tv_sec = default_expiry.tv_sec;
	row->expirydate.tv_usec = default_expiry.tv_usec;

	find = find_in_ktree(users_root, look, users_cmp, ctx);

	if (!find)
		ok = false;
	else {
		if (strcasecmp(DATA_TRANSFER(i_passwordhash)->value, DATA_USERS(find)->passwordhash) == 0)
			ok = true;
		else
			ok = false;
	}

	K_WLOCK(users_list);
	k_add_head(users_list, look);
	K_WUNLOCK(users_list);

	if (!ok)
		return strdup("bad");

	LOGDEBUG("%s.login.%s", id, DATA_TRANSFER(i_username)->value);
	return strdup("ok");
}

static char *cmd_poolstats(char *id, __maybe_unused char *by, __maybe_unused char *code, __maybe_unused char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);

	LOGDEBUG("%s.stats.ok", id);
	printf("%s.stats", id);
	snprintf(reply, siz, "stats.ok");
	return strdup(reply);
}

static char *cmd_newid(char *id, char *by, char *code, char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);
	K_ITEM *i_idname, *i_idvalue, *look;
	IDCONTROL *row;
	char *params[10];
	int par;
	bool ok = false;
	ExecStatusType rescode;
	PGresult *res;
	PGconn *conn;
	char *ins;
	int n;

	LOGDEBUG("%s(): add", __func__);

	i_idname = require_name("idname", 3, (char *)idpatt, reply, siz);
	if (!i_idname)
		return strdup(reply);

	i_idvalue = require_name("idvalue", 1, (char *)intpatt, reply, siz);
	if (!i_idvalue)
		return strdup(reply);

	K_WLOCK(idcontrol_list);
	look = k_unlink_head(idcontrol_list);
	K_WUNLOCK(idcontrol_list);

	row = DATA_IDCONTROL(look);

	STRNCPY(row->idname, DATA_TRANSFER(i_idname)->value);
	TXT_TO_BIGINT("idvalue", DATA_TRANSFER(i_idvalue)->value, row->lastid);
	MODIFYDATEINIT(row, by, code, inet);

	par = 0;
	params[par++] = str_to_buf(row->idname, NULL, 0);
	params[par++] = bigint_to_buf(row->lastid, NULL, 0);
	MODIFYDATEPARAMS(params, par, row);
	PARCHK(par, params);

	ins = "insert into idcontrol "
		"(idname,lastid" MODIFYDATECONTROL ") values (" PQPARAM10 ")";

	conn = dbconnect();

	res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (rescode != PGRES_COMMAND_OK) {
		PGLOGERR("Insert", rescode, conn);
		goto foil;
	}

	ok = true;
foil:
	PQclear(res);
	PQfinish(conn);
	for (n = 0; n < par; n++)
		free(params[n]);

	K_WLOCK(idcontrol_list);
	k_add_head(idcontrol_list, look);
	K_WUNLOCK(idcontrol_list);

	if (!ok) {
		snprintf(reply, siz, "failed.DBE");
		return strdup(reply);
	}

	LOGDEBUG("%s.added.%s", id, DATA_TRANSFER(i_idname)->value);
	snprintf(reply, siz, "added.%s", DATA_TRANSFER(i_idname)->value);
	return strdup(reply);
}

static char *cmd_payments(char *id, __maybe_unused char *by, __maybe_unused char *code, __maybe_unused char *inet)
{
	K_ITEM *i_username, *ulook, *plook, *ufind, *pfind;
	K_TREE_CTX ctx[1];
	USERS *urow;
	PAYMENTS *prow;
	char reply[1024] = "";
	char tmp[1024];
	size_t siz = sizeof(reply);
	char *buf;
	size_t len, off;
	int rows;

	i_username = require_name("username", 3, (char *)userpatt, reply, siz);
	if (!i_username)
		return strdup(reply);

	K_WLOCK(users_list);
	ulook = k_unlink_head(users_list);
	K_WUNLOCK(users_list);
	urow = DATA_USERS(ulook);
	STRNCPY(urow->username, DATA_TRANSFER(i_username)->value);
	urow->expirydate.tv_sec = default_expiry.tv_sec;
	urow->expirydate.tv_usec = default_expiry.tv_usec;
	ufind = find_in_ktree(users_root, ulook, users_cmp, ctx);
	if (!ufind) {
		K_WLOCK(users_list);
		k_add_head(users_list, ulook);
		K_WUNLOCK(users_list);
		return strdup("bad");
	}

	K_WLOCK(payments_list);
	plook = k_unlink_head(payments_list);
	K_WUNLOCK(payments_list);
	prow = DATA_PAYMENTS(plook);
	prow->userid = DATA_USERS(ufind)->userid;
	prow->paydate.tv_sec = 0;
	prow->paydate.tv_usec = 0;
	pfind = find_after_in_ktree(payments_root, plook, payments_cmp, ctx);
	len = 1024;
	buf = malloc(len);
	if (!buf)
		quithere(1, "malloc buf (%d) OOM", (int)len);
	strcpy(buf, "ok.");
	off = strlen(buf);
	rows = 0;
	while (pfind && DATA_PAYMENTS(pfind)->userid == DATA_USERS(ufind)->userid) {
		tv_to_buf(&(DATA_PAYMENTS(pfind)->paydate), reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "paydate%d=%s%c", rows, reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		str_to_buf(DATA_PAYMENTS(pfind)->payaddress, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "payaddress%d=%s%c", rows, reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		bigint_to_buf(DATA_PAYMENTS(pfind)->amount, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "amount%d=%s%c", rows, reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		rows++;
		pfind = next_in_ktree(ctx);
	}
	snprintf(tmp, sizeof(tmp), "rows=%d", rows);
	APPEND_REALLOC(buf, off, len, tmp);

	K_WLOCK(users_list);
	k_add_head(users_list, ulook);
	K_WUNLOCK(users_list);

	K_WLOCK(payments_list);
	k_add_head(payments_list, plook);
	K_WUNLOCK(payments_list);

	LOGDEBUG("%s.payments.%s", id, DATA_TRANSFER(i_username)->value);
	return buf;
}

enum cmd_values {
	CMD_UNSET,
	CMD_REPLY, // Means something was wrong - send back reply
	CMD_SHUTDOWN,
	CMD_PING,
	CMD_ADDUSER,
	CMD_CHKPASS,
	CMD_POOLSTAT,
	CMD_NEWID,
	CMD_PAYMENTS,
	CMD_END
};

#define ACCESS_POOL	"p"
#define ACCESS_SYSTEM	"s"
#define ACCESS_WEB	"w"
#define ACCESS_PROXY	"x"

static struct CMDS {
	enum cmd_values cmd_val;
	char *cmd_str;
	char *(*func)(char *, char *, char *, char *);
	char *access;
} cmds[] = {
	{ CMD_SHUTDOWN,	"shutdown",	NULL,		ACCESS_SYSTEM },
	{ CMD_PING,	"ping",		NULL,		ACCESS_SYSTEM ACCESS_WEB },
//	{ CMD_LOGSHARE,	"logshares",	cmd_logshares,	ACCESS_POOL },
	{ CMD_ADDUSER,	"adduser",	cmd_adduser,	ACCESS_WEB },
	{ CMD_CHKPASS,	"chkpass",	cmd_chkpass,	ACCESS_WEB },
	{ CMD_POOLSTAT,	"poolstats",	cmd_poolstats,	ACCESS_WEB },
	{ CMD_NEWID,	"newid",	cmd_newid,	ACCESS_SYSTEM },
	{ CMD_PAYMENTS,	"payments",	cmd_payments,	ACCESS_WEB },
	{ CMD_END,	NULL,		NULL,		NULL }
};

static enum cmd_values breakdown(char *buf, int *which_cmds, char *id)
{
	K_TREE_CTX ctx[1];
	K_ITEM *item;
	char *copy, *cmd, *data, *next, *eq;

	*which_cmds = CMD_UNSET;
	copy = strdup(buf);
	cmd = strchr(copy, '.');
	if (!cmd || !*cmd) {
		STRNCPYSIZ(id, copy, ID_SIZ);
		LOGINFO("Listener received invalid message: '%s'", buf);
		free(copy);
		return CMD_REPLY;
	}

	*(cmd++) = '\0';
	STRNCPYSIZ(id, copy, ID_SIZ);
	data = strchr(cmd, '.');
	if (data)
		*(data++) = '\0';

	for (*which_cmds = 0; cmds[*which_cmds].cmd_val != CMD_END; (*which_cmds)++) {
		if (strcasecmp(cmd, cmds[*which_cmds].cmd_str) == 0)
			break;
	}

	if (cmds[*which_cmds].cmd_val == CMD_END) {
		LOGINFO("Listener received unknown command: '%s'", buf);
		free(copy);
		return CMD_REPLY;
	}

	next = data;
	K_WLOCK(transfer_list);
	while (next && *next) {
		data = next;
		next = strchr(data, 0x02);
		if (next)
			*(next++) = '\0';

		eq = strchr(data, '=');
		if (!eq)
			eq = "";
		else
			*(eq++) = '\0';

		item = k_unlink_head(transfer_list);
		STRNCPY(DATA_TRANSFER(item)->name, data);
		STRNCPY(DATA_TRANSFER(item)->value, eq);

		if (find_in_ktree(transfer_root, item, transfer_cmp, ctx))
			k_add_head(transfer_list, item);
		else {
			transfer_root = add_to_ktree(transfer_root, item, transfer_cmp);
			k_add_head(transfer_store, item);
		}
	}
	K_WUNLOCK(transfer_list);

	free(copy);
	return cmds[*which_cmds].cmd_val;
}

// TODO: equivalent of api_allow
static void *listener(void *arg)
{
	proc_instance_t *pi = (proc_instance_t *)arg;
	unixsock_t *us = &pi->us;
	char *end, *ans, *rep, *buf = NULL;
	char id[ID_SIZ+1], reply[1024+1];
	enum cmd_values cmd;
	int sockd, which_cmds;
	size_t siz;
	tv_t now;

	rename_proc(pi->sockname);

	setup_data();

	while (true) {
		dealloc(buf);
		sockd = accept(us->sockd, NULL, NULL);
		if (sockd < 0) {
			LOGERR("Failed to accept on socket in listener");
			break;
		}

		cmd = CMD_UNSET;

		buf = recv_unix_msg(sockd);
		// Once we've read the message
		setnow(&now);
		if (buf) {
			end = buf + strlen(buf) - 1;
			// strip trailing \n and \r
			while (end >= buf && (*end == '\n' || *end == '\r'))
				*(end--) = '\0';
		}
		if (!buf || !*buf) {
			// An empty message wont get a reply
			if (!buf)
				LOGWARNING("Failed to get message in listener");
			else
				LOGWARNING("Empty message in listener");
		} else {
			cmd = breakdown(buf, &which_cmds, id);
			switch (cmd) {
				case CMD_REPLY:
					snprintf(reply, sizeof(reply), "%s.%ld.?", id, now.tv_sec);
					send_unix_msg(sockd, reply);
					break;
				case CMD_SHUTDOWN:
					LOGWARNING("Listener received shutdown message, terminating ckdb");
					snprintf(reply, sizeof(reply), "%s.%d.exiting", id, (int)now.tv_sec);
					send_unix_msg(sockd, reply);
					break;
				case CMD_PING:
					LOGDEBUG("Listener received ping request");
					snprintf(reply, sizeof(reply), "%s.%ld.pong", id, now.tv_sec);
					send_unix_msg(sockd, reply);
					break;
				default:
					// TODO: optionally get by/inet from transfer?
					ans = cmds[which_cmds].func(id, (char *)"code",
								    (char *)__func__,
								    (char *)"127.0.0.1");

					siz = strlen(ans) + strlen(id) + 32;
					rep = malloc(siz);
					snprintf(rep, siz, "%s.%ld.%s", id, now.tv_sec, ans);
					free(ans);
					ans = NULL;
					send_unix_msg(sockd, rep);
					free(rep);
					rep = NULL;
					break;
			}
		}
		close(sockd);

		if (cmd == CMD_SHUTDOWN)
			break;

		K_WLOCK(transfer_list);
		transfer_root = free_ktree(transfer_root, NULL);
		k_list_transfer_to_head(transfer_store, transfer_list);
		K_WUNLOCK(transfer_list);
	}

	dealloc(buf);
	close_unix_socket(us->sockd, us->path);
	return NULL;
}

int main(int argc, char **argv)
{
	struct sigaction handler;
	char buf[512];
	ckpool_t ckp;
	int c, ret;
	char *kill;

	memset(&ckp, 0, sizeof(ckp));
	ckp.loglevel = LOG_NOTICE;

	while ((c = getopt(argc, argv, "c:kl:n:p:s:u:")) != -1) {
		switch(c) {
			case 'c':
				ckp.config = optarg;
				break;
			case 'k':
				ckp.killold = true;
				break;
			case 'n':
				ckp.name = strdup(optarg);
				break;
			case 'l':
				ckp.loglevel = atoi(optarg);
				if (ckp.loglevel < LOG_EMERG || ckp.loglevel > LOG_DEBUG) {
					quit(1, "Invalid loglevel (range %d - %d): %d",
					     LOG_EMERG, LOG_DEBUG, ckp.loglevel);
				}
				break;
			case 's':
				ckp.socket_dir = strdup(optarg);
				break;
			case 'u':
				db_user = strdup(optarg);
				kill = optarg;
				while (*kill)
					*(kill++) = ' ';
				break;
			case 'p':
				db_pass = strdup(optarg);
				kill = optarg;
				if (*kill)
					*(kill++) = ' ';
				while (*kill)
					*(kill++) = '\0';
				break;
		}
	}
//	if (!db_pass)
//		zzz
	if (!db_user)
		db_user = "postgres";
	if (!ckp.name)
		ckp.name = "ckdb";
	snprintf(buf, 15, "%s", ckp.name);
	prctl(PR_SET_NAME, buf, 0, 0, 0);
	memset(buf, 0, 15);

	if (!ckp.config) {
		ckp.config = strdup(ckp.name);
		realloc_strcat(&ckp.config, ".conf");
	}

	if (!ckp.socket_dir) {
//		ckp.socket_dir = strdup("/tmp/");
		ckp.socket_dir = strdup("/opt/");
		realloc_strcat(&ckp.socket_dir, ckp.name);
	}
	trail_slash(&ckp.socket_dir);

	/* Ignore sigpipe */
	signal(SIGPIPE, SIG_IGN);

	ret = mkdir(ckp.socket_dir, 0700);
	if (ret && errno != EEXIST)
		quit(1, "Failed to make directory %s", ckp.socket_dir);

//	parse_config(&ckp);

	if (!ckp.logdir)
		ckp.logdir = strdup("logs");

	/* Create the log directory */
	trail_slash(&ckp.logdir);
	ret = mkdir(ckp.logdir, 0700);
	if (ret && errno != EEXIST)
		quit(1, "Failed to make log directory %s", ckp.logdir);

	/* Create the logfile */
	sprintf(buf, "%s%s.log", ckp.logdir, ckp.name);
	ckp.logfp = fopen(buf, "a");
	if (!ckp.logfp)
		quit(1, "Failed to open log file %s", buf);
	ckp.logfd = fileno(ckp.logfp);

	ckp.main.ckp = &ckp;
	ckp.main.processname = strdup("main");
	ckp.main.sockname = strdup("listener");
	write_namepid(&ckp.main);
	create_process_unixsock(&ckp.main);

	srand((unsigned int)time(NULL));
	create_pthread(&ckp.pth_listener, listener, &ckp.main);

	handler.sa_flags = 0;
	sigemptyset(&handler.sa_mask);
	sigaction(SIGTERM, &handler, NULL);
	sigaction(SIGINT, &handler, NULL);

	/* Shutdown from here if the listener is sent a shutdown message */
	join_pthread(ckp.pth_listener);

	clean_up(&ckp);

	return 0;
}
