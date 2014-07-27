/*
 * Copyright 1995-2014 Andrew Smith
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
#include <fenv.h>
#include <jansson.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <regex.h>
#ifdef HAVE_LIBPQ_FE_H
#include <libpq-fe.h>
#elif defined (HAVE_POSTGRESQL_LIBPQ_FE_H)
#include <postgresql/libpq-fe.h>
#endif

#include "ckpool.h"
#include "libckpool.h"

#include "klist.h"
#include "ktree.h"

/* TODO: any tree/list accessed in new threads needs
 * to ensure all code using those trees/lists use locks
 * This code's lock implementation is equivalent to table level locking
 * Consider adding row level locking (a per kitem usage count) if needed
 * */

#define DB_VLOCK "1"
#define DB_VERSION "0.4"

#define WHERE_FFL " - from %s %s() line %d"
#define WHERE_FFL_HERE __FILE__, __func__, __LINE__
#define WHERE_FFL_PASS file, func, line
#define WHERE_FFL_ARGS __maybe_unused const char *file, \
			__maybe_unused const char *func, \
			__maybe_unused const int line

#define STRINT(x) STRINT2(x)
#define STRINT2(x) #x

// So they can fit into a 1 byte flag field
#define TRUE_STR "Y"
#define FALSE_STR "N"

#define coinbase1height(_cb1) _coinbase1height(_cb1, WHERE_FFL_HERE)
#define cmp_height(_cb1a, _cb1b) _cmp_height(_cb1a, _cb1b, WHERE_FFL_HERE)

static char *EMPTY = "";
static char *db_user;
static char *db_pass;

// size limit on the command string
#define CMD_SIZ 31
#define ID_SIZ 31

// size to allocate for pgsql text and display (bigger than needed)
#define DATE_BUFSIZ (63+1)
#define CDATE_BUFSIZ (127+1)
#define BIGINT_BUFSIZ (63+1)
#define INT_BUFSIZ (63+1)
#define DOUBLE_BUFSIZ (63+1)

#define TXT_BIG 256
#define TXT_MED 128
#define TXT_SML 64
#define TXT_FLAG 1

// TAB
#define FLDSEP 0x09

// Ensure long long and int64_t are both 8 bytes (and thus also the same)
#define ASSERT1(condition) __maybe_unused static char sizeof_longlong_must_be_8[(condition)?1:-1]
#define ASSERT2(condition) __maybe_unused static char sizeof_int64_t_must_be_8[(condition)?1:-1]
ASSERT1(sizeof(long long) == 8);
ASSERT2(sizeof(int64_t) == 8);

#define PGOK(_res) ((_res) == PGRES_COMMAND_OK || \
			(_res) == PGRES_TUPLES_OK || \
			(_res) == PGRES_EMPTY_QUERY)

static char *pqerrmsg(PGconn *conn)
{
	char *ptr, *buf = strdup(PQerrorMessage(conn));

	ptr = buf + strlen(buf) - 1;
	while (ptr >= buf && (*ptr == '\n' || *ptr == '\r'))
		*(ptr--) = '\0';
	while (--ptr >= buf) {
		if (*ptr == '\n' || *ptr == '\r' || *ptr == '\t')
			*ptr = ' ';
	}
	return buf;
}

#define PGLOG(__LOG, __str, __rescode, __conn) do { \
		char *__buf = pqerrmsg(__conn); \
		__LOG("%s(): %s failed (%d) '%s'", __func__, \
			__str, (int)rescode, __buf); \
		free(__buf); \
	} while (0)

#define PGLOGERR(_str, _rescode, _conn) PGLOG(LOGERR, _str, _rescode, _conn)
#define PGLOGEMERG(_str, _rescode, _conn) PGLOG(LOGEMERG, _str, _rescode, _conn)

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

#define AR_SIZ 1024

#define APPEND_REALLOC_INIT(_buf, _off, _len) do { \
		_len = AR_SIZ; \
		(_buf) = malloc(_len); \
		if (!(_buf)) \
			quithere(1, "malloc (%d) OOM", (int)_len); \
		(_buf)[0] = '\0'; \
		_off = 0; \
	} while(0)

#define APPEND_REALLOC(_dst, _dstoff, _dstsiz, _src) do { \
		size_t _newlen, _srclen = strlen(_src); \
		_newlen = (_dstoff) + _srclen; \
		if (_newlen >= (_dstsiz)) { \
			_dstsiz = _newlen + AR_SIZ - (_newlen % AR_SIZ); \
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
	TYPE_TVS,
	TYPE_CTV,
	TYPE_BLOB,
	TYPE_DOUBLE
};

#define TXT_TO_STR(__nam, __fld, __data) txt_to_str(__nam, __fld, (__data), sizeof(__data))
#define TXT_TO_BIGINT(__nam, __fld, __data) txt_to_bigint(__nam, __fld, &(__data), sizeof(__data))
#define TXT_TO_INT(__nam, __fld, __data) txt_to_int(__nam, __fld, &(__data), sizeof(__data))
#define TXT_TO_TV(__nam, __fld, __data) txt_to_tv(__nam, __fld, &(__data), sizeof(__data))
#define TXT_TO_CTV(__nam, __fld, __data) txt_to_ctv(__nam, __fld, &(__data), sizeof(__data))
#define TXT_TO_BLOB(__nam, __fld, __data) txt_to_blob(__nam, __fld, __data)
#define TXT_TO_DOUBLE(__nam, __fld, __data) txt_to_double(__nam, __fld, &(__data), sizeof(__data))

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

// 31-Dec-9999 23:59:59+00
#define DATE_S_EOT 253402300799L
#define DATE_uS_EOT 0L
static const tv_t date_eot = { DATE_S_EOT, DATE_uS_EOT };

#define HISTORYDATEINIT(_row, _now, _by, _code, _inet) do { \
		_row->createdate.tv_sec = (_now)->tv_sec; \
		_row->createdate.tv_usec = (_now)->tv_usec; \
		STRNCPY(_row->createby, _by); \
		STRNCPY(_row->createcode, _code); \
		STRNCPY(_row->createinet, _inet); \
		_row->expirydate.tv_sec = default_expiry.tv_sec; \
		_row->expirydate.tv_usec = default_expiry.tv_usec; \
	} while (0)

// Override _row defaults if transfer fields are present
#define HISTORYDATETRANSFER(_row) do { \
		K_ITEM *item; \
		item = optional_name("createdate", 10, NULL); \
		if (item) { \
			long sec, usec; \
			int n; \
			n = sscanf(DATA_TRANSFER(item)->data, "%ld,%ld", &sec, &usec); \
			if (n > 0) { \
				_row->createdate.tv_sec = (time_t)sec; \
				if (n > 1) \
					_row->createdate.tv_usec = usec / 1000; \
				else \
					_row->createdate.tv_usec = 0L; \
			} \
		} \
		item = optional_name("createby", 1, NULL); \
		if (item) \
			STRNCPY(_row->createby, DATA_TRANSFER(item)->data); \
		item = optional_name("createcode", 1, NULL); \
		if (item) \
			STRNCPY(_row->createcode, DATA_TRANSFER(item)->data); \
		item = optional_name("createinet", 1, NULL); \
		if (item) \
			STRNCPY(_row->createinet, DATA_TRANSFER(item)->data); \
	} while (0)

// MODIFY FIELDS
#define MODIFYDATECONTROL ",createdate,createby,createcode,createinet" \
			  ",modifydate,modifyby,modifycode,modifyinet"

#define MODIFYDATECOUNT 8
#define MODIFYUPDATECOUNT 4

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

#define MODIFYUPDATEPARAMS(_params, _mod_pos, _row) do { \
		_params[_mod_pos++] = tv_to_buf(&(_row->modifydate), NULL, 0); \
		_params[_mod_pos++] = str_to_buf(_row->modifyby, NULL, 0); \
		_params[_mod_pos++] = str_to_buf(_row->modifycode, NULL, 0); \
		_params[_mod_pos++] = str_to_buf(_row->modifyinet, NULL, 0); \
	} while (0)

#define MODIFYDATEINIT(_row, _now, _by, _code, _inet) do { \
		_row->createdate.tv_sec = (_now)->tv_sec; \
		_row->createdate.tv_usec = (_now)->tv_usec; \
		STRNCPY(_row->createby, _by); \
		STRNCPY(_row->createcode, _code); \
		STRNCPY(_row->createinet, _inet); \
		_row->modifydate.tv_sec = 0; \
		_row->modifydate.tv_usec = 0; \
		_row->modifyby[0] = '\0'; \
		_row->modifycode[0] = '\0'; \
		_row->modifyinet[0] = '\0'; \
	} while (0)

#define MODIFYUPDATE(_row, _now, _by, _code, _inet) do { \
		_row->modifydate.tv_sec = (_now)->tv_sec; \
		_row->modifydate.tv_usec = (_now)->tv_usec; \
		STRNCPY(_row->modifyby, _by); \
		STRNCPY(_row->modifycode, _code); \
		STRNCPY(_row->modifyinet, _inet); \
	} while (0)

// SIMPLE FIELDS
#define SIMPLEDATECONTROL ",createdate,createby,createcode,createinet"
#define SIMPLEDATECOUNT 4

#define SIMPLEDATEFLDS(_res, _row, _data, _ok) do { \
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
	} while (0)

#define SIMPLEDATECONTROLFIELDS \
	tv_t createdate; \
	char createby[TXT_SML+1]; \
	char createcode[TXT_MED+1]; \
	char createinet[TXT_MED+1]

#define SIMPLEDATEPARAMS(_params, _his_pos, _row) do { \
		_params[_his_pos++] = tv_to_buf(&(_row->createdate), NULL, 0); \
		_params[_his_pos++] = str_to_buf(_row->createby, NULL, 0); \
		_params[_his_pos++] = str_to_buf(_row->createcode, NULL, 0); \
		_params[_his_pos++] = str_to_buf(_row->createinet, NULL, 0); \
	} while (0)

#define SIMPLEDATEINIT(_row, _now, _by, _code, _inet) do { \
		_row->createdate.tv_sec = (_now)->tv_sec; \
		_row->createdate.tv_usec = (_now)->tv_usec; \
		STRNCPY(_row->createby, _by); \
		STRNCPY(_row->createcode, _code); \
		STRNCPY(_row->createinet, _inet); \
	} while (0)

#define SIMPLEDATEDEFAULT(_row, _now ) do { \
		_row->createdate.tv_sec = (_now)->tv_sec; \
		_row->createdate.tv_usec = (_now)->tv_usec; \
		STRNCPY(_row->createby, (char *)"code"); \
		STRNCPY(_row->createcode, (char *)__func__); \
		STRNCPY(_row->createinet, (char *)"127.0.0.1"); \
	} while (0)

// Override _row defaults if transfer fields are present
#define SIMPLEDATETRANSFER(_row) do { \
		K_ITEM *item; \
		item = optional_name("createdate", 10, NULL); \
		if (item) { \
			long sec, usec; \
			int n; \
			n = sscanf(DATA_TRANSFER(item)->data, "%ld,%ld", &sec, &usec); \
			if (n > 0) { \
				_row->createdate.tv_sec = (time_t)sec; \
				if (n > 1) \
					_row->createdate.tv_usec = usec; \
				else \
					_row->createdate.tv_usec = 0L; \
			} \
		} \
		item = optional_name("createby", 1, NULL); \
		if (item) \
			STRNCPY(_row->createby, DATA_TRANSFER(item)->data); \
		item = optional_name("createcode", 1, NULL); \
		if (item) \
			STRNCPY(_row->createcode, DATA_TRANSFER(item)->data); \
		item = optional_name("createinet", 1, NULL); \
		if (item) \
			STRNCPY(_row->createinet, DATA_TRANSFER(item)->data); \
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
#define PQPARAM26 "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26"

#define PARCHK(_par, _params) do { \
		if (_par != (int)(sizeof(_params)/sizeof(_params[0]))) { \
			quithere(1, "params[] usage (%d) != size (%d)", \
					_par, (int)(sizeof(_params)/sizeof(_params[0]))); \
		} \
	} while (0)

#define PARCHKVAL(_par, _val, _params) do { \
		if (_par != _val) { \
			quithere(1, "params[] usage (%d) != expected (%d)", \
					_par, _val); \
		} \
		if (_val > (int)(sizeof(_params)/sizeof(_params[0]))) { \
			quithere(1, "params[] usage (%d) > size (%d)", \
					_val, (int)(sizeof(_params)/sizeof(_params[0]))); \
		} \
	} while (0)

// Tell the summarizer data load is complete
static bool summarizer_go = false;
// Tell the summarizer to die
static bool summarizer_die = false;

static const char *userpatt = "^[!-~]*$"; // no spaces
static const char *mailpatt = "^[A-Za-z0-9_-][A-Za-z0-9_\\.-]*@[A-Za-z0-9][A-Za-z0-9\\.]*[A-Za-z0-9]$";
static const char *idpatt = "^[_A-Za-z][_A-Za-z0-9]*$";
static const char *intpatt = "^[0-9][0-9]*$";
static const char *hashpatt = "^[A-Fa-f0-9]*$";

#define JSON_TRANSFER "json="
#define JSON_TRANSFER_LEN (sizeof(JSON_TRANSFER)-1)

// Methods for sharelog (common function for all)
#define STR_WORKINFO "workinfo"
#define STR_SHARES "shares"
#define STR_SHAREERRORS "shareerror"
#define STR_AGEWORKINFO "ageworkinfo"

// TRANSFER
#define NAME_SIZE 63
#define VALUE_SIZE 1023
typedef struct transfer {
	char name[NAME_SIZE+1];
	char value[VALUE_SIZE+1];
	char *data;
} TRANSFER;

#define ALLOC_TRANSFER 64
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
static K_TREE *userid_root;
static K_LIST *users_list;
static K_STORE *users_store;

/* TODO: for account settings - but do we want manual/auto payouts?
// USERACCOUNTS
typedef struct useraccounts {
	int64_t userid;
	int64_t payoutlimit;
	char autopayout[TXT_FLG+1];
	HISTORYDATECONTROLFIELDS;
} USERACCOUNTS;

#define ALLOC_USERACCOUNTS 1024
#define LIMIT_USERACCOUNTS 0
#define DATA_USERACCOUNTS(_item) ((USERACCOUNTS *)(_item->data))

static K_TREE *useraccounts_root;
static K_LIST *useraccounts_list;
static K_STORE *useraccounts_store;
*/

// WORKERS
typedef struct workers {
	int64_t workerid;
	int64_t userid;
	char workername[TXT_BIG+1]; // includes username
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

#define DIFFICULTYDEFAULT_MIN 10
#define DIFFICULTYDEFAULT_MAX 1000000
#define DIFFICULTYDEFAULT_DEF DIFFICULTYDEFAULT_MIN
#define DIFFICULTYDEFAULT_DEF_STR STRINT(DIFFICULTYDEFAULT_DEF)
#define IDLENOTIFICATIONENABLED "y"
#define IDLENOTIFICATIONDISABLED " "
#define IDLENOTIFICATIONENABLED_DEF IDLENOTIFICATIONDISABLED
#define IDLENOTIFICATIONTIME_MIN 10
#define IDLENOTIFICATIONTIME_MAX 60
#define IDLENOTIFICATIONTIME_DEF IDLENOTIFICATIONTIME_MIN
#define IDLENOTIFICATIONTIME_DEF_STR STRINT(IDLENOTIFICATIONTIME_DEF)

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

// ACCOUNTADJUSTMENT
typedef struct accountadjustment {
	int64_t userid;
	char authority[TXT_BIG+1];
	char *reason;
	int64_t amount;
	HISTORYDATECONTROLFIELDS;
} ACCOUNTADJUSTMENT;

#define ALLOC_ACCOUNTADJUSTMENT 100
#define LIMIT_ACCOUNTADJUSTMENT 0
#define DATA_ACCOUNTADJUSTMENT(_item) ((ACCOUNTADJUSTMENT *)(_item->data))

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
	char *optionvalue;
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
*/

// TODO: aging/discarding workinfo,shares
// WORKINFO workinfo.id.json={...}
typedef struct workinfo {
	int64_t workinfoid;
	char poolinstance[TXT_BIG+1];
	char *transactiontree;
	char *merklehash;
	char prevhash[TXT_BIG+1];
	char coinbase1[TXT_BIG+1];
	char coinbase2[TXT_BIG+1];
	char version[TXT_SML+1];
	char bits[TXT_SML+1];
	char ntime[TXT_SML+1];
	int64_t reward;
	HISTORYDATECONTROLFIELDS;
} WORKINFO;

// ~10 hrs
#define ALLOC_WORKINFO 1400
#define LIMIT_WORKINFO 0
#define DATA_WORKINFO(_item) ((WORKINFO *)(_item->data))

static K_TREE *workinfo_root;
// created during data load then destroyed since not needed later
static K_TREE *workinfo_height_root;
static K_LIST *workinfo_list;
static K_STORE *workinfo_store;
// one in the current block
static K_ITEM *workinfo_current;
// first workinfo of current block
// TODO: have it's own memory?
static tv_t *last_bc;

// SHARES shares.id.json={...}
typedef struct shares {
	int64_t workinfoid;
	int64_t userid;
	char workername[TXT_BIG+1];
	int32_t clientid;
	char enonce1[TXT_SML+1];
	char nonce2[TXT_BIG+1];
	char nonce[TXT_SML+1];
	double diff;
	double sdiff;
	int32_t errn;
	char error[TXT_SML+1];
	char secondaryuserid[TXT_SML+1];
	HISTORYDATECONTROLFIELDS;
} SHARES;

#define ALLOC_SHARES 10000
#define LIMIT_SHARES 0
#define DATA_SHARES(_item) ((SHARES *)(_item->data))

static K_TREE *shares_root;
static K_LIST *shares_list;
static K_STORE *shares_store;

// SHAREERRORS shareerrors.id.json={...}
typedef struct shareerrorss {
	int64_t workinfoid;
	int64_t userid;
	char workername[TXT_BIG+1];
	int32_t clientid;
	int32_t errn;
	char error[TXT_SML+1];
	char secondaryuserid[TXT_SML+1];
	HISTORYDATECONTROLFIELDS;
} SHAREERRORS;

#define ALLOC_SHAREERRORS 10000
#define LIMIT_SHAREERRORS 0
#define DATA_SHAREERRORS(_item) ((SHAREERRORS *)(_item->data))

static K_TREE *shareerrors_root;
static K_LIST *shareerrors_list;
static K_STORE *shareerrors_store;

// SHARESUMMARY
typedef struct sharesummary {
	int64_t userid;
	char workername[TXT_BIG+1];
	int64_t workinfoid;
	double diffacc;
	double diffsta;
	double diffdup;
	double diffhi;
	double diffrej;
	double shareacc;
	double sharesta;
	double sharedup;
	double sharehi;
	double sharerej;
	int64_t sharecount;
	int64_t errorcount;
	int64_t countlastupdate; // non-DB field
	bool inserted; // non-DB field
	bool saveaged; // non-DB field
	tv_t firstshare;
	tv_t lastshare;
	char complete[TXT_FLAG+1];
	MODIFYDATECONTROLFIELDS;
} SHARESUMMARY;

/* After many shares added, we need to update the DB record
   The DB record is added with the 1st share */
#define SHARESUMMARY_UPDATE_EVERY 10

#define ALLOC_SHARESUMMARY 10000
#define LIMIT_SHARESUMMARY 0
#define DATA_SHARESUMMARY(_item) ((SHARESUMMARY *)(_item->data))

#define SUMMARY_NEW 'n'
#define SUMMARY_AGED 'a'
#define SUMMARY_CONFIRM 'y'

static K_TREE *sharesummary_root;
static K_TREE *sharesummary_workinfoid_root;
static K_LIST *sharesummary_list;
static K_STORE *sharesummary_store;

// BLOCKS block.id.json={...}
typedef struct blocks {
	int32_t height;
	char blockhash[TXT_BIG+1];
	int64_t workinfoid;
	int64_t userid;
	char workername[TXT_BIG+1];
	int32_t clientid;
	char enonce1[TXT_SML+1];
	char nonce2[TXT_BIG+1];
	char nonce[TXT_SML+1];
	int64_t reward;
	char confirmed[TXT_FLAG+1];
	HISTORYDATECONTROLFIELDS;
} BLOCKS;

#define ALLOC_BLOCKS 100
#define LIMIT_BLOCKS 0
#define DATA_BLOCKS(_item) ((BLOCKS *)(_item->data))

#define BLOCKS_NEW 'n'
#define BLOCKS_CONFIRM '1'

static K_TREE *blocks_root;
static K_LIST *blocks_list;
static K_STORE *blocks_store;

/*
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
	char poolinstance[TXT_BIG+1];
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
*/

// AUTHS authorise.id.json={...}
typedef struct auths {
	int64_t authid;
	char poolinstance[TXT_BIG+1];
	int64_t userid;
	char workername[TXT_BIG+1];
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

// POOLSTATS poolstats.id.json={...}
// Store every > 9.5m?
#define STATS_PER (9.5*60.0)

typedef struct poolstats {
	char poolinstance[TXT_BIG+1];
	int64_t elapsed;
	int32_t users;
	int32_t workers;
	double hashrate;
	double hashrate5m;
	double hashrate1hr;
	double hashrate24hr;
	SIMPLEDATECONTROLFIELDS;
} POOLSTATS;

#define ALLOC_POOLSTATS 10000
#define LIMIT_POOLSTATS 0
#define DATA_POOLSTATS(_item) ((POOLSTATS *)(_item->data))

static K_TREE *poolstats_root;
static K_LIST *poolstats_list;
static K_STORE *poolstats_store;

// USERSTATS userstats.id.json={...}
// Pool sends each user (staggered) once per 10m
typedef struct userstats {
	char poolinstance[TXT_BIG+1];
	int64_t userid;
	char workername[TXT_BIG+1];
	int64_t elapsed;
	double hashrate;
	double hashrate5m;
	double hashrate1hr;
	double hashrate24hr;
	bool idle; // Non-db field
	char summarylevel[TXT_FLAG+1]; // Initially SUMMARY_NONE in RAM
	tv_t statsdate;
	SIMPLEDATECONTROLFIELDS;
} USERSTATS;

/* USERSTATS protocol includes a boolean 'eos' that when true,
 * we have received the full set of data for the given
 * createdate batch, and thus can move all (complete) records
 * matching the createdate from userstats_eos_store into the tree */

#define ALLOC_USERSTATS 10000
#define LIMIT_USERSTATS 0
#define DATA_USERSTATS(_item) ((USERSTATS *)(_item->data))

static K_TREE *userstats_root;
static K_LIST *userstats_list;
static K_STORE *userstats_store;
// Awaiting EOS
static K_STORE *userstats_eos_store;
// Temporary while summarising
static K_STORE *userstats_summ;

/* 1.5 x how often we expect to get user's stats from ckpool
 * This is used when grouping the sub-worker stats into a single user
 * We add each worker's latest stats to the total - except we ignore
 * any worker with newest stats being older than USERSTATS_PER_S */
#define USERSTATS_PER_S 900

/* on the allusers page, show any with stats in the last ... */
#define ALLUSERS_LIMIT_S 3600

#define SUMMARY_NONE '0'
#define SUMMARY_DB '1'
#define SUMMARY_FULL '2'

/* Userstats get stored in the DB for each time band of this
 * amount from midnight (UTC+00)
 * Thus we simply put each stats value in the time band of the
 * stat's timestamp
 * Userstats are sumarised in the the same userstats table
 * If USERSTATS_DB_S is close to the expected time per USERSTATS
 * then it will have higher variance i.e. obviously: a higher
 * average of stats per sample will mean a lower SD of the number
 * of stats per sample
 * The #if below ensures USERSTATS_DB_S times an integer = a day
 * so the last band is the same size as the rest -
 * and will graph easily
 * Obvious WARNING - the smaller this is, the more stats in the DB
 * This is summary level '1'
 */
#define USERSTATS_DB_S 3600

#if (((24*60*60) % USERSTATS_DB_S) != 0)
#error "USERSTATS_DB_S times an integer must = a day"
#endif

#if ((24*60*60) < USERSTATS_DB_S)
#error "USERSTATS_DB_S must be at most 1 day"
#endif

/* We summarise and discard userstats that are older than the
 * maximum of USERSTATS_DB_S, USERSTATS_PER_S, ALLUSERS_LIMIT_S
 */
#if (USERSTATS_PER_S > ALLUSERS_LIMIT_S)
 #if (USERSTATS_PER_S > USERSTATS_DB_S)
  #define USERSTATS_AGE USERSTATS_PER_S
 #else
  #define USERSTATS_AGE USERSTATS_DB_S
 #endif
#else
 #if (ALLUSERS_LIMIT_S > USERSTATS_DB_S)
  #define USERSTATS_AGE ALLUSERS_LIMIT_S
 #else
  #define USERSTATS_AGE USERSTATS_DB_S
 #endif
#endif

/* summarisation of the userstats after this many days are done
 * at the day level and the above stats are deleted from the db
 * Obvious WARNING - the larger this is, the more stats in the DB
 * This is summary level '2'
 */
#define USERSTATS_DB_D 7

#define tv_newer(_old, _new) (((_old)->tv_sec == (_new)->tv_sec) ? \
				((_old)->tv_usec < (_new)->tv_usec) : \
				((_old)->tv_sec < (_new)->tv_sec))

// WORKERSTATUS from various incoming data
typedef struct workerstatus {
	int64_t userid;
	char workername[TXT_BIG+1];
	tv_t auth;
	tv_t share;
	tv_t stats;
	tv_t idle;
} WORKERSTATUS;

#define ALLOC_WORKERSTATUS 1000
#define LIMIT_WORKERSTATUS 0
#define DATA_WORKERSTATUS(_item) ((WORKERSTATUS *)(_item->data))

static K_TREE *workerstatus_root;
static K_LIST *workerstatus_list;
static K_STORE *workerstatus_store;

static char logname[512];
#define LOGFILE(_msg) rotating_log(logname, _msg)
#define LOGDUP "dup."

void logmsg(int loglevel, const char *fmt, ...)
{
	int logfd = 0;
	char *buf = NULL;
	struct tm *tm;
	time_t now_t;
	va_list ap;
	char stamp[128];
	char *extra = "";

	now_t = time(NULL);
	tm = localtime(&now_t);
	snprintf(stamp, sizeof(stamp),
			"[%d-%02d-%02d %02d:%02d:%02d]",
			tm->tm_year + 1900,
			tm->tm_mon + 1,
			tm->tm_mday,
			tm->tm_hour,
			tm->tm_min,
			tm->tm_sec);

	if (!fmt) {
		fprintf(stderr, "%s %s() called without fmt\n", stamp, __func__);
		return;
	}

	if (!global_ckp)
		extra = " !!NULL global_ckp!!";
	else
		logfd = global_ckp->logfd;

	va_start(ap, fmt);
	VASPRINTF(&buf, fmt, ap);
	va_end(ap);

	if (logfd) {
		FILE *LOGFP = global_ckp->logfp;

		flock(logfd, LOCK_EX);
		fprintf(LOGFP, "%s %s", stamp, buf);
		if (loglevel <= LOG_ERR && errno != 0)
			fprintf(LOGFP, " with errno %d: %s", errno, strerror(errno));
		fprintf(LOGFP, "\n");
		flock(logfd, LOCK_UN);
	}
	if (loglevel <= LOG_WARNING) {
		if (loglevel <= LOG_ERR && errno != 0) {
			fprintf(stderr, "%s %s with errno %d: %s%s\n",
					stamp, buf, errno, strerror(errno), extra);
		} else
			fprintf(stderr, "%s %s%s\n", stamp, buf, extra);
		fflush(stderr);
	}
	free(buf);
}

static void setnow(tv_t *now)
{
	ts_t spec;
	spec.tv_sec = 0;
	spec.tv_nsec = 0;
	clock_gettime(CLOCK_REALTIME, &spec);
	now->tv_sec = spec.tv_sec;
	now->tv_usec = spec.tv_nsec / 1000;
}

static void dsp_transfer(K_ITEM *item, FILE *stream)
{
	TRANSFER *t = NULL;

	if (!stream)
		LOGERR("%s() called with (null) stream", __func__);
	else {
		if (!item)
			fprintf(stream, "%s() called with (null) item\n", __func__);
		else {
			t = DATA_TRANSFER(item);

			fprintf(stream, " name='%s' data='%s' malloc=%c\n",
					t->name, t->data,
					(t->value == t->data) ? 'N' : 'Y');
		}
	}
}

// order by name asc
static double cmp_transfer(K_ITEM *a, K_ITEM *b)
{
	double c = (double)strcmp(DATA_TRANSFER(a)->name,
				  DATA_TRANSFER(b)->name);
	return c;
}

static K_ITEM *find_transfer(char *name)
{
	TRANSFER transfer;
	K_TREE_CTX ctx[1];
	K_ITEM look;

	STRNCPY(transfer.name, name);
	look.data = (void *)(&transfer);
	return find_in_ktree(transfer_root, &look, cmp_transfer, ctx);
}

static K_ITEM *optional_name(char *name, int len, char *patt)
{
	K_ITEM *item;
	char *value;
	regex_t re;
	int ret;

	item = find_transfer(name);
	if (!item)
		return NULL;

	value = DATA_TRANSFER(item)->data;
	if (!value || (int)strlen(value) < len)
		return NULL;

	if (patt) {
		if (regcomp(&re, patt, REG_NOSUB) != 0)
			return NULL;

		ret = regexec(&re, value, (size_t)0, NULL, 0);
		regfree(&re);

		if (ret != 0)
			return NULL;
	}

	return item;
}

#define require_name(_name, _len, _patt, _reply, _siz) \
		_require_name(_name, _len, _patt, _reply, _siz, \
				WHERE_FFL_HERE)

static K_ITEM *_require_name(char *name, int len, char *patt, char *reply,
				size_t siz, WHERE_FFL_ARGS)
{
	K_ITEM *item;
	char *value;
	regex_t re;
	size_t dlen;
	int ret;

	item = find_transfer(name);
	if (!item) {
		LOGERR("%s(): failed, field '%s' missing from %s():%d",
			__func__, name, func, line);
		snprintf(reply, siz, "failed.missing %s", name);
		return NULL;
	}

	value = DATA_TRANSFER(item)->data;
	if (value)
		dlen = strlen(value);
	else
		dlen = 0;
	if (!value || (int)dlen < len) {
		LOGERR("%s(): failed, field '%s' short (%s%d<%d) from %s():%d",
			__func__, name, value ? "" : "null",
			(int)dlen, len, func, line);
		snprintf(reply, siz, "failed.short %s", name);
		return NULL;
	}

	if (patt) {
		if (regcomp(&re, patt, REG_NOSUB) != 0) {
			LOGERR("%s(): failed, field '%s' failed to"
				" compile patt from %s():%d",
				__func__, name, func, line);
			snprintf(reply, siz, "failed.REC %s", name);
			return NULL;
		}

		ret = regexec(&re, value, (size_t)0, NULL, 0);
		regfree(&re);

		if (ret != 0) {
			LOGERR("%s(): failed, field '%s' invalid from %s():%d",
				__func__, name, func, line);
			snprintf(reply, siz, "failed.invalid %s", name);
			return NULL;
		}
	}

	return item;
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
				// allow uS
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
		case TYPE_CTV:
			if (siz != sizeof(tv_t)) {
				quithere(1, "Field %s timeval incorrect structure size %d - should be %d",
						nam, (int)siz, (int)sizeof(tv_t));
			}
			long sec, usec;
			int c;
			((tv_t *)data)->tv_sec = 0L;
			((tv_t *)data)->tv_usec = 0L;
			c = sscanf(fld, "%ld,%ld", &sec, &usec);
			// For converting msg fields - so not fatal if it's no good
			if (c > 0) {
				((tv_t *)data)->tv_sec = (time_t)sec;
				if (c > 1)
					((tv_t *)data)->tv_usec = usec;
				if (((tv_t *)data)->tv_sec >= COMPARE_EXPIRY) {
					((tv_t *)data)->tv_sec = default_expiry.tv_sec;
					((tv_t *)data)->tv_usec = default_expiry.tv_usec;
				}
			}
			break;
		case TYPE_BLOB:
			tmp = strdup(fld);
			if (!tmp)
				quithere(1, "Field %s (%d) OOM", nam, (int)strlen(fld));
			*((char **)data) = tmp;
			break;
		case TYPE_DOUBLE:
			if (siz != sizeof(double)) {
				quithere(1, "Field %s int incorrect structure size %d - should be %d",
						nam, (int)siz, (int)sizeof(double));
			}
			*((double *)data) = atof(fld);
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

// Convert msg S,nS to tv_t
static void txt_to_ctv(char *nam, char *fld, tv_t *data, size_t siz)
{
	txt_to_data(TYPE_CTV, nam, fld, (void *)data, siz);
}

static void txt_to_blob(char *nam, char *fld, char *data)
{
	txt_to_data(TYPE_BLOB, nam, fld, (void *)(&data), 0);
}

static void txt_to_double(char *nam, char *fld, double *data, size_t siz)
{
	txt_to_data(TYPE_DOUBLE, nam, fld, (void *)data, siz);
}

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
				siz = BIGINT_BUFSIZ;
				break;
			case TYPE_INT:
				siz = INT_BUFSIZ;
				break;
			case TYPE_TV:
			case TYPE_TVS:
				siz = DATE_BUFSIZ;
				break;
			case TYPE_CTV:
				siz = CDATE_BUFSIZ;
				break;
			case TYPE_DOUBLE:
				siz = DOUBLE_BUFSIZ;
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
		case TYPE_CTV:
			snprintf(buf, siz, "%ld,%ld",
					   (((struct timeval *)data)->tv_sec),
					   (((struct timeval *)data)->tv_usec));
			break;
		case TYPE_TVS:
			snprintf(buf, siz, "%ld", (((struct timeval *)data)->tv_sec));
			break;
		case TYPE_DOUBLE:
			snprintf(buf, siz, "%f", *((double *)data));
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

static char *int_to_buf(int32_t data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_INT, (void *)(&data), buf, siz);
}

static char *tv_to_buf(tv_t *data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_TV, (void *)data, buf, siz);
}

// Convert tv to S,uS
static char *ctv_to_buf(tv_t *data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_CTV, (void *)data, buf, siz);
}

// Convert tv to seconds (ignore uS)
static char *tvs_to_buf(tv_t *data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_TVS, (void *)data, buf, siz);
}

/* unused yet
static char *blob_to_buf(char *data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_BLOB, (void *)data, buf, siz);
}
*/

static char *double_to_buf(double data, char *buf, size_t siz)
{
	return data_to_buf(TYPE_DOUBLE, (void *)(&data), buf, siz);
}

static PGconn *dbconnect()
{
	char conninfo[128];
	PGconn *conn;

	snprintf(conninfo, sizeof(conninfo), "host=127.0.0.1 dbname=ckdb user=%s", db_user);

	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
		quithere(1, "ERR: Failed to connect to db '%s'", pqerrmsg(conn));

	return conn;
}

static int64_t nextid(PGconn *conn, char *idname, int64_t increment,
			tv_t *now, char *by, char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res;
	char qry[1024];
	char *params[5];
	int par;
	int64_t lastid;
	char *field;
	bool ok;
	int n;

	lastid = 0;

	snprintf(qry, sizeof(qry), "select lastid from idcontrol "
				   "where idname='%s' for update",
				   idname);

	res = PQexec(conn, qry);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
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

	par = 0;
	params[par++] = bigint_to_buf(lastid, NULL, 0);
	params[par++] = tv_to_buf(now, NULL, 0);
	params[par++] = str_to_buf(by, NULL, 0);
	params[par++] = str_to_buf(code, NULL, 0);
	params[par++] = str_to_buf(inet, NULL, 0);
	PARCHK(par, params);

	res = PQexecParams(conn, qry, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
		PGLOGERR("Update", rescode, conn);
		lastid = 0;
	}

	for (n = 0; n < par; n++)
		free(params[n]);
cleanup:
	PQclear(res);
	return lastid;
}

// order by userid asc,workername asc
static double cmp_workerstatus(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_WORKERSTATUS(a)->userid -
			    DATA_WORKERSTATUS(b)->userid);
	if (c == 0.0) {
		c = strcmp(DATA_WORKERSTATUS(a)->workername,
			   DATA_WORKERSTATUS(b)->workername);
	}
	return c;
}

static K_ITEM *find_workerstatus(int64_t userid, char *workername)
{
	WORKERSTATUS workerstatus;
	K_TREE_CTX ctx[1];
	K_ITEM look;

	workerstatus.userid = userid;
	STRNCPY(workerstatus.workername, workername);

	look.data = (void *)(&workerstatus);
	return find_in_ktree(workerstatus_root, &look, cmp_workerstatus, ctx);
}

static K_ITEM *_find_create_workerstatus(int64_t userid, char *workername, bool create)
{
	K_ITEM *item = NULL;
	WORKERSTATUS *row;

	item = find_workerstatus(userid, workername);
	if (!item && create) {
		K_WLOCK(workerstatus_list);
		item = k_unlink_head(workerstatus_list);

		row = DATA_WORKERSTATUS(item);

		bzero(row, sizeof(*row));
		row->userid = userid;
		STRNCPY(row->workername, workername);

		workerstatus_root = add_to_ktree(workerstatus_root, item, cmp_workerstatus);
		k_add_head(workerstatus_store, item);
		K_WUNLOCK(workerstatus_list);
	}
	return item;
}

#define find_create_workerstatus(_u, _w)  _find_create_workerstatus(_u, _w, true)
#define find_workerstatus(_u, _w)  _find_create_workerstatus(_u, _w, false)

static void workerstatus_update(AUTHS *auths, SHARES *shares, USERSTATS *userstats,
				SHARESUMMARY *sharesummary)
{
	WORKERSTATUS *row;
	K_ITEM *item;

	LOGDEBUG("%s()", __func__);

	if (auths) {
		item = find_create_workerstatus(auths->userid, auths->workername);
		row = DATA_WORKERSTATUS(item);
		if (tv_newer(&(row->auth), &(auths->createdate)))
			memcpy(&(row->auth), &(auths->createdate), sizeof(row->auth));
	}

	if (shares) {
		item = find_create_workerstatus(shares->userid, shares->workername);
		row = DATA_WORKERSTATUS(item);
		if (tv_newer(&(row->share), &(shares->createdate)))
			memcpy(&(row->share), &(shares->createdate), sizeof(row->share));
	}

	if (userstats) {
		item = find_create_workerstatus(userstats->userid, userstats->workername);
		row = DATA_WORKERSTATUS(item);
		if (userstats->idle) {
			if (tv_newer(&(row->idle), &(userstats->statsdate)))
				memcpy(&(row->idle), &(userstats->statsdate), sizeof(row->idle));
		} else {
			if (tv_newer(&(row->stats), &(userstats->statsdate)))
				memcpy(&(row->stats), &(userstats->statsdate), sizeof(row->idle));
		}
	}

	if (sharesummary) {
		item = find_create_workerstatus(sharesummary->userid, sharesummary->workername);
		row = DATA_WORKERSTATUS(item);
		if (tv_newer(&(row->share), &(sharesummary->lastshare)))
			memcpy(&(row->share), &(sharesummary->lastshare), sizeof(row->share));
	}
}

// default tree order by username asc,expirydate desc
static double cmp_users(K_ITEM *a, K_ITEM *b)
{
	double c = strcmp(DATA_USERS(a)->username,
			  DATA_USERS(b)->username);
	if (c == 0.0) {
		c = tvdiff(&(DATA_USERS(b)->expirydate),
			   &(DATA_USERS(a)->expirydate));
	}
	return c;
}

// order by userid asc,expirydate desc
static double cmp_userid(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_USERS(a)->userid -
			    DATA_USERS(b)->userid);
	if (c == 0.0) {
		c = tvdiff(&(DATA_USERS(b)->expirydate),
			   &(DATA_USERS(a)->expirydate));
	}
	return c;
}

static K_ITEM *find_users(char *username)
{
	USERS users;
	K_TREE_CTX ctx[1];
	K_ITEM look;

	STRNCPY(users.username, username);
	users.expirydate.tv_sec = default_expiry.tv_sec;
	users.expirydate.tv_usec = default_expiry.tv_usec;

	look.data = (void *)(&users);
	return find_in_ktree(users_root, &look, cmp_users, ctx);
}

static K_ITEM *find_userid(int64_t userid)
{
	USERS users;
	K_TREE_CTX ctx[1];
	K_ITEM look;

	users.userid = userid;
	users.expirydate.tv_sec = default_expiry.tv_sec;
	users.expirydate.tv_usec = default_expiry.tv_usec;

	look.data = (void *)(&users);
	return find_in_ktree(userid_root, &look, cmp_userid, ctx);
}

static bool users_add(PGconn *conn, char *username, char *emailaddress, char *passwordhash,
			tv_t *now, char *by, char *code, char *inet)
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
	char *params[6 + HISTORYDATECOUNT];
	int par;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(users_list);
	item = k_unlink_head(users_list);
	K_WUNLOCK(users_list);

	row = DATA_USERS(item);

	row->userid = nextid(conn, "userid", (int64_t)(666 + (rand() % 334)),
				now, by, code, inet);
	if (row->userid == 0)
		goto unitem;

	// TODO: pre-check the username exists? (to save finding out via a DB error)

	STRNCPY(row->username, username);
	STRNCPY(row->emailaddress, emailaddress);
	STRNCPY(row->passwordhash, passwordhash);

	snprintf(tohash, sizeof(tohash), "%s&#%s", username, emailaddress);
	HASH_BER(tohash, strlen(tohash), 1, hash, tmp);
	__bin2hex(row->secondaryuserid, (void *)(&hash), sizeof(hash));

	HISTORYDATEINIT(row, now, by, code, inet);

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
	if (!PGOK(rescode)) {
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
		users_root = add_to_ktree(users_root, item, cmp_users);
		userid_root = add_to_ktree(userid_root, item, cmp_userid);
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
	if (!PGOK(rescode)) {
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

		users_root = add_to_ktree(users_root, item, cmp_users);
		userid_root = add_to_ktree(userid_root, item, cmp_userid);
		k_add_head(users_store, item);
	}
	if (!ok)
		k_add_head(users_list, item);

	K_WUNLOCK(users_list);
	PQclear(res);

	if (ok) {
		LOGDEBUG("%s(): built", __func__);
		LOGWARNING("%s(): loaded %d users records", __func__, n);
	}

	return ok;
}

void users_reload()
{
	PGconn *conn = dbconnect();

	K_WLOCK(users_list);
	users_root = free_ktree(users_root, NULL);
	userid_root = free_ktree(userid_root, NULL);
	k_list_transfer_to_head(users_store, users_list);
	K_WUNLOCK(users_list);

	users_fill(conn);

	PQfinish(conn);
}

// order by userid asc,workername asc,expirydate desc
static double cmp_workers(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_WORKERS(a)->userid -
			    DATA_WORKERS(b)->userid);
	if (c == 0.0) {
		c = strcmp(DATA_WORKERS(a)->workername,
			   DATA_WORKERS(b)->workername);
		if (c == 0.0) {
			c = tvdiff(&(DATA_WORKERS(b)->expirydate),
				   &(DATA_WORKERS(a)->expirydate));
		}
	}
	return c;
}

static K_ITEM *find_workers(int64_t userid, char *workername)
{
	WORKERS workers;
	K_TREE_CTX ctx[1];
	K_ITEM look;

	workers.userid = userid;
	STRNCPY(workers.workername, workername);
	workers.expirydate.tv_sec = default_expiry.tv_sec;
	workers.expirydate.tv_usec = default_expiry.tv_usec;

	look.data = (void *)(&workers);
	return find_in_ktree(workers_root, &look, cmp_workers, ctx);
}

static K_ITEM *workers_add(PGconn *conn, int64_t userid, char *workername,
			   char *difficultydefault, char *idlenotificationenabled,
			   char *idlenotificationtime, tv_t *now, char *by,
			   char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item, *ret = NULL;
	int n;
	WORKERS *row;
	char *ins;
	char *params[6 + HISTORYDATECOUNT];
	int par;
	int32_t diffdef;
	int32_t nottime;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(workers_list);
	item = k_unlink_head(workers_list);
	K_WUNLOCK(workers_list);

	row = DATA_WORKERS(item);

	row->workerid = nextid(conn, "workerid", (int64_t)1, now, by, code, inet);
	if (row->workerid == 0)
		goto unitem;

	row->userid = userid;
	STRNCPY(row->workername, workername);
	if (difficultydefault && *difficultydefault) {
		diffdef = atoi(difficultydefault);
		if (diffdef < DIFFICULTYDEFAULT_MIN)
			diffdef = DIFFICULTYDEFAULT_MIN;
		if (diffdef > DIFFICULTYDEFAULT_MAX)
			diffdef = DIFFICULTYDEFAULT_MAX;
		row->difficultydefault = diffdef;
	} else
		row->difficultydefault = DIFFICULTYDEFAULT_DEF;

	row->idlenotificationenabled[1] = '\0';
	if (idlenotificationenabled && *idlenotificationenabled) {
		if (tolower(*idlenotificationenabled) == IDLENOTIFICATIONENABLED[0])
			row->idlenotificationenabled[0] = IDLENOTIFICATIONENABLED[0];
		else
			row->idlenotificationenabled[0] = IDLENOTIFICATIONDISABLED[0];
	} else
		row->idlenotificationenabled[0] = IDLENOTIFICATIONENABLED_DEF[0];

	if (idlenotificationtime && *idlenotificationtime) {
		nottime = atoi(idlenotificationtime);
		if (nottime < DIFFICULTYDEFAULT_MIN) {
			row->idlenotificationenabled[0] = IDLENOTIFICATIONDISABLED[0];
			nottime = DIFFICULTYDEFAULT_MIN;
		} else if (nottime > IDLENOTIFICATIONTIME_MAX)
			nottime = row->idlenotificationtime;
		row->idlenotificationtime = nottime;
	} else
		row->idlenotificationtime = IDLENOTIFICATIONTIME_DEF;

	HISTORYDATEINIT(row, now, by, code, inet);

	par = 0;
	params[par++] = bigint_to_buf(row->workerid, NULL, 0);
	params[par++] = bigint_to_buf(row->userid, NULL, 0);
	params[par++] = str_to_buf(row->workername, NULL, 0);
	params[par++] = int_to_buf(row->difficultydefault, NULL, 0);
	params[par++] = str_to_buf(row->idlenotificationenabled, NULL, 0);
	params[par++] = int_to_buf(row->idlenotificationtime, NULL, 0);
	HISTORYDATEPARAMS(params, par, row);
	PARCHK(par, params);

	ins = "insert into workers "
		"(workerid,userid,workername,difficultydefault,"
		"idlenotificationenabled,idlenotificationtime"
		HISTORYDATECONTROL ") values (" PQPARAM11 ")";

	res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
		PGLOGERR("Insert", rescode, conn);
		goto unparam;
	}

	ret = item;
unparam:
	PQclear(res);
	for (n = 0; n < par; n++)
		free(params[n]);
unitem:
	K_WLOCK(workers_list);
	if (!ret)
		k_add_head(workers_list, item);
	else {
		workers_root = add_to_ktree(workers_root, item, cmp_workers);
		k_add_head(workers_store, item);
	}
	K_WUNLOCK(workers_list);

	return ret;
}

static bool workers_update(PGconn *conn, K_ITEM *item, char *difficultydefault,
			   char *idlenotificationenabled, char *idlenotificationtime,
			   tv_t *now, char *by, char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res;
	int n;
	WORKERS *row;
	char *upd, *ins;
	bool ok = false;
	char *params[6 + HISTORYDATECOUNT];
	int par;
	int32_t diffdef;
	char idlenot;
	int32_t nottime;

	LOGDEBUG("%s(): update", __func__);

	row = DATA_WORKERS(item);

	if (difficultydefault && *difficultydefault) {
		diffdef = atoi(difficultydefault);
		if (diffdef < DIFFICULTYDEFAULT_MIN)
			diffdef = row->difficultydefault;
		if (diffdef > DIFFICULTYDEFAULT_MAX)
			diffdef = row->difficultydefault;
	} else
		diffdef = row->difficultydefault;

	if (idlenotificationenabled && *idlenotificationenabled) {
		if (tolower(*idlenotificationenabled) == IDLENOTIFICATIONENABLED[0])
			idlenot = IDLENOTIFICATIONENABLED[0];
		else
			idlenot = IDLENOTIFICATIONDISABLED[0];
	} else
		idlenot = row->idlenotificationenabled[0];

	if (idlenotificationtime && *idlenotificationtime) {
		nottime = atoi(idlenotificationtime);
		if (nottime < IDLENOTIFICATIONTIME_MIN)
			nottime = row->idlenotificationtime;
		if (nottime > IDLENOTIFICATIONTIME_MAX)
			nottime = row->idlenotificationtime;
	} else
		nottime = row->idlenotificationtime;

	HISTORYDATEINIT(row, now, by, code, inet);

	if (diffdef == row->difficultydefault &&
	    idlenot == row->idlenotificationenabled[0] &&
	    nottime == row->idlenotificationtime) {
		ok = true;
		goto early;
	} else {
		upd = "update workers set expirydate=$1 where workerid=$2 and expirydate=$3";
		par = 0;
		params[par++] = tv_to_buf(now, NULL, 0);
		params[par++] = bigint_to_buf(row->workerid, NULL, 0);
		params[par++] = tv_to_buf((tv_t *)&default_expiry, NULL, 0);
		PARCHKVAL(par, 3, params);

		res = PQexec(conn, "Begin");
		rescode = PQresultStatus(res);
		if (!PGOK(rescode)) {
			PGLOGERR("Begin", rescode, conn);
			goto unparam;
		}
		PQclear(res);

		res = PQexecParams(conn, upd, par, NULL, (const char **)params, NULL, NULL, 0);
		rescode = PQresultStatus(res);
		PQclear(res);
		if (!PGOK(rescode)) {
			PGLOGERR("Update", rescode, conn);
			res = PQexec(conn, "Rollback");
			goto unparam;
		}

		for (n = 0; n < par; n++)
			free(params[n]);

		ins = "insert into workers "
			"(workerid,userid,workername,difficultydefault,"
			"idlenotificationenabled,idlenotificationtime"
			HISTORYDATECONTROL ") values (" PQPARAM11 ")";

		row->difficultydefault = diffdef;
		row->idlenotificationenabled[0] = idlenot;
		row->idlenotificationenabled[1] = '\0';
		row->idlenotificationtime = nottime;

		par = 0;
		params[par++] = bigint_to_buf(row->workerid, NULL, 0);
		params[par++] = bigint_to_buf(row->userid, NULL, 0);
		params[par++] = str_to_buf(row->workername, NULL, 0);
		params[par++] = int_to_buf(row->difficultydefault, NULL, 0);
		params[par++] = str_to_buf(row->idlenotificationenabled, NULL, 0);
		params[par++] = int_to_buf(row->idlenotificationtime, NULL, 0);
		HISTORYDATEPARAMS(params, par, row);
		PARCHK(par, params);

		res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
		rescode = PQresultStatus(res);
		PQclear(res);
		if (!PGOK(rescode)) {
			PGLOGERR("Insert", rescode, conn);
			res = PQexec(conn, "Rollback");
			goto unparam;
		}

		res = PQexec(conn, "Commit");
	}

	ok = true;
unparam:
	PQclear(res);
	for (n = 0; n < par; n++)
		free(params[n]);
early:
	return ok;
}

static K_ITEM *new_worker(PGconn *conn, bool update, int64_t userid, char *workername,
			  char *diffdef, char *idlenotificationenabled,
			  char *idlenotificationtime, tv_t *now, char *by,
			  char *code, char *inet)
{
	K_ITEM *item;

	item = find_workers(userid, workername);
	if (item) {
		if (update) {
			workers_update(conn, item, diffdef, idlenotificationenabled,
				       idlenotificationtime, now, by, code, inet);
		}
	} else {
		// TODO: limit how many?
		item = workers_add(conn, userid, workername, diffdef,
				   idlenotificationenabled, idlenotificationtime,
				   now, by, code, inet);
	}
	return item;
}

static K_ITEM *new_default_worker(PGconn *conn, bool update, int64_t userid, char *workername,
			  tv_t *now, char *by, char *code, char *inet)
{
	bool conned = false;
	K_ITEM *item;

	if (conn == NULL) {
		conn = dbconnect();
		conned = true;
	}

	item = new_worker(conn, update, userid, workername, DIFFICULTYDEFAULT_DEF_STR,
				IDLENOTIFICATIONENABLED_DEF, IDLENOTIFICATIONTIME_DEF_STR,
				now, by, code, inet);

	if (conned)
		PQfinish(conn);

	return item;
}

/* unused
static K_ITEM *new_worker_find_user(PGconn *conn, bool update, char *username,
				    char *workername, char *diffdef,
				    char *idlenotificationenabled,
				    char *idlenotificationtime, tv_t *now,
				    char *by, char *code, char *inet)
{
	K_ITEM *item;

	item = find_users(username);
	if (!item)
		return NULL;

	return new_worker(conn, update, DATA_USERS(item)->userid, workername,
			  diffdef, idlenotificationenabled,
			  idlenotificationtime, now, by, code, inet);
}
*/

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
	if (!PGOK(rescode)) {
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

		workers_root = add_to_ktree(workers_root, item, cmp_workers);
		k_add_head(workers_store, item);
	}
	if (!ok)
		k_add_head(workers_list, item);

	K_WUNLOCK(workers_list);
	PQclear(res);

	if (ok) {
		LOGDEBUG("%s(): built", __func__);
		LOGWARNING("%s(): loaded %d workers records", __func__, n);
	}

	return ok;
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

// order by userid asc,paydate asc,payaddress asc,expirydate desc
static double cmp_payments(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_PAYMENTS(a)->userid -
			    DATA_PAYMENTS(b)->userid);
	if (c == 0.0) {
		c = tvdiff(&(DATA_PAYMENTS(a)->paydate),
			   &(DATA_PAYMENTS(b)->paydate));
		if (c == 0.0) {
			c = strcmp(DATA_PAYMENTS(a)->payaddress,
				   DATA_PAYMENTS(b)->payaddress);
			if (c == 0.0) {
				c = tvdiff(&(DATA_PAYMENTS(b)->expirydate),
					   &(DATA_PAYMENTS(a)->expirydate));
			}
		}
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
	if (!PGOK(rescode)) {
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

		payments_root = add_to_ktree(payments_root, item, cmp_payments);
		k_add_head(payments_store, item);
	}
	if (!ok)
		k_add_head(payments_list, item);

	K_WUNLOCK(payments_list);
	PQclear(res);

	if (ok) {
		LOGDEBUG("%s(): built", __func__);
		LOGWARNING("%s(): loaded %d payments records", __func__, n);
	}

	return ok;
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

// order by workinfoid asc,expirydate asc
static double cmp_workinfo(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_WORKINFO(a)->workinfoid -
			    DATA_WORKINFO(b)->workinfoid);
	if (c == 0) {
		c = tvdiff(&(DATA_WORKINFO(a)->expirydate),
			   &(DATA_WORKINFO(b)->expirydate));
	}
	return c;
}

inline int32_t _coinbase1height(char *coinbase1, WHERE_FFL_ARGS)
{
	int32_t height = 0;
	uchar *cb1;
	int siz;

	cb1 = ((uchar *)coinbase1) + 84;
	siz = ((hex2bin_tbl[*cb1]) << 4) + (hex2bin_tbl[*(cb1+1)]);

	// limit to 4 for int32_t and since ... that should last a while :)
	if (siz < 1 || siz > 4) {
		LOGERR("%s(): Invalid coinbase1 block height size (%d)"
			" require: 1..4" WHERE_FFL,
			__func__, siz, WHERE_FFL_PASS);
		return height;
	}

	siz *= 2;
	while (siz-- > 0) {
		height <<= 4;
		height += (int32_t)hex2bin_tbl[*(cb1+(siz^1)+2)];
	}

	return height;
}

static double _cmp_height(char *coinbase1a, char *coinbase1b, WHERE_FFL_ARGS)
{
	double c = (double)(_coinbase1height(coinbase1a, WHERE_FFL_PASS) -
			    _coinbase1height(coinbase1b, WHERE_FFL_PASS));
	return c;
}

// order by height asc,createdate asc
static double cmp_workinfo_height(K_ITEM *a, K_ITEM *b)
{
	double c = cmp_height(DATA_WORKINFO(a)->coinbase1,
			      DATA_WORKINFO(b)->coinbase1);
	if (c == 0) {
		c = tvdiff(&(DATA_WORKINFO(a)->createdate),
			   &(DATA_WORKINFO(b)->createdate));
	}
	return c;
}

static K_ITEM *find_workinfo(int64_t workinfoid)
{
	WORKINFO workinfo;
	K_TREE_CTX ctx[1];
	K_ITEM look;

	workinfo.workinfoid = workinfoid;
	workinfo.expirydate.tv_sec = default_expiry.tv_sec;
	workinfo.expirydate.tv_usec = default_expiry.tv_usec;

	look.data = (void *)(&workinfo);
	return find_in_ktree(workinfo_root, &look, cmp_workinfo, ctx);
}

static int64_t workinfo_add(PGconn *conn, char *workinfoidstr, char *poolinstance,
				char *transactiontree, char *merklehash, char *prevhash,
				char *coinbase1, char *coinbase2, char *version,
				char *bits, char *ntime, char *reward,
				tv_t *now, char *by, char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n;
	int64_t workinfoid = -1;
	WORKINFO *row;
	char *ins;
	char *params[11 + HISTORYDATECOUNT];
	int par;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(workinfo_list);
	item = k_unlink_head(workinfo_list);
	K_WUNLOCK(workinfo_list);

	row = DATA_WORKINFO(item);

	TXT_TO_BIGINT("workinfoid", workinfoidstr, row->workinfoid);
	STRNCPY(row->poolinstance, poolinstance);
	row->transactiontree = strdup(transactiontree);
	row->merklehash = strdup(merklehash);
	STRNCPY(row->prevhash, prevhash);
	STRNCPY(row->coinbase1, coinbase1);
	STRNCPY(row->coinbase2, coinbase2);
	STRNCPY(row->version, version);
	STRNCPY(row->bits, bits);
	STRNCPY(row->ntime, ntime);
	TXT_TO_BIGINT("reward", reward, row->reward);

	HISTORYDATEINIT(row, now, by, code, inet);
	HISTORYDATETRANSFER(row);

	par = 0;
	params[par++] = bigint_to_buf(row->workinfoid, NULL, 0);
	params[par++] = str_to_buf(row->poolinstance, NULL, 0);
	params[par++] = str_to_buf(row->transactiontree, NULL, 0);
	params[par++] = str_to_buf(row->merklehash, NULL, 0);
	params[par++] = str_to_buf(row->prevhash, NULL, 0);
	params[par++] = str_to_buf(row->coinbase1, NULL, 0);
	params[par++] = str_to_buf(row->coinbase2, NULL, 0);
	params[par++] = str_to_buf(row->version, NULL, 0);
	params[par++] = str_to_buf(row->bits, NULL, 0);
	params[par++] = str_to_buf(row->ntime, NULL, 0);
	params[par++] = bigint_to_buf(row->reward, NULL, 0);
	HISTORYDATEPARAMS(params, par, row);
	PARCHK(par, params);

	ins = "insert into workinfo "
		"(workinfoid,poolinstance,transactiontree,merklehash,"
		"prevhash,coinbase1,coinbase2,version,bits,ntime,reward"
		HISTORYDATECONTROL ") values (" PQPARAM16 ")";

	res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
		PGLOGERR("Insert", rescode, conn);
		goto unparam;
	}

	workinfoid = row->workinfoid;

unparam:
	PQclear(res);
	for (n = 0; n < par; n++)
		free(params[n]);

	K_WLOCK(workinfo_list);
	if (workinfoid == -1) {
		free(row->transactiontree);
		free(row->merklehash);
		k_add_head(workinfo_list, item);
	} else {
		workinfo_root = add_to_ktree(workinfo_root, item, cmp_workinfo);
		k_add_head(workinfo_store, item);

		// Remember the bc 'now' when the height changes
		if (workinfo_current) {
			if (cmp_height(DATA_WORKINFO(workinfo_current)->coinbase1,
				       DATA_WORKINFO(item)->coinbase1) != 0)
				last_bc = &(DATA_WORKINFO(item)->createdate);
		}

		workinfo_current = item;
	}
	K_WUNLOCK(workinfo_list);

	return workinfoid;
}

static double cmp_shares(K_ITEM *a, K_ITEM *b);
static double cmp_sharesummary_workinfoid(K_ITEM *a, K_ITEM *b);
static bool sharesummary_update(PGconn *conn, SHARES *s_row, SHAREERRORS *e_row, K_ITEM *ss_item,
				tv_t *now, char *by, char *code, char *inet);

static bool workinfo_age(PGconn *conn, char *workinfoidstr, char *poolinstance,
			 tv_t *now, char *by, char *code, char *inet)
{
	K_ITEM *wi_item, ss_look, *ss_item, s_look, *s_item, *tmp_item;
	K_TREE_CTX ss_ctx[1], s_ctx[1], tmp_ctx[1];
	int64_t workinfoid;
	SHARESUMMARY sharesummary;
	SHARES shares;
	bool ok = false, conned = false;

	LOGDEBUG("%s(): complete", __func__);

	TXT_TO_BIGINT("workinfoid", workinfoidstr, workinfoid);

	wi_item = find_workinfo(workinfoid);
	if (!wi_item)
		goto bye;

	if (strcmp(poolinstance, DATA_WORKINFO(wi_item)->poolinstance) != 0)
		goto bye;

	// Find the first matching sharesummary
	sharesummary.workinfoid = workinfoid;
	sharesummary.userid = -1;
	sharesummary.workername[0] = '\0';

	ok = true;
	ss_look.data = (void *)(&sharesummary);
	ss_item = find_after_in_ktree(sharesummary_workinfoid_root, &ss_look, cmp_sharesummary_workinfoid, ss_ctx);
	while (ss_item && DATA_SHARESUMMARY(ss_item)->workinfoid == workinfoid) {
		if (conn == NULL) {
			conn = dbconnect();
			conned = true;
		}

		if (!sharesummary_update(conn, NULL, NULL, ss_item, now, by, code, inet)) {
			LOGERR("%s(): Failed to age share summary %"PRId64"/%s/%"PRId64,
				__func__, DATA_SHARESUMMARY(ss_item)->userid,
				DATA_SHARESUMMARY(ss_item)->workername,
				DATA_SHARESUMMARY(ss_item)->workinfoid);
			ok = false;
		}

		// Discard the shares either way
		shares.workinfoid = workinfoid;
		shares.userid = DATA_SHARESUMMARY(ss_item)->userid;
		strcpy(shares.workername, DATA_SHARESUMMARY(ss_item)->workername);
		shares.createdate.tv_sec = 0;
		shares.createdate.tv_usec = 0;

		s_look.data = (void *)(&shares);
		s_item = find_after_in_ktree(shares_root, &s_look, cmp_shares, s_ctx);
		K_WLOCK(shares_list);
		while (s_item) {
			if (DATA_SHARES(s_item)->workinfoid != workinfoid ||
			    DATA_SHARES(s_item)->userid != shares.userid ||
			    strcmp(DATA_SHARES(s_item)->workername, shares.workername) != 0)
				break;

			tmp_item = next_in_ktree(s_ctx);
			shares_root = remove_from_ktree(shares_root, s_item, cmp_shares, tmp_ctx);
			k_unlink_item(shares_store, s_item);
			k_add_head(shares_list, s_item);
			s_item = tmp_item;
		}
		K_WUNLOCK(shares_list);
		ss_item = next_in_ktree(ss_ctx);
	}

	if (conned)
		PQfinish(conn);

bye:
	return ok;
}

static bool workinfo_fill(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n, i;
	WORKINFO *row;
	char *params[1];
	int par;
	char *field;
	char *sel;
	int fields = 11;
	bool ok;

	LOGDEBUG("%s(): select", __func__);

	// TODO: select the data based on sharesummary since old data isn't needed
	//  however, the ageing rules for workinfo will decide that also
	//  keep the last block + current?
	sel = "select "
		"workinfoid,poolinstance,transactiontree,merklehash,prevhash,"
		"coinbase1,coinbase2,version,bits,ntime,reward"
		HISTORYDATECONTROL
		" from workinfo where expirydate=$1";
	par = 0;
	params[par++] = tv_to_buf((tv_t *)(&default_expiry), NULL, 0);
	PARCHK(par, params);
	res = PQexecParams(conn, sel, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
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
	K_WLOCK(workinfo_list);
	for (i = 0; i < n; i++) {
		item = k_unlink_head(workinfo_list);
		row = DATA_WORKINFO(item);

		PQ_GET_FLD(res, i, "workinfoid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("workinfoid", field, row->workinfoid);

		PQ_GET_FLD(res, i, "poolinstance", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("poolinstance", field, row->poolinstance);

		PQ_GET_FLD(res, i, "transactiontree", field, ok);
		if (!ok)
			break;
		TXT_TO_BLOB("transactiontree", field, row->transactiontree);

		PQ_GET_FLD(res, i, "merklehash", field, ok);
		if (!ok)
			break;
		TXT_TO_BLOB("merklehash", field, row->merklehash);

		PQ_GET_FLD(res, i, "prevhash", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("prevhash", field, row->prevhash);

		PQ_GET_FLD(res, i, "coinbase1", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("coinbase1", field, row->coinbase1);

		PQ_GET_FLD(res, i, "coinbase2", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("coinbase2", field, row->coinbase2);

		PQ_GET_FLD(res, i, "version", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("version", field, row->version);

		PQ_GET_FLD(res, i, "bits", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("bits", field, row->bits);

		PQ_GET_FLD(res, i, "ntime", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("ntime", field, row->ntime);

		PQ_GET_FLD(res, i, "reward", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("reward", field, row->reward);

		HISTORYDATEFLDS(res, i, row, ok);
		if (!ok)
			break;

		workinfo_root = add_to_ktree(workinfo_root, item, cmp_workinfo);
		workinfo_height_root = add_to_ktree(workinfo_height_root, item, cmp_workinfo_height);
		k_add_head(workinfo_store, item);
	}
	if (!ok)
		k_add_head(workinfo_list, item);

	K_WUNLOCK(workinfo_list);
	PQclear(res);

	if (ok) {
		LOGDEBUG("%s(): built", __func__);
		LOGWARNING("%s(): loaded %d workinfo records", __func__, n);
	}

	return ok;
}

void workinfo_reload()
{
	// TODO: ??? a bad idea?
/*
	PGconn *conn = dbconnect();

	K_WLOCK(workinfo_list);
	workinfo_root = free_ktree(workinfo_root, ???); free transactiontree and merklehash
	k_list_transfer_to_head(workinfo_store, workinfo_list);
	K_WUNLOCK(workinfo_list);

	workinfo_fill(conn);

	PQfinish(conn);
*/
}

// order by workinfoid asc,userid asc,workername asc,createdate asc,nonce asc,expirydate desc
static double cmp_shares(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_SHARES(a)->workinfoid -
			    DATA_SHARES(b)->workinfoid);
	if (c == 0) {
		c = (double)(DATA_SHARES(a)->userid -
			     DATA_SHARES(b)->userid);
		if (c == 0) {
			c = strcmp(DATA_SHARES(a)->workername,
				   DATA_SHARES(b)->workername);
			if (c == 0) {
				c = tvdiff(&(DATA_SHARES(a)->createdate),
					   &(DATA_SHARES(b)->createdate));
				if (c == 0) {
					c = strcmp(DATA_SHARES(a)->nonce,
						   DATA_SHARES(b)->nonce);
					if (c == 0) {
						c = tvdiff(&(DATA_SHARES(b)->expirydate),
							   &(DATA_SHARES(a)->expirydate));
					}
				}
			}
		}
	}
	return c;
}

// Memory (and log file) only
static bool shares_add(char *workinfoid, char *username, char *workername, char *clientid,
			char *enonce1, char *nonce2, char *nonce, char *diff, char *sdiff,
			char *secondaryuserid, tv_t *now, char *by, char *code, char *inet)
{
	K_ITEM *s_item, *u_item, *wi_item, *w_item;
	SHARES *shares;
	bool ok = false;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(shares_list);
	s_item = k_unlink_head(shares_list);
	K_WUNLOCK(shares_list);

	shares = DATA_SHARES(s_item);

	// TODO: allow BTC address later?
	u_item = find_users(username);
	if (!u_item)
		goto unitem;

	shares->userid = DATA_USERS(u_item)->userid;

	TXT_TO_BIGINT("workinfoid", workinfoid, shares->workinfoid);
	STRNCPY(shares->workername, workername);
	TXT_TO_INT("clientid", clientid, shares->clientid);
	STRNCPY(shares->enonce1, enonce1);
	STRNCPY(shares->nonce2, nonce2);
	STRNCPY(shares->nonce, nonce);
	TXT_TO_DOUBLE("diff", diff, shares->diff);
	TXT_TO_DOUBLE("sdiff", sdiff, shares->sdiff);
	STRNCPY(shares->secondaryuserid, secondaryuserid);

	HISTORYDATEINIT(shares, now, by, code, inet);
	HISTORYDATETRANSFER(shares);

	wi_item = find_workinfo(shares->workinfoid);
	if (!wi_item)
		goto unitem;

	w_item = new_default_worker(NULL, false, shares->userid, shares->workername,
					now, by, code, inet);
	if (!w_item)
		goto unitem;

	workerstatus_update(NULL, shares, NULL, NULL);

	sharesummary_update(NULL, shares, NULL, NULL, now, by, code, inet);

	ok = true;
unitem:
	K_WLOCK(shares_list);
	if (!ok)
		k_add_head(shares_list, s_item);
	else {
		shares_root = add_to_ktree(shares_root, s_item, cmp_shares);
		k_add_head(shares_store, s_item);
	}
	K_WUNLOCK(shares_list);

	return ok;
}

static bool shares_fill()
{
	// TODO: reload shares from workinfo from log file
	// and verify workinfo while doing that

	return true;
}

// order by workinfoid asc,userid asc,createdate asc,nonce asc,expirydate desc
static double cmp_shareerrors(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_SHAREERRORS(a)->workinfoid -
			    DATA_SHAREERRORS(b)->workinfoid);
	if (c == 0) {
		c = (double)(DATA_SHAREERRORS(a)->userid -
			     DATA_SHAREERRORS(b)->userid);
		if (c == 0) {
			c = tvdiff(&(DATA_SHAREERRORS(a)->createdate),
				   &(DATA_SHAREERRORS(b)->createdate));
			if (c == 0) {
				c = tvdiff(&(DATA_SHAREERRORS(b)->expirydate),
					   &(DATA_SHAREERRORS(a)->expirydate));
			}
		}
	}
	return c;
}

// Memory (and log file) only
static bool shareerrors_add(char *workinfoid, char *username, char *workername,
			char *clientid, char *errn, char *error, char *secondaryuserid,
			tv_t *now, char *by, char *code, char *inet)
{
	K_ITEM *s_item, *u_item, *wi_item, *w_item;
	SHAREERRORS *shareerrors;
	bool ok = false;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(shareerrors_list);
	s_item = k_unlink_head(shareerrors_list);
	K_WUNLOCK(shareerrors_list);

	shareerrors = DATA_SHAREERRORS(s_item);

	// TODO: allow BTC address later?
	u_item = find_users(username);
	if (!u_item)
		goto unitem;

	shareerrors->userid = DATA_USERS(u_item)->userid;

	TXT_TO_BIGINT("workinfoid", workinfoid, shareerrors->workinfoid);
	STRNCPY(shareerrors->workername, workername);
	TXT_TO_INT("clientid", clientid, shareerrors->clientid);
	TXT_TO_INT("errn", errn, shareerrors->errn);
	STRNCPY(shareerrors->error, error);
	STRNCPY(shareerrors->secondaryuserid, secondaryuserid);

	HISTORYDATEINIT(shareerrors, now, by, code, inet);
	HISTORYDATETRANSFER(shareerrors);

	wi_item = find_workinfo(shareerrors->workinfoid);
	if (!wi_item)
		goto unitem;

	w_item = new_default_worker(NULL, false, shareerrors->userid, shareerrors->workername,
					now, by, code, inet);
	if (!w_item)
		goto unitem;

	sharesummary_update(NULL, NULL, shareerrors, NULL, now, by, code, inet);

	ok = true;
unitem:
	K_WLOCK(shareerrors_list);
	if (!ok)
		k_add_head(shareerrors_list, s_item);
	else {
		shareerrors_root = add_to_ktree(shareerrors_root, s_item, cmp_shareerrors);
		k_add_head(shareerrors_store, s_item);
	}
	K_WUNLOCK(shareerrors_list);

	return ok;
}

static bool shareerrors_fill()
{
	// TODO: reload shareerrors from workinfo from log file
	// and verify workinfo while doing that

	return true;
}

static void dsp_sharesummary(K_ITEM *item, FILE *stream)
{
	SHARESUMMARY *s = NULL;
	char *createdate_buf;

	if (!stream)
		LOGERR("%s() called with (null) stream", __func__);
	else {
		if (!item)
			fprintf(stream, "%s() called with (null) item\n", __func__);
		else {
			s = DATA_SHARESUMMARY(item);

			createdate_buf = tv_to_buf(&(s->createdate), NULL, 0);
			fprintf(stream, " uid=%"PRId64" wn='%s' wid=%"PRId64" "
					"da=%f ds=%f ss=%f c='%s' cd=%s\n",
					s->userid, s->workername, s->workinfoid,
					s->diffacc, s->diffsta, s->sharesta,
					s->complete, createdate_buf);
			free(createdate_buf);
		}
	}
}

// default tree order by userid asc,workername asc,workinfoid asc for reporting
static double cmp_sharesummary(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_SHARESUMMARY(a)->userid -
			    DATA_SHARESUMMARY(b)->userid);
	if (c == 0.0) {
		c = strcmp(DATA_SHARESUMMARY(a)->workername,
			   DATA_SHARESUMMARY(b)->workername);
		if (c == 0.0) {
			c = (double)(DATA_SHARESUMMARY(a)->workinfoid -
				     DATA_SHARESUMMARY(b)->workinfoid);
		}
	}
	return c;
}

// order by workinfoid asc,userid asc,workername asc for flagging complete
static double cmp_sharesummary_workinfoid(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_SHARESUMMARY(a)->workinfoid -
			    DATA_SHARESUMMARY(b)->workinfoid);
	if (c == 0.0) {
		c = (double)(DATA_SHARESUMMARY(a)->userid -
			     DATA_SHARESUMMARY(b)->userid);
		if (c == 0.0) {
			c = strcmp(DATA_SHARESUMMARY(a)->workername,
				   DATA_SHARESUMMARY(b)->workername);
		}
	}
	return c;
}

static K_ITEM *find_sharesummary(int64_t userid, char *workername, int64_t workinfoid)
{
	SHARESUMMARY sharesummary;
	K_TREE_CTX ctx[1];
	K_ITEM look;

	sharesummary.userid = userid;
	STRNCPY(sharesummary.workername, workername);
	sharesummary.workinfoid = workinfoid;

	look.data = (void *)(&sharesummary);
	return find_in_ktree(sharesummary_root, &look, cmp_sharesummary, ctx);
}

static bool sharesummary_update(PGconn *conn, SHARES *s_row, SHAREERRORS *e_row, K_ITEM *ss_item,
				tv_t *now, char *by, char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res;
	SHARESUMMARY *row;
	K_ITEM *item;
	char *ins, *upd;
	bool ok = false, new;
	char *params[18 + MODIFYDATECOUNT];
	int n, par;
	int64_t userid, workinfoid;
	char *workername;
	tv_t *sharecreatedate;
	bool must_update = false, conned = false;

	LOGDEBUG("%s(): update", __func__);

	if (ss_item) {
		if (s_row || e_row) {
			quithere(1, "ERR: %s() only one of s_row, e_row and ss_item allowed",
				    __func__);
		}
		new = false;
		item = ss_item;
		row = DATA_SHARESUMMARY(item);
		must_update = true;
		row->complete[0] = SUMMARY_AGED;
		row->complete[1] = '\0';
	} else {
		if (s_row) {
			if (e_row) {
				quithere(1, "ERR: %s() only one of s_row, e_row (and ss_item) allowed",
					    __func__);
			}
			userid = s_row->userid;
			workername = s_row->workername;
			workinfoid = s_row->workinfoid;
			sharecreatedate = &(s_row->createdate);
		} else {
			if (!e_row) {
				quithere(1, "ERR: %s() all s_row, e_row and ss_item are NULL",
					    __func__);
			}
			userid = e_row->userid;
			workername = e_row->workername;
			workinfoid = e_row->workinfoid;
			sharecreatedate = &(e_row->createdate);
		}

		item = find_sharesummary(userid, workername, workinfoid);
		if (item) {
			new = false;
			row = DATA_SHARESUMMARY(item);
		} else {
			new = true;
			K_WLOCK(sharesummary_list);
			item = k_unlink_head(sharesummary_list);
			K_WUNLOCK(sharesummary_list);
			row = DATA_SHARESUMMARY(item);
			row->userid = userid;
			STRNCPY(row->workername, workername);
			row->workinfoid = workinfoid;
			row->diffacc = row->diffsta = row->diffdup = row->diffhi =
			row->diffrej = row->shareacc = row->sharesta = row->sharedup =
			row->sharehi = row->sharerej = 0.0;
			row->sharecount = row->errorcount = row->countlastupdate = 0;
			row->inserted = false;
			row->saveaged = false;
			row->firstshare.tv_sec = sharecreatedate->tv_sec;
			row->firstshare.tv_usec = sharecreatedate->tv_usec;
			row->lastshare.tv_sec = row->firstshare.tv_sec;
			row->lastshare.tv_usec = row->firstshare.tv_usec;
			row->complete[0] = SUMMARY_NEW;
			row->complete[1] = '\0';
		}

		if (e_row)
			row->errorcount += 1;
		else {
			row->sharecount += 1;
			switch (s_row->errn) {
				case SE_NONE:
					row->diffacc += s_row->diff;
					row->shareacc++;
					break;
				case SE_STALE:
					row->diffsta += s_row->diff;
					row->sharesta++;
					break;
				case SE_DUPE:
					row->diffdup += s_row->diff;
					row->sharedup++;
					break;
				case SE_HIGH_DIFF:
					row->diffhi += s_row->diff;
					row->sharehi++;
					break;
				default:
					row->diffrej += s_row->diff;
					row->sharerej++;
					break;
			}
		}

		if (!new) {
			double td;
			td = tvdiff(sharecreatedate, &(row->firstshare));
			// don't LOGERR '=' in case shares come from ckpool with the same timestamp
			if (td < 0.0) {
				char *tmp1, *tmp2;
				LOGERR("%s(): %s createdate (%s) is < summary firstshare (%s)",
					__func__, s_row ? "shares" : "shareerrors",
					(tmp1 = ctv_to_buf(sharecreatedate, NULL, 0)),
					(tmp2 = ctv_to_buf(&(row->firstshare), NULL, 0)));
				free(tmp2);
				free(tmp1);
				row->firstshare.tv_sec = sharecreatedate->tv_sec;
				row->firstshare.tv_usec = sharecreatedate->tv_usec;
			}
			td = tvdiff(sharecreatedate, &(row->lastshare));
			// don't LOGERR '=' in case shares come from ckpool with the same timestamp
			if (td >= 0.0) {
				row->lastshare.tv_sec = sharecreatedate->tv_sec;
				row->lastshare.tv_usec = sharecreatedate->tv_usec;
			} else {
				char *tmp1, *tmp2;
				LOGERR("%s(): %s createdate (%s) is < summary lastshare (%s)",
					__func__, s_row ? "shares" : "shareerrors",
					(tmp1 = ctv_to_buf(sharecreatedate, NULL, 0)),
					(tmp2 = ctv_to_buf(&(row->lastshare), NULL, 0)));
				free(tmp2);
				free(tmp1);
			}
			if (row->complete[0] != SUMMARY_NEW) {
				LOGDEBUG("%s(): updating sharesummary not '%c' %"PRId64"/%s/%"PRId64"/%s",
					__func__, SUMMARY_NEW, row->userid, row->workername,
					row->workinfoid, row->complete);
			}
		}
	}

	if (conn == NULL) {
		conn = dbconnect();
		conned = true;
	}

	if (new || !(row->inserted)) {
		MODIFYDATEINIT(row, now, by, code, inet);

		par = 0;
		params[par++] = bigint_to_buf(row->userid, NULL, 0);
		params[par++] = str_to_buf(row->workername, NULL, 0);
		params[par++] = bigint_to_buf(row->workinfoid, NULL, 0);
		params[par++] = double_to_buf(row->diffacc, NULL, 0);
		params[par++] = double_to_buf(row->diffsta, NULL, 0);
		params[par++] = double_to_buf(row->diffdup, NULL, 0);
		params[par++] = double_to_buf(row->diffhi, NULL, 0);
		params[par++] = double_to_buf(row->diffrej, NULL, 0);
		params[par++] = double_to_buf(row->shareacc, NULL, 0);
		params[par++] = double_to_buf(row->sharesta, NULL, 0);
		params[par++] = double_to_buf(row->sharedup, NULL, 0);
		params[par++] = double_to_buf(row->sharehi, NULL, 0);
		params[par++] = double_to_buf(row->sharerej, NULL, 0);
		params[par++] = bigint_to_buf(row->sharecount, NULL, 0);
		params[par++] = bigint_to_buf(row->errorcount, NULL, 0);
		params[par++] = tv_to_buf(&(row->firstshare), NULL, 0);
		params[par++] = tv_to_buf(&(row->lastshare), NULL, 0);
		params[par++] = str_to_buf(row->complete, NULL, 0);
		MODIFYDATEPARAMS(params, par, row);
		PARCHK(par, params);

		ins = "insert into sharesummary "
			"(userid,workername,workinfoid,diffacc,diffsta,diffdup,diffhi,"
			"diffrej,shareacc,sharesta,sharedup,sharehi,sharerej,"
			"sharecount,errorcount,firstshare,lastshare,complete"
			MODIFYDATECONTROL ") values (" PQPARAM26 ")";

		res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
		rescode = PQresultStatus(res);
		if (!PGOK(rescode)) {
			PGLOGERR("Insert", rescode, conn);
			goto unparam;
		}

		row->countlastupdate = row->sharecount + row->errorcount;
		row->inserted = true;
		if (row->complete[0] == SUMMARY_AGED)
			row->saveaged = true;
	} else {
		bool stats_update = false;

		MODIFYUPDATE(row, now, by, code, inet);

		if ((row->countlastupdate + SHARESUMMARY_UPDATE_EVERY) <
		    (row->sharecount + row->errorcount))
			stats_update = true;

		if (must_update && row->countlastupdate < (row->sharecount + row->errorcount))
			stats_update = true;

		if (stats_update) {
			par = 0;
			params[par++] = bigint_to_buf(row->userid, NULL, 0);
			params[par++] = str_to_buf(row->workername, NULL, 0);
			params[par++] = bigint_to_buf(row->workinfoid, NULL, 0);
			params[par++] = double_to_buf(row->diffacc, NULL, 0);
			params[par++] = double_to_buf(row->diffsta, NULL, 0);
			params[par++] = double_to_buf(row->diffdup, NULL, 0);
			params[par++] = double_to_buf(row->diffhi, NULL, 0);
			params[par++] = double_to_buf(row->diffrej, NULL, 0);
			params[par++] = double_to_buf(row->shareacc, NULL, 0);
			params[par++] = double_to_buf(row->sharesta, NULL, 0);
			params[par++] = double_to_buf(row->sharedup, NULL, 0);
			params[par++] = double_to_buf(row->sharehi, NULL, 0);
			params[par++] = double_to_buf(row->sharerej, NULL, 0);
			params[par++] = tv_to_buf(&(row->firstshare), NULL, 0);
			params[par++] = tv_to_buf(&(row->lastshare), NULL, 0);
			params[par++] = str_to_buf(row->complete, NULL, 0);
			MODIFYUPDATEPARAMS(params, par, row);
			PARCHKVAL(par, 20, params);

			upd = "update sharesummary "
				"set diffacc=$4,diffsta=$5,diffdup=$6,diffhi=$7,diffrej=$8,"
				"shareacc=$9,sharesta=$10,sharedup=$11,sharehi=$12,"
				"sharerej=$13,firstshare=$14,lastshare=$15,complete=$16"
				",modifydate=$17,modifyby=$18,modifycode=$19,modifyinet=$20 "
				"where userid=$1 and workername=$2 and workinfoid=$3";

			res = PQexecParams(conn, upd, par, NULL, (const char **)params, NULL, NULL, 0);
			rescode = PQresultStatus(res);
			if (!PGOK(rescode)) {
				PGLOGERR("Update", rescode, conn);
				goto unparam;
			}
			row->countlastupdate = row->sharecount + row->errorcount;
			if (row->complete[0] == SUMMARY_AGED)
				row->saveaged = true;
		} else {
			if (!must_update) {
				ok = true;
				goto late;
			} else {
				par = 0;
				params[par++] = bigint_to_buf(row->userid, NULL, 0);
				params[par++] = str_to_buf(row->workername, NULL, 0);
				params[par++] = bigint_to_buf(row->workinfoid, NULL, 0);
				params[par++] = str_to_buf(row->complete, NULL, 0);
				MODIFYUPDATEPARAMS(params, par, row);
				PARCHKVAL(par, 8, params);

				upd = "update sharesummary "
					"set complete=$4,modifydate=$5,modifyby=$6,modifycode=$7,modifyinet=$8 "
					"where userid=$1 and workername=$2 and workinfoid=$3";

				res = PQexecParams(conn, upd, par, NULL, (const char **)params, NULL, NULL, 0);
				rescode = PQresultStatus(res);
				if (!PGOK(rescode)) {
					PGLOGERR("MustUpdate", rescode, conn);
					goto unparam;
				}
				row->countlastupdate = row->sharecount + row->errorcount;
				if (row->complete[0] == SUMMARY_AGED)
					row->saveaged = true;
			}
		}
	}

	ok = true;
unparam:
	PQclear(res);
	for (n = 0; n < par; n++)
		free(params[n]);
late:
	if (conned)
		PQfinish(conn);

	// We keep the new item no matter what 'ok' is, since it will be inserted later
	K_WLOCK(sharesummary_list);
	if (new) {
		sharesummary_root = add_to_ktree(sharesummary_root, item, cmp_sharesummary);
		sharesummary_workinfoid_root = add_to_ktree(sharesummary_workinfoid_root,
							    item,
							    cmp_sharesummary_workinfoid);
		k_add_head(sharesummary_store, item);
	}
	K_WUNLOCK(sharesummary_list);

	return ok;
}

static bool sharesummary_fill(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n, i;
	SHARESUMMARY *row;
	char *field;
	char *sel;
	int fields = 18;
	bool ok;

	LOGDEBUG("%s(): select", __func__);

	// TODO: limit how far back
	sel = "select "
		"userid,workername,workinfoid,diffacc,diffsta,diffdup,diffhi"
		"diffrej,shareacc,sharesta,sharedup,sharehi,sharerej,"
		"sharecount,errorcount,firstshare,lastshare,complete"
		MODIFYDATECONTROL
		" from sharesummary";
	res = PQexec(conn, sel);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
		PGLOGERR("Select", rescode, conn);
		PQclear(res);
		return false;
	}

	n = PQnfields(res);
	if (n != (fields + MODIFYDATECOUNT)) {
		LOGERR("%s(): Invalid field count - should be %d, but is %d",
			__func__, fields + MODIFYDATECOUNT, n);
		PQclear(res);
		return false;
	}

	n = PQntuples(res);
	LOGDEBUG("%s(): tree build count %d", __func__, n);
	ok = true;
	K_WLOCK(sharesummary_list);
	for (i = 0; i < n; i++) {
		item = k_unlink_head(sharesummary_list);
		row = DATA_SHARESUMMARY(item);

		row->inserted = true;

		PQ_GET_FLD(res, i, "userid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("userid", field, row->userid);

		PQ_GET_FLD(res, i, "workername", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("workername", field, row->workername);

		PQ_GET_FLD(res, i, "workinfoid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("workinfoid", field, row->workinfoid);

		PQ_GET_FLD(res, i, "diffacc", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("diffacc", field, row->diffacc);

		PQ_GET_FLD(res, i, "diffsta", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("diffsta", field, row->diffsta);

		PQ_GET_FLD(res, i, "diffdup", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("diffdup", field, row->diffdup);

		PQ_GET_FLD(res, i, "diffhi", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("diffhi", field, row->diffhi);

		PQ_GET_FLD(res, i, "diffrej", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("diffrej", field, row->diffrej);

		PQ_GET_FLD(res, i, "shareacc", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("shareacc", field, row->shareacc);

		PQ_GET_FLD(res, i, "sharesta", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("sharesta", field, row->sharesta);

		PQ_GET_FLD(res, i, "sharedup", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("sharedup", field, row->sharedup);

		PQ_GET_FLD(res, i, "sharehi", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("sharehi", field, row->sharehi);

		PQ_GET_FLD(res, i, "sharerej", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("sharerej", field, row->sharerej);

		PQ_GET_FLD(res, i, "sharecount", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("sharecount", field, row->sharecount);

		PQ_GET_FLD(res, i, "errorcount", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("errorcount", field, row->errorcount);

		row->countlastupdate = row->sharecount + row->errorcount;

		PQ_GET_FLD(res, i, "firstshare", field, ok);
		if (!ok)
			break;
		TXT_TO_TV("firstshare", field, row->firstshare);

		PQ_GET_FLD(res, i, "lastshare", field, ok);
		if (!ok)
			break;
		TXT_TO_TV("lastshare", field, row->lastshare);

		PQ_GET_FLD(res, i, "complete", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("complete", field, row->complete);

		MODIFYDATEFLDS(res, i, row, ok);
		if (!ok)
			break;

		sharesummary_root = add_to_ktree(sharesummary_root, item, cmp_sharesummary);
		sharesummary_workinfoid_root = add_to_ktree(sharesummary_workinfoid_root, item, cmp_sharesummary_workinfoid);
		k_add_head(sharesummary_store, item);

		workerstatus_update(NULL, NULL, NULL, row);
	}
	if (!ok)
		k_add_head(sharesummary_list, item);

	K_WUNLOCK(sharesummary_list);
	PQclear(res);

	if (ok) {
		LOGDEBUG("%s(): built", __func__);
		LOGWARNING("%s(): loaded %d sharesummary records", __func__, n);
	}

	return ok;
}

void sharesummary_reload()
{
	PGconn *conn = dbconnect();

	K_WLOCK(sharesummary_list);
	sharesummary_root = free_ktree(sharesummary_root, NULL);
	sharesummary_workinfoid_root = free_ktree(sharesummary_workinfoid_root, NULL);
	k_list_transfer_to_head(sharesummary_store, sharesummary_list);
	K_WUNLOCK(sharesummary_list);

	sharesummary_fill(conn);

	PQfinish(conn);
}

// order by height asc,blockhash asc,expirydate desc
static double cmp_blocks(K_ITEM *a, K_ITEM *b)
{
	double c = DATA_BLOCKS(a)->height - DATA_BLOCKS(b)->height;
	if (c == 0) {
		c = strcmp(DATA_BLOCKS(a)->blockhash,
			   DATA_BLOCKS(b)->blockhash);
		if (c == 0) {
			c = tvdiff(&(DATA_BLOCKS(a)->expirydate),
				   &(DATA_BLOCKS(b)->expirydate));
		}
	}
	return c;
}

/* unused
static K_ITEM *find_blocks(int32_t height, char *blockhash)
{
	BLOCKS blocks;
	K_TREE_CTX ctx[1];
	K_ITEM look;

	blocks.height = height;
	STRNCPY(blocks.blockhash, blockhash);
	blocks.expirydate.tv_sec = default_expiry.tv_sec;
	blocks.expirydate.tv_usec = default_expiry.tv_usec;

	look.data = (void *)(&blocks);
	return find_in_ktree(blocks_root, &look, cmp_blocks, ctx);
}
*/

static bool blocks_add(PGconn *conn, char *height, char *blockhash,
				char *workinfoid, char *username, char *workername,
				char *clientid, char *enonce1, char *nonce2,
				char *nonce, char *reward, char *confirmed,
				tv_t *now, char *by, char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res = NULL;
	K_ITEM *item, *u_item;
	BLOCKS *row;
	char *upd, *ins;
	char *params[11 + HISTORYDATECOUNT];
	bool ok = false;
	int par = 0;
	int n;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(blocks_list);
	item = k_unlink_head(blocks_list);
	K_WUNLOCK(blocks_list);

	row = DATA_BLOCKS(item);

	TXT_TO_INT("height", height, row->height);
	STRNCPY(row->blockhash, blockhash);
	STRNCPY(row->confirmed, confirmed);

	HISTORYDATEINIT(row, now, by, code, inet);

	switch (confirmed[0]) {
		case BLOCKS_NEW:
			u_item = find_users(username);
			if (!u_item)
				goto early;

			TXT_TO_BIGINT("workinfoid", workinfoid, row->workinfoid);
			STRNCPY(row->workername, workername);
			TXT_TO_INT("clientid", clientid, row->clientid);
			STRNCPY(row->enonce1, enonce1);
			STRNCPY(row->nonce2, nonce2);
			STRNCPY(row->nonce, nonce);
			TXT_TO_BIGINT("reward", reward, row->reward);

			HISTORYDATETRANSFER(row);

			par = 0;
			params[par++] = int_to_buf(row->height, NULL, 0);
			params[par++] = str_to_buf(row->blockhash, NULL, 0);
			params[par++] = bigint_to_buf(row->workinfoid, NULL, 0);
			params[par++] = bigint_to_buf(row->userid, NULL, 0);
			params[par++] = str_to_buf(row->workername, NULL, 0);
			params[par++] = int_to_buf(row->clientid, NULL, 0);
			params[par++] = str_to_buf(row->enonce1, NULL, 0);
			params[par++] = str_to_buf(row->nonce2, NULL, 0);
			params[par++] = str_to_buf(row->nonce, NULL, 0);
			params[par++] = bigint_to_buf(row->reward, NULL, 0);
			params[par++] = str_to_buf(row->confirmed, NULL, 0);
			HISTORYDATEPARAMS(params, par, row);
			PARCHK(par, params);

			ins = "insert into blocks "
				"(height,blockhash,workinfoid,userid,workername,"
				"clientid,enonce1,nonce2,nonce,reward,confirmed"
				HISTORYDATECONTROL ") values (" PQPARAM16 ")";

			res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
			rescode = PQresultStatus(res);
			if (!PGOK(rescode)) {
				PGLOGERR("Insert", rescode, conn);
				goto unparam;
			}
			break;
		case BLOCKS_CONFIRM:
			upd = "update blocks set expirydate=$1 where blockhash=$2 and expirydate=$3";
			par = 0;
			params[par++] = tv_to_buf(now, NULL, 0);
			params[par++] = str_to_buf(row->blockhash, NULL, 0);
			params[par++] = tv_to_buf((tv_t *)&default_expiry, NULL, 0);
			PARCHKVAL(par, 3, params);

			res = PQexec(conn, "Begin");
			rescode = PQresultStatus(res);
			if (!PGOK(rescode)) {
				PGLOGERR("Begin", rescode, conn);
				goto unparam;
			}
			PQclear(res);

			res = PQexecParams(conn, upd, par, NULL, (const char **)params, NULL, NULL, 0);
			rescode = PQresultStatus(res);
			PQclear(res);
			if (!PGOK(rescode)) {
				PGLOGERR("Update", rescode, conn);
				res = PQexec(conn, "Rollback");
				goto unparam;
			}

			for (n = 0; n < par; n++)
				free(params[n]);

			par = 0;
			params[par++] = str_to_buf(row->blockhash, NULL, 0);
			params[par++] = tv_to_buf(now, NULL, 0);
			HISTORYDATEPARAMS(params, par, row);
			PARCHKVAL(par, 2 + HISTORYDATECOUNT, params); // 7 as per ins

			ins = "insert into blocks "
				"(height,blockhash,workinfoid,userid,workername,"
				"clientid,enonce1,nonce2,nonce,reward,confirmed"
				HISTORYDATECONTROL ") values (select "
				"height,blockhash,workinfoid,userid,workername,"
				"clientid,enonce1,nonce2,nonce,reward,confirmed,"
				"$3,$4,$5,$6,$7 from blocks where"
				"blockhash=$1 and expirydate=$2)";

			res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
			rescode = PQresultStatus(res);
			PQclear(res);
			if (!PGOK(rescode)) {
				PGLOGERR("Insert", rescode, conn);
				res = PQexec(conn, "Rollback");
				goto unparam;
			}

			res = PQexec(conn, "Commit");
			break;
	}

	ok = true;
unparam:
	PQclear(res);
	for (n = 0; n < par; n++)
		free(params[n]);
early:
	K_WLOCK(blocks_list);
	if (!ok)
		k_add_head(blocks_list, item);
	else {
		blocks_root = add_to_ktree(blocks_root, item, cmp_blocks);
		k_add_head(blocks_store, item);
	}
	K_WUNLOCK(blocks_list);

	return ok;
}

static bool blocks_fill(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n, i;
	BLOCKS *row;
	char *params[1];
	int par;
	char *field;
	char *sel;
	int fields = 11;
	bool ok;

	LOGDEBUG("%s(): select", __func__);

	sel = "select "
		"height,blockhash,workinfoid,userid,workername,"
		"clientid,enonce1,nonce2,nonce,reward,confirmed"
		HISTORYDATECONTROL
		" from blocks where expirydate=$1";
	par = 0;
	params[par++] = tv_to_buf((tv_t *)(&default_expiry), NULL, 0);
	PARCHK(par, params);
	res = PQexecParams(conn, sel, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
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
	K_WLOCK(blocks_list);
	for (i = 0; i < n; i++) {
		item = k_unlink_head(blocks_list);
		row = DATA_BLOCKS(item);

		PQ_GET_FLD(res, i, "height", field, ok);
		if (!ok)
			break;
		TXT_TO_INT("height", field, row->height);

		PQ_GET_FLD(res, i, "blockhash", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("blockhash", field, row->blockhash);

		PQ_GET_FLD(res, i, "workinfoid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("workinfoid", field, row->workinfoid);

		PQ_GET_FLD(res, i, "userid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("userid", field, row->userid);

		PQ_GET_FLD(res, i, "workername", field, ok);
		if (!ok)
			break;
		TXT_TO_BLOB("workername", field, row->workername);

		PQ_GET_FLD(res, i, "clientid", field, ok);
		if (!ok)
			break;
		TXT_TO_INT("clientid", field, row->clientid);

		PQ_GET_FLD(res, i, "enonce1", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("enonce1", field, row->enonce1);

		PQ_GET_FLD(res, i, "nonce2", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("nonce2", field, row->nonce2);

		PQ_GET_FLD(res, i, "nonce", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("nonce", field, row->nonce);

		PQ_GET_FLD(res, i, "reward", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("reward", field, row->reward);

		PQ_GET_FLD(res, i, "confirmed", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("confirmed", field, row->confirmed);

		HISTORYDATEFLDS(res, i, row, ok);
		if (!ok)
			break;

		blocks_root = add_to_ktree(blocks_root, item, cmp_blocks);
		k_add_head(blocks_store, item);
	}
	if (!ok)
		k_add_head(blocks_list, item);

	K_WUNLOCK(blocks_list);
	PQclear(res);

	if (ok) {
		LOGDEBUG("%s(): built", __func__);
		LOGWARNING("%s(): loaded %d blocks records", __func__, n);
	}

	return ok;
}

void blocks_reload()
{
	PGconn *conn = dbconnect();

	K_WLOCK(blocks_list);
	blocks_root = free_ktree(blocks_root, NULL);
	k_list_transfer_to_head(blocks_store, blocks_list);
	K_WUNLOCK(blocks_list);

	blocks_fill(conn);

	PQfinish(conn);
}

// order by userid asc,createdate asc,authid asc,expirydate desc
static double cmp_auths(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_AUTHS(a)->userid -
			    DATA_AUTHS(b)->userid);
	if (c == 0) {
		c = tvdiff(&(DATA_AUTHS(a)->createdate),
			   &(DATA_AUTHS(b)->createdate));
		if (c == 0) {
			c = (double)(DATA_AUTHS(a)->authid -
				     DATA_AUTHS(b)->authid);
			if (c == 0) {
				c = tvdiff(&(DATA_AUTHS(b)->expirydate),
					   &(DATA_AUTHS(a)->expirydate));
			}
		}
	}
	return c;
}

static char *auths_add(PGconn *conn, char *poolinstance, char *username,
				char *workername, char *clientid,
				char *enonce1, char *useragent,
				tv_t *now, char *by, char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *a_item, *u_item;
	int n;
	AUTHS *row;
	char *ins;
	char *secuserid = NULL;
	char *params[7 + HISTORYDATECOUNT];
	int par;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(auths_list);
	a_item = k_unlink_head(auths_list);
	K_WUNLOCK(auths_list);

	row = DATA_AUTHS(a_item);

	u_item = find_users(username);
	if (!u_item)
		goto unitem;

	STRNCPY(row->poolinstance, poolinstance);
	row->userid = DATA_USERS(u_item)->userid;
	new_worker(conn, false, row->userid, workername, DIFFICULTYDEFAULT_DEF_STR,
		   IDLENOTIFICATIONENABLED_DEF, IDLENOTIFICATIONTIME_DEF_STR, now,
		   by, code, inet);
	STRNCPY(row->workername, workername);
	TXT_TO_INT("clientid", clientid, row->clientid);
	STRNCPY(row->enonce1, enonce1);
	STRNCPY(row->useragent, useragent);

	HISTORYDATEINIT(row, now, by, code, inet);
	HISTORYDATETRANSFER(row);

	// Update even if DB fails
	workerstatus_update(row, NULL, NULL, NULL);

	row->authid = nextid(conn, "authid", (int64_t)1, now, by, code, inet);
	if (row->authid == 0)
		goto unitem;

	par = 0;
	params[par++] = bigint_to_buf(row->authid, NULL, 0);
	params[par++] = str_to_buf(row->poolinstance, NULL, 0);
	params[par++] = bigint_to_buf(row->userid, NULL, 0);
	params[par++] = str_to_buf(row->workername, NULL, 0);
	params[par++] = int_to_buf(row->clientid, NULL, 0);
	params[par++] = str_to_buf(row->enonce1, NULL, 0);
	params[par++] = str_to_buf(row->useragent, NULL, 0);
	HISTORYDATEPARAMS(params, par, row);
	PARCHK(par, params);

	ins = "insert into auths "
		"(authid,poolinstance,userid,workername,clientid,enonce1,useragent"
		HISTORYDATECONTROL ") values (" PQPARAM12 ")";

	res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
		PGLOGERR("Insert", rescode, conn);
		goto unparam;
	}

	secuserid = DATA_USERS(u_item)->secondaryuserid;

unparam:
	PQclear(res);
	for (n = 0; n < par; n++)
		free(params[n]);
unitem:
	K_WLOCK(auths_list);
	if (!secuserid)
		k_add_head(auths_list, a_item);
	else {
		auths_root = add_to_ktree(auths_root, a_item, cmp_auths);
		k_add_head(auths_store, a_item);
	}
	K_WUNLOCK(auths_list);

	return secuserid;
}

static bool auths_fill(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n, i;
	AUTHS *row;
	char *params[1];
	int par;
	char *field;
	char *sel;
	int fields = 6;
	bool ok;

	LOGDEBUG("%s(): select", __func__);

	// TODO: keep last x - since a user may login and mine for 100 days
	sel = "select "
		"authid,userid,workername,clientid,enonce1,useragent"
		HISTORYDATECONTROL
		" from auths where expirydate=$1";
	par = 0;
	params[par++] = tv_to_buf((tv_t *)(&default_expiry), NULL, 0);
	PARCHK(par, params);
	res = PQexecParams(conn, sel, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
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
	K_WLOCK(auths_list);
	for (i = 0; i < n; i++) {
		item = k_unlink_head(auths_list);
		row = DATA_AUTHS(item);

		PQ_GET_FLD(res, i, "authid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("authid", field, row->authid);

		PQ_GET_FLD(res, i, "userid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("userid", field, row->userid);

		PQ_GET_FLD(res, i, "workername", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("workername", field, row->workername);

		PQ_GET_FLD(res, i, "clientid", field, ok);
		if (!ok)
			break;
		TXT_TO_INT("clientid", field, row->clientid);

		PQ_GET_FLD(res, i, "enonce1", field, ok);
		if (!ok)
			break;
		TXT_TO_BLOB("enonce1", field, row->enonce1);

		PQ_GET_FLD(res, i, "useragent", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("useragent", field, row->useragent);

		HISTORYDATEFLDS(res, i, row, ok);
		if (!ok)
			break;

		auths_root = add_to_ktree(auths_root, item, cmp_auths);
		k_add_head(auths_store, item);
		workerstatus_update(row, NULL, NULL, NULL);
	}
	if (!ok)
		k_add_head(auths_list, item);

	K_WUNLOCK(auths_list);
	PQclear(res);

	if (ok) {
		LOGDEBUG("%s(): built", __func__);
		LOGWARNING("%s(): loaded %d auth records", __func__, n);
	}

	return ok;
}

void auths_reload()
{
	PGconn *conn = dbconnect();

	K_WLOCK(auths_list);
	auths_root = free_ktree(auths_root, NULL);
	k_list_transfer_to_head(auths_store, auths_list);
	K_WUNLOCK(auths_list);

	auths_fill(conn);

	PQfinish(conn);
}

// order by poolinstance asc,createdate asc
static double cmp_poolstats(K_ITEM *a, K_ITEM *b)
{
	double c = (double)strcmp(DATA_POOLSTATS(a)->poolinstance,
				  DATA_POOLSTATS(b)->poolinstance);
	if (c == 0) {
		c = tvdiff(&(DATA_POOLSTATS(a)->createdate),
			   &(DATA_POOLSTATS(b)->createdate));
	}
	return c;
}

static bool poolstats_add(PGconn *conn, bool store, char *poolinstance,
				char *elapsed, char *users, char *workers,
				char *hashrate, char *hashrate5m,
				char *hashrate1hr, char *hashrate24hr,
				tv_t *now, char *by, char *code, char *inet)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *p_item;
	int n;
	POOLSTATS *row;
	char *ins;
	char *params[8 + SIMPLEDATECOUNT];
	int par;
	bool ok = false;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(poolstats_list);
	p_item = k_unlink_head(poolstats_list);
	K_WUNLOCK(poolstats_list);

	row = DATA_POOLSTATS(p_item);

	STRNCPY(row->poolinstance, poolinstance);
	TXT_TO_BIGINT("elapsed", elapsed, row->elapsed);
	TXT_TO_INT("users", users, row->users);
	TXT_TO_INT("workers", workers, row->workers);
	TXT_TO_DOUBLE("hashrate", hashrate, row->hashrate);
	TXT_TO_DOUBLE("hashrate5m", hashrate5m, row->hashrate5m);
	TXT_TO_DOUBLE("hashrate1hr", hashrate1hr, row->hashrate1hr);
	TXT_TO_DOUBLE("hashrate24hr", hashrate24hr, row->hashrate24hr);

	SIMPLEDATEINIT(row, now, by, code, inet);
	SIMPLEDATETRANSFER(row);

	par = 0;
	if (store) {
		params[par++] = str_to_buf(row->poolinstance, NULL, 0);
		params[par++] = bigint_to_buf(row->elapsed, NULL, 0);
		params[par++] = int_to_buf(row->users, NULL, 0);
		params[par++] = int_to_buf(row->workers, NULL, 0);
		params[par++] = bigint_to_buf(row->hashrate, NULL, 0);
		params[par++] = bigint_to_buf(row->hashrate5m, NULL, 0);
		params[par++] = bigint_to_buf(row->hashrate1hr, NULL, 0);
		params[par++] = bigint_to_buf(row->hashrate24hr, NULL, 0);
		SIMPLEDATEPARAMS(params, par, row);
		PARCHK(par, params);

		ins = "insert into poolstats "
			"(poolinstance,elapsed,users,workers,hashrate,"
			"hashrate5m,hashrate1hr,hashrate24hr"
			SIMPLEDATECONTROL ") values (" PQPARAM11 ")";

		res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
		rescode = PQresultStatus(res);
		if (!PGOK(rescode)) {
			PGLOGERR("Insert", rescode, conn);
			goto unparam;
		}
	}

	ok = true;
unparam:
	if (store) {
		PQclear(res);
		for (n = 0; n < par; n++)
			free(params[n]);
	}

	K_WLOCK(poolstats_list);
	if (!ok)
		k_add_head(poolstats_list, p_item);
	else {
		poolstats_root = add_to_ktree(poolstats_root, p_item, cmp_poolstats);
		k_add_head(poolstats_store, p_item);
	}
	K_WUNLOCK(poolstats_list);

	return ok;
}

// TODO: data selection - only require ?
static bool poolstats_fill(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n, i;
	POOLSTATS *row;
	char *field;
	char *sel;
	int fields = 8;
	bool ok;

	LOGDEBUG("%s(): select", __func__);

	sel = "select "
		"poolinstance,elapsed,users,workers,hashrate,hashrate5m,"
		"hashrate1hr,hashrate24hr"
		SIMPLEDATECONTROL
		" from poolstats";
	res = PQexec(conn, sel);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
		PGLOGERR("Select", rescode, conn);
		PQclear(res);
		return false;
	}

	n = PQnfields(res);
	if (n != (fields + SIMPLEDATECOUNT)) {
		LOGERR("%s(): Invalid field count - should be %d, but is %d",
			__func__, fields + SIMPLEDATECOUNT, n);
		PQclear(res);
		return false;
	}

	n = PQntuples(res);
	LOGDEBUG("%s(): tree build count %d", __func__, n);
	ok = true;
	K_WLOCK(poolstats_list);
	for (i = 0; i < n; i++) {
		item = k_unlink_head(poolstats_list);
		row = DATA_POOLSTATS(item);

		PQ_GET_FLD(res, i, "poolinstance", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("poolinstance", field, row->poolinstance);

		PQ_GET_FLD(res, i, "elapsed", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("elapsed", field, row->elapsed);

		PQ_GET_FLD(res, i, "users", field, ok);
		if (!ok)
			break;
		TXT_TO_INT("users", field, row->users);

		PQ_GET_FLD(res, i, "workers", field, ok);
		if (!ok)
			break;
		TXT_TO_INT("workers", field, row->workers);

		PQ_GET_FLD(res, i, "hashrate", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("hashrate", field, row->hashrate);

		PQ_GET_FLD(res, i, "hashrate5m", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("hashrate5m", field, row->hashrate5m);

		PQ_GET_FLD(res, i, "hashrate1hr", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("hashrate1hr", field, row->hashrate1hr);

		PQ_GET_FLD(res, i, "hashrate24hr", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("hashrate24hr", field, row->hashrate24hr);

		poolstats_root = add_to_ktree(poolstats_root, item, cmp_poolstats);
		k_add_head(poolstats_store, item);
	}
	if (!ok)
		k_add_head(poolstats_list, item);

	K_WUNLOCK(poolstats_list);
	PQclear(res);

	if (ok) {
		LOGDEBUG("%s(): built", __func__);
		LOGWARNING("%s(): loaded %d poolstats records", __func__, n);
	}

	return ok;
}

void poolstats_reload()
{
	PGconn *conn = dbconnect();

	K_WLOCK(poolstats_list);
	poolstats_root = free_ktree(poolstats_root, NULL);
	k_list_transfer_to_head(poolstats_store, poolstats_list);
	K_WUNLOCK(poolstats_list);

	poolstats_fill(conn);

	PQfinish(conn);
}

static void dsp_userstats(K_ITEM *item, FILE *stream)
{
	USERSTATS *u = NULL;
	char *createdate_buf;

	if (!stream)
		LOGERR("%s() called with (null) stream", __func__);
	else {
		if (!item)
			fprintf(stream, "%s() called with (null) item\n", __func__);
		else {
			u = DATA_USERSTATS(item);

			createdate_buf = tv_to_buf(&(u->createdate), NULL, 0);
			fprintf(stream, " pi='%s' e=%"PRId64" uid=%"PRId64" w='%s' Hs=%f "
					"Hs5m=%f Hs1hr=%f Hs24hr=%f cd=%s\n",
					u->poolinstance, u->elapsed, u->userid,
					u->workername, u->hashrate, u->hashrate5m,
					u->hashrate1hr, u->hashrate24hr, createdate_buf);
			free(createdate_buf);
		}
	}
}

/* order by userid asc,statsdate asc,poolinstance asc,workername asc
   as per required for userstats homepage summarisation */
static double cmp_userstats(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_USERSTATS(a)->userid -
			    DATA_USERSTATS(b)->userid);
	if (c == 0) {
		c = tvdiff(&(DATA_USERSTATS(a)->statsdate),
			   &(DATA_USERSTATS(b)->statsdate));
		if (c == 0) {
			c = (double)strcmp(DATA_USERSTATS(a)->poolinstance,
					   DATA_USERSTATS(b)->poolinstance);
			if (c == 0) {
				c = (double)strcmp(DATA_USERSTATS(a)->workername,
						   DATA_USERSTATS(b)->workername);
			}
		}
	}
	return c;
}

/* order by userid asc,workername asc
   temporary tree for summing userstats when sending user homepage info */
static double cmp_userstats_workername(K_ITEM *a, K_ITEM *b)
{
	double c = (double)(DATA_USERSTATS(a)->userid -
			    DATA_USERSTATS(b)->userid);
	if (c == 0) {
		c = (double)strcmp(DATA_USERSTATS(a)->workername,
				   DATA_USERSTATS(b)->workername);
	}
	return c;
}

static bool userstats_add_db(PGconn *conn, USERSTATS *row)
{
	ExecStatusType rescode;
	PGresult *res;
	char *ins;
	bool ok = false;
	char *params[9 + HISTORYDATECOUNT];
	int par;
	int n;

	LOGDEBUG("%s(): store", __func__);

	par = 0;
	params[par++] = bigint_to_buf(row->userid, NULL, 0);
	params[par++] = str_to_buf(row->workername, NULL, 0);
	params[par++] = bigint_to_buf(row->elapsed, NULL, 0);
	params[par++] = double_to_buf(row->hashrate, NULL, 0);
	params[par++] = double_to_buf(row->hashrate5m, NULL, 0);
	params[par++] = double_to_buf(row->hashrate1hr, NULL, 0);
	params[par++] = double_to_buf(row->hashrate24hr, NULL, 0);
	params[par++] = str_to_buf(row->summarylevel, NULL, 0);
	params[par++] = tv_to_buf(&(row->statsdate), NULL, 0);
	SIMPLEDATEPARAMS(params, par, row);
	PARCHK(par, params);

	ins = "insert into userstats "
		"(userid,workername,elapsed,hashrate,hashrate5m,"
		"hashrate1hr,hashrate24hr,summarylevel,statsdate"
		HISTORYDATECONTROL ") values (" PQPARAM13 ")";

	res = PQexecParams(conn, ins, par, NULL, (const char **)params, NULL, NULL, 0);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
		PGLOGERR("Insert", rescode, conn);
		goto unparam;
	}

	ok = true;
unparam:
	PQclear(res);
	for (n = 0; n < par; n++)
		free(params[n]);

	return ok;
}

static bool userstats_add(char *poolinstance, char *elapsed, char *username,
			  char *workername, char *hashrate, char *hashrate5m,
			  char *hashrate1hr, char *hashrate24hr, bool idle,
			  bool eos, tv_t *now, char *by, char *code, char *inet)
{
	K_ITEM *us_item, *u_item, *us_match, *us_next;
	USERSTATS *row;
	tv_t createdate;

	LOGDEBUG("%s(): add", __func__);

	K_WLOCK(userstats_list);
	us_item = k_unlink_head(userstats_list);
	K_WUNLOCK(userstats_list);

	row = DATA_USERSTATS(us_item);

	STRNCPY(row->poolinstance, poolinstance);
	TXT_TO_BIGINT("elapsed", elapsed, row->elapsed);
	u_item = find_users(username);
	if (!u_item)
		return false;
	row->userid = DATA_USERS(u_item)->userid;
	TXT_TO_STR("workername", workername, row->workername);
	TXT_TO_DOUBLE("hashrate", hashrate, row->hashrate);
	TXT_TO_DOUBLE("hashrate5m", hashrate5m, row->hashrate5m);
	TXT_TO_DOUBLE("hashrate1hr", hashrate1hr, row->hashrate1hr);
	TXT_TO_DOUBLE("hashrate24hr", hashrate24hr, row->hashrate24hr);
	row->idle = idle;
	row->summarylevel[0] = SUMMARY_NONE;
	row->summarylevel[1] = '\0';
	SIMPLEDATEINIT(row, now, by, code, inet);
	SIMPLEDATETRANSFER(row);
	memcpy(&(row->statsdate), &(row->createdate), sizeof(row->statsdate));

	if (eos) {
		// Save it for end processing
		createdate.tv_sec = row->createdate.tv_sec;
		createdate.tv_usec = row->createdate.tv_usec;
	}

	workerstatus_update(NULL, NULL, row, NULL);

	/* group at full key: userid,createdate,poolinstance,workername
	   i.e. ignore instance and group together down at workername */
	us_match = userstats_eos_store->head;
	while (us_match && cmp_userstats(us_item, us_match) != 0.0)
		us_match = us_match->next;

	if (us_match) {
		DATA_USERSTATS(us_match)->hashrate += row->hashrate;
		DATA_USERSTATS(us_match)->hashrate5m += row->hashrate5m;
		DATA_USERSTATS(us_match)->hashrate1hr += row->hashrate1hr;
		DATA_USERSTATS(us_match)->hashrate24hr += row->hashrate24hr;
		// Minimum elapsed of the data set
		if (DATA_USERSTATS(us_match)->elapsed > row->elapsed)
			DATA_USERSTATS(us_match)->elapsed = row->elapsed;
		// Unused
		K_WLOCK(userstats_list);
		k_add_head(userstats_list, us_item);
		K_WUNLOCK(userstats_list);
	} else {
		// New worker
		K_WLOCK(userstats_list);
		k_add_head(userstats_eos_store, us_item);
		K_WUNLOCK(userstats_list);
	}

	if (eos) {
		K_WLOCK(userstats_list);
		us_next = userstats_eos_store->head;
		while (us_next) {
			if (tvdiff(&DATA_USERSTATS(us_next)->createdate, &createdate) != 0.0) {
				char date_buf[DATE_BUFSIZ];
				LOGERR("userstats != eos '%s' discarded: %s/%"PRId64"/%s",
				       tv_to_buf(&createdate, date_buf, DATE_BUFSIZ),
				       DATA_USERSTATS(us_next)->poolinstance,
				       DATA_USERSTATS(us_next)->userid,
				       DATA_USERSTATS(us_next)->workername);
				us_next = us_next->next;
			} else {
				us_match = us_next;
				us_next = us_match->next;
				k_unlink_item(userstats_eos_store, us_match);
				userstats_root = add_to_ktree(userstats_root, us_match,
								cmp_userstats);
				k_add_head(userstats_store, us_match);
			}
		}
		// Discard them
		if (userstats_eos_store->count > 0)
			k_list_transfer_to_head(userstats_eos_store, userstats_list);
		K_WUNLOCK(userstats_list);
	}

	return true;
}

// TODO: data selection - only require ?
static bool userstats_fill(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	K_ITEM *item;
	int n, i;
	USERSTATS *row;
	char *field;
	char *sel;
	int fields = 9;
	bool ok;

	LOGDEBUG("%s(): select", __func__);

	sel = "select "
		"userid,workername,elapsed,hashrate,hashrate5m,hashrate1hr,"
		"hashrate24hr,summarylevel,statsdate"
		SIMPLEDATECONTROL
		" from userstats";
	res = PQexec(conn, sel);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
		PGLOGERR("Select", rescode, conn);
		PQclear(res);
		return false;
	}

	n = PQnfields(res);
	if (n != (fields + SIMPLEDATECOUNT)) {
		LOGERR("%s(): Invalid field count - should be %d, but is %d",
			__func__, fields + SIMPLEDATECOUNT, n);
		PQclear(res);
		return false;
	}

	n = PQntuples(res);
	LOGDEBUG("%s(): tree build count %d", __func__, n);
	ok = true;
	K_WLOCK(userstats_list);
	for (i = 0; i < n; i++) {
		item = k_unlink_head(userstats_list);
		row = DATA_USERSTATS(item);

		// From DB
		row->poolinstance[0] = '\0';

		PQ_GET_FLD(res, i, "userid", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("userid", field, row->userid);

		PQ_GET_FLD(res, i, "workername", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("workername", field, row->workername);

		PQ_GET_FLD(res, i, "elapsed", field, ok);
		if (!ok)
			break;
		TXT_TO_BIGINT("elapsed", field, row->elapsed);

		PQ_GET_FLD(res, i, "hashrate", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("hashrate", field, row->hashrate);

		PQ_GET_FLD(res, i, "hashrate5m", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("hashrate5m", field, row->hashrate5m);

		PQ_GET_FLD(res, i, "hashrate1hr", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("hashrate1hr", field, row->hashrate1hr);

		PQ_GET_FLD(res, i, "hashrate24hr", field, ok);
		if (!ok)
			break;
		TXT_TO_DOUBLE("hashrate24hr", field, row->hashrate24hr);

		PQ_GET_FLD(res, i, "summarylevel", field, ok);
		if (!ok)
			break;
		TXT_TO_STR("summarylevel", field, row->summarylevel);

		PQ_GET_FLD(res, i, "statsdate", field, ok);
		if (!ok)
			break;
		TXT_TO_TV("statsdate", field, row->statsdate);

		// From DB - 1hr means it must have been idle > 10m
		if (row->hashrate5m == 0.0 && row->hashrate1hr == 0.0)
			row->idle = true;
		else
			row->idle = false;

		userstats_root = add_to_ktree(userstats_root, item, cmp_userstats);
		k_add_head(userstats_store, item);

		workerstatus_update(NULL, NULL, row, NULL);
	}
	if (!ok)
		k_add_head(userstats_list, item);

	K_WUNLOCK(userstats_list);
	PQclear(res);

	if (ok) {
		LOGDEBUG("%s(): built", __func__);
		LOGWARNING("%s(): loaded %d userstats records", __func__, n);
	}

	return ok;
}

void userstats_reload()
{
	PGconn *conn = dbconnect();

	K_WLOCK(userstats_list);
	userstats_root = free_ktree(userstats_root, NULL);
	k_list_transfer_to_head(userstats_store, userstats_list);
	K_WUNLOCK(userstats_list);

	userstats_fill(conn);

	PQfinish(conn);
}

static bool check_db_version(PGconn *conn)
{
	ExecStatusType rescode;
	PGresult *res;
	char *field;
	char *sel;
	int fields = 2;
	bool ok;
	int n;

	LOGDEBUG("%s(): select", __func__);

	sel = "select * from version;";
	res = PQexec(conn, sel);
	rescode = PQresultStatus(res);
	if (!PGOK(rescode)) {
		PGLOGEMERG("Select", rescode, conn);
		PQclear(res);
		return false;
	}

	n = PQnfields(res);
	if (n != fields) {
		LOGEMERG("%s(): Invalid field count - should be %d, but is %d",
			__func__, fields, n);
		PQclear(res);
		return false;
	}

	n = PQntuples(res);
	if (n != 1) {
		LOGEMERG("%s(): Invalid record count - should be %d, but is %d",
			__func__, 1, n);
		PQclear(res);
		return false;
	}

	ok = true;
	PQ_GET_FLD(res, 0, "vlock", field, ok);
	if (!ok) {
		LOGEMERG("%s(): Missing field vlock", __func__);
		PQclear(res);
		return false;
	}

	if (strcmp(field, DB_VLOCK)) {
		LOGEMERG("%s(): incorrect vlock '%s' - should be '%s'",
			__func__, field, DB_VLOCK);
		PQclear(res);
		return false;
	}

	ok = true;
	PQ_GET_FLD(res, 0, "version", field, ok);
	if (!ok) {
		LOGEMERG("%s(): Missing field version", __func__);
		PQclear(res);
		return false;
	}

	if (strcmp(field, DB_VERSION)) {
		LOGEMERG("%s(): incorrect version '%s' - should be '%s'",
			__func__, field, DB_VERSION);
		PQclear(res);
		return false;
	}

	PQclear(res);

	LOGWARNING("%s(): DB version (%s) correct", __func__, DB_VERSION);

	return true;
}

static bool getdata()
{
	PGconn *conn = dbconnect();
	bool ok = true;

	if (!(ok = check_db_version(conn)))
		goto matane;
	if (!(ok = users_fill(conn)))
		goto matane;
	if (!(ok = workers_fill(conn)))
		goto matane;
	if (!(ok = payments_fill(conn)))
		goto matane;
	if (!(ok = workinfo_fill(conn)))
		goto matane;
	if (!(ok = shares_fill()))
		goto matane;
	if (!(ok = shareerrors_fill()))
		goto matane;
	if (!(ok = auths_fill(conn)))
		goto matane;
	if (!(ok = poolstats_fill(conn)))
		goto matane;
	ok = userstats_fill(conn);

matane:

	PQfinish(conn);
	return ok;
}

/* TODO:
static PGconn *dbquit(PGconn *conn)
{
	if (conn != NULL)
		PQfinish(conn);
	return NULL;
}
*/

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

static bool setup_data()
{
	K_TREE_CTX ctx[1];
	K_ITEM look, *found;
	WORKINFO wi;

	transfer_list = k_new_list("Transfer", sizeof(TRANSFER), ALLOC_TRANSFER, LIMIT_TRANSFER, true);
	transfer_store = k_new_store(transfer_list);
	transfer_root = new_ktree();
	transfer_list->dsp_func = dsp_transfer;

	users_list = k_new_list("Users", sizeof(USERS), ALLOC_USERS, LIMIT_USERS, true);
	users_store = k_new_store(users_list);
	users_root = new_ktree();
	userid_root = new_ktree();

	workers_list = k_new_list("Workers", sizeof(WORKERS), ALLOC_WORKERS, LIMIT_WORKERS, true);
	workers_store = k_new_store(workers_list);
	workers_root = new_ktree();

	payments_list = k_new_list("Payments", sizeof(PAYMENTS), ALLOC_PAYMENTS, LIMIT_PAYMENTS, true);
	payments_store = k_new_store(payments_list);
	payments_root = new_ktree();

	idcontrol_list = k_new_list("IDControl", sizeof(IDCONTROL), ALLOC_IDCONTROL, LIMIT_IDCONTROL, true);
	idcontrol_store = k_new_store(idcontrol_list);

	workinfo_list = k_new_list("WorkInfo", sizeof(WORKINFO), ALLOC_WORKINFO, LIMIT_WORKINFO, true);
	workinfo_store = k_new_store(workinfo_list);
	workinfo_root = new_ktree();
	workinfo_height_root = new_ktree();

	shares_list = k_new_list("Shares", sizeof(SHARES), ALLOC_SHARES, LIMIT_SHARES, true);
	shares_store = k_new_store(shares_list);
	shares_root = new_ktree();

	shareerrors_list = k_new_list("ShareErrors", sizeof(SHAREERRORS), ALLOC_SHAREERRORS, LIMIT_SHAREERRORS, true);
	shareerrors_store = k_new_store(shareerrors_list);
	shareerrors_root = new_ktree();

	sharesummary_list = k_new_list("ShareSummary", sizeof(SHARESUMMARY), ALLOC_SHARESUMMARY, LIMIT_SHARESUMMARY, true);
	sharesummary_store = k_new_store(sharesummary_list);
	sharesummary_root = new_ktree();
	sharesummary_workinfoid_root = new_ktree();
	sharesummary_list->dsp_func = dsp_sharesummary;

	blocks_list = k_new_list("Blocks", sizeof(BLOCKS), ALLOC_BLOCKS, LIMIT_BLOCKS, true);
	blocks_store = k_new_store(blocks_list);
	blocks_root = new_ktree();

	auths_list = k_new_list("Auths", sizeof(AUTHS), ALLOC_AUTHS, LIMIT_AUTHS, true);
	auths_store = k_new_store(auths_list);
	auths_root = new_ktree();

	poolstats_list = k_new_list("PoolStats", sizeof(POOLSTATS), ALLOC_POOLSTATS, LIMIT_POOLSTATS, true);
	poolstats_store = k_new_store(poolstats_list);
	poolstats_root = new_ktree();

	userstats_list = k_new_list("UserStats", sizeof(USERSTATS), ALLOC_USERSTATS, LIMIT_USERSTATS, true);
	userstats_store = k_new_store(userstats_list);
	userstats_eos_store = k_new_store(userstats_list);
	userstats_summ = k_new_store(userstats_list);
	userstats_root = new_ktree();
	userstats_list->dsp_func = dsp_userstats;

	workerstatus_list = k_new_list("WorkerStatus", sizeof(WORKERSTATUS), ALLOC_WORKERSTATUS, LIMIT_WORKERSTATUS, true);
	workerstatus_store = k_new_store(workerstatus_list);
	workerstatus_root = new_ktree();

	if (!getdata())
		return false;

	workinfo_current = last_in_ktree(workinfo_height_root, ctx);
	if (workinfo_current) {
		STRNCPY(wi.coinbase1, DATA_WORKINFO(workinfo_current)->coinbase1);
		wi.createdate.tv_sec = 0L;
		wi.createdate.tv_usec = 0L;
		look.data = (void *)(&wi);
		// Find the first workinfo for this height
		found = find_after_in_ktree(workinfo_height_root, &look, cmp_workinfo_height, ctx);
		if (found)
			last_bc = &(DATA_WORKINFO(found)->createdate);
		// No longer needed
		workinfo_height_root = free_ktree(workinfo_height_root, NULL);
	}

	return true;
}

static char *cmd_adduser(char *cmd, char *id, tv_t *now, char *by, char *code, char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);

	K_ITEM *i_username, *i_emailaddress, *i_passwordhash;
	PGconn *conn;
	bool ok;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

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
	ok = users_add(conn, DATA_TRANSFER(i_username)->data,
				DATA_TRANSFER(i_emailaddress)->data,
				DATA_TRANSFER(i_passwordhash)->data,
				now, by, code, inet);
	PQfinish(conn);

	if (!ok) {
		LOGERR("%s.failed.DBE", id);
		return strdup("failed.DBE");
	}
	LOGDEBUG("%s.ok.added %s", id, DATA_TRANSFER(i_username)->data);
	snprintf(reply, siz, "ok.added %s", DATA_TRANSFER(i_username)->data);
	return strdup(reply);
}

static char *cmd_chkpass(char *cmd, char *id, __maybe_unused tv_t *now, __maybe_unused char *by,
				__maybe_unused char *code, __maybe_unused char *inet)
{
	K_ITEM *i_username, *i_passwordhash, *u_item;
	char reply[1024] = "";
	size_t siz = sizeof(reply);
	bool ok;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	i_username = require_name("username", 3, (char *)userpatt, reply, siz);
	if (!i_username)
		return strdup(reply);

	i_passwordhash = require_name("passwordhash", 64, (char *)hashpatt, reply, siz);
	if (!i_passwordhash)
		return strdup(reply);

	u_item = find_users(DATA_TRANSFER(i_username)->data);

	if (!u_item)
		ok = false;
	else {
		if (strcasecmp(DATA_TRANSFER(i_passwordhash)->data, DATA_USERS(u_item)->passwordhash) == 0)
			ok = true;
		else
			ok = false;
	}

	if (!ok) {
		LOGERR("%s.failed.%s", id, DATA_TRANSFER(i_username)->data);
		return strdup("failed.");
	}
	LOGDEBUG("%s.ok.%s", id, DATA_TRANSFER(i_username)->data);
	return strdup("ok.");
}

static char *cmd_poolstats(char *cmd, __maybe_unused char *id, tv_t *now, char *by, char *code, char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);
	K_TREE_CTX ctx[1];
	PGconn *conn;
	bool store;

	// log to logfile

	K_ITEM *i_poolinstance, *i_elapsed, *i_users, *i_workers;
	K_ITEM *i_hashrate, *i_hashrate5m, *i_hashrate1hr, *i_hashrate24hr;
	K_ITEM *i_createdate, look, *ps;
	tv_t createdate;
	POOLSTATS row;
	bool ok = false;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	i_poolinstance = require_name("poolinstance", 1, NULL, reply, siz);
	if (!i_poolinstance)
		return strdup(reply);

	i_elapsed = require_name("elapsed", 1, NULL, reply, siz);
	if (!i_elapsed)
		return strdup(reply);

	i_users = require_name("users", 1, NULL, reply, siz);
	if (!i_users)
		return strdup(reply);

	i_workers = require_name("workers", 1, NULL, reply, siz);
	if (!i_workers)
		return strdup(reply);

	i_hashrate = require_name("hashrate", 1, NULL, reply, siz);
	if (!i_hashrate)
		return strdup(reply);

	i_hashrate5m = require_name("hashrate5m", 1, NULL, reply, siz);
	if (!i_hashrate5m)
		return strdup(reply);

	i_hashrate1hr = require_name("hashrate1hr", 1, NULL, reply, siz);
	if (!i_hashrate1hr)
		return strdup(reply);

	i_hashrate24hr = require_name("hashrate24hr", 1, NULL, reply, siz);
	if (!i_hashrate24hr)
		return strdup(reply);

	STRNCPY(row.poolinstance, DATA_TRANSFER(i_poolinstance)->data);
	row.createdate.tv_sec = date_eot.tv_sec;
	row.createdate.tv_usec = date_eot.tv_usec;
	look.data = (void *)(&row);
	ps = find_before_in_ktree(poolstats_root, &look, cmp_poolstats, ctx);
	if (!ps)
		store = true;
	else {
		i_createdate = require_name("createdate", 1, NULL, reply, siz);
		if (!i_createdate)
			return strdup(reply);
		TXT_TO_CTV("createdate", DATA_TRANSFER(i_createdate)->data, createdate);
		if (tvdiff(&createdate, &(row.createdate)) > STATS_PER)
			store = true;
		else
			store = false;
	}

	conn = dbconnect();
	ok = poolstats_add(conn, store, DATA_TRANSFER(i_poolinstance)->data,
					DATA_TRANSFER(i_elapsed)->data,
					DATA_TRANSFER(i_users)->data,
					DATA_TRANSFER(i_workers)->data,
					DATA_TRANSFER(i_hashrate)->data,
					DATA_TRANSFER(i_hashrate5m)->data,
					DATA_TRANSFER(i_hashrate1hr)->data,
					DATA_TRANSFER(i_hashrate24hr)->data,
					now, by, code, inet);
	PQfinish(conn);

	if (!ok) {
		LOGERR("%s.failed.DBE", id);
		return strdup("failed.DBE");
	}
	LOGDEBUG("%s.ok.", id);
	snprintf(reply, siz, "ok.");
	return strdup(reply);
}

static char *cmd_userstats(char *cmd, __maybe_unused char *id, tv_t *now, char *by, char *code, char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);

	// log to logfile

	K_ITEM *i_poolinstance, *i_elapsed, *i_username, *i_workername;
	K_ITEM *i_hashrate, *i_hashrate5m, *i_hashrate1hr, *i_hashrate24hr;
	K_ITEM *i_eos, *i_idle;
	bool ok = false, idle, eos;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	i_poolinstance = require_name("poolinstance", 1, NULL, reply, siz);
	if (!i_poolinstance)
		return strdup(reply);

	i_elapsed = require_name("elapsed", 1, NULL, reply, siz);
	if (!i_elapsed)
		return strdup(reply);

	i_username = require_name("username", 1, NULL, reply, siz);
	if (!i_username)
		return strdup(reply);

	i_workername = require_name("workername", 1, NULL, reply, siz);
	if (!i_workername)
		return strdup(reply);

	i_hashrate = require_name("hashrate", 1, NULL, reply, siz);
	if (!i_hashrate)
		return strdup(reply);

	i_hashrate5m = require_name("hashrate5m", 1, NULL, reply, siz);
	if (!i_hashrate5m)
		return strdup(reply);

	i_hashrate1hr = require_name("hashrate1hr", 1, NULL, reply, siz);
	if (!i_hashrate1hr)
		return strdup(reply);

	i_hashrate24hr = require_name("hashrate24hr", 1, NULL, reply, siz);
	if (!i_hashrate24hr)
		return strdup(reply);

	i_idle = require_name("idle", 1, NULL, reply, siz);
	if (!i_idle)
		return strdup(reply);

	idle = (strcasecmp(DATA_TRANSFER(i_idle)->data, TRUE_STR) == 0);

	i_eos = require_name("eos", 1, NULL, reply, siz);
	if (!i_eos)
		return strdup(reply);

	eos = (strcasecmp(DATA_TRANSFER(i_eos)->data, TRUE_STR) == 0);
		
	ok = userstats_add(DATA_TRANSFER(i_poolinstance)->data,
			   DATA_TRANSFER(i_elapsed)->data,
			   DATA_TRANSFER(i_username)->data,
			   DATA_TRANSFER(i_workername)->data,
			   DATA_TRANSFER(i_hashrate)->data,
			   DATA_TRANSFER(i_hashrate5m)->data,
			   DATA_TRANSFER(i_hashrate1hr)->data,
			   DATA_TRANSFER(i_hashrate24hr)->data,
			   idle, eos, now, by, code, inet);

	if (!ok) {
		LOGERR("%s.failed.DATA", id);
		return strdup("failed.DATA");
	}
	LOGDEBUG("%s.ok.", id);
	snprintf(reply, siz, "ok.");
	return strdup(reply);
}

static char *cmd_newid(char *cmd, char *id, tv_t *now, char *by, char *code, char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);
	K_ITEM *i_idname, *i_idvalue, *look;
	IDCONTROL *row;
	char *params[2 + MODIFYDATECOUNT];
	int par;
	bool ok = false;
	ExecStatusType rescode;
	PGresult *res;
	PGconn *conn;
	char *ins;
	int n;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

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

	STRNCPY(row->idname, DATA_TRANSFER(i_idname)->data);
	TXT_TO_BIGINT("idvalue", DATA_TRANSFER(i_idvalue)->data, row->lastid);
	MODIFYDATEINIT(row, now, by, code, inet);

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
	if (!PGOK(rescode)) {
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
		LOGERR("%s.failed.DBE", id);
		return strdup("failed.DBE");
	}
	LOGDEBUG("%s.ok.added %s %"PRId64, id, DATA_TRANSFER(i_idname)->data, row->lastid);
	snprintf(reply, siz, "ok.added %s %"PRId64,
				DATA_TRANSFER(i_idname)->data, row->lastid);
	return strdup(reply);
}

static char *cmd_payments(char *cmd, char *id, __maybe_unused tv_t *now, __maybe_unused char *by,
				__maybe_unused char *code, __maybe_unused char *inet)
{
	K_ITEM *i_username, look, *u_item, *p_item;
	K_TREE_CTX ctx[1];
	PAYMENTS payments;
	char reply[1024] = "";
	char tmp[1024];
	size_t siz = sizeof(reply);
	char *buf;
	size_t len, off;
	int rows;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	i_username = require_name("username", 3, (char *)userpatt, reply, siz);
	if (!i_username)
		return strdup(reply);

	u_item = find_users(DATA_TRANSFER(i_username)->data);
	if (!u_item)
		return strdup("bad");

	payments.userid = DATA_USERS(u_item)->userid;
	payments.paydate.tv_sec = 0;
	payments.paydate.tv_usec = 0;
	look.data = (void *)(&payments);
	p_item = find_after_in_ktree(payments_root, &look, cmp_payments, ctx);
	APPEND_REALLOC_INIT(buf, off, len);
	APPEND_REALLOC(buf, off, len, "ok.");
	rows = 0;
	while (p_item && DATA_PAYMENTS(p_item)->userid == DATA_USERS(u_item)->userid) {
		tv_to_buf(&(DATA_PAYMENTS(p_item)->paydate), reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "paydate%d=%s%c", rows, reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		str_to_buf(DATA_PAYMENTS(p_item)->payaddress, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "payaddress%d=%s%c", rows, reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		bigint_to_buf(DATA_PAYMENTS(p_item)->amount, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "amount%d=%s%c", rows, reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		rows++;
		p_item = next_in_ktree(ctx);
	}
	snprintf(tmp, sizeof(tmp), "rows=%d", rows);
	APPEND_REALLOC(buf, off, len, tmp);

	LOGDEBUG("%s.ok.%s", id, DATA_TRANSFER(i_username)->data);
	return buf;
}

static char *cmd_workers(char *cmd, char *id, __maybe_unused tv_t *now, __maybe_unused char *by,
				__maybe_unused char *code, __maybe_unused char *inet)
{
	K_ITEM *i_username, *i_stats, wlook, *u_item, *w_item, uslook, *us_item, *ws_item;
	K_TREE_CTX w_ctx[1], us_ctx[1];
	WORKERS workers;
	USERSTATS userstats;
	char reply[1024] = "";
	char tmp[1024];
	size_t siz = sizeof(reply);
	char *buf;
	size_t len, off;
	bool stats;
	int rows;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	i_username = require_name("username", 3, (char *)userpatt, reply, siz);
	if (!i_username)
		return strdup(reply);

	u_item = find_users(DATA_TRANSFER(i_username)->data);
	if (!u_item)
		return strdup("bad");

	i_stats = optional_name("stats", 1, NULL);
	if (!i_stats)
		stats = false;
	else
		stats = (strcasecmp(DATA_TRANSFER(i_stats)->data, TRUE_STR) == 0);

	workers.userid = DATA_USERS(u_item)->userid;
	workers.workername[0] = '\0';
	workers.expirydate.tv_sec = 0;
	workers.expirydate.tv_usec = 0;
	wlook.data = (void *)(&workers);
	w_item = find_after_in_ktree(workers_root, &wlook, cmp_workers, w_ctx);
	APPEND_REALLOC_INIT(buf, off, len);
	APPEND_REALLOC(buf, off, len, "ok.");
	rows = 0;
	while (w_item && DATA_WORKERS(w_item)->userid == DATA_USERS(u_item)->userid) {
		if (tvdiff(&(DATA_WORKERS(w_item)->expirydate), (tv_t *)&default_expiry) == 0.0) {
			str_to_buf(DATA_WORKERS(w_item)->workername, reply, sizeof(reply));
			snprintf(tmp, sizeof(tmp), "workername%d=%s%c", rows, reply, FLDSEP);
			APPEND_REALLOC(buf, off, len, tmp);

			int_to_buf(DATA_WORKERS(w_item)->difficultydefault, reply, sizeof(reply));
			snprintf(tmp, sizeof(tmp), "difficultydefault%d=%s%c", rows, reply, FLDSEP);
			APPEND_REALLOC(buf, off, len, tmp);

			str_to_buf(DATA_WORKERS(w_item)->idlenotificationenabled, reply, sizeof(reply));
			snprintf(tmp, sizeof(tmp), "idlenotificationenabled%d=%s%c", rows, reply, FLDSEP);
			APPEND_REALLOC(buf, off, len, tmp);

			int_to_buf(DATA_WORKERS(w_item)->idlenotificationtime, reply, sizeof(reply));
			snprintf(tmp, sizeof(tmp), "idlenotificationtime%d=%s%c", rows, reply, FLDSEP);
			APPEND_REALLOC(buf, off, len, tmp);

			if (stats) {
				K_TREE *userstats_workername_root = new_ktree();
				K_TREE_CTX usw_ctx[1];
				double w_hashrate5m, w_hashrate1hr;
				int64_t w_elapsed;
				tv_t w_lastshare;

				w_hashrate5m = w_hashrate1hr = 0.0;
				w_elapsed = -1;
				w_lastshare.tv_sec = 0;

				ws_item = find_workerstatus(DATA_USERS(u_item)->userid,
							    DATA_WORKERS(w_item)->workername);
				if (ws_item)
					w_lastshare.tv_sec = DATA_WORKERSTATUS(ws_item)->share.tv_sec;

				// find last stored userid record
				userstats.userid = DATA_USERS(u_item)->userid;
				userstats.statsdate.tv_sec = date_eot.tv_sec;
				userstats.statsdate.tv_usec = date_eot.tv_usec;
				// find/cmp doesn't get to here
				userstats.poolinstance[0] = '\0';
				userstats.workername[0] = '\0';
				uslook.data = (void *)(&userstats);
				K_RLOCK(userstats_list);
				us_item = find_before_in_ktree(userstats_root, &uslook, cmp_userstats, us_ctx);
				while (us_item && DATA_USERSTATS(us_item)->userid == userstats.userid) {
					if (strcmp(DATA_USERSTATS(us_item)->workername, DATA_WORKERS(w_item)->workername) == 0) {
						if (tvdiff(now, &(DATA_USERSTATS(us_item)->statsdate)) < USERSTATS_PER_S) {
							// TODO: add together the latest per pool instance (this is the latest per worker)
							if (!find_in_ktree(userstats_workername_root, us_item, cmp_userstats_workername, usw_ctx)) {
								w_hashrate5m += DATA_USERSTATS(us_item)->hashrate5m;
								w_hashrate1hr += DATA_USERSTATS(us_item)->hashrate1hr;
								if (w_elapsed == -1 || w_elapsed > DATA_USERSTATS(us_item)->elapsed)
									w_elapsed = DATA_USERSTATS(us_item)->elapsed;

								userstats_workername_root = add_to_ktree(userstats_workername_root,
													 us_item,
													 cmp_userstats_workername);
							}
						} else
							break;

					}
					us_item = prev_in_ktree(us_ctx);
				}

				double_to_buf(w_hashrate5m, reply, sizeof(reply));
				snprintf(tmp, sizeof(tmp), "w_hashrate5m%d=%s%c", rows, reply, FLDSEP);
				APPEND_REALLOC(buf, off, len, tmp);

				double_to_buf(w_hashrate1hr, reply, sizeof(reply));
				snprintf(tmp, sizeof(tmp), "w_hashrate1hr%d=%s%c", rows, reply, FLDSEP);
				APPEND_REALLOC(buf, off, len, tmp);

				bigint_to_buf(w_elapsed, reply, sizeof(reply));
				snprintf(tmp, sizeof(tmp), "w_elapsed%d=%s%c", rows, reply, FLDSEP);
				APPEND_REALLOC(buf, off, len, tmp);

				int_to_buf((int)(w_lastshare.tv_sec), reply, sizeof(reply));
				snprintf(tmp, sizeof(tmp), "w_lastshare%d=%s%c", rows, reply, FLDSEP);
				APPEND_REALLOC(buf, off, len, tmp);

				userstats_workername_root = free_ktree(userstats_workername_root, NULL);
				K_RUNLOCK(userstats_list);
			}

			rows++;
		}
		w_item = next_in_ktree(w_ctx);
	}
	snprintf(tmp, sizeof(tmp), "rows=%d", rows);
	APPEND_REALLOC(buf, off, len, tmp);

	LOGDEBUG("%s.ok.%s", id, DATA_TRANSFER(i_username)->data);
	return buf;
}

static char *cmd_allusers(char *cmd, char *id, __maybe_unused tv_t *now, __maybe_unused char *by,
				__maybe_unused char *code, __maybe_unused char *inet)
{
	K_TREE *userstats_workername_root = new_ktree();
	K_ITEM *us_item, *usw_item, *tmp_item, *u_item;
	K_TREE_CTX us_ctx[1], usw_ctx[1];
	char reply[1024] = "";
	char tmp[1024];
	char *buf;
	size_t len, off;
	int rows;
	int64_t userid = -1;
	double u_hashrate1hr = 0.0;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	// Find last records for each user/worker in ALLUSERS_LIMIT_S
	// TODO: include pool_instance
	K_WLOCK(userstats_list);
	us_item = last_in_ktree(userstats_root, us_ctx);
	while (us_item && tvdiff(now, &(DATA_USERSTATS(us_item)->statsdate)) < ALLUSERS_LIMIT_S) {
		usw_item = find_in_ktree(userstats_workername_root, us_item, cmp_userstats_workername, usw_ctx);
		if (!usw_item) {
			usw_item = k_unlink_head(userstats_list);

			DATA_USERSTATS(usw_item)->userid = DATA_USERSTATS(us_item)->userid;
			strcpy(DATA_USERSTATS(usw_item)->workername, DATA_USERSTATS(us_item)->workername);
			DATA_USERSTATS(usw_item)->hashrate1hr = DATA_USERSTATS(us_item)->hashrate1hr;

			userstats_workername_root = add_to_ktree(userstats_workername_root, usw_item, cmp_userstats_workername);
		}
		us_item = us_item->prev;
	}

	APPEND_REALLOC_INIT(buf, off, len);
	APPEND_REALLOC(buf, off, len, "ok.");
	rows = 0;
	// Add up per user
	usw_item = first_in_ktree(userstats_workername_root, usw_ctx);
	while (usw_item) {
		if (DATA_USERSTATS(usw_item)->userid != userid) {
			if (userid != -1) {
				u_item = find_userid(userid);
				if (!u_item) {
					LOGERR("%s() userid %"PRId64" ignored - userstats but not users",
					       __func__, userid);
				} else {
					str_to_buf(DATA_USERS(u_item)->username, reply, sizeof(reply));
					snprintf(tmp, sizeof(tmp), "username%d=%s%c", rows, reply, FLDSEP);
					APPEND_REALLOC(buf, off, len, tmp);

					bigint_to_buf(userid, reply, sizeof(reply));
					snprintf(tmp, sizeof(tmp), "userid%d=%s%c", rows, reply, FLDSEP);
					APPEND_REALLOC(buf, off, len, tmp);

					double_to_buf(u_hashrate1hr, reply, sizeof(reply));
					snprintf(tmp, sizeof(tmp), "u_hashrate1hr%d=%s%c", rows, reply, FLDSEP);
					APPEND_REALLOC(buf, off, len, tmp);

					rows++;
				}
			}
			userid = DATA_USERSTATS(usw_item)->userid;
			u_hashrate1hr = 0;
		}
		u_hashrate1hr += DATA_USERSTATS(usw_item)->hashrate1hr;

		tmp_item = usw_item;
		usw_item = next_in_ktree(usw_ctx);

		k_add_head(userstats_list, tmp_item);
	}
	if (userid != -1) {
		u_item = find_userid(userid);
		if (!u_item) {
			LOGERR("%s() userid %"PRId64" ignored - userstats but not users",
			       __func__, userid);
		} else {
			str_to_buf(DATA_USERS(u_item)->username, reply, sizeof(reply));
			snprintf(tmp, sizeof(tmp), "username%d=%s%c", rows, reply, FLDSEP);
			APPEND_REALLOC(buf, off, len, tmp);

			bigint_to_buf(userid, reply, sizeof(reply));
			snprintf(tmp, sizeof(tmp), "userid%d=%s%c", rows, reply, FLDSEP);
			APPEND_REALLOC(buf, off, len, tmp);

			double_to_buf(u_hashrate1hr, reply, sizeof(reply));
			snprintf(tmp, sizeof(tmp), "u_hashrate1hr%d=%s%c", rows, reply, FLDSEP);
			APPEND_REALLOC(buf, off, len, tmp);

			rows++;
		}
	}

	userstats_workername_root = free_ktree(userstats_workername_root, NULL);
	K_WUNLOCK(userstats_list);

	snprintf(tmp, sizeof(tmp), "rows=%d", rows);
	APPEND_REALLOC(buf, off, len, tmp);

	LOGDEBUG("%s.ok.allusers", id);
	return buf;
}

static char *cmd_sharelog(char *cmd, char *id, tv_t *now, char *by, char *code, char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);
	PGconn *conn;

	// log to logfile with processing success/failure code

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	if (strcasecmp(cmd, STR_WORKINFO) == 0) {
		K_ITEM *i_workinfoid, *i_poolinstance, *i_transactiontree, *i_merklehash;
		K_ITEM *i_prevhash, *i_coinbase1, *i_coinbase2, *i_version, *i_bits;
		K_ITEM *i_ntime, *i_reward;
		int64_t workinfoid;

		i_workinfoid = require_name("workinfoid", 1, NULL, reply, siz);
		if (!i_workinfoid)
			return strdup(reply);

		i_poolinstance = require_name("poolinstance", 1, NULL, reply, siz);
		if (!i_poolinstance)
			return strdup(reply);

		i_transactiontree = require_name("transactiontree", 0, NULL, reply, siz);
		if (!i_transactiontree)
			return strdup(reply);

		i_merklehash = require_name("merklehash", 0, NULL, reply, siz);
		if (!i_merklehash)
			return strdup(reply);

		i_prevhash = require_name("prevhash", 1, NULL, reply, siz);
		if (!i_prevhash)
			return strdup(reply);

		i_coinbase1 = require_name("coinbase1", 1, NULL, reply, siz);
		if (!i_coinbase1)
			return strdup(reply);

		i_coinbase2 = require_name("coinbase2", 1, NULL, reply, siz);
		if (!i_coinbase2)
			return strdup(reply);

		i_version = require_name("version", 1, NULL, reply, siz);
		if (!i_version)
			return strdup(reply);

		i_bits = require_name("bits", 1, NULL, reply, siz);
		if (!i_bits)
			return strdup(reply);

		i_ntime = require_name("ntime", 1, NULL, reply, siz);
		if (!i_ntime)
			return strdup(reply);

		i_reward = require_name("reward", 1, NULL, reply, siz);
		if (!i_reward)
			return strdup(reply);

		conn = dbconnect();
		workinfoid = workinfo_add(conn, DATA_TRANSFER(i_workinfoid)->data,
						DATA_TRANSFER(i_poolinstance)->data,
						DATA_TRANSFER(i_transactiontree)->data,
						DATA_TRANSFER(i_merklehash)->data,
						DATA_TRANSFER(i_prevhash)->data,
						DATA_TRANSFER(i_coinbase1)->data,
						DATA_TRANSFER(i_coinbase2)->data,
						DATA_TRANSFER(i_version)->data,
						DATA_TRANSFER(i_bits)->data,
						DATA_TRANSFER(i_ntime)->data,
						DATA_TRANSFER(i_reward)->data,
						now, by, code, inet);
		PQfinish(conn);

		if (workinfoid == -1) {
			LOGERR("%s.failed.DBE", id);
			return strdup("failed.DBE");
		}
		LOGDEBUG("%s.ok.added %"PRId64, id, workinfoid);
		snprintf(reply, siz, "ok.%"PRId64, workinfoid);
		return strdup(reply);
	} else if (strcasecmp(cmd, STR_SHARES) == 0) {
		K_ITEM *i_workinfoid, *i_username, *i_workername, *i_clientid, *i_enonce1;
		K_ITEM *i_nonce2, *i_nonce, *i_diff, *i_sdiff, *i_secondaryuserid;
		bool ok;

		i_workinfoid = require_name("workinfoid", 1, NULL, reply, siz);
		if (!i_workinfoid)
			return strdup(reply);

		i_username = require_name("username", 1, NULL, reply, siz);
		if (!i_username)
			return strdup(reply);

		i_workername = require_name("workername", 1, NULL, reply, siz);
		if (!i_workername)
			return strdup(reply);

		i_clientid = require_name("clientid", 1, NULL, reply, siz);
		if (!i_clientid)
			return strdup(reply);

		i_enonce1 = require_name("enonce1", 1, NULL, reply, siz);
		if (!i_enonce1)
			return strdup(reply);

		i_nonce2 = require_name("nonce2", 1, NULL, reply, siz);
		if (!i_nonce2)
			return strdup(reply);

		i_nonce = require_name("nonce", 1, NULL, reply, siz);
		if (!i_nonce)
			return strdup(reply);

		i_diff = require_name("diff", 1, NULL, reply, siz);
		if (!i_diff)
			return strdup(reply);

		i_sdiff = require_name("sdiff", 1, NULL, reply, siz);
		if (!i_sdiff)
			return strdup(reply);

		i_secondaryuserid = require_name("secondaryuserid", 1, NULL, reply, siz);
		if (!i_secondaryuserid)
			return strdup(reply);

		ok = shares_add(DATA_TRANSFER(i_workinfoid)->data,
				DATA_TRANSFER(i_username)->data,
				DATA_TRANSFER(i_workername)->data,
				DATA_TRANSFER(i_clientid)->data,
				DATA_TRANSFER(i_enonce1)->data,
				DATA_TRANSFER(i_nonce2)->data,
				DATA_TRANSFER(i_nonce)->data,
				DATA_TRANSFER(i_diff)->data,
				DATA_TRANSFER(i_sdiff)->data,
				DATA_TRANSFER(i_secondaryuserid)->data,
				now, by, code, inet);

		if (!ok) {
			LOGERR("%s.failed.DATA", id);
			return strdup("failed.DATA");
		}
		LOGDEBUG("%s.ok.added %s", id, DATA_TRANSFER(i_nonce)->data);
		snprintf(reply, siz, "ok.added %s", DATA_TRANSFER(i_nonce)->data);
		return strdup(reply);
	} else if (strcasecmp(cmd, STR_SHAREERRORS) == 0) {
		K_ITEM *i_workinfoid, *i_username, *i_workername, *i_clientid, *i_errn;
		K_ITEM *i_error, *i_secondaryuserid;
		bool ok;

		i_workinfoid = require_name("workinfoid", 1, NULL, reply, siz);
		if (!i_workinfoid)
			return strdup(reply);

		i_username = require_name("username", 1, NULL, reply, siz);
		if (!i_username)
			return strdup(reply);

		i_workername = require_name("workername", 1, NULL, reply, siz);
		if (!i_workername)
			return strdup(reply);

		i_clientid = require_name("clientid", 1, NULL, reply, siz);
		if (!i_clientid)
			return strdup(reply);

		i_errn = require_name("errn", 1, NULL, reply, siz);
		if (!i_errn)
			return strdup(reply);

		i_error = require_name("error", 1, NULL, reply, siz);
		if (!i_error)
			return strdup(reply);

		i_secondaryuserid = require_name("secondaryuserid", 1, NULL, reply, siz);
		if (!i_secondaryuserid)
			return strdup(reply);

		ok = shareerrors_add(DATA_TRANSFER(i_workinfoid)->data,
					DATA_TRANSFER(i_username)->data,
					DATA_TRANSFER(i_workername)->data,
					DATA_TRANSFER(i_clientid)->data,
					DATA_TRANSFER(i_errn)->data,
					DATA_TRANSFER(i_error)->data,
					DATA_TRANSFER(i_secondaryuserid)->data,
					now, by, code, inet);
		if (!ok) {
			LOGERR("%s.failed.DATA", id);
			return strdup("failed.DATA");
		}
		LOGDEBUG("%s.ok.added %s", id, DATA_TRANSFER(i_username)->data);
		snprintf(reply, siz, "ok.added %s", DATA_TRANSFER(i_username)->data);
		return strdup(reply);
	} else if (strcasecmp(cmd, STR_AGEWORKINFO) == 0) {
		K_ITEM *i_workinfoid, *i_poolinstance;
		bool ok;

		i_workinfoid = require_name("workinfoid", 1, NULL, reply, siz);
		if (!i_workinfoid)
			return strdup(reply);

		i_poolinstance = require_name("poolinstance", 1, NULL, reply, siz);
		if (!i_poolinstance)
			return strdup(reply);

		ok = workinfo_age(NULL, DATA_TRANSFER(i_workinfoid)->data,
					DATA_TRANSFER(i_poolinstance)->data,
					now, by, code, inet);

		if (!ok) {
			LOGERR("%s.failed.DATA", id);
			return strdup("failed.DATA");
		}
		LOGDEBUG("%s.ok.aged %.*s",
			 id, BIGINT_BUFSIZ,
			 DATA_TRANSFER(i_workinfoid)->data);
		snprintf(reply, siz, "ok.%.*s",
				     BIGINT_BUFSIZ,
				     DATA_TRANSFER(i_workinfoid)->data);
		return strdup(reply);
	}

	LOGERR("%s.bad.cmd %s", cmd);
	return strdup("bad.cmd");
}

// TODO: the confirm update: identify block changes from workinfo height?
static char *cmd_blocks(char *cmd, char *id, tv_t *now, char *by, char *code, char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);
	PGconn *conn;
	K_ITEM *i_height, *i_blockhash, *i_confirmed, *i_workinfoid, *i_username;
	K_ITEM *i_workername, *i_clientid, *i_enonce1, *i_nonce2, *i_nonce, *i_reward;
	char *msg;
	bool ok;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	i_height = require_name("height", 1, NULL, reply, siz);
	if (!i_height)
		return strdup(reply);

	i_blockhash = require_name("blockhash", 1, NULL, reply, siz);
	if (!i_blockhash)
		return strdup(reply);

	i_confirmed = require_name("confirmed", 1, NULL, reply, siz);
	if (!i_confirmed)
		return strdup(reply);

	DATA_TRANSFER(i_confirmed)->data[0] = tolower(DATA_TRANSFER(i_confirmed)->data[0]);
	switch(DATA_TRANSFER(i_confirmed)->data[0]) {
		case BLOCKS_NEW:
			i_workinfoid = require_name("workinfoid", 1, NULL, reply, siz);
			if (!i_workinfoid)
				return strdup(reply);

			i_username = require_name("username", 1, NULL, reply, siz);
			if (!i_username)
				return strdup(reply);

			i_workername = require_name("workername", 1, NULL, reply, siz);
			if (!i_workername)
				return strdup(reply);

			i_clientid = require_name("clientid", 1, NULL, reply, siz);
			if (!i_clientid)
				return strdup(reply);

			i_enonce1 = require_name("enonce1", 1, NULL, reply, siz);
			if (!i_enonce1)
				return strdup(reply);

			i_nonce2 = require_name("nonce2", 1, NULL, reply, siz);
			if (!i_nonce2)
				return strdup(reply);

			i_nonce = require_name("nonce", 1, NULL, reply, siz);
			if (!i_nonce)
				return strdup(reply);

			i_reward = require_name("reward", 1, NULL, reply, siz);
			if (!i_reward)
				return strdup(reply);

			msg = "added";
			conn = dbconnect();
			ok = blocks_add(conn, DATA_TRANSFER(i_height)->data,
					      DATA_TRANSFER(i_blockhash)->data,
					      DATA_TRANSFER(i_confirmed)->data,
					      DATA_TRANSFER(i_workinfoid)->data,
					      DATA_TRANSFER(i_username)->data,
					      DATA_TRANSFER(i_workername)->data,
					      DATA_TRANSFER(i_clientid)->data,
					      DATA_TRANSFER(i_enonce1)->data,
					      DATA_TRANSFER(i_nonce2)->data,
					      DATA_TRANSFER(i_nonce)->data,
					      DATA_TRANSFER(i_reward)->data,
					      now, by, code, inet);
			break;
		case BLOCKS_CONFIRM:
			msg = "confirmed";
			conn = dbconnect();
			ok = blocks_add(conn, DATA_TRANSFER(i_height)->data,
					      DATA_TRANSFER(i_blockhash)->data,
					      DATA_TRANSFER(i_confirmed)->data,
					      EMPTY, EMPTY, EMPTY, EMPTY,
					      EMPTY, EMPTY, EMPTY, EMPTY,
					      now, by, code, inet);
			break;
		default:
			LOGERR("%s.failed.invalid conf='%s'",
			       id, DATA_TRANSFER(i_confirmed)->data);
			return strdup("failed.DATA");
	}

	PQfinish(conn);

	if (!ok) {
		LOGERR("%s.failed.DBE", id);
		return strdup("failed.DBE");
	}

	LOGDEBUG("%s.ok.blocks %s", id, msg);
	snprintf(reply, siz, "ok.%s", msg);
	return strdup(reply);
}

static char *cmd_auth(char *cmd, char *id, tv_t *now, char *by, char *code, char *inet)
{
	char reply[1024] = "";
	size_t siz = sizeof(reply);
	PGconn *conn;
	K_ITEM *i_poolinstance, *i_username, *i_workername, *i_clientid;
	K_ITEM *i_enonce1, *i_useragent;
	char *secuserid;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	i_poolinstance = require_name("poolinstance", 1, NULL, reply, siz);
	if (!i_poolinstance)
		return strdup(reply);

	i_username = require_name("username", 1, NULL, reply, siz);
	if (!i_username)
		return strdup(reply);

	i_workername = require_name("workername", 1, NULL, reply, siz);
	if (!i_workername)
		return strdup(reply);

	i_clientid = require_name("clientid", 1, NULL, reply, siz);
	if (!i_clientid)
		return strdup(reply);

	i_enonce1 = require_name("enonce1", 1, NULL, reply, siz);
	if (!i_enonce1)
		return strdup(reply);

	i_useragent = require_name("useragent", 0, NULL, reply, siz);
	if (!i_useragent)
		return strdup(reply);

	conn = dbconnect();
	secuserid = auths_add(conn, DATA_TRANSFER(i_poolinstance)->data,
				    DATA_TRANSFER(i_username)->data,
				    DATA_TRANSFER(i_workername)->data,
				    DATA_TRANSFER(i_clientid)->data,
				    DATA_TRANSFER(i_enonce1)->data,
				    DATA_TRANSFER(i_useragent)->data,
				    now, by, code, inet);
	PQfinish(conn);

	if (!secuserid) {
		LOGDEBUG("%s.failed.DBE", id);
		return strdup("failed.DBE");
	}

	LOGDEBUG("%s.ok.auth added for %s", id, secuserid);
	snprintf(reply, siz, "ok.%s", secuserid);
	return strdup(reply);
}

static char *cmd_homepage(char *cmd, char *id, __maybe_unused tv_t *now, __maybe_unused char *by,
				__maybe_unused char *code, __maybe_unused char *inet)
{
	K_ITEM *i_username, *u_item, *b_item, *p_item, *us_item, look;
	double u_hashrate5m, u_hashrate1hr;
	char reply[1024], tmp[1024], *buf;
	USERSTATS userstats;
	int64_t u_elapsed;
	K_TREE_CTX ctx[1], w_ctx[1];
	size_t len, off;
	bool has_uhr;

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	i_username = optional_name("username", 1, NULL);

	APPEND_REALLOC_INIT(buf, off, len);
	APPEND_REALLOC(buf, off, len, "ok.");
	if (last_bc) {
		tvs_to_buf(last_bc, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "lastbc=%s%c", reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);
	} else {
		snprintf(tmp, sizeof(tmp), "lastbc=?%c", FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);
	}

	// TODO: handle orphans
	b_item = last_in_ktree(blocks_root, ctx);
	if (b_item) {
		tvs_to_buf(&(DATA_BLOCKS(b_item)->createdate), reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "lastblock=%s%cconfirmed=%s%c",
					   reply, FLDSEP,
					   DATA_BLOCKS(b_item)->confirmed, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);
	} else {
		snprintf(tmp, sizeof(tmp), "lastblock=?%cconfirmed=?%c", FLDSEP, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);
	}

	// TODO: assumes only one poolinstance (for now)
	p_item = last_in_ktree(poolstats_root, ctx);

	if (p_item) {
		int_to_buf(DATA_POOLSTATS(p_item)->users, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "users=%s%c", reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		int_to_buf(DATA_POOLSTATS(p_item)->workers, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "workers=%s%c", reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		double_to_buf(DATA_POOLSTATS(p_item)->hashrate5m, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "p_hashrate5m=%s%c", reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		double_to_buf(DATA_POOLSTATS(p_item)->hashrate1hr, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "p_hashrate1hr=%s%c", reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		bigint_to_buf(DATA_POOLSTATS(p_item)->elapsed, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "p_elapsed=%s%c", reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);
	} else {
		snprintf(tmp, sizeof(tmp), "users=?%cworkers=?%cp_hashrate5m=?%c"
					   "p_hashrate1hr=?%cp_elapsed=?%c",
					   FLDSEP, FLDSEP, FLDSEP, FLDSEP, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);
	}

	u_item = NULL;
	if (i_username)
		u_item = find_users(DATA_TRANSFER(i_username)->data);

	has_uhr = false;
	if (p_item && u_item) {
		K_TREE *userstats_workername_root = new_ktree();
		u_hashrate5m = u_hashrate1hr = 0.0;
		u_elapsed = -1;
		// find last stored userid record
		userstats.userid = DATA_USERS(u_item)->userid;
		userstats.statsdate.tv_sec = date_eot.tv_sec;
		userstats.statsdate.tv_usec = date_eot.tv_usec;
		// find/cmp doesn't get to here
		STRNCPY(userstats.poolinstance, "");
		STRNCPY(userstats.workername, "");
		look.data = (void *)(&userstats);
		K_RLOCK(userstats_list);
		us_item = find_before_in_ktree(userstats_root, &look, cmp_userstats, ctx);
		while (us_item &&
		       DATA_USERSTATS(us_item)->userid == userstats.userid &&
		       tvdiff(now, &(DATA_USERSTATS(us_item)->statsdate)) < USERSTATS_PER_S) {
			// TODO: add the latest per pool instance (this is the latest per worker)
			// Ignore summarised data from the DB, it should be old so irrelevant
			if (DATA_USERSTATS(us_item)->poolinstance[0] &&
			    !find_in_ktree(userstats_workername_root, us_item, cmp_userstats_workername, w_ctx)) {
				u_hashrate5m += DATA_USERSTATS(us_item)->hashrate5m;
				u_hashrate1hr += DATA_USERSTATS(us_item)->hashrate1hr;
				if (u_elapsed == -1 ||
				    u_elapsed > DATA_USERSTATS(us_item)->elapsed)
					u_elapsed = DATA_USERSTATS(us_item)->elapsed;
				has_uhr = true;
				userstats_workername_root = add_to_ktree(userstats_workername_root,
									 us_item,
									 cmp_userstats_workername);
			}
			us_item = prev_in_ktree(ctx);
		}
		userstats_workername_root = free_ktree(userstats_workername_root, NULL);
		K_RUNLOCK(userstats_list);
	}

	if (has_uhr) {
		double_to_buf(u_hashrate5m, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "u_hashrate5m=%s%c", reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		double_to_buf(u_hashrate1hr, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "u_hashrate1hr=%s%c", reply, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);

		bigint_to_buf(u_elapsed, reply, sizeof(reply));
		snprintf(tmp, sizeof(tmp), "u_elapsed=%s", reply);
		APPEND_REALLOC(buf, off, len, tmp);
	} else {
		snprintf(tmp, sizeof(tmp), "u_hashrate5m=?%cu_hashrate1hr=?%cu_elapsed=?",
					   FLDSEP, FLDSEP);
		APPEND_REALLOC(buf, off, len, tmp);
	}

	LOGDEBUG("%s.ok.home,user=%s", id,
		 i_username ? DATA_TRANSFER(i_username)->data : "N");
	return buf;
}

static __maybe_unused char *cmd_dsp(char *cmd, char *id, __maybe_unused tv_t *now,
				    __maybe_unused char *by, __maybe_unused char *code,
				    __maybe_unused char *inet)
{
	__maybe_unused K_ITEM *i_file;
	__maybe_unused char reply[1024] = "";
	__maybe_unused size_t siz = sizeof(reply);

	LOGDEBUG("%s(): cmd '%s'", __func__, cmd);

	// WARNING: This is a gaping security hole - only use in development
	LOGDEBUG("%s.disabled.dsp", id);
	return strdup("disabled.dsp");
/*
	i_file = require_name("file", 1, NULL, reply, siz);
	if (!i_file)
		return strdup(reply);

	dsp_ktree(transfer_list, transfer_root, DATA_TRANSFER(i_file)->data);

	dsp_ktree(sharesummary_list, sharesummary_root, DATA_TRANSFER(i_file)->data);

	dsp_ktree(userstats_list, userstats_root, DATA_TRANSFER(i_file)->data);

	LOGDEBUG("%s.ok.dsp.file='%s'", id, DATA_TRANSFER(i_file)->data);
	return strdup("ok.dsp");
*/
}

enum cmd_values {
	CMD_UNSET,
	CMD_REPLY, // Means something was wrong - send back reply
	CMD_SHUTDOWN,
	CMD_PING,
	CMD_SHARELOG,
	CMD_AUTH,
	CMD_ADDUSER,
	CMD_CHKPASS,
	CMD_POOLSTAT,
	CMD_USERSTAT,
	CMD_BLOCK,
	CMD_NEWID,
	CMD_PAYMENTS,
	CMD_WORKERS,
	CMD_ALLUSERS,
	CMD_HOMEPAGE,
	CMD_DSP,
	CMD_END
};

// TODO: limit access
#define ACCESS_POOL	"p"
#define ACCESS_SYSTEM	"s"
#define ACCESS_WEB	"w"
#define ACCESS_PROXY	"x"
#define ACCESS_CKDB	"c"

static struct CMDS {
	enum cmd_values cmd_val;
	char *cmd_str;
	char *(*func)(char *, char *, tv_t *, char *, char *, char *);
	char *access;
} cmds[] = {
	{ CMD_SHUTDOWN,	"shutdown",		NULL,		ACCESS_SYSTEM },
	{ CMD_PING,	"ping",			NULL,		ACCESS_SYSTEM ACCESS_POOL ACCESS_WEB },
	{ CMD_SHARELOG,	STR_WORKINFO,		cmd_sharelog,	ACCESS_POOL },
	{ CMD_SHARELOG,	STR_SHARES,		cmd_sharelog,	ACCESS_POOL },
	{ CMD_SHARELOG,	STR_SHAREERRORS,	cmd_sharelog,	ACCESS_POOL },
	{ CMD_SHARELOG,	STR_AGEWORKINFO,	cmd_sharelog,	ACCESS_POOL },
	{ CMD_AUTH,	"authorise",		cmd_auth,	ACCESS_POOL },
	{ CMD_ADDUSER,	"adduser",		cmd_adduser,	ACCESS_WEB },
	{ CMD_CHKPASS,	"chkpass",		cmd_chkpass,	ACCESS_WEB },
	{ CMD_POOLSTAT,	"poolstats",		cmd_poolstats,	ACCESS_POOL },
	{ CMD_USERSTAT,	"userstats",		cmd_userstats,	ACCESS_POOL },
	{ CMD_BLOCK,	"block",		cmd_blocks,	ACCESS_POOL },
	{ CMD_NEWID,	"newid",		cmd_newid,	ACCESS_SYSTEM },
	{ CMD_PAYMENTS,	"payments",		cmd_payments,	ACCESS_WEB },
	{ CMD_WORKERS,	"workers",		cmd_workers,	ACCESS_WEB },
	{ CMD_ALLUSERS,	"allusers",		cmd_allusers,	ACCESS_WEB },
	{ CMD_HOMEPAGE,	"homepage",		cmd_homepage,	ACCESS_WEB },
	{ CMD_DSP,	"dsp",			cmd_dsp,	ACCESS_SYSTEM },
	{ CMD_END,	NULL,			NULL,		NULL }
};

static enum cmd_values breakdown(char *buf, int *which_cmds, char *cmd, char *id)
{
	K_TREE_CTX ctx[1];
	K_ITEM *item;
	char *cmdptr, *idptr, *data, *next, *eq;

	*which_cmds = CMD_UNSET;
	*cmd = *id = '\0';

	cmdptr = strdup(buf);
	idptr = strchr(cmdptr, '.');
	if (!idptr || !*idptr) {
		STRNCPYSIZ(cmd, cmdptr, CMD_SIZ);
		STRNCPYSIZ(id, cmdptr, ID_SIZ);
		LOGERR("Listener received invalid message: '%s'", buf);
		free(cmdptr);
		return CMD_REPLY;
	}

	*(idptr++) = '\0';
	STRNCPYSIZ(cmd, cmdptr, CMD_SIZ);
	data = strchr(idptr, '.');
	if (data)
		*(data++) = '\0';
	STRNCPYSIZ(id, idptr, ID_SIZ);

	for (*which_cmds = 0; cmds[*which_cmds].cmd_val != CMD_END; (*which_cmds)++) {
		if (strcasecmp(cmd, cmds[*which_cmds].cmd_str) == 0)
			break;
	}

	if (cmds[*which_cmds].cmd_val == CMD_END) {
		LOGERR("Listener received unknown command: '%s'", buf);
		free(cmdptr);
		return CMD_REPLY;
	}

	next = data;
	if (next && strncmp(next, JSON_TRANSFER, JSON_TRANSFER_LEN) == 0) {
		json_t *json_data;
		json_error_t err_val;
		void *json_iter;
		const char *json_key, *json_str;
		json_t *json_value;
		int json_typ;
		size_t siz;
		bool ok;

		next += JSON_TRANSFER_LEN;
		json_data = json_loads(next, JSON_DISABLE_EOF_CHECK, &err_val);
		if (!json_data) {
			LOGERR("Json decode error from command: '%s'", cmd);
			free(cmdptr);
			return CMD_REPLY;
		}
		json_iter = json_object_iter(json_data);
		K_WLOCK(transfer_list);
		while (json_iter) {
			json_key = json_object_iter_key(json_iter);
			json_value = json_object_iter_value(json_iter);
			item = k_unlink_head(transfer_list);
			ok = true;
			json_typ = json_typeof(json_value);
			switch (json_typ) {
			 case JSON_STRING:
				json_str = json_string_value(json_value);
				siz = strlen(json_str);
				if (siz >= sizeof(DATA_TRANSFER(item)->value))
					DATA_TRANSFER(item)->data = strdup(json_str);
				else {
					STRNCPY(DATA_TRANSFER(item)->value, json_str);
					DATA_TRANSFER(item)->data = DATA_TRANSFER(item)->value;
				}
				break;
			 case JSON_REAL:
				snprintf(DATA_TRANSFER(item)->value,
					 sizeof(DATA_TRANSFER(item)->value),
					 "%f", json_real_value(json_value));
				DATA_TRANSFER(item)->data = DATA_TRANSFER(item)->value;
				break;
			 case JSON_INTEGER:
				snprintf(DATA_TRANSFER(item)->value,
					 sizeof(DATA_TRANSFER(item)->value),
					 "%"PRId64,
					 (int64_t)json_integer_value(json_value));
				DATA_TRANSFER(item)->data = DATA_TRANSFER(item)->value;
				break;
			 case JSON_TRUE:
			 case JSON_FALSE:
				snprintf(DATA_TRANSFER(item)->value,
					 sizeof(DATA_TRANSFER(item)->value),
					 "%s", (json_typ == JSON_TRUE) ?
							TRUE_STR : FALSE_STR);
				DATA_TRANSFER(item)->data = DATA_TRANSFER(item)->value;
				break;
			 case JSON_ARRAY:
				{
					/* only one level array of strings for now (merkletree)
					 * ignore other data */
					size_t i, len, off, count = json_array_size(json_value);
					json_t *json_element;
					bool first = true;

					APPEND_REALLOC_INIT(DATA_TRANSFER(item)->data, off, len);
					for (i = 0; i < count; i++) {
						json_element = json_array_get(json_value, i);
						if (json_is_string(json_element)) {
							json_str = json_string_value(json_element);
							siz = strlen(json_str);
							if (first)
								first = false;
							else {
								APPEND_REALLOC(DATA_TRANSFER(item)->data,
										off, len, " ");
							}
							APPEND_REALLOC(DATA_TRANSFER(item)->data,
									off, len, json_str);
						} else
							LOGERR("%s() unhandled json type %d in array %s"
							       " in cmd %s", __func__,
							       json_typ, json_key, cmd);
					}
				}
				break;
			 default:
				LOGERR("%s() unhandled json type %d in cmd %s",
				       __func__, json_typ, cmd);
				ok = false;
				break;
			}

			if (ok)
				STRNCPY(DATA_TRANSFER(item)->name, json_key);
			if (!ok || find_in_ktree(transfer_root, item, cmp_transfer, ctx)) {
				if (DATA_TRANSFER(item)->data != DATA_TRANSFER(item)->value)
					free(DATA_TRANSFER(item)->data);
				k_add_head(transfer_list, item);
			} else {
				transfer_root = add_to_ktree(transfer_root, item, cmp_transfer);
				k_add_head(transfer_store, item);
			}
			json_iter = json_object_iter_next(json_data, json_iter);
		}
		K_WUNLOCK(transfer_list);
		json_decref(json_data);
	} else {
		K_WLOCK(transfer_list);
		while (next && *next) {
			data = next;
			next = strchr(data, FLDSEP);
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
			DATA_TRANSFER(item)->data = DATA_TRANSFER(item)->value;

			if (find_in_ktree(transfer_root, item, cmp_transfer, ctx)) {
				if (DATA_TRANSFER(item)->data != DATA_TRANSFER(item)->value)
					free(DATA_TRANSFER(item)->data);
				k_add_head(transfer_list, item);
			} else {
				transfer_root = add_to_ktree(transfer_root, item, cmp_transfer);
				k_add_head(transfer_store, item);
			}
		}
		K_WUNLOCK(transfer_list);
	}
	free(cmdptr);
	return cmds[*which_cmds].cmd_val;
}

static void summarise_poolstats()
{
// TODO
}

// TODO: daily
// TODO: consider limiting how much/long this processes each time
static void summarise_userstats()
{
	K_TREE_CTX ctx[1], ctx2[1];
	K_ITEM *tail, *new, *prev, *tmp;
	USERSTATS *userstats;
	double statrange, factor;
	bool locked, upgrade;
	tv_t now, when;
	PGconn *conn = NULL;
	int count;

	locked = false;
	while (1764) {
		setnow(&now);
		upgrade = false;
		locked = true;
		K_ILOCK(userstats_list);
		tail = last_in_ktree(userstats_root, ctx);
		// Last non DB stat
		while (tail && DATA_USERSTATS(tail)->poolinstance[0] == '\0')
			tail = prev_in_ktree(ctx);

		if (!tail)
			break;

		statrange = tvdiff(&now, &(DATA_USERSTATS(tail)->statsdate));
		// Is there data ready for summarising?
		if (statrange <= USERSTATS_AGE)
			break;

		memcpy(&when,  &(DATA_USERSTATS(tail)->statsdate), sizeof(when));
		/* Convent when to the start of the timeframe after the one it is in
		 * assume timeval ignores leapseconds ... */
		when.tv_sec = when.tv_sec - (when.tv_sec % USERSTATS_DB_S) + USERSTATS_DB_S;
		when.tv_usec = 0;

		// Is the whole timerange up to before 'when' ready for summarising?
		statrange = tvdiff(&now, &when);
		if (statrange < USERSTATS_AGE)
			break;

		prev = prev_in_ktree(ctx);

		upgrade = true;
		K_ULOCK(userstats_list);
		new = k_unlink_head(userstats_list);
		userstats = DATA_USERSTATS(new);
		memcpy(userstats, DATA_USERSTATS(tail), sizeof(USERSTATS));

		remove_from_ktree(userstats_root, tail, cmp_userstats, ctx2);
		k_unlink_item(userstats_store, tail);
		k_add_head(userstats_summ, tail);

		count = 1;
		while (prev) {
			if (DATA_USERSTATS(prev)->userid != userstats->userid)
				break;
			if (strcmp(DATA_USERSTATS(prev)->workername, userstats->workername))
				break;
			statrange = tvdiff(&when, &(DATA_USERSTATS(prev)->statsdate));
			if (statrange <= 0)
				break;

			count++;
			userstats->hashrate += DATA_USERSTATS(prev)->hashrate;
			userstats->hashrate5m += DATA_USERSTATS(prev)->hashrate5m;
			userstats->hashrate1hr += DATA_USERSTATS(prev)->hashrate1hr;
			userstats->hashrate24hr += DATA_USERSTATS(prev)->hashrate24hr;
			if (userstats->elapsed > DATA_USERSTATS(prev)->elapsed)
				userstats->elapsed = DATA_USERSTATS(prev)->elapsed;

			tmp = prev_in_ktree(ctx);
			remove_from_ktree(userstats_root, prev, cmp_userstats, ctx2);
			k_unlink_item(userstats_store, prev);
			k_add_head(userstats_summ, prev);
			prev = tmp;
		}

		if (userstats->hashrate5m > 0.0 || userstats->hashrate1hr > 0.0)
			userstats->idle = false;
		else
			userstats->idle = true;

		userstats->summarylevel[0] = SUMMARY_DB;
		userstats->summarylevel[1] = '\0';

		// Expect 6 per instance
		factor = (double)count / 6.0;
		userstats->hashrate *= factor;
		userstats->hashrate5m *= factor;
		userstats->hashrate1hr *= factor;
		userstats->hashrate24hr *= factor;

		memcpy(&(userstats->statsdate), &when, sizeof(when));
		// Stats to the end of this timeframe
		userstats->statsdate.tv_sec -= 1;
		userstats->statsdate.tv_usec = 999999;

		SIMPLEDATEDEFAULT(userstats, &now);

		if (!conn)
			conn = dbconnect();

		// TODO: Consider releasing the lock for the DB insert?
		if (!userstats_add_db(conn, userstats)) {
			// Put them back and cancel the summarisation
			tmp = userstats_summ->head;
			while (tmp) {
				add_to_ktree(userstats_root, tmp, cmp_userstats);
				tmp = tmp->next;
			}
			k_list_transfer_to_tail(userstats_summ, userstats_store);
			break;
		}

		k_list_transfer_to_tail(userstats_summ, userstats_list);
		add_to_ktree(userstats_root, new, cmp_userstats);

		if (upgrade)
			K_WUNLOCK(userstats_list);
		else
			K_IUNLOCK(userstats_list);
		locked = false;
	}

	if (locked) {
		if (upgrade)
			K_WUNLOCK(userstats_list);
		else
			K_IUNLOCK(userstats_list);
	}

	if (conn)
		PQfinish(conn);
}

static void *summariser(__maybe_unused void *arg)
{
	pthread_detach(pthread_self());

	while (!summarizer_die && !summarizer_go)
		cksleep_ms(42);

	while (!summarizer_die) {
		sleep(19);

		if (!summarizer_die)
			summarise_poolstats();

		if (!summarizer_die)
			summarise_userstats();
	}

	return NULL;
}

// TODO: equivalent of api_allow
static void *listener(void *arg)
{
	proc_instance_t *pi = (proc_instance_t *)arg;
	unixsock_t *us = &pi->us;
	char *end, *ans, *rep, *buf = NULL, *dot;
	char *last_msg = NULL, *last_reply = NULL;
	char cmd[CMD_SIZ+1], id[ID_SIZ+1], reply[1024+1];
	// Minimise the size in case of garbage
	char duptype[16+1];
	enum cmd_values cmdnum, last_cmd = 9001;
	int sockd, which_cmds;
	pthread_t summzer;
	K_ITEM *item;
	size_t siz;
	tv_t now;
	bool dup;

	create_pthread(&summzer, summariser, NULL);

	rename_proc(pi->sockname);

	if (!setup_data()) {
		LOGEMERG("ABORTING");
		return NULL;
	}

	LOGWARNING("%s(): ckdb ready", __func__);

	summarizer_go = true;

	while (true) {
		dealloc(buf);
		sockd = accept(us->sockd, NULL, NULL);
		if (sockd < 0) {
			LOGERR("Failed to accept on socket in listener");
			break;
		}

		cmdnum = CMD_UNSET;

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
			/* For duplicates:
			 *  System: shutdown and ping are always processed,
			 *	    so for any others, send a ping between them
			 *	    if you need to send the same message twice
			 *  Web: if the pool didn't do anything since the original
			 *	 then the reply could be wrong for any reply that
			 *	 changes over time ... however if the pool is busy
			 *	 and we get the same request repeatedly, this will
			 *	 reduce the load - thus always send the same reply
			 *  Pool: must not process it, must send back the same reply
			 *  TODO: remember last message/reply per source
			 */
			if (last_msg && strcmp(last_msg, buf) == 0) {
				dup = true;
				// This means an exact duplicate of the last non-dup
				snprintf(reply, sizeof(reply), "%s%ld,%ld", LOGDUP, now.tv_sec, now.tv_usec);
				LOGFILE(reply);
				cmdnum = last_cmd;

				STRNCPY(duptype, buf);
				dot = strchr(duptype, '.');
				if (dot)
					*dot = '\0';
				LOGWARNING("Duplicate '%s' message received", duptype);
			} else {
				dup = false;
				LOGFILE(buf);
				cmdnum = breakdown(buf, &which_cmds, cmd, id);
				last_cmd = cmdnum;
			}
			switch (cmdnum) {
				case CMD_REPLY:
					if (dup)
						send_unix_msg(sockd, last_reply);
					else {
						snprintf(reply, sizeof(reply), "%s.%ld.?.", id, now.tv_sec);
						if (last_reply)
							free(last_reply);
						last_reply = strdup(reply);
						send_unix_msg(sockd, reply);
					}
					break;
				case CMD_SHUTDOWN:
					LOGWARNING("Listener received shutdown message, terminating ckdb");
					snprintf(reply, sizeof(reply), "%s.%d.ok.exiting", id, (int)now.tv_sec);
					send_unix_msg(sockd, reply);
					break;
				case CMD_PING:
					LOGDEBUG("Listener received ping request");
					// Generate a new reply each time even on dup
					snprintf(reply, sizeof(reply), "%s.%ld.ok.pong", id, now.tv_sec);
					send_unix_msg(sockd, reply);
					break;
				default:
					if (dup)
						send_unix_msg(sockd, last_reply);
					else {
						// TODO: optionally get by/code/inet from transfer here instead?
						ans = cmds[which_cmds].func(cmd, id, &now, (char *)"code",
									    (char *)__func__,
									    (char *)"127.0.0.1");
						siz = strlen(ans) + strlen(id) + 32;
						rep = malloc(siz);
						snprintf(rep, siz, "%s.%ld.%s", id, now.tv_sec, ans);
						free(ans);
						ans = NULL;
						if (last_reply)
							free(last_reply);
						last_reply = strdup(rep);
						send_unix_msg(sockd, rep);
						free(rep);
						rep = NULL;
					}
					break;
			}
		}
		close(sockd);

		if (cmdnum == CMD_SHUTDOWN)
			break;

		K_WLOCK(transfer_list);
		transfer_root = free_ktree(transfer_root, NULL);
		item = transfer_store->head;
		while (item) {
			if (DATA_TRANSFER(item)->data != DATA_TRANSFER(item)->value)
				free(DATA_TRANSFER(item)->data);
			item = item->next;
		}
		k_list_transfer_to_head(transfer_store, transfer_list);
		K_WUNLOCK(transfer_list);
	}

	dealloc(buf);
	if (last_reply)
		free(last_reply);
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
	tv_t now;

	feenableexcept(FE_DIVBYZERO | FE_INVALID | FE_OVERFLOW);

	global_ckp = &ckp;
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

	snprintf(logname, sizeof(logname), "%s%s",
				ckp.logdir, ckp.name);

	ckp.main.ckp = &ckp;
	ckp.main.processname = strdup("main");
	ckp.main.sockname = strdup("listener");
	write_namepid(&ckp.main);
	create_process_unixsock(&ckp.main);

	setnow(&now);
	srand((unsigned int)(now.tv_usec * 4096 + now.tv_sec % 4096));
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
