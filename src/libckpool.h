/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

/* This file should contain all exported functions of libckpool */

#ifndef LIBCKPOOL_H
#define LIBCKPOOL_H

#include <errno.h>
#include <jansson.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <syslog.h>

#define mutex_lock(_lock) _mutex_lock(_lock, __FILE__, __func__, __LINE__)
#define mutex_unlock_noyield(_lock) _mutex_unlock_noyield(_lock, __FILE__, __func__, __LINE__)
#define mutex_unlock(_lock) _mutex_unlock(_lock, __FILE__, __func__, __LINE__)
#define mutex_trylock(_lock) _mutex_trylock(_lock, __FILE__, __func__, __LINE__)
#define wr_lock(_lock) _wr_lock(_lock, __FILE__, __func__, __LINE__)
#define wr_trylock(_lock) _wr_trylock(_lock, __FILE__, __func__, __LINE__)
#define rd_lock(_lock) _rd_lock(_lock, __FILE__, __func__, __LINE__)
#define rw_unlock(_lock) _rw_unlock(_lock, __FILE__, __func__, __LINE__)
#define rd_unlock_noyield(_lock) _rd_unlock_noyield(_lock, __FILE__, __func__, __LINE__)
#define wr_unlock_noyield(_lock) _wr_unlock_noyield(_lock, __FILE__, __func__, __LINE__)
#define rd_unlock(_lock) _rd_unlock(_lock, __FILE__, __func__, __LINE__)
#define wr_unlock(_lock) _wr_unlock(_lock, __FILE__, __func__, __LINE__)
#define mutex_init(_lock) _mutex_init(_lock, __FILE__, __func__, __LINE__)
#define rwlock_init(_lock) _rwlock_init(_lock, __FILE__, __func__, __LINE__)
#define cklock_init(_lock) _cklock_init(_lock, __FILE__, __func__, __LINE__)
#define ck_rlock(_lock) _ck_rlock(_lock, __FILE__, __func__, __LINE__)
#define ck_ilock(_lock) _ck_ilock(_lock, __FILE__, __func__, __LINE__)
#define ck_uilock(_lock) _ck_uilock(_lock, __FILE__, __func__, __LINE__)
#define ck_ulock(_lock) _ck_ulock(_lock, __FILE__, __func__, __LINE__)
#define ck_wlock(_lock) _ck_wlock(_lock, __FILE__, __func__, __LINE__)
#define ck_dwlock(_lock) _ck_dwlock(_lock, __FILE__, __func__, __LINE__)
#define ck_dwilock(_lock) _ck_dwilock(_lock, __FILE__, __func__, __LINE__)
#define ck_dlock(_lock) _ck_dlock(_lock, __FILE__, __func__, __LINE__)
#define ck_runlock(_lock) _ck_runlock(_lock, __FILE__, __func__, __LINE__)
#define ck_wunlock(_lock) _ck_wunlock(_lock, __FILE__, __func__, __LINE__)

#define ckalloc(len) _ckalloc(len, __FILE__, __func__, __LINE__)
#define ckzalloc(len) _ckzalloc(len, __FILE__, __func__, __LINE__)

#define dealloc(ptr) _dealloc((void *)&(ptr))

/* Placeholders for when we have more comprehensive logging facilities */
#define LOGERR(fmt, ...) do { \
	if (fmt) { \
		fprintf(stderr, fmt, ##__VA_ARGS__); \
		if (errno)\
			fprintf(stderr, " with errno %d:%s", errno, strerror(errno)); \
		fprintf(stderr, "\n"); \
		fflush(stderr); \
	} \
} while (0)

#define LOGMSG(fmt, ...) do { \
	if (fmt) { \
		fprintf(stderr, fmt, ##__VA_ARGS__); \
		fprintf(stderr, "\n"); \
		fflush(stderr); \
	} \
} while (0)

#define LOGWARNING(fmt, ...) LOGMSG(fmt, ##__VA_ARGS__)
#define LOGNOTICE(fmt, ...) LOGMSG(fmt, ##__VA_ARGS__)
#define LOGINFO(fmt, ...) LOGMSG(fmt, ##__VA_ARGS__)
#define LOGDEBUG(fmt, ...) LOGMSG(fmt, ##__VA_ARGS__)

#define IN_FMT_FFL " in %s %s():%d"
#define quitfrom(status, _file, _func, _line, fmt, ...) do { \
	if (fmt) { \
		fprintf(stderr, fmt IN_FMT_FFL, ##__VA_ARGS__, _file, _func, _line); \
		fprintf(stderr, "\n"); \
		fflush(stderr); \
	} \
	exit(status); \
} while (0)

#define quit(status, fmt, ...) do { \
	if (fmt) { \
		fprintf(stderr, fmt, ##__VA_ARGS__); \
		if (status || errno)\
			fprintf(stderr, " with errno %d:%s", errno, strerror(errno)); \
		fprintf(stderr, "\n"); \
		fflush(stderr); \
	} \
	exit(status); \
} while (0)

#define PAGESIZE (4096)

/* ck locks, a write biased variant of rwlocks */
struct cklock {
	pthread_mutex_t mutex;
	pthread_rwlock_t rwlock;
};

typedef struct cklock cklock_t;

struct connsock {
	int fd;
	char *url;
	char *port;
	char *auth;
	char *buf;
};

typedef struct connsock connsock_t;

struct unixsock {
	int sockd;
	char *path;
};

typedef struct unixsock unixsock_t;

struct proc_instance;
typedef struct proc_instance proc_instance_t;
struct ckpool_instance;
typedef struct ckpool_instance ckpool_t;

struct proc_instance {
	ckpool_t *ckp;
	unixsock_t us;
	char *processname;
	char *sockname;
	int pid;
	int (*process)(proc_instance_t *);
};

struct ckpool_instance {
	/* Main process name */
	char *name;
	/* Directory where sockets are created */
	char *socket_dir;
	/* Filename of config file */
	char *config;

	/* Process instance data of parent/child processes */
	proc_instance_t main;
	proc_instance_t generator;
	proc_instance_t stratifier;

	/* Threads of main process */
	pthread_t pth_listener;
	pthread_t pth_watchdog;

	/* Bitcoind data */
	char *btcdurl;
	char *btcdauth;
	char *btcdpass;
	char *btcaddress;
	char *btcsig;
};

void create_pthread(pthread_t *thread, void *(*start_routine)(void *), void *arg);
void join_pthread(pthread_t thread);

void _mutex_lock(pthread_mutex_t *lock, const char *file, const char *func, const int line);
void _mutex_unlock_noyield(pthread_mutex_t *lock, const char *file, const char *func, const int line);
void _mutex_unlock(pthread_mutex_t *lock, const char *file, const char *func, const int line);
int _mutex_trylock(pthread_mutex_t *lock, __maybe_unused const char *file, __maybe_unused const char *func, __maybe_unused const int line);
void _wr_lock(pthread_rwlock_t *lock, const char *file, const char *func, const int line);
int _wr_trylock(pthread_rwlock_t *lock, __maybe_unused const char *file, __maybe_unused const char *func, __maybe_unused const int line);
void _rd_lock(pthread_rwlock_t *lock, const char *file, const char *func, const int line);
void _rw_unlock(pthread_rwlock_t *lock, const char *file, const char *func, const int line);
void _rd_unlock_noyield(pthread_rwlock_t *lock, const char *file, const char *func, const int line);
void _wr_unlock_noyield(pthread_rwlock_t *lock, const char *file, const char *func, const int line);
void _rd_unlock(pthread_rwlock_t *lock, const char *file, const char *func, const int line);
void _wr_unlock(pthread_rwlock_t *lock, const char *file, const char *func, const int line);
void _mutex_init(pthread_mutex_t *lock, const char *file, const char *func, const int line);
void mutex_destroy(pthread_mutex_t *lock);
void _rwlock_init(pthread_rwlock_t *lock, const char *file, const char *func, const int line);
void rwlock_destroy(pthread_rwlock_t *lock);

void _cklock_init(cklock_t *lock, const char *file, const char *func, const int line);
void cklock_destroy(cklock_t *lock);
void _ck_rlock(cklock_t *lock, const char *file, const char *func, const int line);
void _ck_ilock(cklock_t *lock, const char *file, const char *func, const int line);
void _ck_uilock(cklock_t *lock, const char *file, const char *func, const int line);
void _ck_ulock(cklock_t *lock, const char *file, const char *func, const int line);
void _ck_wlock(cklock_t *lock, const char *file, const char *func, const int line);
void _ck_dwlock(cklock_t *lock, const char *file, const char *func, const int line);
void _ck_dwilock(cklock_t *lock, const char *file, const char *func, const int line);
void _ck_dlock(cklock_t *lock, const char *file, const char *func, const int line);
void _ck_runlock(cklock_t *lock, const char *file, const char *func, const int line);
void _ck_wunlock(cklock_t *lock, const char *file, const char *func, const int line);

static inline bool sock_connecting(void)
{
	return errno == EINPROGRESS;
}

static inline bool sock_blocks(void)
{
	return (errno == EAGAIN || errno == EWOULDBLOCK);
}
static inline bool sock_timeout(void)
{
	return (errno == ETIMEDOUT);
}
static inline bool interrupted(void)
{
	return (errno == EINTR);
}

bool extract_sockaddr(char *url, char **sockaddr_url, char **sockaddr_port);
void keep_sockalive(int fd);
void noblock_socket(int fd);
void block_socket(int fd);
int connect_socket(char *url, char *port);
int write_socket(int fd, const void *buf, size_t nbyte);
int read_socket_line(connsock_t *cs);
void empty_socket(int fd);
void close_unix_socket(const int sockd, const char *server_path);
int open_unix_server(const char *server_path);
int open_unix_client(const char *server_path);
char *recv_unix_msg(int sockd);
bool send_unix_msg(int sockd, const char *buf);
bool send_proc(proc_instance_t *pi, const char *msg);
char *send_recv_proc(proc_instance_t *pi, const char *msg);

json_t *json_rpc_call(connsock_t *cs, const char *rpc_req);

void align_len(size_t *len);
void realloc_strcat(char **ptr, const char *s);
void *_ckalloc(size_t len, const char *file, const char *func, const int line);
void *_ckzalloc(size_t len, const char *file, const char *func, const int line);
void _dealloc(void **ptr);
void __bin2hex(void *vs, const void *vp, size_t len);
void *bin2hex(const void *vp, size_t len);
bool hex2bin(void *p, const void *vhexstr, size_t len);
char *http_base64(const char *src);
void b58tobin(char *b58bin, const char *b58);

void address_to_pubkeytxn(char *pkh, const char *addr);
int ser_number(uchar *s, int32_t val);
bool fulltest(const uchar *hash, const uchar *target);

void copy_tv(tv_t *dest, const tv_t *src);
void ts_to_tv(tv_t *val, const ts_t *spec);
void tv_to_ts(ts_t *spec, const tv_t *val);
void us_to_tv(tv_t *val, int64_t us);
void us_to_ts(ts_t *spec, int64_t us);
void ms_to_ts(ts_t *spec, int64_t ms);
void ms_to_tv(tv_t *val, int64_t ms);
void tv_time(tv_t *tv);
void ts_time(ts_t *ts);

void cksleep_prepare_r(ts_t *ts);
void nanosleep_abstime(ts_t *ts_end);
void timeraddspec(ts_t *a, const ts_t *b);
void cksleep_ms_r(ts_t *ts_start, int ms);
void cksleep_us_r(ts_t *ts_start, int64_t us);
void cksleep_ms(int ms);
void cksleep_us(int64_t us);

double us_tvdiff(tv_t *end, tv_t *start);
int ms_tvdiff(tv_t *end, tv_t *start);
double tvdiff(tv_t *end, tv_t *start);

void decay_time(double *f, double fadd, double fsecs, double interval);
void suffix_string(double val, char *buf, size_t bufsiz, int sigdigits);

double le256todouble(const uchar *target);
double diff_from_target(uchar *target);
double diff_from_header(uchar *header);
void target_from_diff(uchar *target, double diff);

void gen_hash(uchar *data, uchar *hash, int len);

#endif /* LIBCKPOOL_H */
