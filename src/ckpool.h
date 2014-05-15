/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#ifndef CKPOOL_H
#define CKPOOL_H

#include "config.h"

#include "libckpool.h"

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
	/* Filename of config file */
	char *config;
	/* Kill old instance with same name */
	bool killold;
	/* Logging level */
	int loglevel;
	/* Main process name */
	char *name;
	/* Directory where sockets are created */
	char *socket_dir;
	/* Directory where logs are written */
	char *logdir;

	/* Process instance data of parent/child processes */
	proc_instance_t main;

	int proc_instances;
	proc_instance_t **children;

	proc_instance_t *generator;
	proc_instance_t *stratifier;
	proc_instance_t *connector;

	/* Threads of main process */
	pthread_t pth_listener;
	pthread_t pth_watchdog;

	/* Bitcoind data */
	int btcds;
	char **btcdurl;
	char **btcdauth;
	char **btcdpass;
	int blockpoll; // How frequently in ms to poll bitcoind for block updates

	/* Difficulty settings */
	int mindiff; // Default 1
	int startdiff; // Default 42

	/* Coinbase data */
	char *btcaddress; // Address to mine to
	char *btcsig; // Optional signature to add to coinbase

	/* Stratum options */
	int update_interval; // Seconds between stratum updates
	char *serverurl;

	/* Proxy options */
	bool proxy;
	int proxies;
	char **proxyurl;
	char **proxyauth;
	char **proxypass;
};

ckpool_t *global_ckp;

/* Placeholders for when we have more comprehensive logging facilities */

#define LOGMSG(_loglevel, fmt, ...) do { \
	if (global_ckp->loglevel >= _loglevel && fmt) { \
		struct tm *tm; \
		time_t now_t; \
		now_t = time(NULL); \
		tm = localtime(&now_t); \
		fprintf(stderr, "[%d-%02d-%02d %02d:%02d:%02d] ", \
			tm->tm_year + 1900, \
			tm->tm_mon + 1, \
			tm->tm_mday, \
			tm->tm_hour, \
			tm->tm_min, \
			tm->tm_sec); \
		fprintf(stderr, fmt, ##__VA_ARGS__); \
		if (_loglevel <= LOG_ERR) \
			fprintf(stderr, " with errno %d: %s", errno, strerror(errno)); \
		fprintf(stderr, "\n"); \
		fflush(stderr); \
	} \
} while (0)

#define LOGEMERG(fmt, ...) LOGMSG(LOG_EMERG, fmt, ##__VA_ARGS__)
#define LOGALERT(fmt, ...) LOGMSG(LOG_ALERT, fmt, ##__VA_ARGS__)
#define LOGCRIT(fmt, ...) LOGMSG(LOG_CRIT, fmt, ##__VA_ARGS__)
#define LOGERR(fmt, ...) LOGMSG(LOG_ERR, fmt, ##__VA_ARGS__)
#define LOGWARNING(fmt, ...) LOGMSG(LOG_WARNING, fmt, ##__VA_ARGS__)
#define LOGNOTICE(fmt, ...) LOGMSG(LOG_NOTICE, fmt, ##__VA_ARGS__)
#define LOGINFO(fmt, ...) LOGMSG(LOG_INFO, fmt, ##__VA_ARGS__)
#define LOGDEBUG(fmt, ...) LOGMSG(LOG_DEBUG, fmt, ##__VA_ARGS__)

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
		fprintf(stderr, "\n"); \
		fflush(stderr); \
	} \
	exit(status); \
} while (0)

#endif /* CKPOOL_H */
