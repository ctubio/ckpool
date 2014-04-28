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
	/* Logging level */
	int loglevel;

	/* Process instance data of parent/child processes */
	proc_instance_t main;
	proc_instance_t generator;
	proc_instance_t stratifier;
	proc_instance_t connector;

	/* Threads of main process */
	pthread_t pth_listener;
	pthread_t pth_watchdog;

	/* Bitcoind data */
	char *btcdurl;
	char *btcdauth;
	char *btcdpass;
	int blockpoll; // How frequently in ms to poll bitcoind for block updates

	/* Coinbase data */
	char *btcaddress; // Address to mine to
	char *btcsig; // Optional signature to add to coinbase

	/* Stratum options */
	int update_interval; // Seconds between stratum updates
	char *serverurl;
};

ckpool_t *global_ckp;

#endif /* CKPOOL_H */
