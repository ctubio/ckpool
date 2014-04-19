/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "ckpool.h"
#include "libckpool.h"
#include "bitcoin.h"

static char *socket_dir = "/tmp/ckpool/";

static void create_pthread(pthread_t *thread, void *(*start_routine)(void *), void *arg)
{
	int ret = pthread_create(thread, NULL, start_routine,  arg);

	if (unlikely(ret))
		quit(1, "Failed to pthread_create");
}

static void join_pthread(pthread_t thread)
{
	int ret = pthread_join(thread, NULL);

	if (unlikely(ret))
		quit(1, "Failed to pthread_join");
}

static void *listener(void *arg)
{
	unixsock_t *us = (unixsock_t *)arg;

	close_unix_socket(us->sockd, us->path);
	return NULL;
}

int main(int argc, char **argv)
{
	pthread_t pth_listener;
	unixsock_t uslistener;
	ckpool_t ckp;
	int c, ret;

	memset(&ckp, 0, sizeof(ckp));
	while ((c = getopt(argc, argv, "c:n:")) != -1) {
		switch (c) {
			case 'c':
				ckp.config = optarg;
				break;
			case 'n':
				ckp.name = optarg;
				break;
			case 's':
				socket_dir = optarg;
				break;
		}
	}

	ret = mkdir(socket_dir, 0700);
	if (ret && errno != EEXIST)
		quit(1, "Failed to make directory %s", socket_dir);

	uslistener.path = strdup(socket_dir);
	realloc_strcat(&uslistener.path, "listener");
	LOGDEBUG("Opening %s", uslistener.path);
	uslistener.sockd = open_unix_server(uslistener.path);
	if (unlikely(uslistener.sockd < 0))
		quit(1, "Failed to open listener socket");
	create_pthread(&pth_listener, listener, &uslistener);

	join_pthread(pth_listener);

	return 0;
}
