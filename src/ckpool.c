/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "ckpool.h"
#include "libckpool.h"
#include "generator.h"

static char *socket_dir = "/tmp/ckpool";

static void *listener(void *arg)
{
	unixsock_t *us = (unixsock_t *)arg;
	int sockd;

retry:
	sockd = accept(us->sockd, NULL, NULL);
	if (sockd < 0) {
		if (interrupted())
			goto retry;
		LOGERR("Failed to accept on socket in listener");
		goto out;
	}
	/* Insert parsing and repeat code here */
out:
	if (sockd >= 0)
		close(sockd);
	close_unix_socket(us->sockd, us->path);
	return NULL;
}

/* Open the file in path, check if there is a pid in there that still exists
 * and if not, write the pid into that file. */
static bool write_pid(const char *path, pid_t pid)
{
	struct stat statbuf;
	FILE *fp;
	int ret;

	if (!stat(path, &statbuf)) {
		int oldpid;

		LOGWARNING("File %s exists", path);
		fp = fopen(path, "r");
		if (!fp) {
			LOGERR("Failed to open file %s", path);
			return false;
		}
		ret = fscanf(fp, "%d", &oldpid);
		fclose(fp);
		if (ret == 1 && !(kill(oldpid, 0))) {
			LOGWARNING("Process %s pid %d still exists", path, oldpid);
			return false;
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

int main(int argc, char **argv)
{
	pthread_t pth_listener;
	unixsock_t uslistener;
	pid_t mainpid;
	ckpool_t ckp;
	int c, ret;
	char *s;

	memset(&ckp, 0, sizeof(ckp));
	while ((c = getopt(argc, argv, "c:gn:s:")) != -1) {
		switch (c) {
			case 'c':
				ckp.config = optarg;
				break;
			case 'g':
				/* Launch generator only */
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

	mainpid = getpid();

	s = strdup(socket_dir);
	realloc_strcat(&s, "main.pid");
	if (!write_pid(s, mainpid))
		quit(1, "Failed to write pid");
	dealloc(s);

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
