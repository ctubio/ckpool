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

static void create_process_unixsock(ckpool_t *ckp, unixsock_t *us, const char *process)
{
	us->path = strdup(ckp->socket_dir);
	realloc_strcat(&us->path, process);
	LOGDEBUG("Opening %s", us->path);
	us->sockd = open_unix_server(us->path);
	if (unlikely(us->sockd < 0))
		quit(1, "Failed to open %s socket", process);
}

void write_namepid(ckpool_t *ckp, const char *process)
{
	int pid = getpid();
	char s[1024];

	sprintf(s, "%s%s.pid", ckp->socket_dir, process);
	if (!write_pid(s, pid))
		quit(1, "Failed to write %s pid %d", process, pid);
}

int launch_generator(ckpool_t *ckp)
{
	pid_t pid;

	pid = fork();
	if (pid < 0)
		quit(1, "Failed to fork in launch_generator");
	if (!pid) {
		unixsock_t us;

		write_namepid(ckp, "generator");
		create_process_unixsock(ckp, &us, "generator");
		generator(&us);
	}

	return 0;
}

int main(int argc, char **argv)
{
	pthread_t pth_listener;
	unixsock_t uslistener;
	ckpool_t ckp;
	int c, ret;

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
				ckp.socket_dir = strdup(optarg);
				break;
		}
	}

	if (!ckp.socket_dir)
		ckp.socket_dir = strdup("/tmp/ckpool");

	realloc_strcat(&ckp.socket_dir, "/");
	ret = mkdir(ckp.socket_dir, 0700);
	if (ret && errno != EEXIST)
		quit(1, "Failed to make directory %s", ckp.socket_dir);

	write_namepid(&ckp, "main");
	create_process_unixsock(&ckp, &uslistener, "listener");
	create_pthread(&pth_listener, listener, &uslistener);

	/* Launch separate processes from here */
	launch_generator(&ckp);

	/* Shutdown from here */
	join_pthread(pth_listener);
	dealloc(ckp.socket_dir);

	return 0;
}
