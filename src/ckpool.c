/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include <sys/prctl.h>
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

static void rename_proc(const char *name)
{
	char buf[16];

	snprintf(buf, 16, "ckp@%s", name);
	prctl(PR_SET_NAME, buf, 0, 0, 0);
}

static void *listener(void *arg)
{
	proc_instance_t *pi = (proc_instance_t *)arg;
	unixsock_t *us = &pi->us;
	int sockd;

	rename_proc(pi->sockname);
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
	int pid = getpid();
	char s[1024];

	sprintf(s, "%s%s.pid", pi->ckp->socket_dir, pi->processname);
	if (!write_pid(s, pid))
		quit(1, "Failed to write %s pid %d", pi->processname, pid);
}

static void launch_process(proc_instance_t *pi)
{
	pid_t pid;

	pid = fork();
	if (pid < 0)
		quit(1, "Failed to fork %s in launch_process", pi->processname);
	if (!pid) {
		rename_proc(pi->processname);
		write_namepid(pi);
		create_process_unixsock(pi);
		exit(pi->process(pi));
	}
}

int main(int argc, char **argv)
{
	proc_instance_t proc_main;
	proc_instance_t proc_generator;
	pthread_t pth_listener;
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

	proc_main.ckp = &ckp;
	proc_main.processname = strdup("main");
	proc_main.sockname = strdup("listener");
	write_namepid(&proc_main);
	create_process_unixsock(&proc_main);

	create_pthread(&pth_listener, listener, &proc_main);

	/* Launch separate processes from here */
	proc_generator.ckp = &ckp;
	proc_generator.processname = strdup("generator");
	proc_generator.sockname = proc_generator.processname;
	proc_generator.process = &generator;
	launch_process(&proc_generator);

	/* Shutdown from here */
	join_pthread(pth_listener);
	dealloc(ckp.socket_dir);

	return 0;
}
