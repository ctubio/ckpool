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

/* Only global variable, to be used only by sighandler */
static ckpool_t *global_ckp;

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
	char s[256];

	pi->pid = getpid();
	sprintf(s, "%s%s.pid", pi->ckp->socket_dir, pi->processname);
	if (!write_pid(s, pi->pid))
		quit(1, "Failed to write %s pid %d", pi->processname, pi->pid);
}

static void rm_namepid(proc_instance_t *pi)
{
	char s[256];

	sprintf(s, "%s%s.pid", pi->ckp->socket_dir, pi->processname);
	unlink(s);
}

static void launch_process(proc_instance_t *pi)
{
	pid_t pid;

	pid = fork();
	if (pid < 0)
		quit(1, "Failed to fork %s in launch_process", pi->processname);
	if (!pid) {
		int ret;

		rename_proc(pi->processname);
		write_namepid(pi);
		create_process_unixsock(pi);
		ret = pi->process(pi);
		close_unix_socket(pi->us.sockd, pi->us.path);
		rm_namepid(pi);
		exit(ret);
	}
}

static void clean_up(ckpool_t *ckp)
{
	rm_namepid(&ckp->main);
	dealloc(ckp->socket_dir);
}

static void shutdown_children(ckpool_t *ckp, int sig)
{
	kill(ckp->generator.pid, sig);
}

static void sighandler(int sig)
{
	shutdown_children(global_ckp, sig);
	clean_up(global_ckp);
	exit(0);
}

static void json_get_string(char **store, json_t *val, const char *res)
{
	json_t *entry = json_object_get(val, res);
	const char *buf;

	*store = NULL;
	if (json_is_null(entry)) {
		LOGDEBUG("Json did not find entry %s", res);
		return;
	}
	if (!json_is_string(entry)) {
		LOGWARNING("Json entry %s is not string", res);
		return;
	}
	buf = json_string_value(entry);
	LOGDEBUG("Json found entry %s: %s", res, buf);
	*store = strdup(buf);
}

static void parse_config(ckpool_t *ckp)
{
	json_error_t err_val;
	json_t *json_conf;

	json_conf = json_load_file(ckp->config, JSON_DISABLE_EOF_CHECK, &err_val);
	if (!json_conf) {
		LOGWARNING("Json decode error for config file %s: (%d): %s", ckp->config,
			   err_val.line, err_val.text);
		return;
	}
	json_get_string(&ckp->btcdurl, json_conf, "btcdurl");
	json_get_string(&ckp->btcdauth, json_conf, "btcdauth");
	json_get_string(&ckp->btcdpass, json_conf, "btcdpass");
	json_decref(json_conf);
}

int main(int argc, char **argv)
{
	struct sigaction handler;
	pthread_t pth_listener;
	ckpool_t ckp;
	int c, ret;

	global_ckp = &ckp;
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

	if (!ckp.name)
		ckp.name = strdup("ckpool");
	if (!ckp.config)
		ckp.config = strdup("ckpool.conf");
	if (!ckp.socket_dir)
		ckp.socket_dir = strdup("/tmp/ckpool");

	realloc_strcat(&ckp.socket_dir, "/");

	/* Ignore sigpipe */
	signal(SIGPIPE, SIG_IGN);

	ret = mkdir(ckp.socket_dir, 0700);
	if (ret && errno != EEXIST)
		quit(1, "Failed to make directory %s", ckp.socket_dir);

	parse_config(&ckp);

	ckp.main.ckp = &ckp;
	ckp.main.processname = strdup("main");
	ckp.main.sockname = strdup("listener");
	write_namepid(&ckp.main);
	create_process_unixsock(&ckp.main);

	create_pthread(&pth_listener, listener, &ckp.main);

	/* Launch separate processes from here */
	ckp.generator.ckp = &ckp;
	ckp.generator.processname = strdup("generator");
	ckp.generator.sockname = ckp.generator.processname;
	ckp.generator.process = &generator;
	launch_process(&ckp.generator);

	/* Install signal handlers only for the master process to be able to
	 * shut down all child processes */
	handler.sa_handler = &sighandler;
	handler.sa_flags = 0;
	sigemptyset(&handler.sa_mask);
	sigaction(SIGTERM, &handler, NULL);
	sigaction(SIGINT, &handler, NULL);

	/* Shutdown from here */
	join_pthread(pth_listener);

	shutdown_children(&ckp, SIGTERM);
	clean_up(&ckp);

	return 0;
}
