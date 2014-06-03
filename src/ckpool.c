/*
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

#include "ckpool.h"
#include "libckpool.h"
#include "generator.h"
#include "stratifier.h"
#include "connector.h"

ckpool_t *global_ckp;

static void *listener(void *arg)
{
	proc_instance_t *pi = (proc_instance_t *)arg;
	unixsock_t *us = &pi->us;
	char *buf = NULL;
	int sockd;

	rename_proc(pi->sockname);
retry:
	dealloc(buf);
	sockd = accept(us->sockd, NULL, NULL);
	if (sockd < 0) {
		if (interrupted())
			goto retry;
		LOGERR("Failed to accept on socket in listener");
		goto out;
	}
	/* Insert parsing and repeat code here */
	buf = recv_unix_msg(sockd);
	if (!buf) {
		LOGWARNING("Failed to get message in listener");
		close(sockd);
		goto retry;
	}
	if (!strncasecmp(buf, "shutdown", 8)) {
		LOGWARNING("Listener received shutdown message, terminating ckpool");
		close(sockd);
		goto out;
	}
	if (!strncasecmp(buf, "ping", 4)) {
		LOGDEBUG("Listener received ping request");
		send_unix_msg(sockd, "pong");
	}
	close(sockd);
	goto retry;
out:
	dealloc(buf);
	close_unix_socket(us->sockd, us->path);
	return NULL;
}

bool ping_main(ckpool_t *ckp)
{
	char *buf;

	buf = send_recv_proc(&ckp->main, "ping");
	if (!buf)
		return false;
	free(buf);
	return true;
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

/* Disable signal handlers for child processes, but simply pass them onto the
 * parent process to shut down cleanly. */
static void childsighandler(int sig)
{
	pid_t ppid = getppid();

	LOGWARNING("Child process received signal %d, forwarding signal to %s main process",
		   sig, global_ckp->name);
	kill(ppid, sig);
}

static void launch_process(proc_instance_t *pi)
{
	pid_t pid;

	pid = fork();
	if (pid < 0)
		quit(1, "Failed to fork %s in launch_process", pi->processname);
	if (!pid) {
		struct sigaction handler;
		int ret;

		handler.sa_handler = &childsighandler;
		handler.sa_flags = 0;
		sigemptyset(&handler.sa_mask);
		sigaction(SIGTERM, &handler, NULL);
		sigaction(SIGINT, &handler, NULL);

		/* Detach child processes leaving main only to communicate with
		 * the terminal. */
		ioctl(0, TIOCNOTTY, NULL);
		rename_proc(pi->processname);
		write_namepid(pi);
		ret = pi->process(pi);
		close_unix_socket(pi->us.sockd, pi->us.path);
		rm_namepid(pi);
		exit(ret);
	}
	pi->pid = pid;
}

static void launch_processes(ckpool_t *ckp)
{
	int i;

	for (i = 0; i < ckp->proc_instances; i++)
		launch_process(ckp->children[i]);
}

int process_exit(ckpool_t *ckp, proc_instance_t *pi, int ret)
{
	if (ret) {
		/* Abnormal termination, kill entire process */
		LOGWARNING("%s %s exiting with return code %d, shutting down!",
			   ckp->name, pi->processname, ret);
		send_proc(&ckp->main, "shutdown");
		sleep(1);
		ret = 1;
	} else /* Should be part of a normal shutdown */
		LOGNOTICE("%s %s exited normally", ckp->name, pi->processname);

	return ret;
}

static void clean_up(ckpool_t *ckp)
{
	int i, children = ckp->proc_instances;

	rm_namepid(&ckp->main);
	dealloc(ckp->socket_dir);
	ckp->proc_instances = 0;
	for (i = 0; i < children; i++)
		dealloc(ckp->children[i]);
	dealloc(ckp->children);
	fclose(ckp->logfp);
}

static void __shutdown_children(ckpool_t *ckp, int sig)
{
	int i;

	pthread_cancel(ckp->pth_watchdog);
	join_pthread(ckp->pth_watchdog);

	for (i = 0; i < ckp->proc_instances; i++) {
		pid_t pid = ckp->children[i]->pid;
		if (!kill(pid, 0))
			kill(pid, sig);
	}
}

static void shutdown_children(ckpool_t *ckp, int sig)
{
	pthread_cancel(ckp->pth_watchdog);
	join_pthread(ckp->pth_watchdog);

	__shutdown_children(ckp, sig);
}

static void sighandler(int sig)
{
	int i;

	pthread_cancel(global_ckp->pth_watchdog);
	join_pthread(global_ckp->pth_watchdog);

	for (i = 0; i < global_ckp->proc_instances; i++)
		send_proc(global_ckp->children[i], "shutdown");

	if (sig != 9) {
		/* Wait a second, then send SIGTERM */
		sleep(1);
		__shutdown_children(global_ckp, SIGTERM);
		/* Wait another second, then send SIGKILL */
		sleep(1);
		__shutdown_children(global_ckp, SIGKILL);
		pthread_cancel(global_ckp->pth_listener);
		exit(0);
	} else {
		__shutdown_children(global_ckp, SIGKILL);
		exit(1);
	}
}

static void json_get_string(char **store, json_t *val, const char *res)
{
	json_t *entry = json_object_get(val, res);
	const char *buf;

	*store = NULL;
	if (!entry || json_is_null(entry)) {
		LOGDEBUG("Json did not find entry %s", res);
		return;
	}
	if (!json_is_string(entry)) {
		LOGWARNING("Json entry %s is not a string", res);
		return;
	}
	buf = json_string_value(entry);
	LOGDEBUG("Json found entry %s: %s", res, buf);
	*store = strdup(buf);
}

static void json_get_int(int *store, json_t *val, const char *res)
{
	json_t *entry = json_object_get(val, res);

	if (!entry) {
		LOGDEBUG("Json did not find entry %s", res);
		return;
	}
	if (!json_is_integer(entry)) {
		LOGWARNING("Json entry %s is not an integer", res);
		return;
	}
	*store = json_integer_value(entry);
}

static void parse_btcds(ckpool_t *ckp, json_t *arr_val, int arr_size)
{
	json_t *val;
	int i;

	ckp->btcds = arr_size;
	ckp->btcdurl = ckzalloc(sizeof(char *) * arr_size);
	ckp->btcdauth = ckzalloc(sizeof(char *) * arr_size);
	ckp->btcdpass = ckzalloc(sizeof(char *) * arr_size);
	for (i = 0; i < arr_size; i++) {
		val = json_array_get(arr_val, i);
		json_get_string(&ckp->btcdurl[i], val, "url");
		json_get_string(&ckp->btcdauth[i], val, "auth");
		json_get_string(&ckp->btcdpass[i], val, "pass");
	}
}

static void parse_proxies(ckpool_t *ckp, json_t *arr_val, int arr_size)
{
	json_t *val;
	int i;

	ckp->proxies = arr_size;
	ckp->proxyurl = ckzalloc(sizeof(char *) * arr_size);
	ckp->proxyauth = ckzalloc(sizeof(char *) * arr_size);
	ckp->proxypass = ckzalloc(sizeof(char *) * arr_size);
	for (i = 0; i < arr_size; i++) {
		val = json_array_get(arr_val, i);
		json_get_string(&ckp->proxyurl[i], val, "url");
		json_get_string(&ckp->proxyauth[i], val, "auth");
		json_get_string(&ckp->proxypass[i], val, "pass");
	}
}

static void parse_config(ckpool_t *ckp)
{
	json_t *json_conf, *arr_val;
	json_error_t err_val;

	json_conf = json_load_file(ckp->config, JSON_DISABLE_EOF_CHECK, &err_val);
	if (!json_conf) {
		LOGWARNING("Json decode error for config file %s: (%d): %s", ckp->config,
			   err_val.line, err_val.text);
		return;
	}
	arr_val = json_object_get(json_conf, "btcd");
	if (arr_val && json_is_array(arr_val)) {
		int arr_size = json_array_size(arr_val);

		if (arr_size)
			parse_btcds(ckp, arr_val, arr_size);
	}
	json_get_string(&ckp->btcaddress, json_conf, "btcaddress");
	json_get_string(&ckp->btcsig, json_conf, "btcsig");
	json_get_int(&ckp->blockpoll, json_conf, "blockpoll");
	json_get_int(&ckp->update_interval, json_conf, "update_interval");
	json_get_string(&ckp->serverurl, json_conf, "serverurl");
	json_get_int(&ckp->mindiff, json_conf, "mindiff");
	json_get_int(&ckp->startdiff, json_conf, "startdiff");
	json_get_string(&ckp->logdir, json_conf, "logdir");
	arr_val = json_object_get(json_conf, "proxy");
	if (arr_val && json_is_array(arr_val)) {
		int arr_size = json_array_size(arr_val);

		if (arr_size)
			parse_proxies(ckp, arr_val, arr_size);
	}
	json_decref(json_conf);
}

static proc_instance_t *prepare_child(ckpool_t *ckp, int (*process)(), char *name)
{
	proc_instance_t *pi = ckzalloc(sizeof(proc_instance_t));

	ckp->children = realloc(ckp->children, sizeof(proc_instance_t *) * (ckp->proc_instances + 1));
	ckp->children[ckp->proc_instances++] = pi;
	pi->ckp = ckp;
	pi->processname = name;
	pi->sockname = pi->processname;
	pi->process = process;
	create_process_unixsock(pi);
	return pi;
}

static proc_instance_t *child_by_pid(ckpool_t *ckp, pid_t pid)
{
	proc_instance_t *pi = NULL;
	int i;

	for (i = 0; i < ckp->proc_instances; i++) {
		if (ckp->children[i]->pid == pid) {
			pi = ckp->children[i];
			break;
		}
	}
	return pi;
}

static void *watchdog(void *arg)
{
	time_t last_relaunch_t = time(NULL);
	ckpool_t *ckp = (ckpool_t *)arg;

	rename_proc("watchdog");
	while (42) {
		proc_instance_t *pi;
		time_t relaunch_t;
		int pid, status;

		pid = waitpid(0, &status, 0);
		pi = child_by_pid(ckp, pid);
		if (pi && WIFEXITED(status)) {
			LOGWARNING("Child process %s exited, terminating!", pi->processname);
			break;
		}
		relaunch_t = time(NULL);
		if (relaunch_t == last_relaunch_t) {
			LOGEMERG("Respawning processes too fast, exiting!");
			break;
		}
		last_relaunch_t = relaunch_t;
		if (pi) {
			LOGERR("%s process dead! Relaunching", pi->processname);
			launch_process(pi);
		} else {
			LOGEMERG("Unknown child process %d dead, exiting!", pid);
			break;
		}
	}
	send_proc(&ckp->main, "shutdown");
	return NULL;
}

int main(int argc, char **argv)
{
	struct sigaction handler;
	int len, c, ret, i;
	char buf[512] = {};
	ckpool_t ckp;

	global_ckp = &ckp;
	memset(&ckp, 0, sizeof(ckp));
	ckp.loglevel = LOG_NOTICE;

	while ((c = getopt(argc, argv, "c:kl:n:ps:")) != -1) {
		switch (c) {
			case 'c':
				ckp.config = optarg;
				break;
			case 'k':
				ckp.killold = true;
				break;
			case 'l':
				ckp.loglevel = atoi(optarg);
				if (ckp.loglevel < LOG_EMERG || ckp.loglevel > LOG_DEBUG) {
					quit(1, "Invalid loglevel (range %d - %d): %d",
					     LOG_EMERG, LOG_DEBUG, ckp.loglevel);
				}
				break;
			case 'n':
				ckp.name = optarg;
				break;
			case 'p':
				ckp.proxy = true;
				break;
			case 's':
				ckp.socket_dir = strdup(optarg);
				break;
				break;
		}
	}

	if (!ckp.name) {
		if (ckp.proxy)
			ckp.name = "ckproxy";
		else
			ckp.name = "ckpool";
	}
	snprintf(buf, 15, "%s", ckp.name);
	prctl(PR_SET_NAME, buf, 0, 0, 0);
	memset(buf, 0, 15);

	if (!ckp.config) {
		ckp.config = strdup(ckp.name);
		realloc_strcat(&ckp.config, ".conf");
	}
	if (!ckp.socket_dir) {
		ckp.socket_dir = strdup("/tmp/");
		realloc_strcat(&ckp.socket_dir, ckp.name);
	}
	len = strlen(ckp.socket_dir);
	if (memcmp(&ckp.socket_dir[len], "/", 1))
		realloc_strcat(&ckp.socket_dir, "/");

	/* Ignore sigpipe */
	signal(SIGPIPE, SIG_IGN);

	ret = mkdir(ckp.socket_dir, 0700);
	if (ret && errno != EEXIST)
		quit(1, "Failed to make directory %s", ckp.socket_dir);

	parse_config(&ckp);
	/* Set defaults if not found in config file */
	if (!ckp.btcds && !ckp.proxy) {
		ckp.btcds = 1;
		ckp.btcdurl = ckzalloc(sizeof(char *));
		ckp.btcdauth = ckzalloc(sizeof(char *));
		ckp.btcdpass = ckzalloc(sizeof(char *));
	}
	if (ckp.btcds) {
		for (i = 0; i < ckp.btcds; i++) {
			if (!ckp.btcdurl[i])
				ckp.btcdurl[i] = strdup("localhost:8332");
			if (!ckp.btcdauth[i])
				ckp.btcdauth[i] = strdup("user");
			if (!ckp.btcdpass[i])
				ckp.btcdpass[i] = strdup("pass");
		}
	}

	if (!ckp.btcaddress)
		ckp.btcaddress = strdup("15qSxP1SQcUX3o4nhkfdbgyoWEFMomJ4rZ");
	if (!ckp.blockpoll)
		ckp.blockpoll = 500;
	if (!ckp.update_interval)
		ckp.update_interval = 30;
	if (!ckp.mindiff)
		ckp.mindiff = 1;
	if (!ckp.startdiff)
		ckp.startdiff = 42;
	if (!ckp.logdir)
		ckp.logdir = strdup("logs");
	if (ckp.proxy && !ckp.proxies)
		quit(0, "No proxy entries found in config file %s", ckp.config);

	/* Create the log directory */
	len = strlen(ckp.logdir);
	if (memcmp(&ckp.logdir[len], "/", 1))
		realloc_strcat(&ckp.logdir, "/");
	ret = mkdir(ckp.logdir, 0700);
	if (ret && errno != EEXIST)
		quit(1, "Failed to make log directory %s", ckp.logdir);

	/* Create the logfile */
	sprintf(buf, "%s%s.log", ckp.logdir, ckp.name);
	ckp.logfp = fopen(buf, "a");
	if (!ckp.logfp)
		quit(1, "Failed to make open log file %s", buf);
	ckp.logfd = fileno(ckp.logfp);

	ckp.main.ckp = &ckp;
	ckp.main.processname = strdup("main");
	ckp.main.sockname = strdup("listener");
	write_namepid(&ckp.main);
	create_process_unixsock(&ckp.main);

	create_pthread(&ckp.pth_listener, listener, &ckp.main);

	/* Launch separate processes from here */
	ckp.generator = prepare_child(&ckp, &generator, "generator");
	ckp.stratifier = prepare_child(&ckp, &stratifier, "stratifier");
	ckp.connector = prepare_child(&ckp, &connector, "connector");

	launch_processes(&ckp);

	create_pthread(&ckp.pth_watchdog, watchdog, &ckp);

	handler.sa_handler = &sighandler;
	handler.sa_flags = 0;
	sigemptyset(&handler.sa_mask);
	sigaction(SIGTERM, &handler, NULL);
	sigaction(SIGINT, &handler, NULL);

	/* Shutdown from here if the listener is sent a shutdown message */
	join_pthread(ckp.pth_listener);

	shutdown_children(&ckp, SIGTERM);
	clean_up(&ckp);

	return 0;
}
