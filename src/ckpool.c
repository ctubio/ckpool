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
#include <sys/wait.h>
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
	close(sockd);
	goto retry;
out:
	dealloc(buf);
	close_unix_socket(us->sockd, us->path);
	return NULL;
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
				LOGEMERG("Process %s pid %d still exists", path, oldpid);
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

		rename_proc(pi->processname);
		write_namepid(pi);
		ret = pi->process(pi);
		close_unix_socket(pi->us.sockd, pi->us.path);
		rm_namepid(pi);
		exit(ret);
	}
	pi->pid = pid;
}

static void clean_up(ckpool_t *ckp)
{
	rm_namepid(&ckp->main);
	dealloc(ckp->socket_dir);
}

static void __shutdown_children(ckpool_t *ckp, int sig)
{
	pthread_cancel(ckp->pth_watchdog);
	join_pthread(ckp->pth_watchdog);

	if (!kill(ckp->generator.pid, 0))
		kill(ckp->generator.pid, sig);
	if (!kill(ckp->stratifier.pid, 0))
		kill(ckp->stratifier.pid, sig);
	if (!kill(ckp->connector.pid, 0))
		kill(ckp->connector.pid, sig);
}

static void shutdown_children(ckpool_t *ckp, int sig)
{
	pthread_cancel(ckp->pth_watchdog);
	join_pthread(ckp->pth_watchdog);

	__shutdown_children(ckp, sig);
}

static void sighandler(int sig)
{
	pthread_cancel(global_ckp->pth_watchdog);
	join_pthread(global_ckp->pth_watchdog);

	/* First attempt, send a shutdown message */
	send_proc(&global_ckp->generator, "shutdown");
	send_proc(&global_ckp->stratifier, "shutdown");
	send_proc(&global_ckp->connector, "shutdown");

	if (sig != 9) {
		/* Wait a second, then send SIGTERM */
		sleep(1);
		__shutdown_children(global_ckp, SIGTERM);
		/* Wait another second, then send SIGKILL */
		sleep(1);
		__shutdown_children(global_ckp, SIGKILL);
		pthread_cancel(global_ckp->pth_listener);
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
	json_get_string(&ckp->btcaddress, json_conf, "btcaddress");
	json_get_string(&ckp->btcsig, json_conf, "btcsig");
	json_get_int(&ckp->blockpoll, json_conf, "blockpoll");
	json_get_int(&ckp->update_interval, json_conf, "update_interval");
	json_get_string(&ckp->serverurl, json_conf, "serverurl");
	json_get_int(&ckp->mindiff, json_conf, "mindiff");
	json_get_int(&ckp->startdiff, json_conf, "startdiff");
	json_get_string(&ckp->logdir, json_conf, "logdir");
	json_decref(json_conf);
}

static void test_functions(ckpool_t *ckp)
{
	char *path = ckp->generator.us.path, *buf;
	int genfd;

	genfd = open_unix_client(ckp->generator.us.path);
	if (genfd < 0) {
		LOGWARNING("Failed to open generator socket %s", path);
		return;
	}
	send_unix_msg(genfd, "getbase");
	buf = recv_unix_msg(genfd);
	dealloc(buf);
#if 0
	genfd = open_unix_client(ckp->generator.us.path);
	if (genfd < 0) {
		LOGWARNING("Failed to open generator socket %s", path);
		return;
	}
	send_unix_msg(genfd, "shutdown");
#endif
}

static void prepare_generator(ckpool_t *ckp)
{
	proc_instance_t *pi = &ckp->generator;

	pi->ckp = ckp;
	pi->processname = strdup("generator");
	pi->sockname = pi->processname;
	pi->process = &generator;
	create_process_unixsock(pi);
}

static void prepare_stratifier(ckpool_t *ckp)
{
	proc_instance_t *pi = &ckp->stratifier;

	pi->ckp = ckp;
	pi->processname = strdup("stratifier");
	pi->sockname = pi->processname;
	pi->process = &stratifier;
	create_process_unixsock(pi);
}

static void prepare_connector(ckpool_t *ckp)
{
	proc_instance_t *pi = &ckp->connector;

	pi->ckp = ckp;
	pi->processname = strdup("connector");
	pi->sockname = pi->processname;
	pi->process = &connector;
	create_process_unixsock(pi);
}

static void *watchdog(void *arg)
{
	ckpool_t *ckp = (ckpool_t *)arg;

	rename_proc("watchdog");
	while (42) {
		int pid, status;

		pid = wait(&status);
		if (pid == ckp->generator.pid) {
			LOGERR("Generator process dead! Relaunching");
			launch_process(&ckp->generator);
		} else if (pid == ckp->stratifier.pid) {
			LOGERR("Stratifier process dead! Relaunching");
			launch_process(&ckp->stratifier);
		} else if (pid == ckp->connector.pid) {
			LOGERR("Connector process dead! Relaunching");
			launch_process(&ckp->connector);
		}
	}
	return NULL;
}

int main(int argc, char **argv)
{
	struct sigaction handler;
	int len, c, ret;
	ckpool_t ckp;

	global_ckp = &ckp;
	memset(&ckp, 0, sizeof(ckp));
	ckp.loglevel = LOG_NOTICE;

	while ((c = getopt(argc, argv, "c:kl:n:s:")) != -1) {
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
			case 's':
				ckp.socket_dir = strdup(optarg);
				break;
				break;
		}
	}

	if (!ckp.name)
		ckp.name = strdup("ckpool");
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
	if (!ckp.btcdurl)
		ckp.btcdurl = strdup("localhost:8332");
	if (!ckp.btcdauth)
		ckp.btcdauth = strdup("user");
	if (!ckp.btcdpass)
		ckp.btcdpass = strdup("pass");
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

	len = strlen(ckp.logdir);
	if (memcmp(&ckp.logdir[len], "/", 1))
		realloc_strcat(&ckp.logdir, "/");
	ret = mkdir(ckp.logdir, 0700);
	if (ret && errno != EEXIST)
		quit(1, "Failed to make log directory %s", ckp.logdir);

	ckp.main.ckp = &ckp;
	ckp.main.processname = strdup("main");
	ckp.main.sockname = strdup("listener");
	write_namepid(&ckp.main);
	create_process_unixsock(&ckp.main);

	create_pthread(&ckp.pth_listener, listener, &ckp.main);

	/* Launch separate processes from here */
	prepare_generator(&ckp);
	prepare_stratifier(&ckp);
	prepare_connector(&ckp);
	launch_process(&ckp.generator);
	launch_process(&ckp.stratifier);
	launch_process(&ckp.connector);

	handler.sa_handler = &sighandler;
	handler.sa_flags = 0;
	sigemptyset(&handler.sa_mask);
	sigaction(SIGTERM, &handler, NULL);
	sigaction(SIGINT, &handler, NULL);

	test_functions(&ckp);

	create_pthread(&ckp.pth_watchdog, watchdog, &ckp);

	/* Shutdown from here if the listener is sent a shutdown message */
	join_pthread(ckp.pth_listener);

	shutdown_children(&ckp, SIGTERM);
	clean_up(&ckp);

	return 0;
}
