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

/* Log everything to the logfile, but display warnings on the console as well */
void logmsg(int loglevel, const char *fmt, ...) {
	if (global_ckp->loglevel >= loglevel && fmt) {
		int logfd = global_ckp->logfd;
		char *buf = NULL;
		struct tm *tm;
		time_t now_t;
		va_list ap;

		va_start(ap, fmt);
		VASPRINTF(&buf, fmt, ap);
		va_end(ap);

		now_t = time(NULL);
		tm = localtime(&now_t);
		if (logfd) {
			FILE *LOGFP = global_ckp->logfp;

			flock(logfd, LOCK_EX);
			fprintf(LOGFP, "[%d-%02d-%02d %02d:%02d:%02d] %s",
				tm->tm_year + 1900,
				tm->tm_mon + 1,
				tm->tm_mday,
				tm->tm_hour,
				tm->tm_min,
				tm->tm_sec,
				buf);
			if (loglevel <= LOG_ERR)
				fprintf(LOGFP, " with errno %d: %s", errno, strerror(errno));
			fprintf(LOGFP, "\n");
			fflush(LOGFP);
			flock(logfd, LOCK_UN);
		}
		if (loglevel <= LOG_WARNING) {\
			fprintf(stderr, "%s", buf);
			if (loglevel <= LOG_ERR)
				fprintf(stderr, " with errno %d: %s", errno, strerror(errno));
			fprintf(stderr, "\n");
			fflush(stderr);
		}
		free(buf);
	}
}

/* Listen for incoming global requests. Always returns a response if possible */
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
		LOGERR("Failed to accept on socket in listener");
		goto out;
	}
	/* Insert parsing and repeat code here */
	buf = recv_unix_msg(sockd);
	if (!buf) {
		LOGWARNING("Failed to get message in listener");
	} else if (!strncasecmp(buf, "shutdown", 8)) {
		LOGWARNING("Listener received shutdown message, terminating ckpool");
		send_unix_msg(sockd, "exiting");
		goto out;
	} else if (!strncasecmp(buf, "ping", 4)) {
		LOGDEBUG("Listener received ping request");
		send_unix_msg(sockd, "pong");
	} else {
		LOGINFO("Listener received unhandled message: %s", buf);
		send_unix_msg(sockd, "unknown");
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

/* Read from a socket into cs->buf till we get an '\n', converting it to '\0'
 * and storing how much extra data we've received, to be moved to the beginning
 * of the buffer for use on the next receive. */
int read_socket_line(connsock_t *cs, int timeout)
{
	tv_t tv_timeout = {timeout, 0};
	char *eom = NULL;
	size_t buflen;
	int ret = -1;
	fd_set rd;

	if (unlikely(cs->fd < 0))
		goto out;

	if (unlikely(!cs->buf))
		cs->buf = ckzalloc(PAGESIZE);
	else if (cs->buflen) {
		memmove(cs->buf, cs->buf + cs->bufofs, cs->buflen);
		memset(cs->buf + cs->buflen, 0, cs->bufofs);
		cs->bufofs = cs->buflen;
		cs->buflen = 0;
		cs->buf[cs->bufofs] = '\0';
		eom = strchr(cs->buf, '\n');
	}

	while (42) {
		char readbuf[PAGESIZE] = {};

		FD_ZERO(&rd);
		FD_SET(cs->fd, &rd);
		if (eom)
			tv_timeout.tv_sec = tv_timeout.tv_usec = 0;
		ret = select(cs->fd + 1, &rd, NULL, NULL, &tv_timeout);
		if (eom && !ret)
			break;
		if (ret < 1) {
			if (!ret)
				LOGDEBUG("Select timed out in read_socket_line");
			else
				LOGERR("Select failed in read_socket_line");
			goto out;
		}
		ret = recv(cs->fd, readbuf, PAGESIZE - 4, 0);
		if (ret < 1) {
			LOGERR("Failed to recv in read_socket_line");
			ret = -1;
			goto out;
		}
		buflen = cs->bufofs + ret + 1;
		cs->buf = realloc(cs->buf, buflen);
		if (unlikely(!cs->buf))
			quit(1, "Failed to alloc buf of %d bytes in read_socket_line", (int)buflen);
		memcpy(cs->buf + cs->bufofs, readbuf, ret);
		cs->bufofs += ret;
		cs->buf[cs->bufofs] = '\0';
		eom = strchr(cs->buf, '\n');
	}
	ret = eom - cs->buf;

	cs->buflen = cs->buf + cs->bufofs - eom - 1;
	if (cs->buflen)
		cs->bufofs = eom - cs->buf + 1;
	else
		cs->bufofs = 0;
	*eom = '\0';
out:
	if (ret < 0) {
		dealloc(cs->buf);
		if (cs->fd > 0) {
			close(cs->fd);
			cs->fd = -1;
		}
	}
	return ret;
}

static void childsighandler(int sig);

/* Send a single message to a process instance when there will be no response,
 * closing the socket immediately. */
bool _send_proc(proc_instance_t *pi, const char *msg, const char *file, const char *func, const int line)
{
	char *path = pi->us.path;
	bool ret = false;
	int sockd;

	if (unlikely(!path || !strlen(path))) {
		LOGERR("Attempted to send message %s to null path in send_proc", msg ? msg : "");
		goto out;
	}
	if (unlikely(!msg || !strlen(msg))) {
		LOGERR("Attempted to send null message to socket %s in send_proc", path);
		goto out;
	}
	if (unlikely(kill(pi->pid, 0))) {
		LOGALERT("Attempting to send message %s to dead process %s", msg, pi->processname);
		goto out;
	}
	sockd = open_unix_client(path);
	if (unlikely(sockd < 0)) {
		LOGWARNING("Failed to open socket %s", path);
		goto out;
	}
	if (unlikely(!send_unix_msg(sockd, msg)))
		LOGWARNING("Failed to send %s to socket %s", msg, path);
	else
		ret = true;
	close(sockd);
out:
	if (unlikely(!ret)) {
		LOGERR("Failure in send_proc from %s %s:%d", file, func, line);
		childsighandler(15);
	}
	return ret;
}

/* Send a single message to a process instance and retrieve the response, then
 * close the socket. */
char *_send_recv_proc(proc_instance_t *pi, const char *msg, const char *file, const char *func, const int line)
{
	char *path = pi->us.path, *buf = NULL;
	int sockd;

	if (unlikely(!path || !strlen(path))) {
		LOGERR("Attempted to send message %s to null path in send_proc", msg ? msg : "");
		goto out;
	}
	if (unlikely(!msg || !strlen(msg))) {
		LOGERR("Attempted to send null message to socket %s in send_proc", path);
		goto out;
	}
	if (unlikely(kill(pi->pid, 0))) {
		LOGALERT("Attempting to send message %s to dead process %s", msg, pi->processname);
		goto out;
	}
	sockd = open_unix_client(path);
	if (unlikely(sockd < 0)) {
		LOGWARNING("Failed to open socket %s", path);
		goto out;
	}
	if (unlikely(!send_unix_msg(sockd, msg)))
		LOGWARNING("Failed to send %s to socket %s", msg, path);
	else
		buf = recv_unix_msg(sockd);
	close(sockd);
out:
	if (unlikely(!buf))
		LOGERR("Failure in send_recv_proc from %s %s:%d", file, func, line);
	return buf;
}


json_t *json_rpc_call(connsock_t *cs, const char *rpc_req)
{
	char *http_req = NULL;
	json_error_t err_val;
	json_t *val = NULL;
	int len, ret;

	if (unlikely(cs->fd < 0)) {
		LOGWARNING("FD %d invalid in json_rpc_call", cs->fd);
		goto out;
	}
	if (unlikely(!cs->url)) {
		LOGWARNING("No URL in json_rpc_call");
		goto out;
	}
	if (unlikely(!cs->port)) {
		LOGWARNING("No port in json_rpc_call");
		goto out;
	}
	if (unlikely(!cs->auth)) {
		LOGWARNING("No auth in json_rpc_call");
		goto out;
	}
	if (unlikely(!rpc_req)) {
		LOGWARNING("Null rpc_req passed to json_rpc_call");
		goto out;
	}
	len = strlen(rpc_req);
	if (unlikely(!len)) {
		LOGWARNING("Zero length rpc_req passed to json_rpc_call");
		goto out;
	}
	http_req = ckalloc(len + 256); // Leave room for headers
	sprintf(http_req,
		 "POST / HTTP/1.1\n"
		 "Authorization: Basic %s\n"
		 "Host: %s:%s\n"
		 "Content-type: application/json\n"
		 "Content-Length: %d\n\n%s",
		 cs->auth, cs->url, cs->port, len, rpc_req);

	len = strlen(http_req);
	ret = write_socket(cs->fd, http_req, len);
	if (ret != len) {
		LOGWARNING("Failed to write to socket in json_rpc_call");
		goto out_empty;
	}
	ret = read_socket_line(cs, 5);
	if (ret < 1) {
		LOGWARNING("Failed to read socket line in json_rpc_call");
		goto out_empty;
	}
	if (strncasecmp(cs->buf, "HTTP/1.1 200 OK", 15)) {
		LOGWARNING("HTTP response not ok: %s", cs->buf);
		goto out_empty;
	}
	do {
		ret = read_socket_line(cs, 5);
		if (ret < 1) {
			LOGWARNING("Failed to read http socket lines in json_rpc_call");
			goto out_empty;
		}
	} while (strncmp(cs->buf, "{", 1));

	val = json_loads(cs->buf, 0, &err_val);
	if (!val)
		LOGWARNING("JSON decode failed(%d): %s", err_val.line, err_val.text);
out_empty:
	empty_socket(cs->fd);
	if (!val) {
		/* Assume that a failed request means the socket will be closed
		 * and reopen it */
		LOGWARNING("Reopening socket to %s:%s", cs->url, cs->port);
		close(cs->fd);
		cs->fd = connect_socket(cs->url, cs->port);
	}
out:
	free(http_req);
	dealloc(cs->buf);
	return val;
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
	char buf[512] = {};
	int c, ret, i;
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
	trail_slash(&ckp.socket_dir);

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
	trail_slash(&ckp.logdir);
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
