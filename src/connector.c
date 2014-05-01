/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include "config.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>

#include "ckpool.h"
#include "libckpool.h"
#include "uthash.h"
#include "utlist.h"

#define MAX_MSGSIZE 1024

struct connector_instance {
	cklock_t lock;
	proc_instance_t *pi;
	int serverfd;
	int nfds;
};

typedef struct connector_instance conn_instance_t;

struct client_instance {
	UT_hash_handle hh;
	struct sockaddr address;
	socklen_t address_len;
	char buf[PAGESIZE];
	int bufofs;
	int fd;
	int id;
};

typedef struct client_instance client_instance_t;

/* For the hashtable of all clients */
static client_instance_t *clients;
static int client_id;

struct sender_send {
	struct sender_send *next;
	struct sender_send *prev;

	client_instance_t *client;
	char *buf;
	int len;
};

typedef struct sender_send sender_send_t;

/* For the linked list of pending sends */
static sender_send_t *sender_sends;

/* For protecting the pending sends list */
static pthread_mutex_t sender_lock;
static pthread_cond_t sender_cond;

/* Accepts incoming connections to the server socket and generates client
 * instances */
void *acceptor(void *arg)
{
	conn_instance_t *ci = (conn_instance_t *)arg;
	client_instance_t *client;
	int fd;

	rename_proc("acceptor");

retry:
	client = ckzalloc(sizeof(client_instance_t));
	client->address_len = sizeof(client->address);
reaccept:
	fd = accept(ci->serverfd, &client->address, &client->address_len);
	if (unlikely(fd < 0)) {
		if (interrupted())
			goto reaccept;
		LOGERR("Failed to accept on socket %d in acceptor", ci->serverfd);
		dealloc(client);
		goto out;
	}
	keep_sockalive(fd);

	LOGINFO("Connected new client %d on socket %d", ci->nfds, fd);

	client->fd = fd;

	ck_wlock(&ci->lock);
	client->id = client_id++;
	HASH_ADD_INT(clients, id, client);
	ci->nfds++;
	ck_wunlock(&ci->lock);

	goto retry;
out:
	return NULL;
}

/* Invalidate this instance */
static void invalidate_client(ckpool_t *ckp, client_instance_t *client)
{
	char buf[256];

	close(client->fd);
	client->fd = -1;
	sprintf(buf, "dropclient=%d", client->id);
	send_proc(&ckp->stratifier, buf);
}

static void send_client(conn_instance_t *ci, int id, char *buf);

static void parse_client_msg(conn_instance_t *ci, client_instance_t *client)
{
	ckpool_t *ckp = ci->pi->ckp;
	char msg[PAGESIZE], *eol;
	int buflen, ret;
	bool moredata;
	json_t *val;

retry:
	buflen = PAGESIZE - client->bufofs;
	ret = recv(client->fd, client->buf + client->bufofs, buflen, MSG_DONTWAIT);
	if (ret < 1) {
		/* Nothing else ready to be read */
		if (!ret)
			return;

		LOGINFO("Client fd %d disconnected", client->fd);
		invalidate_client(ckp, client);
		return;
	}
	client->bufofs += ret;
	if (client->bufofs == PAGESIZE)
		moredata = true;
	else
		moredata = false;
reparse:
	eol = memchr(client->buf, '\n', client->bufofs);
	if (!eol) {
		if (unlikely(client->bufofs > MAX_MSGSIZE)) {
			LOGWARNING("Client fd %d overloaded buffer without EOL, disconnecting", client->fd);
			invalidate_client(ckp, client);
			return;
		}
		if (moredata)
			goto retry;
		return;
	}

	/* Do something useful with this message now */
	buflen = eol - client->buf + 1;
	if (unlikely(buflen > MAX_MSGSIZE)) {
		LOGWARNING("Client fd %d message oversize, disconnecting", client->fd);
		invalidate_client(ckp, client);
		return;
	}
	memcpy(msg, client->buf, buflen);
	msg[buflen] = 0;
	client->bufofs -= buflen;
	memmove(client->buf, client->buf + buflen, client->bufofs);
	val = json_loads(msg, 0, NULL);
	if (!val) {
		char *buf = strdup("Invalid JSON, disconnecting\n");

		LOGINFO("Client id %d sent invalid json message %s", client->id, msg);
		send_client(ci, client->id, buf);
		invalidate_client(ckp, client);
		return;
	} else {
		char *s;

		json_object_set_new_nocheck(val, "client_id", json_integer(client->id));
		s = json_dumps(val, 0);
		send_proc(&ckp->stratifier, s);
		free(s);
		json_decref(val);
	}

	if (client->bufofs)
		goto reparse;
}

/* Waits on fds ready to read on from the list stored in conn_instance and
 * handles the incoming messages */
void *receiver(void *arg)
{
	conn_instance_t *ci = (conn_instance_t *)arg;
	client_instance_t *client;
	struct pollfd fds[65536];
	int ret, nfds, i;

	rename_proc("creceiver");

retry:
	nfds = 0;

	ck_rlock(&ci->lock);
	for (client = clients; client != NULL; client = client->hh.next) {
		/* Invalid client */
		if (client->fd == -1)
			continue;
		fds[nfds].fd = client->fd;
		fds[nfds].events = POLLIN;
		nfds++;
	}
	ck_runlock(&ci->lock);

	if (!nfds) {
		cksleep_ms(100);
		goto retry;
	}
repoll:
	ret = poll(fds, nfds, 1000);
	if (ret < 0) {
		if (interrupted())
			goto repoll;
		LOGERR("Failed to poll in receiver");
		goto out;
	}
	if (!ret)
		goto retry;
	for (i = 0; i < nfds; i++) {

		if (!(fds[i].revents & POLLIN))
			continue;

		client = NULL;

		ck_rlock(&ci->lock);
		for (client = clients; client != NULL; client = client->hh.next) {
			if (client->fd == fds[i].fd)
				break;
		}
		ck_runlock(&ci->lock);

		if (!client) {
			/* Not yet invalidated */
			LOGDEBUG("Failed to find client with fd %d in hashtable!", fds[i].fd);
			close(fds[i].fd);
		} else
			parse_client_msg(ci, client);

		if (--ret < 1)
			break;
	}
	goto retry;

out:
	return NULL;
}

/* Use a thread to send queued messages, using select() to only send to sockets
 * ready for writing immediately to not delay other messages. */
void *sender(void *arg)
{
	conn_instance_t *ci = (conn_instance_t *)arg;
	ckpool_t *ckp = ci->pi->ckp;

	rename_proc("creceiver");

	while (42) {
		sender_send_t *sender_send;
		client_instance_t *client;
		tv_t timeout_tv = {0, 0};
		int ret, fd, ofs = 0;
		fd_set writefds;

		mutex_lock(&sender_lock);
		if (!sender_sends) {
			ts_t timeout_ts;

			ts_realtime(&timeout_ts);
			timeout_ts.tv_sec += 1;
			pthread_cond_timedwait(&sender_cond, &sender_lock, &timeout_ts);
		}
		sender_send = sender_sends;
		if (likely(sender_send))
			DL_DELETE(sender_sends, sender_send);
		mutex_unlock(&sender_lock);

		if (!sender_send)
			continue;

		client = sender_send->client;

		ck_rlock(&ci->lock);
		fd = client->fd;
		ck_runlock(&ci->lock);

		if (fd == -1) {
			LOGDEBUG("Discarding message sent to invalidated client");
			free(sender_send->buf);
			free(sender_send);
			continue;
		}
		FD_ZERO(&writefds);
		FD_SET(fd, &writefds);
		ret = select(fd + 1, NULL, &writefds, NULL, &timeout_tv);
		if (ret < 1) {
			LOGDEBUG("Client %d not ready for writes", client->id);

			/* Append it to the tail of the list */
			mutex_lock(&sender_lock);
			DL_APPEND(sender_sends, sender_send);
			mutex_unlock(&sender_lock);

			continue;
		}
		while (sender_send->len) {
			ret = send(fd, sender_send->buf + ofs, sender_send->len , 0);
			if (unlikely(ret < 0)) {
				if (interrupted())
					continue;
				LOGWARNING("Client id %d disconnected", client->id);
				invalidate_client(ckp, client);
				break;
			}
			ofs += ret;
			sender_send->len -= ret;
		}
		free(sender_send->buf);
		free(sender_send);
	}

	return NULL;
}

/* Send a client by id a heap allocated buffer, allowing this function to
 * free the ram. */
static void send_client(conn_instance_t *ci, int id, char *buf)
{
	sender_send_t *sender_send;
	client_instance_t *client;
	int fd = -1, len;

	if (unlikely(!buf)) {
		LOGWARNING("Connector send_client sent a null buffer");
		return;
	}
	len = strlen(buf);
	if (unlikely(!len)) {
		LOGWARNING("Connector send_client sent a zero length buffer");
		free(buf);
		return;
	}

	ck_rlock(&ci->lock);
	HASH_FIND_INT(clients, &id, client);
	if (likely(client))
		fd = client->fd;
	ck_runlock(&ci->lock);

	if (unlikely(fd == -1)) {
		if (client)
			LOGWARNING("Client id %d disconnected", id);
		else
			LOGWARNING("Connector failed to find client id %d", id);
		return;
	}

	sender_send = ckzalloc(sizeof(sender_send_t));
	sender_send->client = client;
	sender_send->buf = buf;
	sender_send->len = len;

	mutex_lock(&sender_lock);
	DL_APPEND(sender_sends, sender_send);
	pthread_cond_signal(&sender_cond);
	mutex_unlock(&sender_lock);
}

static int connector_loop(proc_instance_t *pi, conn_instance_t *ci)
{
	int sockd, client_id, ret = 0;
	unixsock_t *us = &pi->us;
	char *buf = NULL;
	json_t *json_msg;

retry:
	sockd = accept(us->sockd, NULL, NULL);
	if (sockd < 0) {
		if (interrupted())
			goto retry;
		LOGERR("Failed to accept on connector socket, retrying in 5s");
		sleep(5);
		goto retry;
	}

	buf = recv_unix_msg(sockd);
	close(sockd);
	if (!buf) {
		LOGWARNING("Failed to get message in connector_loop");
		goto retry;
	}
	LOGDEBUG("Connector received message: %s", buf);
	if (!strncasecmp(buf, "shutdown", 8))
		goto out;
	json_msg = json_loads(buf, 0, NULL);
	if (unlikely(!json_msg)) {
		LOGWARNING("Invalid json message: %s", buf);
		goto retry;
	}

	/* Extract the client id from the json message and remove its entry */
	client_id = json_integer_value(json_object_get(json_msg, "client_id"));
	json_object_del(json_msg, "client_id");
	dealloc(buf);
	buf = json_dumps(json_msg, 0);
	realloc_strcat(&buf, "\n");
	send_client(ci, client_id, buf);
	json_decref(json_msg);

	goto retry;
out:
	return ret;
}

int connector(proc_instance_t *pi)
{
	pthread_t pth_sender, pth_acceptor, pth_receiver;
	char *url = NULL, *port = NULL;
	ckpool_t *ckp = pi->ckp;
	int sockd, ret = 0;
	conn_instance_t ci;
	const int on = 1;
	int tries = 0;

	if (ckp->serverurl) {
		if (!extract_sockaddr(ckp->serverurl, &url, &port)) {
			LOGWARNING("Failed to extract server address from %s", ckp->serverurl);
			ret = 1;
			goto out;
		}
		do {
			sockd = bind_socket(url, port);
			if (sockd > 0)
				break;
			LOGWARNING("Connector failed to bind to socket, retrying in 5s");
			sleep(5);
		} while (++tries < 25);

		dealloc(url);
		dealloc(port);
		if (sockd < 0) {
			LOGERR("Connector failed to bind to socket for 2 minutes");
			ret = 1;
			goto out;
		}
	} else {
		struct sockaddr_in serv_addr;

		sockd = socket(AF_INET, SOCK_STREAM, 0);
		if (sockd < 0) {
			LOGERR("Connector failed to open socket");
			ret = 1;
			goto out;
		}
		setsockopt(sockd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
		memset(&serv_addr, 0, sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
		serv_addr.sin_port = htons(3333);
		do {
			ret = bind(sockd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
			if (!ret)
				break;
			LOGWARNING("Connector failed to bind to socket, retrying in 5s");
			sleep(5);
		} while (++tries < 25);
		if (ret < 0) {
			LOGERR("Connector failed to bind to socket for 2 minutes");
			close(sockd);
			goto out;
		}
	}
	if (tries)
		LOGWARNING("Connector successfully bound to socket");

	ret = listen(sockd, 10);
	if (ret < 0) {
		LOGERR("Connector failed to listen on socket");
		close(sockd);
		goto out;
	}

	cklock_init(&ci.lock);
	memset(&ci, 0, sizeof(ci));
	ci.pi = pi;
	ci.serverfd = sockd;
	ci.nfds = 0;
	mutex_init(&sender_lock);
	cond_init(&sender_cond);
	create_pthread(&pth_sender, sender, &ci);
	create_pthread(&pth_acceptor, acceptor, &ci);
	create_pthread(&pth_receiver, receiver, &ci);

	ret = connector_loop(pi, &ci);

	//join_pthread(pth_acceptor);
out:
	LOGINFO("%s connector exiting with return code %d", ckp->name, ret);
	if (ret) {
		send_proc(&ckp->main, "shutdown");
		sleep(1);
	}
	exit(ret);
}
