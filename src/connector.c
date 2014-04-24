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

/* Accepts incoming connections to the server socket and generates client
 * instances */
void *acceptor(void *arg)
{
	conn_instance_t *ci = (conn_instance_t *)arg;
	client_instance_t cli, *client;
	int fd;

	rename_proc("acceptor");

retry:
	cli.address_len = sizeof(cli.address);
	fd = accept(ci->serverfd, &cli.address, &cli.address_len);
	if (unlikely(fd < 0)) {
		if (interrupted())
			goto retry;
		LOGERR("Failed to accept on socket %d in acceptor", ci->serverfd);
		goto out;
	}

	LOGINFO("Connected new client %d on socket %d", ci->nfds, fd);

	client = ckzalloc(sizeof(client_instance_t));
	memcpy(client, &cli, sizeof(client_instance_t));
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
static void invalidate_client(client_instance_t *client)
{
	close(client->fd);
	client->fd = -1;
}

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
		/* If we're reading on the first pass, there is supposed to be
		 * data waiting for us according to poll() so no data implies
		 * a lost connection. On repeat reads may have just read all
		 * the data */
		if (moredata)
			return;

		LOGINFO("Client fd %d disconnected", client->fd);
		invalidate_client(client);
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
			invalidate_client(client);
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
		invalidate_client(client);
		return;
	}
	memcpy(msg, client->buf, buflen);
	msg[buflen] = 0;
	client->bufofs -= buflen;
	memmove(client->buf, client->buf + buflen, client->bufofs);
	val = json_loads(msg, 0, NULL);
	if (!val)
		LOGWARNING("Client fd %d sent invalid json message %s", client->fd, msg);
	else {
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
	client_instance_t *client, *tmp, *tmpa;
	struct pollfd fds[65536];
	int ret, nfds, i;

	rename_proc("receiver");

retry:
	memset(fds, 0, sizeof(fds));
	nfds = 0;

	ck_rlock(&ci->lock);
	HASH_ITER(hh, clients, client, tmp) {
		/* Invalid client */
		if (client->fd == -1)
			continue;
		fds[nfds].fd = client->fd;
		fds[nfds].events = POLLIN;
		nfds++;
	}
	ck_runlock(&ci->lock);

	ret = poll(fds, nfds, 1);
	if (ret < 0) {
		if (interrupted())
			goto retry;
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
		HASH_ITER(hh, clients, tmp, tmpa) {
			if (tmp->fd == fds[i].fd) {
				client = tmp;
				break;
			}
		}
		ck_runlock(&ci->lock);

		if (!client)
			LOGWARNING("Failed to find client with fd %d in hashtable!", fds[i].fd);
		else
			parse_client_msg(ci, client);

		if (--ret < 1)
			break;
	}
	goto retry;

out:
	return NULL;
}

int connector(proc_instance_t *pi)
{
	pthread_t pth_acceptor, pth_receiver;
	char *url = NULL, *port = NULL;
	ckpool_t *ckp = pi->ckp;
	int sockd, ret = 0;
	conn_instance_t ci;

	if (ckp->serverurl) {
		if (!extract_sockaddr(ckp->serverurl, &url, &port)) {
			LOGWARNING("Failed to extract server address from %s", ckp->serverurl);
			ret = 1;
			goto out;
		}
		sockd = bind_socket(url, port);
		dealloc(url);
		dealloc(port);
		if (sockd < 0) {
			LOGERR("Connector failed to bind to socket");
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
		memset(&serv_addr, 0, sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
		serv_addr.sin_port = htons(3333);
		ret = bind(sockd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
		if (ret < 0) {
			LOGERR("Connector failed to bind to socket");
			close(sockd);
			goto out;
		}
	}

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
	create_pthread(&pth_acceptor, acceptor, &ci);
	create_pthread(&pth_receiver, receiver, &ci);

	join_pthread(pth_acceptor);
	ret = 1;
out:
	LOGINFO("%s connector exiting with return code %d", ckp->name, ret);
	if (ret) {
		send_proc(&ckp->main, "shutdown");
		sleep(1);
	}
	return ret;
}
