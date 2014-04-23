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

struct connector_instance {
	cklock_t lock;
	proc_instance_t *pi;
	int serverfd;
	int nfds;
	struct pollfd fds[65536];
};

typedef struct connector_instance conn_instance_t;

struct client_instance {
	struct sockaddr address;
	socklen_t address_len;
};

typedef struct client_instance client_instance_t;

/* Accepts incoming connections to the server socket and generates client
 * instances */
void *acceptor(void *arg)
{
	conn_instance_t *ci = (conn_instance_t *)arg;
	client_instance_t cli;
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

	ck_wlock(&ci->lock);
	ci->fds[ci->nfds].fd = fd;
	ci->fds[ci->nfds].events = POLLIN;
	ci->nfds++;
	ck_wunlock(&ci->lock);


	goto retry;
out:
	return NULL;
}

/* Waits on fds ready to read on from the list stored in conn_instance and
 * handles the incoming messages */
void *receiver(void *arg)
{
	conn_instance_t *ci = (conn_instance_t *)arg;
	struct pollfd fds[65536];
	int ret, nfds, i;
	connsock_t cs;

	rename_proc("receiver");

	memset(&cs, 0, sizeof(cs));
retry:
	dealloc(cs.buf);

	ck_rlock(&ci->lock);
	memcpy(&fds, ci->fds, sizeof(fds));
	nfds = ci->nfds;
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
		cs.fd = fds[i].fd;
		if (read_socket_line(&cs)) {
			LOGWARNING("Received %s", cs.buf);
			dealloc(cs.buf);
		}
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
