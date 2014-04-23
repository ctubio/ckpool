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
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>

#include "ckpool.h"
#include "libckpool.h"

struct connector_instance {
	cklock_t lock;
	proc_instance_t *pi;
	int serverfd;
	int clients;
};

typedef struct connector_instance conn_instance_t;

struct client_instance {
	connsock_t cs;
	struct sockaddr address;
	socklen_t address_len;
};

typedef struct client_instance client_instance_t;

void *acceptor(void *arg)
{
	conn_instance_t *ci = (conn_instance_t *)arg;
	proc_instance_t *pi = ci->pi;
	unixsock_t *us = &pi->us;
	ckpool_t *ckp = pi->ckp;
	client_instance_t cli;

	rename_proc("acceptor");

retry:
	cli.address_len = sizeof(cli.address);
	cli.cs.fd = accept(ci->serverfd, &cli.address, &cli.address_len);
	if (unlikely(cli.cs.fd < 0)) {
		if (interrupted())
			goto retry;
		LOGERR("Failed to accept on socket %d in acceptor", ci->serverfd);
		goto out;
	}
	/* Do something here with the client instance instead of just reading
	 * a line. */
	if (read_socket_line(&cli.cs))
		LOGWARNING("Received %s", cli.cs.buf);
	dealloc(cli.cs.buf);
	close(cli.cs.fd);
	goto retry;
out:
	return NULL;
}

int connector(proc_instance_t *pi)
{
	char *url = NULL, *port = NULL;
	ckpool_t *ckp = pi->ckp;
	pthread_t pth_acceptor;
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
	ci.pi = pi;
	ci.serverfd = sockd;
	ci.clients = 0;
	create_pthread(&pth_acceptor, acceptor, &ci);

	join_pthread(pth_acceptor);
out:
	LOGINFO("%s connector exiting with return code %d", ckp->name, ret);
	if (ret) {
		send_proc(&ckp->main, "shutdown");
		sleep(1);
	}
	return ret;
}
