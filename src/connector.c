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

int connector(proc_instance_t *pi)
{
	char *url = NULL, *port = NULL;
	ckpool_t *ckp = pi->ckp;
	int sockd, ret = 0;

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
			ret = 1;
			close(sockd);
			goto out;
		}
	}

out:
	LOGINFO("%s connector exiting with return code %d", ckp->name, ret);
	if (ret) {
		send_proc(&ckp->main, "shutdown");
		sleep(1);
	}
	return ret;
}
