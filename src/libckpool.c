/*
 * Copyright 2011-2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include "config.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "ckpool.h"
#include "libckpool.h"

void keep_sockalive(int fd)
{
	const int tcp_one = 1;
	const int tcp_keepidle = 45;
	const int tcp_keepintvl = 30;
	int flags = fcntl(fd, F_GETFL, 0);

	fcntl(fd, F_SETFL, O_NONBLOCK | flags);

	setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (const void *)&tcp_one, sizeof(tcp_one));
	setsockopt(fd, SOL_TCP, TCP_NODELAY, (const void *)&tcp_one, sizeof(tcp_one));
	setsockopt(fd, SOL_TCP, TCP_KEEPCNT, &tcp_one, sizeof(tcp_one));
	setsockopt(fd, SOL_TCP, TCP_KEEPIDLE, &tcp_keepidle, sizeof(tcp_keepidle));
	setsockopt(fd, SOL_TCP, TCP_KEEPINTVL, &tcp_keepintvl, sizeof(tcp_keepintvl));
}
