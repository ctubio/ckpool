/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "libckpool.h"

int main(int argc, char **argv)
{
	char *name = NULL, *socket_dir = NULL, *buf = NULL;
	bool proxy = false;
	int c;

	while ((c = getopt(argc, argv, "n:s:p")) != -1) {
		switch(c) {
			case 'n':
				name = strdup(optarg);
				break;
			case 's':
				socket_dir = strdup(optarg);
				break;
			case 'p':
				proxy = true;
				break;
		}
	}
	if (!socket_dir)
		socket_dir = strdup("/tmp");
	trail_slash(&socket_dir);
	if (!name) {
		if (proxy)
			name = strdup("ckproxy");
		else
			name = strdup("ckpool");
	}
	realloc_strcat(&socket_dir, name);
	dealloc(name);
	trail_slash(&socket_dir);
	realloc_strcat(&socket_dir, "listener");

	while (42) {
		int sockd, len;
		size_t n;

		dealloc(buf);
		len = getline(&buf, &n, stdin);
		if (len == -1) {
			LOGERR("Failed to get a valid line");
			break;
		}
		len = strlen(buf);
		if (len < 2) {
			LOGERR("No message");
			continue;
		}
		buf[len - 1] = '\0'; // Strip /n
		LOGDEBUG("Got message: %s", buf);

		sockd = open_unix_client(socket_dir);
		if (sockd < 0) {
			LOGERR("Failed to open socket: %s", socket_dir);
			break;
		}
		if (!send_unix_msg(sockd, buf)) {
			LOGERR("Failed to end unix msg: %s", buf);
			break;
		}
		dealloc(buf);
		buf = recv_unix_msg(sockd);
		close(sockd);
		if (!buf) {
			LOGERR("Received empty message");
			continue;
		}
		LOGNOTICE("Received response: %s", buf);
	}

	dealloc(buf);
	dealloc(socket_dir);
	return 0;
}
