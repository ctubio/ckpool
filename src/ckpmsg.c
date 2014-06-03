/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include "config.h"

#include <string.h>
#include <unistd.h>

#include "libckpool.h"

int main(int argc, char **argv)
{
	char *name = NULL, *socket_dir = NULL;
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
	free(name);
	trail_slash(&socket_dir);
	realloc_strcat(&socket_dir, "listener");

	free(socket_dir);
	return 0;
}
