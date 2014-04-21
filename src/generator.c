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

#include "ckpool.h"
#include "libckpool.h"
#include "generator.h"

int generator(proc_instance_t *pi)
{
	ckpool_t *ckp = pi->ckp;
	char *userpass = NULL;
	connsock_t cs;
	int ret = 0;

	memset(&cs, 0, sizeof(cs));
	if (!ckp->btcdurl)
		ckp->btcdurl = strdup("localhost:8332");
	if (!ckp->btcdauth)
		ckp->btcdauth = strdup("user");
	if (!ckp->btcdpass)
		ckp->btcdpass = strdup("pass");
	if (!extract_sockaddr(ckp->btcdurl, &cs.url, &cs.port)) {
		LOGWARNING("Failed to extract address from %s", ckp->btcdurl);
		ret = 1;
		goto out;
	}
	userpass = strdup(ckp->btcdauth);
	realloc_strcat(&userpass, ckp->btcdpass);
	cs.auth = http_base64(userpass);
	if (!cs.auth) {
		LOGWARNING("Failed to create base64 auth from %s", userpass);
		ret = 1;
		goto out;
	}
	dealloc(userpass);
	cs.fd = connect_socket(cs.url, cs.port);
	if (cs.fd < 0) {
		LOGWARNING("Failed to connect socket to %s:%s", cs.url, cs.port);
		ret = 1;
		goto out;
	}

out:
	/* Clean up here */
	dealloc(cs.url);
	dealloc(cs.port);
	dealloc(userpass);

	LOGINFO("%s generator exiting with return code %d", ckp->name, ret);
	return ret;
}
