/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include "ckpool.h"
#include "libckpool.h"
#include "generator.h"

int generator(proc_instance_t *pi)
{
	LOGDEBUG("Process %s started", pi->processname);

	return 0;
}