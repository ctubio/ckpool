/*
 * Copyright 2011-2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#ifndef BITCOIN_H
#define BITCOIN_H

bool validate_address(connsock_t *cs, const char *address);
bool gen_gbtbase(connsock_t *cs, gbtbase_t *gbt);

#endif /* BITCOIN_H */
