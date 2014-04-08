/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

/* This file should contain all exported functions of libckpool */

#ifndef LIBCKPOOL_H
#define LIBCKPOOL_H

#include <stdbool.h>

void keep_sockalive(int fd);
void align_len(size_t *len);
void __bin2hex(uchar *s, const uchar *p, size_t len);
void *bin2hex(const uchar *p, size_t len);
bool hex2bin(uchar *p, const uchar *hexstr, size_t len);
void b58tobin(uchar *b58bin, const uchar *b58);
void address_to_pubkeyhash(uchar *pkh, const uchar *addr);
int ser_number(uchar *s, int32_t val);

#endif /* LIBCKPOOL_H */
