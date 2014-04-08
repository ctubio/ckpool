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
void address_to_pubkeytxn(uchar *pkh, const uchar *addr);
int ser_number(uchar *s, int32_t val);
bool fulltest(const uchar *hash, const uchar *target);
void copy_tv(tv_t *dest, const tv_t *src);
void ts_to_tv(tv_t *val, const ts_t *spec);
void tv_to_ts(ts_t *spec, const tv_t *val);
void us_to_tv(tv_t *val, int64_t us);
void us_to_ts(ts_t *spec, int64_t us);
void ms_to_ts(ts_t *spec, int64_t ms);
void ms_to_tv(tv_t *val, int64_t ms);
void tv_time(tv_t *tv);
void ts_time(ts_t *ts);
void cksleep_prepare_r(ts_t *ts);
void nanosleep_abstime(ts_t *ts_end);
void timeraddspec(ts_t *a, const ts_t *b);
void cksleep_ms_r(ts_t *ts_start, int ms);
void cksleep_us_r(ts_t *ts_start, int64_t us);
void cksleep_ms(int ms);
void cksleep_us(int64_t us);
double us_tvdiff(tv_t *end, tv_t *start);
int ms_tvdiff(tv_t *end, tv_t *start);
double tvdiff(tv_t *end, tv_t *start);

#endif /* LIBCKPOOL_H */
