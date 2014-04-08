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
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

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

void noblock_socket(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);

	fcntl(fd, F_SETFL, O_NONBLOCK | flags);
}

void block_socket(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);

	fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}

/* Align a size_t to 4 byte boundaries for fussy arches */
void align_len(size_t *len)
{
	if (*len % 4)
		*len += 4 - (*len % 4);
}

/* Adequate size s==len*2 + 1 must be alloced to use this variant */
void __bin2hex(uchar *s, const uchar *p, size_t len)
{
	static const char hex[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	int i;

	for (i = 0; i < (int)len; i++) {
		*s++ = hex[p[i] >> 4];
		*s++ = hex[p[i] & 0xF];
	}
	*s++ = '\0';
}

/* Returns a malloced array string of a binary value of arbitrary length. The
 * array is rounded up to a 4 byte size to appease architectures that need
 * aligned array  sizes */
void *bin2hex(const uchar *p, size_t len)
{
	size_t slen;
	uchar *s;

	slen = len * 2 + 1;
	align_len(&slen);
	s = calloc(slen, 1);
	if (likely(s))
		__bin2hex(s, p, len);

	/* Returns NULL if calloc failed. */
	return s;
}

static const int hex2bin_tbl[256] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	 0,  1,  2,  3,  4,  5,  6,  7,  8,  9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

/* Does the reverse of bin2hex but does not allocate any ram */
bool hex2bin(uchar *p, const uchar *hexstr, size_t len)
{
	int nibble1, nibble2;
	uchar idx;
	bool ret = false;

	while (*hexstr && len) {
		if (unlikely(!hexstr[1]))
			return ret;

		idx = *hexstr++;
		nibble1 = hex2bin_tbl[idx];
		idx = *hexstr++;
		nibble2 = hex2bin_tbl[idx];

		if (unlikely((nibble1 < 0) || (nibble2 < 0)))
			return ret;

		*p++ = (((uchar)nibble1) << 4) | ((uchar)nibble2);
		--len;
	}

	if (likely(len == 0 && *hexstr == 0))
		ret = true;
	return ret;
}

static const int b58tobin_tbl[] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1,  0,  1,  2,  3,  4,  5,  6,  7,  8, -1, -1, -1, -1, -1, -1,
	-1,  9, 10, 11, 12, 13, 14, 15, 16, -1, 17, 18, 19, 20, 21, -1,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, -1, -1, -1, -1, -1,
	-1, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, -1, 44, 45, 46,
	47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57
};

/* b58bin should always be at least 25 bytes long and already checked to be
 * valid. */
void b58tobin(uchar *b58bin, const uchar *b58)
{
	uint32_t c, bin32[7];
	int len, i, j;
	uint64_t t;

	memset(bin32, 0, 7 * sizeof(uint32_t));
	len = strlen((const char *)b58);
	for (i = 0; i < len; i++) {
		c = b58[i];
		c = b58tobin_tbl[c];
		for (j = 6; j >= 0; j--) {
			t = ((uint64_t)bin32[j]) * 58 + c;
			c = (t & 0x3f00000000ull) >> 32;
			bin32[j] = t & 0xffffffffull;
		}
	}
	*(b58bin++) = bin32[0] & 0xff;
	for (i = 1; i < 7; i++) {
		*((uint32_t *)b58bin) = htobe32(bin32[i]);
		b58bin += sizeof(uint32_t);
	}
}

void address_to_pubkeytxn(uchar *pkh, const uchar *addr)
{
	uchar b58bin[25];

	memset(b58bin, 0, 25);
	b58tobin(b58bin, addr);
	pkh[0] = 0x76;
	pkh[1] = 0xa9;
	pkh[2] = 0x14;
	memcpy(&pkh[3], &b58bin[1], 20);
	pkh[23] = 0x88;
	pkh[24] = 0xac;
}

/*  For encoding nHeight into coinbase, return how many bytes were used */
int ser_number(uchar *s, int32_t val)
{
	int32_t *i32 = (int32_t *)&s[1];
	int len;

	if (val < 128)
		len = 1;
	else if (val < 16512)
		len = 2;
	else if (val < 2113664)
		len = 3;
	else
		len = 4;
	*i32 = htole32(val);
	s[0] = len++;
	return len;
}

/* For testing a le encoded 256 byte hash against a target */
bool fulltest(const uchar *hash, const uchar *target)
{
	uint32_t *hash32 = (uint32_t *)hash;
	uint32_t *target32 = (uint32_t *)target;
	bool ret = true;
	int i;

	for (i = 28 / 4; i >= 0; i--) {
		uint32_t h32tmp = le32toh(hash32[i]);
		uint32_t t32tmp = le32toh(target32[i]);

		if (h32tmp > t32tmp) {
			ret = false;
			break;
		}
		if (h32tmp < t32tmp) {
			ret = true;
			break;
		}
	}
	return ret;
}

void copy_tv(tv_t *dest, const tv_t *src)
{
	memcpy(dest, src, sizeof(tv_t));
}

void ts_to_tv(tv_t *val, const ts_t *spec)
{
	val->tv_sec = spec->tv_sec;
	val->tv_usec = spec->tv_nsec / 1000;
}

void tv_to_ts(ts_t *spec, const tv_t *val)
{
	spec->tv_sec = val->tv_sec;
	spec->tv_nsec = val->tv_usec * 1000;
}

void us_to_tv(tv_t *val, int64_t us)
{
	lldiv_t tvdiv = lldiv(us, 1000000);

	val->tv_sec = tvdiv.quot;
	val->tv_usec = tvdiv.rem;
}

void us_to_ts(ts_t *spec, int64_t us)
{
	lldiv_t tvdiv = lldiv(us, 1000000);

	spec->tv_sec = tvdiv.quot;
	spec->tv_nsec = tvdiv.rem * 1000;
}

void ms_to_ts(ts_t *spec, int64_t ms)
{
	lldiv_t tvdiv = lldiv(ms, 1000);

	spec->tv_sec = tvdiv.quot;
	spec->tv_nsec = tvdiv.rem * 1000000;
}

void ms_to_tv(tv_t *val, int64_t ms)
{
	lldiv_t tvdiv = lldiv(ms, 1000);

	val->tv_sec = tvdiv.quot;
	val->tv_usec = tvdiv.rem * 1000;
}

void tv_time(tv_t *tv)
{
	gettimeofday(tv, NULL);
}

void ts_time(ts_t *ts)
{
	clock_gettime(CLOCK_MONOTONIC, ts);
}

void cksleep_prepare_r(ts_t *ts)
{
	ts_time(ts);
}

void nanosleep_abstime(ts_t *ts_end)
{
	int ret;

	do {
		ret = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, ts_end, NULL);
	} while (ret == EINTR);
}

void timeraddspec(ts_t *a, const ts_t *b)
{
	a->tv_sec += b->tv_sec;
	a->tv_nsec += b->tv_nsec;
	if (a->tv_nsec >= 1000000000) {
		a->tv_nsec -= 1000000000;
		a->tv_sec++;
	}
}

/* Reentrant version of cksleep functions allow start time to be set separately
 * from the beginning of the actual sleep, allowing scheduling delays to be
 * counted in the sleep. */
void cksleep_ms_r(ts_t *ts_start, int ms)
{
	ts_t ts_end;

	ms_to_ts(&ts_end, ms);
	timeraddspec(&ts_end, ts_start);
	nanosleep_abstime(&ts_end);
}

void cksleep_us_r(ts_t *ts_start, int64_t us)
{
	ts_t ts_end;

	us_to_ts(&ts_end, us);
	timeraddspec(&ts_end, ts_start);
	nanosleep_abstime(&ts_end);
}

void cksleep_ms(int ms)
{
	ts_t ts_start;

	cksleep_prepare_r(&ts_start);
	cksleep_ms_r(&ts_start, ms);
}

void cksleep_us(int64_t us)
{
	ts_t ts_start;

	cksleep_prepare_r(&ts_start);
	cksleep_us_r(&ts_start, us);
}

/* Returns the microseconds difference between end and start times as a double */
double us_tvdiff(tv_t *end, tv_t *start)
{
	/* Sanity check. We should only be using this for small differences so
	 * limit the max to 60 seconds. */
	if (unlikely(end->tv_sec - start->tv_sec > 60))
		return 60000000;
	return (end->tv_sec - start->tv_sec) * 1000000 + (end->tv_usec - start->tv_usec);
}

/* Returns the milliseconds difference between end and start times */
int ms_tvdiff(tv_t *end, tv_t *start)
{
	/* Like us_tdiff, limit to 1 hour. */
	if (unlikely(end->tv_sec - start->tv_sec > 3600))
		return 3600000;
	return (end->tv_sec - start->tv_sec) * 1000 + (end->tv_usec - start->tv_usec) / 1000;
}

/* Returns the seconds difference between end and start times as a double */
double tvdiff(tv_t *end, tv_t *start)
{
	return end->tv_sec - start->tv_sec + (end->tv_usec - start->tv_usec) / 1000000.0;
}
