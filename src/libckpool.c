/*
 * Copyright 2014 Con Kolivas
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#include "config.h"

#include <sys/types.h>
#include <sys/socket.h>
#ifdef HAVE_LINUX_UN_H
#include <linux/un.h>
#else
#include <sys/un.h>
#endif
#include <sys/prctl.h>
#include <sys/stat.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <math.h>

#include "ckpool.h"
#include "libckpool.h"
#include "sha2.h"

#ifndef UNIX_PATH_MAX
#define UNIX_PATH_MAX 108
#endif

void rename_proc(const char *name)
{
	char buf[16];

	snprintf(buf, 15, "ckp@%s", name);
	buf[15] = '\0';
	prctl(PR_SET_NAME, buf, 0, 0, 0);
}

void create_pthread(pthread_t *thread, void *(*start_routine)(void *), void *arg)
{
	int ret = pthread_create(thread, NULL, start_routine,  arg);

	if (unlikely(ret))
		quit(1, "Failed to pthread_create");
}

void join_pthread(pthread_t thread)
{
	int ret = pthread_join(thread, NULL);

	if (unlikely(ret))
		quit(1, "Failed to pthread_join");
}


/* Place holders for when we add lock debugging */
#define GETLOCK(_lock, _file, _func, _line)
#define GOTLOCK(_lock, _file, _func, _line)
#define TRYLOCK(_lock, _file, _func, _line)
#define DIDLOCK(_ret, _lock, _file, _func, _line)
#define GUNLOCK(_lock, _file, _func, _line)
#define INITLOCK(_typ, _lock, _file, _func, _line)

void _mutex_lock(pthread_mutex_t *lock, const char *file, const char *func, const int line)
{
	GETLOCK(lock, file, func, line);
	if (unlikely(pthread_mutex_lock(lock)))
		quitfrom(1, file, func, line, "WTF MUTEX ERROR ON LOCK!");
	GOTLOCK(lock, file, func, line);
}

void _mutex_unlock_noyield(pthread_mutex_t *lock, const char *file, const char *func, const int line)
{
	if (unlikely(pthread_mutex_unlock(lock)))
		quitfrom(1, file, func, line, "WTF MUTEX ERROR ON UNLOCK!");
	GUNLOCK(lock, file, func, line);
}

void _mutex_unlock(pthread_mutex_t *lock, const char *file, const char *func, const int line)
{
	_mutex_unlock_noyield(lock, file, func, line);
	sched_yield();
}

int _mutex_trylock(pthread_mutex_t *lock, __maybe_unused const char *file, __maybe_unused const char *func, __maybe_unused const int line)
{
	TRYLOCK(lock, file, func, line);
	int ret = pthread_mutex_trylock(lock);
	DIDLOCK(ret, lock, file, func, line);
	return ret;
}

void _wr_lock(pthread_rwlock_t *lock, const char *file, const char *func, const int line)
{
	GETLOCK(lock, file, func, line);
	if (unlikely(pthread_rwlock_wrlock(lock)))
		quitfrom(1, file, func, line, "WTF WRLOCK ERROR ON LOCK!");
	GOTLOCK(lock, file, func, line);
}

int _wr_trylock(pthread_rwlock_t *lock, __maybe_unused const char *file, __maybe_unused const char *func, __maybe_unused const int line)
{
	TRYLOCK(lock, file, func, line);
	int ret = pthread_rwlock_trywrlock(lock);
	DIDLOCK(ret, lock, file, func, line);
	return ret;
}

void _rd_lock(pthread_rwlock_t *lock, const char *file, const char *func, const int line)
{
	GETLOCK(lock, file, func, line);
	if (unlikely(pthread_rwlock_rdlock(lock)))
		quitfrom(1, file, func, line, "WTF RDLOCK ERROR ON LOCK!");
	GOTLOCK(lock, file, func, line);
}

void _rw_unlock(pthread_rwlock_t *lock, const char *file, const char *func, const int line)
{
	if (unlikely(pthread_rwlock_unlock(lock)))
		quitfrom(1, file, func, line, "WTF RWLOCK ERROR ON UNLOCK!");
	GUNLOCK(lock, file, func, line);
}

void _rd_unlock_noyield(pthread_rwlock_t *lock, const char *file, const char *func, const int line)
{
	_rw_unlock(lock, file, func, line);
}

void _wr_unlock_noyield(pthread_rwlock_t *lock, const char *file, const char *func, const int line)
{
	_rw_unlock(lock, file, func, line);
}

void _rd_unlock(pthread_rwlock_t *lock, const char *file, const char *func, const int line)
{
	_rw_unlock(lock, file, func, line);
	sched_yield();
}

void _wr_unlock(pthread_rwlock_t *lock, const char *file, const char *func, const int line)
{
	_rw_unlock(lock, file, func, line);
	sched_yield();
}

void _mutex_init(pthread_mutex_t *lock, const char *file, const char *func, const int line)
{
	if (unlikely(pthread_mutex_init(lock, NULL)))
		quitfrom(1, file, func, line, "Failed to pthread_mutex_init");
	INITLOCK(lock, CGLOCK_MUTEX, file, func, line);
}

void mutex_destroy(pthread_mutex_t *lock)
{
	/* Ignore return code. This only invalidates the mutex on linux but
	 * releases resources on windows. */
	pthread_mutex_destroy(lock);
}

void _rwlock_init(pthread_rwlock_t *lock, const char *file, const char *func, const int line)
{
	if (unlikely(pthread_rwlock_init(lock, NULL)))
		quitfrom(1, file, func, line, "Failed to pthread_rwlock_init");
	INITLOCK(lock, CGLOCK_RW, file, func, line);
}

void rwlock_destroy(pthread_rwlock_t *lock)
{
	pthread_rwlock_destroy(lock);
}

void _cond_init(pthread_cond_t *cond, const char *file, const char *func, const int line)
{
	if (unlikely(pthread_cond_init(cond, NULL)))
		quitfrom(1, file, func, line, "Failed to pthread_cond_init!");
}

void _cklock_init(cklock_t *lock, const char *file, const char *func, const int line)
{
	_mutex_init(&lock->mutex, file, func, line);
	_rwlock_init(&lock->rwlock, file, func, line);
}

void cklock_destroy(cklock_t *lock)
{
	rwlock_destroy(&lock->rwlock);
	mutex_destroy(&lock->mutex);
}

/* Read lock variant of cklock. Cannot be promoted. */
void _ck_rlock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_mutex_lock(&lock->mutex, file, func, line);
	_rd_lock(&lock->rwlock, file, func, line);
	_mutex_unlock_noyield(&lock->mutex, file, func, line);
}

/* Intermediate variant of cklock - behaves as a read lock but can be promoted
 * to a write lock or demoted to read lock. */
void _ck_ilock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_mutex_lock(&lock->mutex, file, func, line);
}

/* Unlock intermediate variant without changing to read or write version */
void _ck_uilock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_mutex_unlock(&lock->mutex, file, func, line);
}

/* Upgrade intermediate variant to a write lock */
void _ck_ulock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_wr_lock(&lock->rwlock, file, func, line);
}

/* Write lock variant of cklock */
void _ck_wlock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_mutex_lock(&lock->mutex, file, func, line);
	_wr_lock(&lock->rwlock, file, func, line);
}

/* Downgrade write variant to a read lock */
void _ck_dwlock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_wr_unlock_noyield(&lock->rwlock, file, func, line);
	_rd_lock(&lock->rwlock, file, func, line);
	_mutex_unlock_noyield(&lock->mutex, file, func, line);
}

/* Demote a write variant to an intermediate variant */
void _ck_dwilock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_wr_unlock(&lock->rwlock, file, func, line);
}

/* Downgrade intermediate variant to a read lock */
void _ck_dlock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_rd_lock(&lock->rwlock, file, func, line);
	_mutex_unlock_noyield(&lock->mutex, file, func, line);
}

void _ck_runlock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_rd_unlock(&lock->rwlock, file, func, line);
}

void _ck_wunlock(cklock_t *lock, const char *file, const char *func, const int line)
{
	_wr_unlock_noyield(&lock->rwlock, file, func, line);
	_mutex_unlock(&lock->mutex, file, func, line);
}


bool extract_sockaddr(char *url, char **sockaddr_url, char **sockaddr_port)
{
	char *url_begin, *url_end, *ipv6_begin, *ipv6_end, *port_start = NULL;
	int url_len, port_len = 0;
	char *url_address, *port;
	size_t hlen;

	if (!url) {
		LOGWARNING("Null length url string passed to extract_sockaddr");
		return false;
	}
	*sockaddr_url = url;
	url_begin = strstr(url, "//");
	if (!url_begin)
		url_begin = url;
	else
		url_begin += 2;

	/* Look for numeric ipv6 entries */
	ipv6_begin = strstr(url_begin, "[");
	ipv6_end = strstr(url_begin, "]");
	if (ipv6_begin && ipv6_end && ipv6_end > ipv6_begin)
		url_end = strstr(ipv6_end, ":");
	else
		url_end = strstr(url_begin, ":");
	if (url_end) {
		url_len = url_end - url_begin;
		port_len = strlen(url_begin) - url_len - 1;
		if (port_len < 1)
			return false;
		port_start = url_end + 1;
	} else
		url_len = strlen(url_begin);

	if (url_len < 1) {
		LOGWARNING("Null length URL passed to extract_sockaddr");
		return false;
	}

	hlen = url_len + 1;
	url_address = ckalloc(hlen);
	sprintf(url_address, "%.*s", url_len, url_begin);

	port = ckalloc(8);
	if (port_len) {
		char *slash;

		snprintf(port, 6, "%.*s", port_len, port_start);
		slash = strchr(port, '/');
		if (slash)
			*slash = '\0';
	} else
		strcpy(port, "80");

	*sockaddr_port = port;
	*sockaddr_url = url_address;

	return true;
}

void keep_sockalive(int fd)
{
	const int tcp_one = 1;
	const int tcp_keepidle = 45;
	const int tcp_keepintvl = 30;

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

int bind_socket(char *url, char *port)
{
	struct addrinfo servinfobase, *servinfo, hints, *p;
	int ret, sockd = -1;
	const int on = 1;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	servinfo = &servinfobase;

	if (getaddrinfo(url, port, &hints, &servinfo) != 0) {
		LOGWARNING("Failed to resolve (?wrong URL) %s:%s", url, port);
		goto out;
	}
	for (p = servinfo; p != NULL; p = p->ai_next) {
		sockd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
		if (sockd > 0)
			break;
	}
	if (sockd < 0) {
		LOGWARNING("Failed to open socket for %s:%s", url, port);
		goto out;
	}
	setsockopt(sockd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
	ret = bind(sockd, p->ai_addr, p->ai_addrlen);
	if (ret < 0) {
		LOGWARNING("Failed to bind socket for %s:%s", url, port);
		close(sockd);
		sockd = -1;
		goto out;
	}

out:
	return sockd;
}

int connect_socket(char *url, char *port)
{
	struct addrinfo servinfobase, *servinfo, hints, *p;
	int sockd = -1;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	servinfo = &servinfobase;

	if (getaddrinfo(url, port, &hints, &servinfo) != 0) {
		LOGWARNING("Failed to resolve (?wrong URL) %s:%s", url, port);
		goto out;
	}

	for (p = servinfo; p != NULL; p = p->ai_next) {
		sockd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
		if (sockd == -1) {
			LOGDEBUG("Failed socket");
			continue;
		}

		/* Iterate non blocking over entries returned by getaddrinfo
		 * to cope with round robin DNS entries, finding the first one
		 * we can connect to quickly. */
		noblock_socket(sockd);
		if (connect(sockd, p->ai_addr, p->ai_addrlen) == -1) {
			struct timeval tv_timeout = {1, 0};
			int selret;
			fd_set rw;

			if (!sock_connecting()) {
				close(sockd);
				LOGDEBUG("Failed sock connect");
				continue;
			}
retry:
			FD_ZERO(&rw);
			FD_SET(sockd, &rw);
			selret = select(sockd + 1, NULL, &rw, NULL, &tv_timeout);
			if  (selret > 0 && FD_ISSET(sockd, &rw)) {
				socklen_t len;
				int err, n;

				len = sizeof(err);
				n = getsockopt(sockd, SOL_SOCKET, SO_ERROR, (void *)&err, &len);
				if (!n && !err) {
					LOGDEBUG("Succeeded delayed connect");
					block_socket(sockd);
					break;
				}
			}
			if (selret < 0 && interrupted())
				goto retry;
			close(sockd);
			sockd = -1;
			LOGDEBUG("Select timeout/failed connect");
			continue;
		}
		LOGDEBUG("Succeeded immediate connect");
		if (sockd >= 0)
			block_socket(sockd);

		break;
	}
	if (p == NULL) {
		LOGNOTICE("Failed to connect to %s:%s", url, port);
		sockd = -1;
	}
	freeaddrinfo(servinfo);
out:
	return sockd;
}

int write_socket(int fd, const void *buf, size_t nbyte)
{
	tv_t tv_timeout = {1, 0};
	fd_set writefds;
	int ret;

retry:
	FD_ZERO(&writefds);
	FD_SET(fd, &writefds);
	ret = select(fd + 1, NULL, &writefds, NULL, &tv_timeout);
	if (ret < 0 && interrupted())
		goto retry;
	if (ret < 1) {
		if (!ret)
			LOGNOTICE("Select timed out in write_socket");
		else
			LOGERR("Select failed in write_socket");
		goto out;
	}
	ret = write(fd, buf, nbyte);
	if (ret < 0)
		LOGWARNING("Failed to write in write_socket");
out:
	return ret;
}

/* Peek in a socket, and then receive only one line at a time, allocing enough
 * memory in *buf */
int read_socket_line(connsock_t *cs)
{
	char readbuf[PAGESIZE], *eom = NULL;
	size_t buflen = 0, bufofs = 0;
	tv_t timeout = {5, 0};
	int ret, bufsiz;
	fd_set rd;

	dealloc(cs->buf);
retry:
	FD_ZERO(&rd);
	FD_SET(cs->fd, &rd);
	ret = select(cs->fd + 1, &rd, NULL, NULL, &timeout);
	if (ret < 0 && interrupted())
		goto retry;
	if (ret < 1) {
		if (!ret)
			LOGNOTICE("Select1 timed out in read_socket_line");
		else
			LOGERR("Select1 failed in read_socket_line");
		goto out;
	}
	bufsiz = PAGESIZE;
	readbuf[bufsiz - 1] = '\0';
	while (!eom) {
		int extralen;

		FD_ZERO(&rd);
		FD_SET(cs->fd, &rd);
		timeout.tv_sec = 1;
		timeout.tv_usec = 0;
		ret = select(cs->fd + 1, &rd, NULL, NULL, &timeout);
		if (ret < 0 && interrupted())
			continue;
		if (ret < 1) {
			if (!ret)
				LOGNOTICE("Select2 timed out in read_socket_line");
			else
				LOGERR("Select2 failed in read_socket_line");
			goto out;
		}
		ret = recv(cs->fd, readbuf, bufsiz - 2, MSG_PEEK);
		if (ret < 0) {
			LOGERR("Failed to recv in read_socket_line");
			goto out;
		}
		if (!ret)
			continue;
		eom = strchr(readbuf, '\n');
		if (eom)
			extralen = eom - readbuf + 1;
		else
			extralen = ret;
		buflen += extralen + 1;
		align_len(&buflen);
		cs->buf = realloc(cs->buf, buflen);
		if (unlikely(!cs->buf))
			quit(1, "Failed to alloc buf of %d bytes in read_socket_line", (int)buflen);
		ret = recv(cs->fd, cs->buf + bufofs, extralen, 0);
		if (ret < 0) {
			LOGERR("Failed to recv %d bytes in read_socket_line", (int)buflen);
			goto out;
		}
		bufofs += ret;
	}
	eom = cs->buf + bufofs;
	eom[0] = '\0';
	ret = bufofs + 1;
out:
	if (ret < 1)
		dealloc(cs->buf);
	return ret;
}

void empty_socket(int fd)
{
	int ret;

	do {
		char buf[PAGESIZE];
		tv_t timeout = {0, 0};
		fd_set rd;

		FD_ZERO(&rd);
		FD_SET(fd, &rd);
		ret = select(fd + 1, &rd, NULL, NULL, &timeout);
		if (ret < 0 && interrupted())
			continue;
		if (ret > 0) {
			ret = recv(fd, buf, PAGESIZE - 1, 0);
			buf[ret] = 0;
			LOGDEBUG("Discarding: %s", buf);
		}
	} while (ret > 0);
}

void close_unix_socket(const int sockd, const char *server_path)
{
	int ret;

	ret = close(sockd);
	if (unlikely(ret < 0))
		LOGERR("Failed to close sock %d %s", sockd, server_path);
	ret = unlink(server_path);
	if (unlikely(ret < 0))
		LOGERR("Failed to unlink %s", server_path);
}

int open_unix_server(const char *server_path)
{
	struct sockaddr_un serveraddr;
	int sockd = -1, len, ret;
	struct stat buf;

	if (likely(server_path)) {
		len = strlen(server_path);
		if (unlikely(len < 1 || len > UNIX_PATH_MAX)) {
			LOGERR("Invalid server path length %d in open_unix_server", len);
			goto out;
		}
	} else {
		LOGERR("Null passed as server_path to open_unix_server");
		goto out;
	}

	if (!stat(server_path, &buf)) {
		if ((buf.st_mode & S_IFMT) == S_IFSOCK) {
			ret = unlink(server_path);
			if (ret) {
				LOGERR("Unlink of %s failed in open_unix_server", server_path);
				goto out;
			}
			LOGDEBUG("Unlinked %s to recreate socket", server_path);
		} else {
			LOGWARNING("%s already exists and is not a socket, not removing",
				   server_path);
			goto out;
		}
	}

	sockd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (unlikely(sockd < 0)) {
		LOGERR("Failed to open socket in open_unix_server");
		goto out;
	}
	memset(&serveraddr, 0, sizeof(serveraddr));
	serveraddr.sun_family = AF_UNIX;
	strcpy(serveraddr.sun_path, server_path);

	ret = bind(sockd, (struct sockaddr *)&serveraddr, sizeof(serveraddr));
	if (unlikely(ret < 0)) {
		LOGERR("Failed to bind to socket in open_unix_server");
		close_unix_socket(sockd, server_path);
		sockd = -1;
		goto out;
	}

	ret = listen(sockd, 1);
	if (unlikely(ret < 0)) {
		LOGERR("Failed to listen to socket in open_unix_server");
		close_unix_socket(sockd, server_path);
		sockd = -1;
		goto out;
	}

	LOGDEBUG("Opened server path %s successfully on socket %d", server_path, sockd);
out:
	return sockd;
}

int open_unix_client(const char *server_path)
{
	struct sockaddr_un serveraddr;
	int sockd = -1, len, ret;

	if (likely(server_path)) {
		len = strlen(server_path);
		if (unlikely(len < 1 || len > UNIX_PATH_MAX)) {
			LOGERR("Invalid server path length %d in open_unix_client", len);
			goto out;
		}
	} else {
		LOGERR("Null passed as server_path to open_unix_client");
		goto out;
	}

	sockd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (unlikely(sockd < 0)) {
		LOGERR("Failed to open socket in open_unix_client");
		goto out;
	}
	memset(&serveraddr, 0, sizeof(serveraddr));
	serveraddr.sun_family = AF_UNIX;
	strcpy(serveraddr.sun_path, server_path);

	ret = connect(sockd, (struct sockaddr *)&serveraddr, sizeof(serveraddr));
	if (unlikely(ret < 0)) {
		LOGERR("Failed to bind to socket in open_unix_client");
		close(sockd);
		sockd = -1;
		goto out;
	}
out:
	return sockd;
}

/* Use a standard message across the unix sockets:
 * 4 byte length of message as little endian encoded uint32_t followed by the
 * string. Return NULL in case of failure. */
char *recv_unix_msg(int sockd)
{
	tv_t tv_timeout = {60, 0};
	char *buf = NULL;
	uint32_t msglen;
	fd_set readfs;
	int ret, ofs;

	FD_ZERO(&readfs);
	FD_SET(sockd, &readfs);
	ret = select(sockd + 1, &readfs, NULL, NULL, &tv_timeout);
	if (ret < 1) {
		LOGERR("Select1 failed in recv_unix_msg");
		goto out;
	}
	/* Get message length */
	ret = read(sockd, &msglen, 4);
	if (ret < 4) {
		LOGERR("Failed to read 4 byte length in recv_unix_msg");
		goto out;
	}
	msglen = le32toh(msglen);
	if (unlikely(msglen < 1)) {
		LOGWARNING("Invalid message length zero sent to recv_unix_msg");
		goto out;
	}
	buf = ckalloc(msglen + 1);
	buf[msglen] = 0;
	ofs = 0;
	while (msglen) {
		tv_timeout.tv_sec = 60;
		tv_timeout.tv_usec = 0;

		FD_ZERO(&readfs);
		FD_SET(sockd, &readfs);
		ret = select(sockd + 1, &readfs, NULL, NULL, &tv_timeout);
		if (ret < 1) {
			LOGERR("Select2 failed in recv_unix_msg");
			dealloc(buf);
			goto out;
		}
		ret = read(sockd, buf + ofs, msglen);
		if (unlikely(ret < 0)) {
			LOGERR("Failed to read %d bytes in recv_unix_msg", msglen);
			dealloc(buf);
			goto out;
		}
		ofs += ret;
		msglen -= ret;
	}
out:
	return buf;
}

bool send_unix_msg(int sockd, const char *buf)
{
	tv_t tv_timeout = {1, 0};
	uint32_t msglen, len;
	fd_set writefds;
	int ret, ofs;

	len = strlen(buf);
	if (unlikely(!len)) {
		LOGWARNING("Zero length message sent to send_unix_msg");
		return false;
	}
	msglen = htole32(len);
	FD_ZERO(&writefds);
	FD_SET(sockd, &writefds);
	ret = select(sockd + 1, NULL, &writefds, NULL, &tv_timeout);
	if (ret < 1) {
		LOGERR("Select1 failed in send_unix_msg");
		return false;
	}
	ret = write(sockd, &msglen, 4);
	if (unlikely(ret < 4)) {
		LOGERR("Failed to write 4 byte length in send_unix_msg");
		return false;
	}
	ofs = 0;
	while (len) {
		tv_timeout.tv_sec = 1;
		tv_timeout.tv_usec = 0;

		FD_ZERO(&writefds);
		FD_SET(sockd, &writefds);
		ret = select(sockd + 1, NULL, &writefds, NULL, &tv_timeout);
		if (ret < 1) {
			LOGERR("Select2 failed in send_unix_msg");
			return false;
		}
		ret = write(sockd, buf + ofs, len);
		if (unlikely(ret < 0)) {
			LOGERR("Failed to write %d bytes in send_unix_msg", len);
			return false;
		}
		ofs += ret;
		len -= ret;
	}
	return true;
}

/* Send a single message to a process instance when there will be no response,
 * closing the socket immediately. */
bool send_proc(proc_instance_t *pi, const char *msg)
{
	char *path = pi->us.path;
	bool ret = false;
	int sockd;

	if (unlikely(!path || !strlen(path))) {
		LOGERR("Attempted to send message %s to null path in send_proc", msg ? msg : "");
		goto out;
	}
	if (unlikely(!msg || !strlen(msg))) {
		LOGERR("Attempted to send null message to socket %s in send_proc", path);
		goto out;
	}
	sockd = open_unix_client(path);
	if (unlikely(sockd < 0)) {
		LOGWARNING("Failed to open socket %s", path);
		goto out;
	}
	if (unlikely(!send_unix_msg(sockd, msg)))
		LOGWARNING("Failed to send %s to socket %s", msg, path);
	else
		ret = true;
	close(sockd);
out:
	return ret;
}

/* Send a single message to a process instance and retrieve the response, then
 * close the socket. */
char *send_recv_proc(proc_instance_t *pi, const char *msg)
{
	char *path = pi->us.path, *buf = NULL;
	int sockd;

	if (unlikely(!path || !strlen(path))) {
		LOGERR("Attempted to send message %s to null path in send_proc", msg ? msg : "");
		goto out;
	}
	if (unlikely(!msg || !strlen(msg))) {
		LOGERR("Attempted to send null message to socket %s in send_proc", path);
		goto out;
	}
	sockd = open_unix_client(path);
	if (unlikely(sockd < 0)) {
		LOGWARNING("Failed to open socket %s", path);
		goto out;
	}
	if (unlikely(!send_unix_msg(sockd, msg)))
		LOGWARNING("Failed to send %s to socket %s", msg, path);
	else
		buf = recv_unix_msg(sockd);
	close(sockd);
out:
	return buf;
}


json_t *json_rpc_call(connsock_t *cs, const char *rpc_req)
{
	char http_req[PAGESIZE];
	json_error_t err_val;
	json_t *val = NULL;
	int len, ret;

	if (unlikely(cs->fd < 0)) {
		LOGWARNING("FD %d invalid in json_rpc_call", cs->fd);
		goto out;
	}
	if (unlikely(!cs->url)) {
		LOGWARNING("No URL in json_rpc_call");
		goto out;
	}
	if (unlikely(!cs->port)) {
		LOGWARNING("No port in json_rpc_call");
		goto out;
	}
	if (unlikely(!cs->auth)) {
		LOGWARNING("No auth in json_rpc_call");
		goto out;
	}
	if (unlikely(!rpc_req)) {
		LOGWARNING("Null rpc_req passed to json_rpc_call");
		goto out;
	}
	len = strlen(rpc_req);
	if (unlikely(!len)) {
		LOGWARNING("Zero length rpc_req passed to json_rpc_call");
		goto out;
	}
	snprintf(http_req, PAGESIZE,
		 "POST / HTTP/1.1\n"
		 "Authorization: Basic %s\n"
		 "Host: %s:%s\n"
		 "Content-type: application/json\n"
		 "Content-Length: %d\n\n%s",
		 cs->auth, cs->url, cs->port, len, rpc_req);

	len = strlen(http_req);
	ret = write_socket(cs->fd, http_req, len);
	if (ret != len) {
		LOGWARNING("Failed to write to socket in json_rpc_call");
		goto out_empty;
	}
	ret = read_socket_line(cs);
	if (ret < 1) {
		LOGWARNING("Failed to read socket line in json_rpc_call");
		goto out_empty;
	}
	if (strncasecmp(cs->buf, "HTTP/1.1 200 OK", 15)) {
		LOGWARNING("HTTP response not ok: %s", cs->buf);
		goto out_empty;
	}
	do {
		ret = read_socket_line(cs);
		if (ret < 1) {
			LOGWARNING("Failed to read http socket lines in json_rpc_call");
			goto out_empty;
		}
	} while (strncmp(cs->buf, "{", 1));

	val = json_loads(cs->buf, 0, &err_val);
	if (!val)
		LOGWARNING("JSON decode failed(%d): %s", err_val.line, err_val.text);
out_empty:
	empty_socket(cs->fd);
	if (!val) {
		/* Assume that a failed request means the socket will be closed
		 * and reopen it */
		LOGWARNING("Reopening socket to %s:%s", cs->url, cs->port);
		close(cs->fd);
		cs->fd = connect_socket(cs->url, cs->port);
	}
out:
	dealloc(cs->buf);
	return val;
}


/* Align a size_t to 4 byte boundaries for fussy arches */
void align_len(size_t *len)
{
	if (*len % 4)
		*len += 4 - (*len % 4);
}

void realloc_strcat(char **ptr, const char *s)
{
	size_t old, new, len;
	char *ofs;

	if (unlikely(!*s)) {
		LOGWARNING("Passed empty pointer to realloc_strcat");
		return;
	}
	new = strlen(s);
	if (unlikely(!new)) {
		LOGWARNING("Passed empty string to realloc_strcat");
		return;
	}
	if (!*ptr)
		old = 0;
	else
		old = strlen(*ptr);
	len = old + new + 1;
	align_len(&len);
	*ptr = realloc(*ptr, len);
	if (!*ptr)
		quit(1, "Failed to realloc ptr of size %d in realloc_strcat", (int)len);
	ofs = *ptr + old;
	sprintf(ofs, "%s", s);
}

void *_ckalloc(size_t len, const char *file, const char *func, const int line)
{
	void *ptr;

	align_len(&len);
	ptr = malloc(len);
	if (unlikely(!ptr))
		quitfrom(1, file, func, line, "Failed to ckalloc!");
	return ptr;
}

void *_ckzalloc(size_t len, const char *file, const char *func, const int line)
{
	void *ptr;

	align_len(&len);
	ptr = calloc(len, 1);
	if (unlikely(!ptr))
		quitfrom(1, file, func, line, "Failed to ckalloc!");
	return ptr;
}

void _dealloc(void **ptr)
{
	free(*ptr);
	*ptr = NULL;
}

/* Adequate size s==len*2 + 1 must be alloced to use this variant */
void __bin2hex(void *vs, const void *vp, size_t len)
{
	static const char hex[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	const uchar *p = vp;
	uchar *s = vs;
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
void *bin2hex(const void *vp, size_t len)
{
	const uchar *p = vp;
	size_t slen;
	uchar *s;

	slen = len * 2 + 1;
	s = ckzalloc(slen);
	__bin2hex(s, p, len);

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
bool hex2bin(void *vp, const void *vhexstr, size_t len)
{
	const uchar *hexstr = vhexstr;
	int nibble1, nibble2;
	bool ret = false;
	uchar *p = vp;
	uchar idx;

	while (*hexstr && len) {
		if (unlikely(!hexstr[1])) {
			LOGWARNING("Early end of string in hex2bin");
			return ret;
		}

		idx = *hexstr++;
		nibble1 = hex2bin_tbl[idx];
		idx = *hexstr++;
		nibble2 = hex2bin_tbl[idx];

		if (unlikely((nibble1 < 0) || (nibble2 < 0))) {
			LOGWARNING("Invalid binary encoding in hex2bin");
			return ret;
		}

		*p++ = (((uchar)nibble1) << 4) | ((uchar)nibble2);
		--len;
	}

	if (likely(len == 0 && *hexstr == 0))
		ret = true;
	if (!ret)
		LOGWARNING("Failed hex2bin decode");
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
void b58tobin(char *b58bin, const char *b58)
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

static const char base64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* Return a malloced string of *src encoded into mime base 64 */
char *http_base64(const char *src)
{
	char *str, *dst;
	size_t l, hlen;
	int t, r;

	l = strlen((const char *)src);
	hlen = ((l + 2) / 3) * 4 + 1;
	str = ckalloc(hlen);
	dst = str;
	r = 0;

	while (l >= 3) {
		t = (src[0] << 16) | (src[1] << 8) | src[2];
		dst[0] = base64[(t >> 18) & 0x3f];
		dst[1] = base64[(t >> 12) & 0x3f];
		dst[2] = base64[(t >> 6) & 0x3f];
		dst[3] = base64[(t >> 0) & 0x3f];
		src += 3; l -= 3;
		dst += 4; r += 4;
	}

	switch (l) {
		case 2:
			t = (src[0] << 16) | (src[1] << 8);
			dst[0] = base64[(t >> 18) & 0x3f];
			dst[1] = base64[(t >> 12) & 0x3f];
			dst[2] = base64[(t >> 6) & 0x3f];
			dst[3] = '=';
			dst += 4;
			r += 4;
			break;
		case 1:
			t = src[0] << 16;
			dst[0] = base64[(t >> 18) & 0x3f];
			dst[1] = base64[(t >> 12) & 0x3f];
			dst[2] = dst[3] = '=';
			dst += 4;
			r += 4;
			break;
		case 0:
			break;
	}
	*dst = 0;
	return (str);
}

void address_to_pubkeytxn(char *pkh, const char *addr)
{
	char b58bin[25];

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

/* Create an exponentially decaying average over interval */
void decay_time(double *f, double fadd, double fsecs, double interval)
{
	double ftotal, fprop;

	if (fsecs <= 0)
		return;
	fprop = 1.0 - 1 / (exp(fsecs / interval));
	ftotal = 1.0 + fprop;
	*f += (fadd / fsecs * fprop);
	*f /= ftotal;
}

/* Convert a double value into a truncated string for displaying with its
 * associated suitable for Mega, Giga etc. Buf array needs to be long enough */
void suffix_string(double val, char *buf, size_t bufsiz, int sigdigits)
{
	const double kilo = 1000;
	const double mega = 1000000;
	const double giga = 1000000000;
	const double tera = 1000000000000;
	const double peta = 1000000000000000;
	const double exa  = 1000000000000000000;
	char suffix[2] = "";
	bool decimal = true;
	double dval;

	if (val >= exa) {
		val /= peta;
		dval = val / kilo;
		strcpy(suffix, "E");
	} else if (val >= peta) {
		val /= tera;
		dval = val / kilo;
		strcpy(suffix, "P");
	} else if (val >= tera) {
		val /= giga;
		dval = val / kilo;
		strcpy(suffix, "T");
	} else if (val >= giga) {
		val /= mega;
		dval = val / kilo;
		strcpy(suffix, "G");
	} else if (val >= mega) {
		val /= kilo;
		dval = val / kilo;
		strcpy(suffix, "M");
	} else if (val >= kilo) {
		dval = val / kilo;
		strcpy(suffix, "K");
	} else {
		dval = val;
		decimal = false;
	}

	if (!sigdigits) {
		if (decimal)
			snprintf(buf, bufsiz, "%.3g%s", dval, suffix);
		else
			snprintf(buf, bufsiz, "%d%s", (unsigned int)dval, suffix);
	} else {
		/* Always show sigdigits + 1, padded on right with zeroes
		 * followed by suffix */
		int ndigits = sigdigits - 1 - (dval > 0.0 ? floor(log10(dval)) : 0);

		snprintf(buf, bufsiz, "%*.*f%s", sigdigits + 1, ndigits, dval, suffix);
	}
}

/* truediffone == 0x00000000FFFF0000000000000000000000000000000000000000000000000000
 * Generate a 256 bit binary LE target by cutting up diff into 64 bit sized
 * portions or vice versa. */
static const double truediffone = 26959535291011309493156476344723991336010898738574164086137773096960.0;
static const double bits192 = 6277101735386680763835789423207666416102355444464034512896.0;
static const double bits128 = 340282366920938463463374607431768211456.0;
static const double bits64 = 18446744073709551616.0;

/* Converts a little endian 256 bit value to a double */
double le256todouble(const uchar *target)
{
	uint64_t *data64;
	double dcut64;

	data64 = (uint64_t *)(target + 24);
	dcut64 = le64toh(*data64) * bits192;

	data64 = (uint64_t *)(target + 16);
	dcut64 += le64toh(*data64) * bits128;

	data64 = (uint64_t *)(target + 8);
	dcut64 += le64toh(*data64) * bits64;

	data64 = (uint64_t *)(target);
	dcut64 += le64toh(*data64);

	return dcut64;
}

/* Return a difficulty from a binary target */
double diff_from_target(uchar *target)
{
	double d64, dcut64;

	d64 = truediffone;
	dcut64 = le256todouble(target);
	if (unlikely(!dcut64))
		dcut64 = 1;
	return d64 / dcut64;
}

/* Return the network difficulty from the block header which is in packed form,
 * as a double. */
double diff_from_header(uchar *header)
{
	double numerator;
	uint32_t diff32;
	uint8_t pow;
	int powdiff;

	pow = header[72];
	powdiff = (8 * (0x1d - 3)) - (8 * (pow - 3));
	diff32 = be32toh(*((uint32_t *)(header + 72))) & 0x00FFFFFF;
	numerator = 0xFFFFULL << powdiff;
	return numerator / (double)diff32;
}

void target_from_diff(uchar *target, double diff)
{
	uint64_t *data64, h64;
	double d64, dcut64;

	if (unlikely(diff == 0.0)) {
		/* This shouldn't happen but best we check to prevent a crash */
		memset(target, 0xff, 32);
		return;
	}

	d64 = truediffone;
	d64 /= diff;

	dcut64 = d64 / bits192;
	h64 = dcut64;
	data64 = (uint64_t *)(target + 24);
	*data64 = htole64(h64);
	dcut64 = h64;
	dcut64 *= bits192;
	d64 -= dcut64;

	dcut64 = d64 / bits128;
	h64 = dcut64;
	data64 = (uint64_t *)(target + 16);
	*data64 = htole64(h64);
	dcut64 = h64;
	dcut64 *= bits128;
	d64 -= dcut64;

	dcut64 = d64 / bits64;
	h64 = dcut64;
	data64 = (uint64_t *)(target + 8);
	*data64 = htole64(h64);
	dcut64 = h64;
	dcut64 *= bits64;
	d64 -= dcut64;

	h64 = d64;
	data64 = (uint64_t *)(target);
	*data64 = htole64(h64);
}

void gen_hash(uchar *data, uchar *hash, int len)
{
	uchar hash1[32];

	sha256(data, len, hash1);
	sha256(hash1, 32, hash);
}
