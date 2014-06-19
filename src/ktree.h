/*
 * Copyright 2003-2014 Andrew Smith
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See COPYING for more details.
 */

#ifndef ___KTREE_H
#define ___KTREE_H

#include "klist.h"
#include "libckpool.h"

#define quithere(status, fmt, ...) \
	quitfrom(status, __FILE__, __func__, __LINE__, fmt, ##__VA_ARGS__)

typedef struct ktree
{
	bool	isNil;
	bool	red;
	struct	ktree	*parent;
	struct	ktree	*left;
	struct	ktree	*right;
	K_ITEM	*data;
	long	test;
} K_TREE;

typedef void *K_TREE_CTX;

extern K_TREE *new_ktree();
extern void dump_ktree(K_TREE *root, char *(*dsp_funct)(K_ITEM *));
extern void dsp_ktree(K_LIST *list, K_TREE *root, char *filename);
extern K_ITEM *first_in_ktree(K_TREE *root, K_TREE_CTX *ctx);
extern K_ITEM *last_in_ktree(K_TREE *root, K_TREE_CTX *ctx);
extern K_ITEM *next_in_ktree(K_TREE_CTX *ctx);
extern K_ITEM *prev_in_ktree(K_TREE_CTX *ctx);
extern K_TREE *add_to_ktree(K_TREE *root, K_ITEM *data, double (*cmp_func)(K_ITEM *, K_ITEM *));
extern K_ITEM *find_in_ktree(K_TREE *root, K_ITEM *data, double (*cmp_funct)(K_ITEM *, K_ITEM *), K_TREE_CTX *ctx);
extern K_ITEM *find_after_in_ktree(K_TREE *ktree, K_ITEM *data, double (*cmp_funct)(K_ITEM *, K_ITEM *), K_TREE_CTX *ctx);
extern K_ITEM *find_before_in_ktree(K_TREE *ktree, K_ITEM *data, double (*cmp_funct)(K_ITEM *, K_ITEM *), K_TREE_CTX *ctx);
extern K_TREE *remove_from_ktree(K_TREE *root, K_ITEM *data, double (*cmp_funct)(K_ITEM *, K_ITEM *), K_TREE_CTX *ctx);
extern K_TREE *free_ktree(K_TREE *root, void (*free_funct)(void *));

#endif
