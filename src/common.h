/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef COMMON_H
#define COMMON_H

#ifdef HAVE_CONFIG_H
 #include <config.h>
#endif

#include <unistd.h>
#include <utime.h>

#include <stdlib.h>
#include <stdint.h>             /* uint32_t */
#include <sys/types.h>          /* size_t */
#include <errno.h>
#include <string.h>
#include <limits.h>
#include <stdio.h>

#include <glib.h>
#include <glib/gstdio.h>

#define EMPTY_SHA1  "0000000000000000000000000000000000000000"

#define CURRENT_ENC_VERSION 2

#define DEFAULT_PROTO_VERSION 1
#define CURRENT_PROTO_VERSION 7

#define CURRENT_REPO_VERSION 1

#define SEAF_PATH_MAX 4096

#define MAX_GET_FINISHED_FILES 10

#endif
