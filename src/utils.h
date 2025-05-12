/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef CCNET_UTILS_H
#define CCNET_UTILS_H

#include <time.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdarg.h>
#include <glib.h>
#include <glib-object.h>
#include <stdlib.h>
#include <sys/stat.h>

#if defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
#include <event2/util.h>
#else
#include <evutil.h>
#endif

#ifdef __linux__
#include <endian.h>
#endif

#ifdef __OpenBSD__
#include <machine/endian.h>
#endif

#define SeafStat struct stat


int seaf_stat (const char *path, SeafStat *st);
int seaf_fstat (int fd, SeafStat *st);


int seaf_set_file_time (const char *path, guint64 mtime);


int
seaf_util_unlink (const char *path);

int
seaf_util_rmdir (const char *path);

int
seaf_util_mkdir (const char *path, mode_t mode);

int
seaf_util_open (const char *path, int flags);

int
seaf_util_create (const char *path, int flags, mode_t mode);

int
seaf_util_rename (const char *oldpath, const char *newpath);

gboolean
seaf_util_exists (const char *path);

gint64
seaf_util_lseek (int fd, gint64 offset, int whence);

gssize
seaf_getxattr (const char *path, const char *name, void *value, size_t size);

int
seaf_setxattr (const char *path, const char *name, const void *value, size_t size);

int
seaf_listxattr (const char *path, char *list, size_t size);

int
seaf_removexattr (const char *path, const char *name);

int
seaf_truncate (const char *path, gint64 length);

int
seaf_rm_recursive (const char *path);

#ifndef O_BINARY
#define O_BINARY 0
#endif

/* Read "n" bytes from a descriptor. */
gssize	readn(int fd, void *vptr, size_t n);
gssize writen(int fd, const void *vptr, size_t n);

/* Read "n" bytes from a socket. */
gssize	recvn(evutil_socket_t fd, void *vptr, size_t n);
gssize sendn(evutil_socket_t fd, const void *vptr, size_t n);

#define seaf_pipe_t int

int
seaf_pipe (seaf_pipe_t handles[2]);
int
seaf_pipe_read (seaf_pipe_t fd, char *buf, int len);
int
seaf_pipe_write (seaf_pipe_t fd, const char *buf, int len);
int
seaf_pipe_close (seaf_pipe_t fd);

gssize seaf_pipe_readn (seaf_pipe_t fd, void *vptr, size_t n);
gssize seaf_pipe_writen (seaf_pipe_t fd, const void *vptr, size_t n);

void seaf_sleep (unsigned int seconds);

void rawdata_to_hex (const unsigned char *rawdata, char *hex_str, int n_bytes);
int hex_to_rawdata (const char *hex_str, unsigned char *rawdata, int n_bytes);

#define sha1_to_hex(sha1, hex) rawdata_to_hex((sha1), (hex), 20)
#define hex_to_sha1(hex, sha1) hex_to_rawdata((hex), (sha1), 20)

/* If msg is NULL-terminated, set len to -1 */
int calculate_sha1 (unsigned char *sha1, const char *msg, int len);
int ccnet_sha1_equal (const void *v1, const void *v2);
unsigned int ccnet_sha1_hash (const void *v);

char* gen_uuid ();
void gen_uuid_inplace (char *buf);
gboolean is_uuid_valid (const char *uuid_str);

gboolean
is_object_id_valid (const char *obj_id);

/* dir operations */
int checkdir_with_mkdir (const char *path);
char* ccnet_expand_path (const char *src);

void string_list_free (GList *str_list);

/*
 * Utility functions for converting data to/from network byte order.
 */

static inline uint64_t
bswap64 (uint64_t val)
{
    uint64_t ret;
    uint8_t *ptr = (uint8_t *)&ret;

    ptr[0]=((val)>>56)&0xFF;
    ptr[1]=((val)>>48)&0xFF;
    ptr[2]=((val)>>40)&0xFF;
    ptr[3]=((val)>>32)&0xFF;
    ptr[4]=((val)>>24)&0xFF;
    ptr[5]=((val)>>16)&0xFF;
    ptr[6]=((val)>>8)&0xFF;
    ptr[7]=(val)&0xFF;

    return ret;
}

static inline uint64_t
hton64(uint64_t val)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN || defined __APPLE__
    return bswap64 (val);
#else
    return val;
#endif
}

static inline uint64_t 
ntoh64(uint64_t val) 
{
#if __BYTE_ORDER == __LITTLE_ENDIAN || defined __APPLE__
    return bswap64 (val);
#else
    return val;
#endif
}

static inline void put64bit(uint8_t **ptr,uint64_t val)
{
    uint64_t val_n = hton64 (val);
    *((uint64_t *)(*ptr)) = val_n;
    (*ptr)+=8;
}

static inline void put32bit(uint8_t **ptr,uint32_t val)
{
    uint32_t val_n = htonl (val);
    *((uint32_t *)(*ptr)) = val_n;
    (*ptr)+=4;
}

static inline void put16bit(uint8_t **ptr,uint16_t val)
{
    uint16_t val_n = htons (val);
    *((uint16_t *)(*ptr)) = val_n;
    (*ptr)+=2;
}

static inline uint64_t get64bit(const uint8_t **ptr)
{
    uint64_t val_h = ntoh64 (*((uint64_t *)(*ptr)));
    (*ptr)+=8;
    return val_h;
}

static inline uint32_t get32bit(const uint8_t **ptr)
{
    uint32_t val_h = ntohl (*((uint32_t *)(*ptr)));
    (*ptr)+=4;
    return val_h;
}

static inline uint16_t get16bit(const uint8_t **ptr)
{
    uint16_t val_h = ntohs (*((uint16_t *)(*ptr)));
    (*ptr)+=2;
    return val_h;
}


#include <jansson.h>

const char *
json_object_get_string_member (json_t *object, const char *key);

gboolean
json_object_has_member (json_t *object, const char *key);

gint64
json_object_get_int_member (json_t *object, const char *key);

void
json_object_set_string_member (json_t *object, const char *key, const char *value);

void
json_object_set_int_member (json_t *object, const char *key, gint64 value);

/* Replace invalid UTF-8 bytes with '?' */
void
clean_utf8_data (char *data, int len);

/* zlib related functions. */

int
seaf_compress (guint8 *input, int inlen, guint8 **output, int *outlen);

int
seaf_decompress (guint8 *input, int inlen, guint8 **output, int *outlen);

char*
format_path (const char *path);

#define mk_permissions(mode) (((mode) & 0100) ? 0755 : 0644)
static inline unsigned int create_mode(unsigned int mode)
{
    if (S_ISDIR(mode))
        return S_IFDIR;
    return S_IFREG | mk_permissions(mode);
}

char *
parse_fileserver_addr (const char *server_addr);
#endif
