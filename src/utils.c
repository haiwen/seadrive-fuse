/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif // HAVE_CONFIG_H
#include "common.h"

#include "utils.h"

#include "log.h"

#include <arpa/inet.h>

#include <utime.h>
#include <unistd.h>
#include <pwd.h>
#include <uuid/uuid.h>

#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>

#include <string.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>

#include <glib.h>
#include <glib/gstdio.h>

#include <jansson.h>

#include <zlib.h>

#include <sys/xattr.h>

void
rawdata_to_hex (const unsigned char *rawdata, char *hex_str, int n_bytes)
{
    static const char hex[] = "0123456789abcdef";
    int i;

    for (i = 0; i < n_bytes; i++) {
        unsigned int val = *rawdata++;
        *hex_str++ = hex[val >> 4];
        *hex_str++ = hex[val & 0xf];
    }
    *hex_str = '\0';
}

static unsigned hexval(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return ~0;
}

int
hex_to_rawdata (const char *hex_str, unsigned char *rawdata, int n_bytes)
{
    int i;
    for (i = 0; i < n_bytes; i++) {
        unsigned int val = (hexval(hex_str[0]) << 4) | hexval(hex_str[1]);
        if (val & ~0xff)
            return -1;
        *rawdata++ = val;
        hex_str += 2;
    }
    return 0;
}

int
checkdir_with_mkdir (const char *pathname)
{
    return g_mkdir_with_parents(pathname, 0755);
}

int
seaf_stat (const char *path, SeafStat *st)
{
    return stat (path, st);
}

int
seaf_fstat (int fd, SeafStat *st)
{
    return fstat (fd, st);
}

int
seaf_set_file_time (const char *path, guint64 mtime)
{
    struct stat st;
    struct utimbuf times;

    if (stat (path, &st) < 0) {
        seaf_warning ("Failed to stat %s: %s.\n", path, strerror(errno));
        return -1;
    }

    times.actime = st.st_atime;
    times.modtime = (time_t)mtime;

    return utime (path, &times);
}

int
seaf_util_unlink (const char *path)
{
    return unlink (path);
}

int
seaf_util_rmdir (const char *path)
{
    return rmdir (path);
}

int
seaf_util_mkdir (const char *path, mode_t mode)
{
    return mkdir (path, mode);
}

int
seaf_util_open (const char *path, int flags)
{
    return open (path, flags);
}

int
seaf_util_create (const char *path, int flags, mode_t mode)
{
    return open (path, flags, mode);
}

int
seaf_util_rename (const char *oldpath, const char *newpath)
{
    return rename (oldpath, newpath);
}

gboolean
seaf_util_exists (const char *path)
{
    return (access (path, F_OK) == 0);
}

gint64
seaf_util_lseek (int fd, gint64 offset, int whence)
{
    return lseek (fd, offset, whence);
}

gssize
seaf_getxattr (const char *path, const char *name, void *value, size_t size)
{
#ifdef __linux__
    return getxattr (path, name, value, size);
#endif

#ifdef __APPLE__
    return getxattr (path, name, value, size, 0, 0);
#endif
}

int
seaf_setxattr (const char *path, const char *name, const void *value, size_t size)
{
#ifdef __linux__
    return setxattr (path, name, value, size, 0);
#endif

#ifdef __APPLE__
    return setxattr (path, name, value, size, 0, 0);
#endif
}

int
seaf_removexattr (const char *path, const char *name)
{
#ifdef __APPLE__
    return removexattr (path, name, 0);
#else
    return removexattr (path, name);
#endif
}

int
seaf_truncate (const char *path, gint64 length)
{
    return truncate (path, (off_t)length);
}

int
seaf_rm_recursive (const char *path)
{
    SeafStat st;
    int ret = 0;
    GDir *dir;
    const char *dname;
    char *subpath;
    GError *error = NULL;

    if (seaf_stat (path, &st) < 0) {
        seaf_warning ("Failed to stat %s: %s\n", path, strerror(errno));
        return -1;
    }

    if (S_ISREG(st.st_mode)) {
        ret = seaf_util_unlink (path);
        return ret;
    } else if (S_ISDIR (st.st_mode)) {
        dir = g_dir_open (path, 0, &error);
        if (error) {
            seaf_warning ("Failed to open dir %s: %s\n", path, error->message);
            return -1;
        }

        while ((dname = g_dir_read_name (dir)) != NULL) {
            subpath = g_build_filename (path, dname, NULL);
            ret = seaf_rm_recursive (subpath);
            g_free (subpath);
            if (ret < 0)
                break;
        }

        g_dir_close (dir);

        if (ret == 0)
            ret = seaf_util_rmdir (path);

        return ret;
    }

    return ret;
}


gssize						/* Read "n" bytes from a descriptor. */
readn(int fd, void *vptr, size_t n)
{
	size_t	nleft;
	gssize	nread;
	char	*ptr;

	ptr = vptr;
	nleft = n;
	while (nleft > 0) {
		if ( (nread = read(fd, ptr, nleft)) < 0) {
			if (errno == EINTR)
				nread = 0;		/* and call read() again */
			else
				return(-1);
		} else if (nread == 0)
			break;				/* EOF */

		nleft -= nread;
		ptr   += nread;
	}
	return(n - nleft);		/* return >= 0 */
}

gssize						/* Write "n" bytes to a descriptor. */
writen(int fd, const void *vptr, size_t n)
{
	size_t		nleft;
	gssize		nwritten;
	const char	*ptr;

	ptr = vptr;
	nleft = n;
	while (nleft > 0) {
		if ( (nwritten = write(fd, ptr, nleft)) <= 0) {
			if (nwritten < 0 && errno == EINTR)
				nwritten = 0;		/* and call write() again */
			else
				return(-1);			/* error */
		}

		nleft -= nwritten;
		ptr   += nwritten;
	}
	return(n);
}


gssize						/* Read "n" bytes from a descriptor. */
recvn(evutil_socket_t fd, void *vptr, size_t n)
{
    size_t	nleft;
    gssize	nread;
    char	*ptr;

    ptr = vptr;
    nleft = n;
    while (nleft > 0) {
        if ( (nread = read(fd, ptr, nleft)) < 0)
        {
            if (errno == EINTR) {
                nread = 0;		/* and call read() again */
            }
            else {
                return(-1);
            }
        } else if (nread == 0)
            break;				/* EOF */

        nleft -= nread;
        ptr   += nread;
    }
    return(n - nleft);		/* return >= 0 */
}

gssize						/* Write "n" bytes to a descriptor. */
sendn(evutil_socket_t fd, const void *vptr, size_t n)
{
    size_t		nleft;
    gssize		nwritten;
    const char	*ptr;

    ptr = vptr;
    nleft = n;
    while (nleft > 0) {
        if ( (nwritten = write(fd, ptr, nleft)) <= 0)
        {
            if (nwritten < 0 && errno == EINTR) {
                nwritten = 0;		/* and call write() again */
            } else {
                return -1;
            }
        }

        nleft -= nwritten;
        ptr   += nwritten;
    }
    return(n);
}

int
seaf_pipe (seaf_pipe_t handles[2])
{
    return pipe (handles);
}

int
seaf_pipe_read (seaf_pipe_t fd, char *buf, int len)
{
    return read (fd, buf, len);
}

int
seaf_pipe_write (seaf_pipe_t fd, const char *buf, int len)
{
    return write (fd, buf, len);
}

int
seaf_pipe_close (seaf_pipe_t fd)
{
    return close (fd);
}

gssize seaf_pipe_readn (seaf_pipe_t fd, void *vptr, size_t n)
{
    return readn (fd, vptr, n);
}

gssize seaf_pipe_writen (seaf_pipe_t fd, const void *vptr, size_t n)
{
    return writen (fd, vptr, n);
}

void
seaf_sleep (unsigned int seconds)
{
    sleep (seconds);
}

char*
ccnet_expand_path (const char *src)
{
    const char *next_in, *ntoken;
    char new_path[SEAF_PATH_MAX + 1];
    char *next_out;
    int len;

   /* special cases */
    if (!src || *src == '\0')
        return NULL;
    if (strlen(src) > SEAF_PATH_MAX)
        return NULL;

    next_in = src;
    next_out = new_path;
    *next_out = '\0';

    if (*src == '~') {
        /* handle src start with '~' or '~<user>' like '~plt' */
        struct passwd *pw = NULL;

        for ( ; *next_in != '/' && *next_in != '\0'; next_in++) ;

        len = next_in - src;
        if (len == 1) {
            pw = getpwuid (geteuid());
        } else {
            /* copy '~<user>' to new_path */
            memcpy (new_path, src, len);
            new_path[len] = '\0';
            pw = getpwnam (new_path + 1);
        }
        if (pw == NULL)
            return NULL;

        len = strlen (pw->pw_dir);
        memcpy (new_path, pw->pw_dir, len);
        next_out = new_path + len;
        *next_out = '\0';

        if (*next_in == '\0')
            return strdup (new_path);
    } else if (*src != '/') {
        getcwd (new_path, SEAF_PATH_MAX);
        for ( ; *next_out; next_out++) ; /* to '\0' */
    }

    while (*next_in != '\0') {
        /* move ntoken to the next not '/' char  */
        for (ntoken = next_in; *ntoken == '/'; ntoken++) ;

        for (next_in = ntoken; *next_in != '/'
                 && *next_in != '\0'; next_in++) ;

        len = next_in - ntoken;

        if (len == 0) {
            /* the path ends with '/', keep it */
            *next_out++ = '/';
            *next_out = '\0';
            break;
        }

        if (len == 2 && ntoken[0] == '.' && ntoken[1] == '.')
        {
            /* '..' */
            for (; next_out > new_path && *next_out != '/'; next_out--)
                ;
            *next_out = '\0';
        } else if (ntoken[0] != '.' || len != 1) {
            /* not '.' */
            *next_out++ = '/';
            memcpy (next_out, ntoken, len);
            next_out += len;
            *next_out = '\0';
        }
    }

    /* the final special case */
    if (new_path[0] == '\0') {
        new_path[0] = '/';
        new_path[1] = '\0';
    }
    return strdup (new_path);
}


int
calculate_sha1 (unsigned char *sha1, const char *msg, int len)
{
    SHA_CTX c;

    if (len < 0)
        len = strlen(msg);

    SHA1_Init(&c);
    SHA1_Update(&c, msg, len);
	SHA1_Final(sha1, &c);
    return 0;
}

uint32_t
ccnet_sha1_hash (const void *v)
{
    /* 31 bit hash function */
    const unsigned char *p = v;
    uint32_t h = 0;
    int i;

    for (i = 0; i < 20; i++)
        h = (h << 5) - h + p[i];

    return h;
}

int
ccnet_sha1_equal (const void *v1,
                  const void *v2)
{
    const unsigned char *p1 = v1;
    const unsigned char *p2 = v2;
    int i;

    for (i = 0; i < 20; i++)
        if (p1[i] != p2[i])
            return 0;

    return 1;
}

char* gen_uuid ()
{
    char *uuid_str = g_malloc (37);
    uuid_t uuid;

    uuid_generate (uuid);
    uuid_unparse_lower (uuid, uuid_str);

    return uuid_str;
}

void gen_uuid_inplace (char *buf)
{
    uuid_t uuid;

    uuid_generate (uuid);
    uuid_unparse_lower (uuid, buf);
}

gboolean
is_uuid_valid (const char *uuid_str)
{
    uuid_t uuid;

    if (!uuid_str)
        return FALSE;

    if (uuid_parse (uuid_str, uuid) < 0)
        return FALSE;
    return TRUE;
}



gboolean
is_object_id_valid (const char *obj_id)
{
    if (!obj_id)
        return FALSE;

    int len = strlen(obj_id);
    int i;
    char c;

    if (len != 40)
        return FALSE;

    for (i = 0; i < len; ++i) {
        c = obj_id[i];
        if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
            continue;
        return FALSE;
    }

    return TRUE;
}

void
string_list_free (GList *str_list)
{
    GList *ptr = str_list;

    while (ptr) {
        g_free (ptr->data);
        ptr = ptr->next;
    }

    g_list_free (str_list);
}

/* JSON related utils. For compatibility with json-glib. */

const char *
json_object_get_string_member (json_t *object, const char *key)
{
    json_t *string = json_object_get (object, key);
    if (!string)
        return NULL;
    return json_string_value (string);
}

gboolean
json_object_has_member (json_t *object, const char *key)
{
    return (json_object_get (object, key) != NULL);
}

gint64
json_object_get_int_member (json_t *object, const char *key)
{
    json_t *integer = json_object_get (object, key);
    return json_integer_value (integer);
}

void
json_object_set_string_member (json_t *object, const char *key, const char *value)
{
    json_object_set_new (object, key, json_string (value));
}

void
json_object_set_int_member (json_t *object, const char *key, gint64 value)
{
    json_object_set_new (object, key, json_integer (value));
}

void
clean_utf8_data (char *data, int len)
{
    const char *s, *e;
    char *p;
    gboolean is_valid;

    s = data;
    p = data;

    while ((s - data) != len) {
        is_valid = g_utf8_validate (s, len - (s - data), &e);
        if (is_valid)
            break;

        if (s != e)
            p += (e - s);
        *p = '?';
        ++p;
        s = e + 1;
    }
}

/* zlib related wrapper functions. */

#define ZLIB_BUF_SIZE 16384

int
seaf_compress (guint8 *input, int inlen, guint8 **output, int *outlen)
{
    int ret;
    unsigned have;
    z_stream strm;
    guint8 out[ZLIB_BUF_SIZE];
    GByteArray *barray;

    if (inlen == 0)
        return -1;

    /* allocate deflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, Z_DEFAULT_COMPRESSION);
    if (ret != Z_OK) {
        seaf_warning ("deflateInit failed.\n");
        return -1;
    }

    strm.avail_in = inlen;
    strm.next_in = input;
    barray = g_byte_array_new ();

    do {
        strm.avail_out = ZLIB_BUF_SIZE;
        strm.next_out = out;
        ret = deflate(&strm, Z_FINISH);    /* no bad return value */
        have = ZLIB_BUF_SIZE - strm.avail_out;
        g_byte_array_append (barray, out, have);
    } while (ret != Z_STREAM_END);

    *outlen = barray->len;
    *output = g_byte_array_free (barray, FALSE);

    /* clean up and return */
    (void)deflateEnd(&strm);
    return 0;
}

int
seaf_decompress (guint8 *input, int inlen, guint8 **output, int *outlen)
{
    int ret;
    unsigned have;
    z_stream strm;
    unsigned char out[ZLIB_BUF_SIZE];
    GByteArray *barray;

    if (inlen == 0) {
        seaf_warning ("Empty input for zlib, invalid.\n");
        return -1;
    }

    /* allocate inflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    ret = inflateInit(&strm);
    if (ret != Z_OK) {
        seaf_warning ("inflateInit failed.\n");
        return -1;
    }

    strm.avail_in = inlen;
    strm.next_in = input;
    barray = g_byte_array_new ();

    do {
        strm.avail_out = ZLIB_BUF_SIZE;
        strm.next_out = out;
        ret = inflate(&strm, Z_NO_FLUSH);
        if (ret < 0) {
            seaf_warning ("Failed to inflate.\n");
            goto out;
        }
        have = ZLIB_BUF_SIZE - strm.avail_out;
        g_byte_array_append (barray, out, have);
    } while (ret != Z_STREAM_END);

out:
    /* clean up and return */
    (void)inflateEnd(&strm);

    if (ret == Z_STREAM_END) {
        *outlen = barray->len;
        *output = g_byte_array_free (barray, FALSE);
        return 0;
    } else {
        g_byte_array_free (barray, TRUE);
        return -1;
    }
}

char*
format_path (const char *path)
{
    int path_len = strlen (path);
    char *rpath;
    if (path[0] == '/') {
        rpath = g_strdup (path + 1);
        path_len--;
    } else {
        rpath = g_strdup (path);
    }
    while (path_len > 1 && rpath[path_len-1] == '/') {
        rpath[path_len-1] = '\0';
        path_len--;
    }

    return rpath;
}

char *
parse_fileserver_addr (const char *server_addr)
{
    char *location = strstr(server_addr, "//");
    if (!location)
        return NULL;
    location += 2;

    char *sep = strchr (location, '/');
    if (!sep) {
        return g_strdup(server_addr);
    } else {
        return g_strndup (server_addr, sep - server_addr);
    }
}
