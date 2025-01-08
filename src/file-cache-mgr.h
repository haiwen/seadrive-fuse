#ifndef FILE_CACHE_MGR_H
#define FILE_CACHE_MGR_H

#include "utils.h"
#include "seafile-crypt.h"

struct FileCacheMgrPriv;

typedef struct FileCacheMgr {
    struct FileCacheMgrPriv *priv;
} FileCacheMgr;

typedef struct _BlockBuffer {
    char *content;
    int size;
} BlockBuffer;

typedef struct CachedFileHandle {
    struct CachedFile *cached_file;
    pthread_mutex_t lock;
    int fd;
    SeafileCrypt *crypt;
    BlockBuffer blk_buffer;
    gint64 file_size;
    gboolean is_readonly;
    gboolean fetch_canceled;

    gint64 start_download_time;
    gboolean notified_download_start;
    char *server;
    char *user;
} CachedFileHandle;

FileCacheMgr *
file_cache_mgr_new (const char *parent_dir);

void
file_cache_mgr_init (FileCacheMgr *mgr);

void
file_cache_mgr_start (FileCacheMgr *mgr);

CachedFileHandle *
file_cache_mgr_open (FileCacheMgr *mgr, const char *repo_id,
                     const char *file_path, int flags);

void
file_cache_mgr_close_file_handle (CachedFileHandle *file_handle);

gboolean
cached_file_handle_is_readonly (CachedFileHandle *file_handle);

gssize
file_cache_mgr_read (FileCacheMgr *mgr, CachedFileHandle *handle,
                     char *buf, size_t size, guint64 offset);

gssize
file_cache_mgr_read_by_path (FileCacheMgr *mgr,
                             const char *repo_id, const char *path,
                             char *buf, size_t size, guint64 offset);

gssize
file_cache_mgr_write (FileCacheMgr *mgr, CachedFileHandle *handle,
                      const char *buf, size_t size, off_t offset);

gssize
file_cache_mgr_write_by_path (FileCacheMgr *mgr,
                              const char *repo_id, const char *path,
                              const char *buf, size_t size, off_t offset);

int
file_cache_mgr_truncate (FileCacheMgr *mgr,
                         const char *repo_id,
                         const char *path,
                         off_t length,
                         gboolean *not_cached);

/* This function should only be called after the file was deleted in fs tree. */
int
file_cache_mgr_unlink (FileCacheMgr *mgr,
                       const char *repo_id,
                       const char *file_path);

int
file_cache_mgr_rename (FileCacheMgr *mgr,
                       const char *old_repo_id,
                       const char *old_path,
                       const char *new_repo_id,
                       const char *new_path);

int
file_cache_mgr_mkdir (FileCacheMgr *mgr, const char *repo_id, const char *dir);

int
file_cache_mgr_rmdir (FileCacheMgr *mgr, const char *repo_id, const char *dir);

struct FileCacheStat {
    char file_id[41];
    gint64 mtime;
    gint64 size;
    gboolean is_uploaded;
};
typedef struct FileCacheStat FileCacheStat;

/* Get stat of the cached file. The stat info is from the local file system, not
 * the extended attrs associated with the cached file.
 */

int
file_cache_mgr_stat_handle (FileCacheMgr *mgr,
                            CachedFileHandle *handle,
                            FileCacheStat *st);

int
file_cache_mgr_stat (FileCacheMgr *mgr,
                     const char *repo_id,
                     const char *path,
                     FileCacheStat *st);

gboolean
file_cache_mgr_is_file_cached (FileCacheMgr *mgr,
                               const char *repo_id,
                               const char *path);

gboolean
file_cache_mgr_is_file_outdated (FileCacheMgr *mgr,
                                 const char *repo_id,
                                 const char *path);

/* Update extended attrs of the cached file. */

int
file_cache_mgr_set_attrs (FileCacheMgr *mgr,
                          const char *repo_id,
                          const char *path,
                          gint64 mtime,
                          gint64 size,
                          const char *file_id);

int
file_cache_mgr_get_attrs (FileCacheMgr *mgr,
                          const char *repo_id,
                          const char *path,
                          FileCacheStat *st);

int
file_cache_mgr_set_file_uploaded (FileCacheMgr *mgr,
                                  const char *repo_id,
                                  const char *path,
                                  gboolean uploaded);

gboolean
file_cache_mgr_is_file_uploaded (FileCacheMgr *mgr,
                                 const char *repo_id,
                                 const char *path,
                                 const char *file_id);

gboolean
file_cache_mgr_is_file_opened (FileCacheMgr *mgr,
                               const char *repo_id,
                               const char *path);

gboolean
file_cache_mgr_is_file_changed (FileCacheMgr *mgr,
                                const char *repo_id,
                                const char *path,
                                gboolean print_msg);

/* Get repo id and path of the file from a handle. */
void
file_cache_mgr_get_file_info_from_handle (FileCacheMgr *mgr,
                                          CachedFileHandle *handle,
                                          char **repo_id,
                                          char **file_path);

int
file_cache_mgr_index_file (FileCacheMgr *mgr,
                           const char *repo_id,
                           int repo_version,
                           const char *file_path,
                           SeafileCrypt *crypt,
                           gboolean write_data,
                           unsigned char sha1[],
                           gboolean *changed);

typedef void (*FileCacheTraverseCB) (const char *repo_id,
                                     const char *file_path,
                                     SeafStat *st,
                                     void *user_data);

int
file_cache_mgr_traverse_path (FileCacheMgr *mgr,
                              const char *repo_id,
                              const char *path,
                              FileCacheTraverseCB file_cb,
                              FileCacheTraverseCB dir_cb,
                              void *user_data);

int
file_cache_mgr_set_clean_cache_interval (FileCacheMgr *mgr, int seconds);

int
file_cache_mgr_get_clean_cache_interval (FileCacheMgr *mgr);

int
file_cache_mgr_set_cache_size_limit (FileCacheMgr *mgr, gint64 limit);

gint64
file_cache_mgr_get_cache_size_limit (FileCacheMgr *mgr);

int
file_cache_mgr_delete_repo_cache (FileCacheMgr *mgr, const char *repo_id);

gboolean
file_cache_mgr_is_fetching_file (FileCacheMgr *mgr);

void
file_cache_mgr_cache_file (FileCacheMgr *mgr, const char *repo_id, const char *path, struct _RepoTreeStat *st);

int
file_cache_mgr_utimen (FileCacheMgr *mgr, const char *repo_id, const char *path, time_t mtime, time_t atime);

/* Functions that only works for files and folders under root directory.
 * Except for library folders, the root directory can also contain some special
 * files/folders created by the OS.
 * These top-level files and folders are directly mapped into the "file-cache/root"
 * folder.
 */

int
file_cache_mgr_uncache_path (const char *repo_id, const char *path);

// {"downloading_files": [{"file_path":, "downloaded":, "total_download":}, ], "downloaded_files": [ten latest downloaded files]}
json_t *
file_cache_mgr_get_download_progress (FileCacheMgr *mgr);

int
file_cache_mgr_cancel_download (FileCacheMgr *mgr,
                                const char *server,
                                const char *user,
                                const char *full_file_path, GError **error);

#endif
