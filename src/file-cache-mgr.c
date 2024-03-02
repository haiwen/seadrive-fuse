#include "common.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <curl/curl.h>

#include "seafile-session.h"
#include "seafile-config.h"
#include "seafile-error.h"
#define DEBUG_FLAG SEAFILE_DEBUG_SYNC
#include "log.h"
#include "repo-tree.h"
#include "utils.h"
#include "timer.h"

#define MAX_THREADS 3
#define DEFAULT_CLEAN_CACHE_INTERVAL 600 // 10 minutes
#define DEFAULT_CACHE_SIZE_LIMIT (10000000000LL) // 10GB

/* true if the cached content is already on the server, otherwise false.
 * This attr is used in cache cleaning.
 */
#define SEAFILE_UPLOADED_ATTR "user.uploaded"
/* These 3 attrs can be seen outside. */
#define SEAFILE_MTIME_ATTR "user.seafile-mtime"
#define SEAFILE_SIZE_ATTR "user.seafile-size"
#define SEAFILE_FILE_ID_ATTR "user.file-id"

typedef struct CachedFile {
    // Identifier(repo_id/file_path) for CachedFile in memory
    char *file_key;
    // Record CachedFile open number
    gint n_open;
    // Repo unique name used when get download progress
    char *repo_uname;

    // Caculate download progress
    gint64 downloaded;
    gint64 total_download;

    gboolean force_canceled;
    gint64 last_cancel_time;
} CachedFile;

typedef struct FileCacheMgrPriv {
    // Parent dir(seafile_dir/file_cache) to store cached files
    char *base_path;

    // repo_id/file_path <-> CachedFile
    GHashTable *cached_files;
    pthread_mutex_t cache_lock;

    GThreadPool *tpool;

    // repo_id/file_path <-> CachedFileHandle
    GHashTable *cache_tasks;
    pthread_mutex_t task_lock;

    /* Cache block size list for files. */
    GHashTable *block_map_cache;
    pthread_rwlock_t block_map_lock;

    SeafTimer *clean_timer;

    int clean_cache_interval;
    gint64 cache_size_limit;

    GQueue *downloaded_files;
    pthread_mutex_t downloaded_files_lock;
} FileCacheMgrPriv;

static char *
cached_file_ondisk_path (CachedFile *file)
{
    FileCacheMgrPriv *priv = seaf->file_cache_mgr->priv;
    char **tokens = g_strsplit (file->file_key, "/", 2);
    char *repo_id = tokens[0];
    char *path = tokens[1];

    char *ondisk_path = g_build_filename (priv->base_path, repo_id, path, NULL);

    g_strfreev (tokens);
    return ondisk_path;
}

static void
free_cached_file (CachedFile *file)
{
    if (!file)
        return;

    g_free (file->file_key);
    g_free (file->repo_uname);
    g_free (file);
}

static void
cached_file_ref (CachedFile *file)
{
    if (!file)
        return;
    g_atomic_int_inc (&file->n_open);
}

static void
cached_file_unref (CachedFile *file)
{
    if (!file)
        return;
    if (g_atomic_int_dec_and_test (&file->n_open))
        free_cached_file (file);
}

static CachedFileHandle *
cached_file_handle_new ()
{
    CachedFileHandle *handle = g_new0 (CachedFileHandle, 1);
    pthread_mutex_init (&handle->lock, NULL);
    return handle;
}

static void
free_cached_file_handle (CachedFileHandle *file_handle)
{
    if (!file_handle)
        return;

    close (file_handle->fd);
    pthread_mutex_destroy (&file_handle->lock);
    if (!file_handle->is_in_root)
        cached_file_unref (file_handle->cached_file);
    if (file_handle->crypt)
        g_free (file_handle->crypt);
    g_free (file_handle);
}

gboolean
cached_file_handle_is_readonly (CachedFileHandle *file_handle)
{
    return file_handle->is_readonly;
}

gboolean
cached_file_handle_is_in_root (CachedFileHandle *handle)
{
    return handle->is_in_root;
}

static void
fetch_file_worker (gpointer data, gpointer user_data);

static void *
clean_cache_worker (void *data);

static void *
remove_deleted_cache_worker (void *data);

static void *
check_download_file_time_worker (void *data);

FileCacheMgr *
file_cache_mgr_new (const char *parent_dir)
{
    GError *error = NULL;
    FileCacheMgr *mgr = g_new0 (FileCacheMgr, 1);
    FileCacheMgrPriv *priv = g_new0 (FileCacheMgrPriv, 1);

    priv->tpool = g_thread_pool_new (fetch_file_worker, priv, MAX_THREADS, FALSE, &error);
    if (!priv->tpool) {
        seaf_warning ("Failed to create thread pool for cache file: %s.\n",
                      error == NULL ? "" : error->message);
        g_free (priv);
        g_free (mgr);
        return NULL;
    }

    priv->base_path = g_build_filename (parent_dir, "file-cache", NULL);
    if (g_mkdir_with_parents (priv->base_path, 0777) < 0) {
        seaf_warning ("Failed to create file_cache dir: %s.\n", strerror (errno));
        g_free (priv->base_path);
        g_thread_pool_free (priv->tpool, TRUE, FALSE);
        g_free (priv);
        g_free (mgr);
        return NULL;
    }

    priv->cached_files = g_hash_table_new_full (g_str_hash, g_str_equal, NULL,
                                                (GDestroyNotify)cached_file_unref);
    pthread_mutex_init (&priv->cache_lock, NULL);

    priv->cache_tasks = g_hash_table_new_full (g_str_hash, g_str_equal,
                                               (GDestroyNotify)g_free,
                                               NULL);
    pthread_mutex_init (&priv->task_lock, NULL);

    priv->block_map_cache = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);
    pthread_rwlock_init (&priv->block_map_lock, NULL);

    priv->downloaded_files = g_queue_new ();
    pthread_mutex_init (&priv->downloaded_files_lock, NULL);

    mgr->priv = priv;

    return mgr;
}

/* static void */
/* load_downloaded_file_list (FileCacheMgr *mgr); */

void
file_cache_mgr_init (FileCacheMgr *mgr)
{
    int clean_cache_interval;
    gint64 cache_size_limit;
    gboolean exists;

    clean_cache_interval = seafile_session_config_get_int (seaf,
                                                           KEY_CLEAN_CACHE_INTERVAL,
                                                           &exists);
    if (!exists) {
        clean_cache_interval = DEFAULT_CLEAN_CACHE_INTERVAL;
    }

    cache_size_limit = seafile_session_config_get_int64 (seaf,
                                                         KEY_CACHE_SIZE_LIMIT,
                                                         &exists);
    if (!exists) {
        cache_size_limit = DEFAULT_CACHE_SIZE_LIMIT;
    }

    mgr->priv->clean_cache_interval = clean_cache_interval;
    mgr->priv->cache_size_limit = cache_size_limit;

    /* load_downloaded_file_list (mgr); */
}

void
file_cache_mgr_start (FileCacheMgr *mgr)
{
    pthread_t tid;
    int rc;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    rc = pthread_create (&tid, &attr, clean_cache_worker, mgr);
    if (rc != 0) {
        seaf_warning ("Failed to create clean cache worker thread.\n");
    }

    rc = pthread_create (&tid, &attr, remove_deleted_cache_worker, mgr);
    if (rc != 0) {
        seaf_warning ("Failed to create remove deleted cache worker thread.\n");
    }

    rc = pthread_create (&tid, &attr, check_download_file_time_worker, mgr->priv);
    if (rc != 0) {
        seaf_warning ("Failed to check download file time worker thread.\n");
    }
}

static void
cancel_fetch_task_by_file (FileCacheMgr *mgr, const char *file_key)
{
    CachedFileHandle *handle;

    pthread_mutex_lock (&mgr->priv->task_lock);

    handle = g_hash_table_lookup (mgr->priv->cache_tasks, file_key);
    if (handle)
        handle->fetch_canceled = TRUE;

    pthread_mutex_unlock (&mgr->priv->task_lock);
}

/* Cached file handling. */

static void
send_file_download_notification (const char *type, const char *repo_id, const char *path)
{
    json_t *msg;
    char *repo_uname, *fullpath;

    msg = json_object ();
    json_object_set_new (msg, "type", json_string(type));
    repo_uname = seaf_repo_manager_get_repo_display_name (seaf->repo_mgr, repo_id);
    fullpath = g_strconcat (repo_uname, "/", path, NULL);
    json_object_set_new (msg, "path", json_string(fullpath));

    mq_mgr_push_msg (seaf->mq_mgr, SEADRIVE_EVENT_CHAN, msg);

    g_free (repo_uname);
    g_free (fullpath);
}

static size_t
fill_block (void *contents, size_t size, size_t nmemb, void *userp)
{
    CachedFileHandle *file_handle = userp;
    size_t realsize = size *nmemb;
    char *buffer = contents;
    int ret = 0;
    char *dec_out = NULL;
    int dec_out_len = -1;

    if (file_handle->crypt) {
        ret = seafile_decrypt (&dec_out, &dec_out_len, buffer, realsize, file_handle->crypt);

        if (ret != 0){
            seaf_warning ("Decrypt block failed.\n");
            return -1;
        }

        ret = writen (file_handle->fd, dec_out, dec_out_len);
    } else {
        ret = writen (file_handle->fd, buffer, realsize);
    }

    if (ret < 0) {
        seaf_warning ("Failed to write cache file %s: %s.\n",
                      file_handle->cached_file->file_key, strerror (errno));
    } else {
        file_handle->cached_file->downloaded += ret;
    }

    if (!file_handle->crypt)
        g_atomic_int_add (&(seaf->sync_mgr->recv_bytes), realsize);

    g_free (dec_out);
    return ret;
}

static size_t
get_encrypted_block_cb (void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size *nmemb;
    CachedFileHandle *file_handle = userp;

    if (file_handle->cached_file->force_canceled) {
        seaf_message ("Cancel fetching %s\n", file_handle->cached_file->file_key);
        return 0;
    }
    /* If the task is marked as canceled and the fetch task is the only
     * remaining referer to the cached file, it should be stopped.
     */
    if (file_handle->fetch_canceled && file_handle->cached_file->n_open <= 2) {
        seaf_debug ("Cancel fetching %s after close\n", file_handle->cached_file->file_key);
        return 0;
    }

    file_handle->blk_buffer.content = g_realloc (file_handle->blk_buffer.content, file_handle->blk_buffer.size + realsize);
    if (!file_handle->blk_buffer.content) {
        seaf_warning ("Not enough memory.\n");
        return 0;
    }
    memcpy (file_handle->blk_buffer.content + file_handle->blk_buffer.size, contents, realsize);
    file_handle->blk_buffer.size += realsize;
    g_atomic_int_add (&(seaf->sync_mgr->recv_bytes), realsize);
    return realsize;
}

static size_t
get_block_cb (void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size *nmemb;
    CachedFileHandle *file_handle = userp;

    if (file_handle->cached_file->force_canceled) {
        seaf_message ("Cancel fetching %s\n", file_handle->cached_file->file_key);
        return 0;
    }
    /* If the task is marked as canceled and the fetch task is the only
     * remaining referer to the cached file, it should be stopped.
     */
    if (file_handle->fetch_canceled && file_handle->cached_file->n_open <= 2) {
        seaf_debug ("Cancel fetching %s after close\n", file_handle->cached_file->file_key);
        return 0;
    }

    g_atomic_int_add (&(seaf->sync_mgr->recv_bytes), realsize);
    fill_block (contents, size, nmemb, userp);
    return realsize;
}

static int
mark_file_cached (const char *ondisk_path, RepoTreeStat *st)
{
    if (seaf_set_file_time (ondisk_path, st->mtime) < 0) {
        seaf_warning ("Failed to set mtime for %s.\n", ondisk_path);
        return -1;
    }

    char attr[64];
    int len;

    if (seaf_setxattr (ondisk_path, SEAFILE_FILE_ID_ATTR, st->id, 41) < 0) {
        seaf_warning ("Failed to set file-id xattr for %s: %s.\n",
                      ondisk_path, strerror(errno));
        return -1;
    }

    len = snprintf (attr, sizeof(attr), "%"G_GINT64_FORMAT, st->size);
    if (seaf_setxattr (ondisk_path, SEAFILE_SIZE_ATTR, attr, len+1) < 0) {
        seaf_warning ("Failed to set seafile-size xattr for %s: %s.\n",
                      ondisk_path, strerror(errno));
        return -1;
    }

    len = snprintf (attr, sizeof(attr), "%"G_GINT64_FORMAT, st->mtime);
    if (seaf_setxattr (ondisk_path, SEAFILE_MTIME_ATTR, attr, len+1) < 0) {
        seaf_warning ("Failed to set seafile-mtime xattr for %s: %s.\n",
                      ondisk_path, strerror(errno));
        return -1;
    }

    return 0;
}

static int
get_file_from_server (HttpServerInfo *server_info, SeafRepo *repo,
                      const char *path,
                      gint64 block_offset,
                      CachedFileHandle *handle)
{
    int http_status = 200;
    if (http_tx_manager_get_file (seaf->http_tx_mgr, server_info->host,
                                  server_info->use_fileserver_port, repo->token,
                                  repo->id,
                                  path, block_offset,
                                  get_block_cb, handle,
                                  &http_status) < 0) {
        if (!handle->fetch_canceled && !handle->cached_file->force_canceled)
            seaf_message ("Failed to get file %s from server\n", path);
        if (handle->notified_download_start)
            send_file_download_notification ("file-download.stop", repo->id, path);
        return -1;
    }

    return 0;
}

static void
calculate_block_offset (Seafile *file, gint64 *block_map, CachedFileHandle *handle,
                        int *block_offset, gint64 *file_offset)
{
    SeafStat st;
    gint64 size;
    int i;
    gint64 offset, prev_offset;

    if (seaf_fstat (handle->fd, &st) < 0) {
        seaf_warning ("Failed to stat cached file %s: %s\n",
                      handle->cached_file->file_key, strerror(errno));
        return;
    }

    size = (gint64)st.st_size;
    offset = 0;
    prev_offset = 0;

    for (i = 0; i < file->n_blocks; ++i) {
        prev_offset = offset;
        offset += block_map[i];
        if (offset > size)
            break;
    }

    *block_offset = i;
    *file_offset = prev_offset;
}

#define CACHE_BLOCK_MAP_THRESHOLD 3000000 /* 3MB */

static void
fetch_file_worker (gpointer data, gpointer user_data)
{
    CachedFileHandle *file_handle = data;
    FileCacheMgrPriv *priv = user_data;
    SeafRepo *repo = NULL;
    RepoTreeStat st;
    Seafile *file = NULL;
    HttpServerInfo *server_info = NULL;
    char *file_key = NULL;
    char **key_comps = NULL;
    char *repo_id;
    char *file_path;
    char *ondisk_path = NULL;
    int http_status = 200;
    gint64 *block_map = NULL;
    int n_blocks = 0;
    int block_offset = 0;
    gint64 file_offset = 0;
    gboolean have_invisible = FALSE;

    file_key = g_strdup (file_handle->cached_file->file_key);

    key_comps = g_strsplit (file_handle->cached_file->file_key, "/", 2);
    repo_id = key_comps[0];
    file_path = key_comps[1];

    ondisk_path = g_build_filename (priv->base_path, repo_id, file_path, NULL);

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to get repo %.8s.\n", repo_id);
        goto out;
    }

    have_invisible = seaf_repo_manager_include_invisible_perm (seaf->repo_mgr, repo->id);

    server_info = seaf_sync_manager_get_server_info (seaf->sync_mgr, repo->server, repo->user);
    if (!server_info) {
        seaf_warning ("Failed to get current server info.\n");
        goto out;
    }

    if (repo->encrypted && repo->is_passwd_set)
        file_handle->crypt = seafile_crypt_new (repo->enc_version, repo->enc_key, repo->enc_iv);

    if (repo_tree_stat_path (repo->tree, file_path, &st) < 0) {
        seaf_warning ("Failed to stat repo tree path %s in repo %s.\n",
                      file_path, repo_id);
        goto out;
    }

    file_handle->start_download_time = (gint64)time(NULL);

    if (!seaf_fs_manager_object_exists (seaf->fs_mgr,
                                        repo->id,
                                        repo->version,
                                        st.id)) {
        if (http_tx_manager_get_fs_object (seaf->http_tx_mgr,
                                           server_info->host,
                                           server_info->use_fileserver_port,
                                           repo->token,
                                           repo->id,
                                           st.id,
                                           file_path) < 0) {
            seaf_warning ("Failed to get file object %s of %s from server %s.\n",
                          st.id, file_key, server_info->host);
            goto out;
        }
    }

    file = seaf_fs_manager_get_seafile (seaf->fs_mgr, repo->id, repo->version, st.id);
    if (!file) {
        seaf_warning ("Failed to get file object %s in repo %s.\n",
                      st.id, repo_id);
        goto out;
    }

    if (file->file_size >= CACHE_BLOCK_MAP_THRESHOLD) {
        pthread_rwlock_rdlock (&priv->block_map_lock);
        block_map = g_hash_table_lookup (priv->block_map_cache, st.id);
        pthread_rwlock_unlock (&priv->block_map_lock);

        if (!block_map) {
            if (http_tx_manager_get_file_block_map (seaf->http_tx_mgr,
                                                    server_info->host,
                                                    server_info->use_fileserver_port,
                                                    repo->token,
                                                    repo->id,
                                                    st.id,
                                                    &block_map,
                                                    &n_blocks) == 0) {
                if (n_blocks != file->n_blocks) {
                    seaf_warning ("Block number return from server does not match"
                                  "seafile object. File-id is %s. File is %s"
                                  "Returned %d, expect %d\n",
                                  st.id, file_key, n_blocks, file->n_blocks);
                } else {
                    pthread_rwlock_wrlock (&priv->block_map_lock);
                    g_hash_table_replace (priv->block_map_cache, g_strdup(st.id), block_map);
                    pthread_rwlock_unlock (&priv->block_map_lock);
                }
            } else {
                seaf_warning ("Failed to get block map for file object %s of %s "
                              "server %s.\n",
                              st.id, file_key, server_info->host);
            }
        }

        if (block_map) {
            calculate_block_offset (file, block_map, file_handle,
                                    &block_offset, &file_offset);
        }
    }

    seaf_sync_manager_update_active_path (seaf->sync_mgr, repo_id,
                                          file_path, st.mode, SYNC_STATUS_SYNCING);

    file_handle->cached_file->downloaded = 0;
    file_handle->cached_file->total_download = file->file_size;

    if (file_offset > 0) {
        seaf_util_lseek (file_handle->fd, file_offset, SEEK_SET);
    }

    if (have_invisible && !file_handle->crypt) {
        if (get_file_from_server (server_info, repo, file_path, block_offset, file_handle) < 0) {
            goto out;
        }
    }
    int i = block_offset;
    if (file_handle->crypt) {
        for (; i < file->n_blocks; i++) {
            http_status = 200;
            memset (&file_handle->blk_buffer, 0, sizeof(file_handle->blk_buffer));
            if (http_tx_manager_get_block (seaf->http_tx_mgr, server_info->host,
                                           server_info->use_fileserver_port, repo->token,
                                           repo->id, file->blk_sha1s[i],
                                           get_encrypted_block_cb, file_handle,
                                           &http_status) < 0) {
                if (!file_handle->fetch_canceled && !file_handle->cached_file->force_canceled)
                    seaf_warning ("Failed to get block %s of %s from server %s.\n",
                                  file->blk_sha1s[i], file_key, server_info->host);
                seaf_sync_manager_delete_active_path (seaf->sync_mgr, repo_id, file_path);
                if (file_handle->notified_download_start)
                    send_file_download_notification ("file-download.stop", repo_id, file_path);
                goto out;
            }
            fill_block (file_handle->blk_buffer.content, file_handle->blk_buffer.size, 1, file_handle);
            g_free (file_handle->blk_buffer.content);
        }
    } else {
        for (; i < file->n_blocks; i++) {
            http_status = 200;
            if (http_tx_manager_get_block (seaf->http_tx_mgr, server_info->host,
                                           server_info->use_fileserver_port, repo->token,
                                           repo->id, file->blk_sha1s[i],
                                           get_block_cb, file_handle,
                                           &http_status) < 0) {
                if (!file_handle->fetch_canceled && !file_handle->cached_file->force_canceled)
                    seaf_warning ("Failed to get block %s of %s from server %s.\n",
                                  file->blk_sha1s[i], file_key, server_info->host);
                seaf_sync_manager_delete_active_path (seaf->sync_mgr, repo_id, file_path);
                if (file_handle->notified_download_start)
                    send_file_download_notification ("file-download.stop", repo_id, file_path);
                goto out;
            }
        }
    }

    if (file_handle->cached_file->downloaded != file->file_size) {
        seaf_warning("Failed to download file %s in repo %s.\n", file_path, repo_id);
        seaf_sync_manager_delete_active_path(seaf->sync_mgr, repo_id, file_path);
        seaf_util_unlink (ondisk_path);
        goto out;
    }

    pthread_mutex_lock (&priv->downloaded_files_lock);

    g_queue_push_head (priv->downloaded_files,
                       g_build_filename (file_handle->cached_file->repo_uname,
                                         file_path, NULL));
    if (priv->downloaded_files->length > MAX_GET_FINISHED_FILES) {
        g_free (g_queue_pop_tail (priv->downloaded_files));
    }

    pthread_mutex_unlock (&priv->downloaded_files_lock);

    /* Must close file handle (thus the fd of this handle) before updating file
     * mtime. On Windows, closing an fd will update file mtime.
     */
    gboolean notified_download_start = file_handle->notified_download_start;
    free_cached_file_handle (file_handle);
    file_handle = NULL;

    if (mark_file_cached (ondisk_path, &st) == 0) {
        seaf_sync_manager_update_active_path (seaf->sync_mgr, repo_id,
                                              file_path, st.mode, SYNC_STATUS_SYNCED);
        if (notified_download_start)
            send_file_download_notification ("file-download.done", repo_id, file_path);
    }

    /* File content must be on the server when it's first downloaded. */
    file_cache_mgr_set_file_uploaded (seaf->file_cache_mgr,
                                      repo_id, file_path, TRUE);

out:
    /* If server returns 5xx error, the error response will be written into the
     * file contents. In this case, remove the "corrupted" cached file.
     */
    if (http_status >= 500) {
        seaf_util_unlink (ondisk_path);
    }

    g_strfreev (key_comps);
    g_free (ondisk_path);
    if (server_info)
        seaf_sync_manager_free_server_info (server_info);
    if (repo)
        seaf_repo_unref (repo);
    if (file)
        seafile_unref (file);

    pthread_mutex_lock (&priv->task_lock);
    g_hash_table_remove (priv->cache_tasks, file_key);
    pthread_mutex_unlock (&priv->task_lock);

    if (file_handle)
        free_cached_file_handle (file_handle);

    g_free (file_key);
}

typedef enum CachedFileStatus {
    CACHED_FILE_STATUS_NOT_EXISTS = 0,
    CACHED_FILE_STATUS_FETCHING,
    CACHED_FILE_STATUS_CACHED,
    CACHED_FILE_STATUS_OUTDATED,
} CachedFileStatus;

static CachedFileStatus
check_cached_file_status (FileCacheMgrPriv *priv,
                          CachedFile *file,
                          RepoTreeStat *st)
{
    char *ondisk_path = NULL;
    char mtime_attr[64], size_attr[64];
    char file_id_attr[41];
    gssize len;
    SeafStat file_st;
    char mtime_str[16], size_str[64];
    CachedFileStatus ret = CACHED_FILE_STATUS_NOT_EXISTS;

    ondisk_path = cached_file_ondisk_path (file);

    if (!seaf_util_exists (ondisk_path)) {
        ret = CACHED_FILE_STATUS_NOT_EXISTS;
        goto out;
    }

    if (seaf_stat (ondisk_path, &file_st) < 0) {
        seaf_warning ("Failed to stat file %s:%s.\n", file->file_key, strerror(errno));
        ret = CACHED_FILE_STATUS_NOT_EXISTS;
        goto out;
    }

    /* seafile-mtime attr is set after seafile-size and file-id attrs,
     * so if seafile-mtime attr exists, the other two attrs should exist.
     */
    len = seaf_getxattr (ondisk_path, SEAFILE_MTIME_ATTR,
                         mtime_attr, sizeof(mtime_attr));
    if (len < 0) {
        ret = CACHED_FILE_STATUS_FETCHING;
        goto out;
    }

    len = seaf_getxattr (ondisk_path, SEAFILE_SIZE_ATTR,
                         size_attr, sizeof(size_attr));
    if (len < 0) {
        ret = CACHED_FILE_STATUS_FETCHING;
        goto out;
    }

    len = seaf_getxattr (ondisk_path, SEAFILE_FILE_ID_ATTR,
                         file_id_attr, sizeof(file_id_attr));
    if (len < 0) {
        ret = CACHED_FILE_STATUS_FETCHING;
        goto out;
    }

    if (strcmp (st->id, file_id_attr) == 0) {
        /* Cached file is up-to-date. */
        ret = CACHED_FILE_STATUS_CACHED;
        goto out;
    }

    snprintf (mtime_str, sizeof(mtime_str),
              "%"G_GINT64_FORMAT, (gint64)file_st.st_mtime);
    snprintf (size_str, sizeof(size_str),
              "%"G_GINT64_FORMAT, (gint64)file_st.st_size);

    if (strcmp (mtime_str, mtime_attr) == 0 &&
        strcmp (size_str, size_attr) == 0) {
        // File id is changed but mtime is not changed, try to refetch the file
        ret = CACHED_FILE_STATUS_OUTDATED;
        goto out;
    }

    // The cached file is changed locally, while the internal ID of that file
    // is also updated. This means a conflict situation. The syncing algorithm
    // tries to avoid this conflict from happening by delaying update to internal
    // file id. But if this happens, we try not to overwrite local changes.
    ret = CACHED_FILE_STATUS_CACHED;

out:
    g_free (ondisk_path);
    return ret;
}

static int remove_file_attrs (const char *path);

static int
start_cache_task (FileCacheMgrPriv *priv, CachedFile *file, RepoTreeStat *st)
{
    char *ondisk_path = NULL;
    int flags;
    CachedFileHandle *handle = NULL;
    int fd;
    int ret = 0;

    pthread_mutex_lock (&priv->task_lock);

    /* Only one fetch task can be running for each file. */
    if (!g_hash_table_lookup (priv->cache_tasks, file->file_key)) {
        CachedFileStatus status = check_cached_file_status (priv, file, st);

        if (status == CACHED_FILE_STATUS_CACHED) {
            /* If the cached file is up-to-date, don't need to fetch again. */
            goto out;
        }

        gboolean overwrite = FALSE;
        if (status == CACHED_FILE_STATUS_NOT_EXISTS ||
            status == CACHED_FILE_STATUS_OUTDATED) {
            overwrite = TRUE;
        }
        /* If cached file is in "FETCHING" status, its download was
         * interrupted by restart.
         */

        ondisk_path = cached_file_ondisk_path (file);

        if (status == CACHED_FILE_STATUS_OUTDATED) {
            remove_file_attrs (ondisk_path);
        }

        flags = (O_WRONLY | O_CREAT);
        if (overwrite)
            flags |= O_TRUNC;

        fd = seaf_util_create (ondisk_path, flags, 0664);
        if (fd < 0) {
            seaf_warning ("Failed to open file %s:%s.\n",
                          file->file_key, strerror (errno));
            ret = -1;
            goto out;
        }

        /* Only non-zero size file needs to be fetched from server. */
        if (st->size != 0) {
            handle = cached_file_handle_new ();
            handle->cached_file = file;
            handle->fd = fd;
            handle->cached_file->total_download = st->size;

            cached_file_ref (file);

            g_hash_table_replace (priv->cache_tasks, g_strdup(file->file_key), handle);
            g_thread_pool_push (priv->tpool, handle, NULL);
        } else {
            mark_file_cached (ondisk_path, st);
            close (fd);
        }
    }

out:
    pthread_mutex_unlock (&priv->task_lock);
    g_free (ondisk_path);

    return ret;
}

static int
make_parent_dir (CachedFile *cached_file)
{
    char *file_path = NULL;
    char *parent_dir = NULL;
    int ret = 0;

    file_path = cached_file_ondisk_path (cached_file);

    parent_dir = g_path_get_dirname (file_path);
    if (g_file_test (parent_dir, G_FILE_TEST_IS_DIR)) {
        goto out;
    }

    // Make sure parent dir has been created
    if (checkdir_with_mkdir (parent_dir) < 0) {
        seaf_warning ("Failed to make parent dir %s: %s.\n", parent_dir, strerror (errno));
        ret = -1;
    }
out:
    g_free (parent_dir);
    g_free (file_path);

    return ret;
}

static CachedFile *
get_cached_file (FileCacheMgrPriv *priv,
                 const char *repo_id,
                 const char *repo_uname,
                 const char *file_path,
                 RepoTreeStat *st)
{
    CachedFile *cached_file = NULL;
    char *file_key;

    file_key = g_strconcat (repo_id, "/", file_path, NULL);

    pthread_mutex_lock (&priv->cache_lock);

    cached_file = g_hash_table_lookup (priv->cached_files, file_key);
    if (!cached_file) {
        cached_file = g_new0 (CachedFile, 1);
        cached_file->file_key = file_key;
        cached_file->repo_uname = g_strdup (repo_uname);

        g_hash_table_replace (priv->cached_files, cached_file->file_key, cached_file);
        /* Keep 1 open number for internal reference. */
        cached_file_ref (cached_file);
    } else {
        g_free (file_key);

        /* If a file fetch task was cancelled before, reset this flag when the file
         * is opened again. Some applications retry the open operation after failure.
         * We have to prevent opening the file for a while after its download was
         * cancelled.
         */
        gint64 now = (gint64)time(NULL);
        if (now - cached_file->last_cancel_time < 2) {
            pthread_mutex_unlock (&priv->cache_lock);
            cached_file = NULL;
            goto out;
        }

        cached_file->force_canceled = FALSE;
        cached_file->last_cancel_time = 0;
    }

    cached_file_ref (cached_file);

    pthread_mutex_unlock (&priv->cache_lock);

    if (make_parent_dir (cached_file) < 0) {
        cached_file_unref (cached_file);
        cached_file = NULL;
        goto out;
    }

    if (start_cache_task (priv, cached_file, st) < 0) {
        seaf_warning ("Failed to start cache task for file %s in repo %s.\n",
                      file_path, repo_id);
        cached_file_unref (cached_file);
        cached_file = NULL;
    }

out:
    return cached_file;
}

void
file_cache_mgr_cache_file (FileCacheMgr *mgr, const char *repo_id, const char *path, RepoTreeStat *st)
{
    FileCacheMgrPriv *priv = mgr->priv;
    CachedFile *cached_file = NULL;
    char *file_key;
    SeafRepo *repo;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to find repo %s.\n", repo_id);
        return;
    }

    file_key = g_strconcat (repo_id, "/", path, NULL);

    pthread_mutex_lock (&priv->cache_lock);

    cached_file = g_hash_table_lookup (priv->cached_files, file_key);
    if (!cached_file) {
        cached_file = g_new0 (CachedFile, 1);
        cached_file->file_key = file_key;
        cached_file->repo_uname = g_strdup (repo->repo_uname);

        g_hash_table_replace (priv->cached_files, cached_file->file_key, cached_file);
        /* Keep 1 open number for internal reference. */
        cached_file_ref (cached_file);
    } else {
        g_free (file_key);
    }

    cached_file_ref (cached_file);

    pthread_mutex_unlock (&priv->cache_lock);

    if (make_parent_dir (cached_file) < 0) {
        goto out;
    }

    if (start_cache_task (priv, cached_file, st) < 0) {
        seaf_warning ("Failed to start cache task for file %s in repo %s.\n",
                      path, repo_id);
    }

out:
    cached_file_unref (cached_file);

    seaf_repo_unref (repo);
}

static CachedFileHandle *
open_cached_file_handle (FileCacheMgrPriv *priv,
                         const char *repo_id,
                         const char *repo_uname,
                         const char *file_path,
                         RepoTreeStat *st,
                         int flags)
{
    CachedFile *file = NULL;
    CachedFileHandle *file_handle = NULL;
    char *ondisk_path = NULL;
    int fd;

    file = get_cached_file (priv, repo_id, repo_uname, file_path, st);
    if (!file) {
        return NULL;
    }

    ondisk_path = cached_file_ondisk_path (file);

    fd = seaf_util_open (ondisk_path, O_RDWR);
    if (fd < 0) {
        seaf_warning ("Failed to open file %s:%s.\n", ondisk_path, strerror (errno));
        cached_file_unref (file);
        goto out;
    }

    file_handle = cached_file_handle_new ();
    file_handle->cached_file = file;
    file_handle->fd = fd;
    file_handle->file_size = st->size;

    if (flags & O_RDONLY)
        file_handle->is_readonly = TRUE;

out:
    g_free (ondisk_path);
    return file_handle;
}

CachedFileHandle *
file_cache_mgr_open (FileCacheMgr *mgr, const char *repo_id,
                     const char *file_path, int flags)
{
    FileCacheMgrPriv *priv = mgr->priv;
    SeafRepo *repo = NULL;
    RepoTreeStat tree_stat;
    CachedFileHandle *handle = NULL;
    int rc;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to get repo %s.\n", repo_id);
        goto out;
    }

    rc = repo_tree_stat_path (repo->tree, file_path, &tree_stat);
    if (rc < 0) {
        seaf_warning ("Failed to stat tree path %s in repo %s: %s.\n",
                      file_path, repo_id, strerror(-rc));
        goto out;
    }

    /* If we need to fetch a file from server, but network or server was
     * already down, return failure.
     */
    if ((!file_cache_mgr_is_file_cached (mgr, repo_id, file_path) ||
         file_cache_mgr_is_file_outdated (mgr, repo_id, file_path)) &&
        tree_stat.size != 0 &&
        seaf_sync_manager_is_server_disconnected (seaf->sync_mgr)) {
        return NULL;
    }

    handle = open_cached_file_handle (priv, repo_id, repo->repo_uname,
                                      file_path, &tree_stat, flags);
    if (!handle) {
        seaf_warning ("Failed to open handle to cached file %s in repo %s.\n",
                      file_path, repo_id);
    }

out:
    seaf_repo_unref (repo);
    return handle;
}

void
file_cache_mgr_close_file_handle (CachedFileHandle *file_handle)
{
    if (file_handle->is_in_root) {
        free_cached_file_handle (file_handle);
        return;
    }
        
    char *file_key = g_strdup(file_handle->cached_file->file_key);

    free_cached_file_handle (file_handle);

    cancel_fetch_task_by_file (seaf->file_cache_mgr, file_key);
    g_free (file_key);
}

#define CHECK_CACHE_INTERVAL 100000 /* 100ms */
#define READ_CACHE_TIMEOUT 5000000   /* 5 seconds. */
#define WRITE_CACHE_TIMEOUT 5000000  /* 5 seconds. */
#define RANDOM_READ_THRESHOLD 100000 /* 100KB */

static gssize
get_file_range_from_server (const char *server, const char *user,
                            const char *repo_id, const char *path, char *buf,
                            guint64 offset, size_t size)
{
    seaf_debug ("Get file range %"G_GUINT64_FORMAT"-%"G_GUINT64_FORMAT" of file %s/%s.\n",
                offset, offset+size-1, repo_id, path);

    SeafAccount *account = seaf_repo_manager_get_account (seaf->repo_mgr, server, user);
    if (!account) {
        seaf_warning ("Failed to get account.\n");
        return -1;
    }

    gssize ret = http_tx_manager_get_file_range (seaf->http_tx_mgr,
                                                  account->server,
                                                  account->token,
                                                  repo_id,
                                                  path,
                                                  buf,
                                                  offset, size);

    seaf_account_free (account);
    if (ret < 0)
        return -EIO;
    else
        return ret;
}

gssize
file_cache_mgr_read (FileCacheMgr *mgr, CachedFileHandle *handle,
                     char *buf, size_t size, guint64 offset)
{
    CachedFile *file = handle->cached_file;
    char *ondisk_path = NULL;
    char *repo_id = NULL, *path = NULL;
    char attr[64];
    SeafStat st;
    gssize ret;
    int wait_time = 0;
    SeafRepo *repo = NULL;

    ondisk_path = cached_file_ondisk_path (file);

    if (!seaf_util_exists (ondisk_path)) {
        seaf_warning ("Cached file %s does not exist when read.\n", file->file_key);
        ret = -EIO;
        goto out;
    }

    file_cache_mgr_get_file_info_from_handle (mgr, handle, &repo_id, &path);

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to get repo %s\n", repo_id);
        ret = -EIO;
        goto out;
    }

    /* Wait until the intended region is cached. */
    while (1) {
        if (seaf_getxattr (ondisk_path, SEAFILE_MTIME_ATTR, attr, sizeof(attr)) > 0)
            break;

        if (seaf_stat (ondisk_path, &st) < 0) {
            seaf_warning ("Failed to stat cache file %s: %s.\n",
                          ondisk_path, strerror (errno));
            ret = -EIO;
            goto out;
        }

        if ((offset + (guint64)size <= (guint64)handle->file_size) &&
            (offset + (guint64)size <= (guint64)st.st_size)) {
            break;
        }

        if ((offset > (guint64)st.st_size) &&
            (offset - (guint64)st.st_size > RANDOM_READ_THRESHOLD)) {
            ret = get_file_range_from_server (repo->server, repo->user, repo_id, path, buf, offset, size);
            goto out;
        }

        if (wait_time >= READ_CACHE_TIMEOUT) {
            seaf_debug ("Read cache file %s timeout.\n", ondisk_path);
            ret = -EIO;
            goto out;
        }

        /* Sleep 100ms to wait the file to be cached. */
        g_usleep (CHECK_CACHE_INTERVAL);
        wait_time += CHECK_CACHE_INTERVAL;
    }

    pthread_mutex_lock (&handle->lock);

    gint64 rc = seaf_util_lseek (handle->fd, (gint64)offset, SEEK_SET);
    if (rc < 0) {
        seaf_warning ("Failed to lseek file %s: %s.\n", ondisk_path, strerror(errno));
        ret = -errno;
        pthread_mutex_unlock (&handle->lock);
        goto out;
    }

    ret = readn (handle->fd, buf, size);
    if (ret < 0) {
        seaf_warning ("Failed to read file %s: %s.\n", ondisk_path,
                      strerror (errno));
        ret = -errno;
        pthread_mutex_unlock (&handle->lock);
        goto out;
    }

    pthread_mutex_unlock (&handle->lock);

out:
    g_free (ondisk_path);
    g_free (repo_id);
    g_free (path);
    return ret;
}

/* On Windows, read operation may be issued after close is called.
 * So sometimes when we read, the cache task may have been canceled.
 * To ensure the file will be cached, we try to start cache task
 * for every read operation. Since we make sure there can be only one
 * cache task running for each file, it won't cause problems.
 */
static CachedFile *
start_cache_task_before_read (FileCacheMgr *mgr,
                              const char *repo_id,
                              const char *path,
                              RepoTreeStat *st)
{
    FileCacheMgrPriv *priv = mgr->priv;
    char *file_key;
    CachedFile *file;

    file_key = g_strconcat (repo_id, "/", path, NULL);

    pthread_mutex_lock (&priv->cache_lock);

    file = g_hash_table_lookup (priv->cached_files, file_key);
    if (!file) {
        pthread_mutex_unlock (&priv->cache_lock);
        g_free (file_key);
        return NULL;
    }

    cached_file_ref (file);

    pthread_mutex_unlock (&priv->cache_lock);

    start_cache_task (priv, file, st);

    g_free (file_key);

    return file;
}

gssize
file_cache_mgr_read_by_path (FileCacheMgr *mgr,
                             const char *repo_id, const char *path,
                             char *buf, size_t size, guint64 offset)
{
    SeafRepo *repo = NULL;
    RepoTreeStat tree_st;
    char *ondisk_path = NULL;
    CachedFile *file = NULL;
    char attr[64];
    SeafStat st;
    int fd;
    gssize ret;
    int wait_time = 0;

    ondisk_path = g_build_path ("/", mgr->priv->base_path, repo_id, path, NULL);

    if (!seaf_util_exists (ondisk_path)) {
        seaf_warning ("Cached file %s/%s does not exist when read.\n", repo_id, path);
        ret = -EIO;
        goto out;
    }

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to get repo %s.\n", repo_id);
        ret = -ENOENT;
        goto out;
    }

    if (repo_tree_stat_path (repo->tree, path, &tree_st) < 0) {
        ret = -ENOENT;
        goto out;
    }

    file = start_cache_task_before_read (mgr, repo_id, path, &tree_st);
    if (!file) {
        ret = -EIO;
        goto out;
    }

    /* Wait until the intended region is cached. */
    while (1) {
        if (seaf_getxattr (ondisk_path, SEAFILE_MTIME_ATTR, attr, sizeof(attr)) > 0)
            break;

        if (seaf_stat (ondisk_path, &st) < 0) {
            seaf_warning ("Failed to stat cache file %s: %s.\n",
                          ondisk_path, strerror (errno));
            ret = -EIO;
            goto out;
        }

        if ((offset + (guint64)size <= (guint64)tree_st.size) &&
            (offset + (guint64)size <= (guint64)st.st_size)) {
            break;
        }

        if ((offset > (guint64)st.st_size) &&
            (offset - (guint64)st.st_size > RANDOM_READ_THRESHOLD)) {
            ret = get_file_range_from_server (repo->server, repo->user, repo_id, path, buf, offset, size);
            goto out;
        }

        if (wait_time >= READ_CACHE_TIMEOUT) {
            seaf_debug ("Read cache file %s timeout.\n", ondisk_path);
            ret = -EIO;
            goto out;
        }

        /* Sleep 100ms to wait the file to be cached. */
        g_usleep (CHECK_CACHE_INTERVAL);
        wait_time += CHECK_CACHE_INTERVAL;
    }

    fd = seaf_util_open (ondisk_path, O_RDONLY);
    if (fd < 0) {
        seaf_warning ("Failed to open cached file %s: %s\n",
                      ondisk_path, strerror(errno));
        ret = -EIO;
        goto out;
    }

    gint64 rc = seaf_util_lseek (fd, (gint64)offset, SEEK_SET);
    if (rc < 0) {
        seaf_warning ("Failed to lseek file %s: %s.\n", ondisk_path, strerror(errno));
        ret = -errno;
        close (fd);
        goto out;
    }

    ret = readn (fd, buf, size);
    if (ret < 0) {
        seaf_warning ("Failed to read file %s: %s.\n", ondisk_path,
                      strerror (errno));
        ret = -errno;
        close (fd);
        goto out;
    }

    close (fd);

out:
    seaf_repo_unref (repo);
    g_free (ondisk_path);
    cached_file_unref (file);
    return ret;
}

/* gssize */
/* file_cache_mgr_read_by_path (FileCacheMgr *mgr, */
/*                              const char *repo_id, const char *path, */
/*                              char *buf, size_t size, guint64 offset) */
/* { */
/*     char *ondisk_path = NULL; */
/*     char attr[64]; */
/*     int fd; */
/*     gssize ret; */

/*     ondisk_path = g_build_path ("/", mgr->priv->base_path, repo_id, path, NULL); */

/*     if (!seaf_util_exists (ondisk_path)) { */
/*         seaf_warning ("Cached file %s/%s does not exist when read.\n", repo_id, path); */
/*         ret = -EIO; */
/*         goto out; */
/*     } */

/*     if (seaf_getxattr (ondisk_path, SEAFILE_MTIME_ATTR, attr, sizeof(attr)) < 0) { */
/*         seaf_warning ("File %s/%s is not cached yet when read.\n", repo_id, path); */
/*         ret = -EIO; */
/*         goto out; */
/*     } */

/*     fd = seaf_util_open (ondisk_path, O_RDONLY); */
/*     if (fd < 0) { */
/*         seaf_warning ("Failed to open cached file %s: %s\n", */
/*                       ondisk_path, strerror(errno)); */
/*         ret = -EIO; */
/*         goto out; */
/*     } */

/*     gint64 rc = seaf_util_lseek (fd, (gint64)offset, SEEK_SET); */
/*     if (rc < 0) { */
/*         seaf_warning ("Failed to lseek file %s: %s.\n", ondisk_path, strerror(errno)); */
/*         ret = -errno; */
/*         close (fd); */
/*         goto out; */
/*     } */

/*     ret = readn (fd, buf, size); */
/*     if (ret < 0) { */
/*         seaf_warning ("Failed to read file %s: %s.\n", ondisk_path, */
/*                       strerror (errno)); */
/*         ret = -errno; */
/*         close (fd); */
/*         goto out; */
/*     } */

/*     close (fd); */

/* out: */
/*     g_free (ondisk_path); */
/*     return ret; */
/* } */


gssize
file_cache_mgr_write (FileCacheMgr *mgr, CachedFileHandle *handle,
                      const char *buf, size_t size, off_t offset)
{
    CachedFile *file = handle->cached_file;
    char *ondisk_path = NULL;
    char attr[64];
    gssize ret = 0;
    int wait_time = 0;

    ondisk_path = cached_file_ondisk_path (file);

    if (!seaf_util_exists (ondisk_path)) {
        seaf_warning ("Cached file %s does not exist when write.\n", file->file_key);
        ret = -EIO;
        goto out;
    }

    while (1) {
        if (seaf_getxattr (ondisk_path, SEAFILE_MTIME_ATTR, attr, sizeof(attr)) > 0)
            break;

        if (wait_time >= WRITE_CACHE_TIMEOUT) {
            seaf_warning ("Write cache file %s timeout.\n", ondisk_path);
            ret = -EIO;
            goto out;
        }

        g_usleep (CHECK_CACHE_INTERVAL);
        wait_time += CHECK_CACHE_INTERVAL;
    }

    pthread_mutex_lock (&handle->lock);

    gint64 rc;
    if (offset >= 0)
        rc = seaf_util_lseek (handle->fd, offset, SEEK_SET);
    else
        rc = seaf_util_lseek (handle->fd, 0, SEEK_END);
    if (rc < 0) {
        seaf_warning ("Failed to lseek file %s: %s.\n", ondisk_path, strerror(errno));
        ret = -errno;
        pthread_mutex_unlock (&handle->lock);
        goto out;
    }

    ret = writen (handle->fd, buf, size);
    if (ret < 0) {
        seaf_warning ("Failed to write file %s: %s.\n", ondisk_path,
                      strerror (errno));
        ret = -EIO;
        pthread_mutex_unlock (&handle->lock);
        goto out;
    }

    pthread_mutex_unlock (&handle->lock);

out:
    g_free (ondisk_path);
    return ret;
}

static int
fetch_and_truncate_file (FileCacheMgr *mgr,
                         const char *repo_id,
                         const char *file_path,
                         const char *fullpath,
                         off_t length)
{
    CachedFileHandle *handle;
    char attr[64];

    handle = file_cache_mgr_open (mgr, repo_id, file_path, 0);
    if (!handle) {
        return -EIO;
    }

    while (1) {
        if (seaf_getxattr (fullpath, SEAFILE_MTIME_ATTR, attr, sizeof(attr)) > 0)
            break;
        g_usleep (CHECK_CACHE_INTERVAL);
    }

    file_cache_mgr_close_file_handle (handle);

    int ret = seaf_truncate (fullpath, length);
    if (ret < 0) {
        ret = -errno;
        seaf_warning ("Failed to truncate %s: %s.\n", fullpath, strerror(-ret));
    }

    return ret;
}

int
file_cache_mgr_truncate (FileCacheMgr *mgr,
                         const char *repo_id,
                         const char *file_path,
                         off_t length,
                         gboolean *not_cached)
{
    FileCacheMgrPriv *priv = mgr->priv;
    char *fullpath = NULL;
    char attr[64];
    int ret = 0;

    if (not_cached)
        *not_cached = FALSE;

    fullpath = g_build_filename (priv->base_path, repo_id, file_path, NULL);

    if (!seaf_util_exists (fullpath)) {
        /* We don't need to do anything if the file is not cached and the file
         * is going to be truncated to zero size.
         */
        if (length != 0) {
            ret = fetch_and_truncate_file (mgr, repo_id, file_path, fullpath, length);
        } else {
            if (not_cached)
                *not_cached = TRUE;
        }
        goto out;
    }

    /* If cache file exists, there are two situations:
     * 1. file is still being fetched;
     * 2. file is already cached.
     * We need to wait until file is cached. Otherwise we'll interfere other
     * processes from reading/writing the file.
     * Usually case 2 is true. So we can immediately truncate the file.
     */
    while (1) {
        if (seaf_getxattr (fullpath, SEAFILE_MTIME_ATTR, attr, sizeof(attr)) > 0)
            break;
        g_usleep (CHECK_CACHE_INTERVAL);
    }

    ret = seaf_truncate (fullpath, length);
    if (ret < 0) {
        ret = -errno;
        seaf_warning ("Failed to truncate %s: %s.\n", fullpath, strerror(-ret));
    }

out:
    g_free (fullpath);
    return ret;
}

int
file_cache_mgr_unlink (FileCacheMgr *mgr, const char *repo_id,
                       const char *file_path)
{
    FileCacheMgrPriv *priv = mgr->priv;
    char *file_key = g_strconcat (repo_id, "/", file_path, NULL);
    char *ondisk_path = NULL;

    pthread_mutex_lock (&priv->cache_lock);

    /* Removing cached_file from the hash table will decrease n_open by 1.
     * When all open handle to this file is closed, the cached file structure
     * will be freed.
     * If later a file with the same name is created and opened, a new cache file
     * structure will be allocated.
     */
    g_hash_table_remove (priv->cached_files, file_key);

    /* The cached file on disk cannot be found after deletion. Access to the
     * old file path will fail since read/write will check extended attrs of
     * the ondisk cached file. This is incompatible with POSIX semantics but
     * is simpler to implement. We don't have to track changes to file paths
     * in this way.
     */
    ondisk_path = g_build_filename (priv->base_path, repo_id, file_path, NULL);
    int ret = seaf_util_unlink (ondisk_path);
    if (ret < 0 && errno != ENOENT) {
        ret = -errno;
        seaf_warning ("Failed to unlink %s: %s.\n", ondisk_path, strerror(-ret));
    }
    g_free (ondisk_path);

    pthread_mutex_unlock (&priv->cache_lock);

    g_free (file_key);

    return 0;
}

int
file_cache_mgr_rename (FileCacheMgr *mgr,
                       const char *old_repo_id,
                       const char *old_path,
                       const char *new_repo_id,
                       const char *new_path)
{
    FileCacheMgrPriv *priv = mgr->priv;
    char *ondisk_path_old = NULL, *ondisk_path_new = NULL;
    char *new_path_parent = NULL;
    int ret = 0;

    /* We don't change the file paths in the CachedFile structures. Instead we
     * just rename the ondisk cached file paths. The in-memory CachedFile structures
     * will remain "dangling" and will fail to access files. They'll be cleaned up
     * when we clean cached files.
     */

    ondisk_path_old = g_build_filename (priv->base_path, old_repo_id, old_path, NULL);
    ondisk_path_new = g_build_filename (priv->base_path, new_repo_id, new_path, NULL);

    if (!seaf_util_exists (ondisk_path_old)) {
        goto out;
    }

    new_path_parent = g_path_get_dirname (ondisk_path_new);

    if (checkdir_with_mkdir (new_path_parent) < 0) {
        seaf_warning ("[cache] Failed to create path %s: %s\n", new_path_parent, strerror(errno));
        ret = -1;
        goto out;
    }

    ret = seaf_util_rename (ondisk_path_old, ondisk_path_new);
    if (ret < 0) {
        if (errno == EACCES) {
            if (seaf_util_unlink (ondisk_path_new) < 0) {
                seaf_warning ("[cache] Failed to unlink %s before rename: %s.\n",
                              ondisk_path_new, strerror(errno));
            }
            ret = seaf_util_rename (ondisk_path_old, ondisk_path_new);
            if (ret < 0) {
                ret = -errno;
                seaf_warning ("[cache] Failed to rename %s to %s: %s\n",
                              ondisk_path_old, ondisk_path_new, strerror(errno));
            }
        }
    }

out:
    g_free (ondisk_path_old);
    g_free (ondisk_path_new);
    g_free (new_path_parent);

    return ret;
}

int
file_cache_mgr_mkdir (FileCacheMgr *mgr, const char *repo_id, const char *dir)
{
    FileCacheMgrPriv *priv = mgr->priv;
    char *ondisk_path = g_build_filename (priv->base_path, repo_id, dir, NULL);
    int ret = checkdir_with_mkdir (ondisk_path);
    if (ret < 0) {
        ret = -errno;
        seaf_warning ("[cache] Failed to create dir %s: %s\n", ondisk_path, strerror(errno));
    }
    g_free (ondisk_path);
    return ret;
}

int
file_cache_mgr_rmdir (FileCacheMgr *mgr, const char *repo_id, const char *dir)
{
    FileCacheMgrPriv *priv = mgr->priv;
    char *ondisk_path = g_build_filename (priv->base_path, repo_id, dir, NULL);
    int ret = seaf_util_rmdir (ondisk_path);
    if (ret < 0) {
        ret = -errno;
        seaf_warning ("Failed to rmdir %s: %s.\n", ondisk_path, strerror(-ret));
    }
    g_free (ondisk_path);
    return 0;
}

int
file_cache_mgr_stat_handle (FileCacheMgr *mgr,
                            CachedFileHandle *handle,
                            FileCacheStat *st)
{
    SeafStat file_st;
    int ret = 0;

    ret = seaf_fstat (handle->fd, &file_st);
    if (ret < 0) {
        return ret;
    }

    st->mtime = file_st.st_mtime;
    st->size = file_st.st_size;

    return 0;
}

int
file_cache_mgr_stat (FileCacheMgr *mgr,
                     const char *repo_id,
                     const char *path,
                     FileCacheStat *st)
{
    char *fullpath;
    SeafStat file_st;
    int ret = 0;

    fullpath = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);

    ret = seaf_stat (fullpath, &file_st);
    if (ret < 0) {
        g_free (fullpath);
        return ret;
    }

    st->mtime = file_st.st_mtime;
    st->size = file_st.st_size;

    g_free (fullpath);
    return 0;
}

int
file_cache_mgr_set_attrs (FileCacheMgr *mgr,
                          const char *repo_id,
                          const char *path,
                          gint64 mtime,
                          gint64 size,
                          const char *file_id)
{
    char *fullpath = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);
    char attr[64];
    int len;

    if (!seaf_util_exists (fullpath)) {
        seaf_warning ("File %s does not exist while trying to set its attr.\n",
                      fullpath);
        g_free (fullpath);
        return -1;
    }

    if (seaf_setxattr (fullpath, SEAFILE_FILE_ID_ATTR, file_id, 41) < 0) {
        seaf_warning ("Failed to set file-id attr for %s.\n", fullpath);
        g_free (fullpath);
        return -1;
    }

    len = snprintf (attr, sizeof(attr), "%"G_GINT64_FORMAT, size);
    if (seaf_setxattr (fullpath, SEAFILE_SIZE_ATTR, attr, len+1) < 0) {
        seaf_warning ("Failed to set seafile-size attr for %s.\n", fullpath);
        g_free (fullpath);
        return -1;
    }

    len = snprintf (attr, sizeof(attr), "%"G_GINT64_FORMAT, mtime);
    if (seaf_setxattr (fullpath, SEAFILE_MTIME_ATTR, attr, len+1) < 0) {
        seaf_warning ("Failed to set seafile-mtime attr for %s.\n", fullpath);
        g_free (fullpath);
        return -1;
    }

    g_free (fullpath);
    return 0;
}

static int
get_file_attrs (const char *path, FileCacheStat *st)
{
    char attr[64];
    gint64 mtime, size;
    char file_id[41];
    gboolean uploaded;

    if (seaf_getxattr (path, SEAFILE_MTIME_ATTR, attr, sizeof(attr)) < 0) {
        seaf_debug ("Failed to get seafile-mtime attr of %s.\n", path);
        return -1;
    }
    mtime = strtoll (attr, NULL, 10);

    if (seaf_getxattr (path, SEAFILE_SIZE_ATTR, attr, sizeof(attr)) < 0) {
        seaf_debug ("Failed to get seafile-size attr of %s.\n", path);
        return -1;
    }
    size = strtoll (attr, NULL, 10);

    if (seaf_getxattr (path, SEAFILE_FILE_ID_ATTR, file_id, sizeof(file_id)) < 0) {
        seaf_debug ("Failed to get file-id attr of %s.\n", path);
        return -1;
    }

    if (seaf_getxattr (path, SEAFILE_UPLOADED_ATTR, attr, sizeof(attr)) < 0) {
        seaf_debug ("Failed to get uploaded attr of %s.\n", path);
        uploaded = FALSE;
    }
    if (g_strcmp0(attr, "true") == 0)
        uploaded = TRUE;
    else
        uploaded = FALSE;

    st->mtime = mtime;
    st->size = size;
    memcpy (st->file_id, file_id, 40);
    st->is_uploaded = uploaded;

    return 0;
}

int
file_cache_mgr_get_attrs (FileCacheMgr *mgr,
                          const char *repo_id,
                          const char *path,
                          FileCacheStat *st)
{
    char *fullpath = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);
    int ret = get_file_attrs (fullpath, st);
    g_free (fullpath);
    return ret;
}

static int
remove_file_attrs (const char *path)
{
    seaf_removexattr (path, SEAFILE_MTIME_ATTR);
    seaf_removexattr (path, SEAFILE_SIZE_ATTR);
    seaf_removexattr (path, SEAFILE_FILE_ID_ATTR);
    return 0;
}

gboolean
file_cache_mgr_is_file_cached (FileCacheMgr *mgr,
                               const char *repo_id,
                               const char *path)
{
    char *fullpath = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);
    char attr[64];

    gboolean ret = (seaf_getxattr(fullpath, SEAFILE_MTIME_ATTR, attr, sizeof(attr)) > 0);

    g_free (fullpath);
    return ret;
}

gboolean
file_cache_mgr_is_file_outdated (FileCacheMgr *mgr,
                                 const char *repo_id,
                                 const char *path)
{
    char *fullpath = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);
    char file_id_attr[41];
    SeafRepo *repo = NULL;
    int len;
    RepoTreeStat st;
    gboolean ret;

    memset (file_id_attr, 0, sizeof (file_id_attr));
    len = seaf_getxattr (fullpath, SEAFILE_FILE_ID_ATTR,
                         file_id_attr, sizeof(file_id_attr));
    if (len < 0) {
        ret = FALSE;
        goto out;
    }

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to get repo %.8s.\n", repo_id);
        ret = FALSE;
        goto out;
    }

    if (repo_tree_stat_path (repo->tree, path, &st) < 0) {
        seaf_warning ("Failed to stat repo tree path %s in repo %s.\n",
                      path, repo_id);
        ret = FALSE;
        goto out;
    }

    ret = (strcmp (file_id_attr, st.id) != 0);

out:
    g_free (fullpath);
    if (repo)
        seaf_repo_unref (repo);
    return ret;
}

int
file_cache_mgr_set_file_uploaded (FileCacheMgr *mgr,
                                  const char *repo_id,
                                  const char *path,
                                  gboolean uploaded)
{
    char *fullpath = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);
    const char *attr;
    int ret = 0;

    if (!seaf_util_exists (fullpath)) {
        g_free (fullpath);
        return -1;
    }

    if (uploaded)
        attr = "true";
    else
        attr = "false";

    if (seaf_setxattr (fullpath, SEAFILE_UPLOADED_ATTR, attr, strlen(attr)+1) < 0)
        ret = -1;

    g_free (fullpath);
    return ret;
}

gboolean
file_cache_mgr_is_file_uploaded (FileCacheMgr *mgr,
                                 const char *repo_id,
                                 const char *path,
                                 const char *file_id)
{
    char *fullpath = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);
    char attr[64];
    gboolean ret = FALSE;
    SeafRepo *repo = NULL;
    SeafBranch *master = NULL;
    SeafCommit *commit = NULL;
    SeafDirent *dent = NULL;

    if (seaf_getxattr (fullpath, SEAFILE_UPLOADED_ATTR, attr, sizeof(attr)) < 0)
        ret = FALSE;

    if (g_strcmp0 (attr, "true") == 0)
        ret = TRUE;
    else {
        ret = FALSE;

        repo = seaf_repo_manager_get_repo(seaf->repo_mgr, repo_id);
        if (!repo) {
            seaf_warning ("Failed to get repo %.8s.\n", repo_id);
            goto out;
        }
        master = seaf_branch_manager_get_branch (seaf->branch_mgr, repo_id, "master");
        if (!master) {
            seaf_warning ("No master branch found for repo %s(%.8s).\n",
                      repo->name, repo_id);
            goto out;
        }
        commit = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                                 repo_id, repo->version,
                                                 master->commit_id);
        if (!commit) {
            seaf_warning ("Failed to get commit %.8s for repo %.8s.\n", repo->head->commit_id, repo_id);
            goto out;
        }
        dent = seaf_fs_manager_get_dirent_by_path(seaf->fs_mgr,
                                                  repo_id,
                                                  repo->version,
                                                  commit->root_id,
                                                  path,
                                                  NULL);
        if (!dent) {
            seaf_warning ("Failed to get path %s for repo %.8s.\n", path, repo_id);
            goto out;
        }

        if (strcmp (file_id, dent->id) == 0)
            ret = TRUE;
    }

out:
    seaf_repo_unref (repo);
    seaf_branch_unref (master);
    seaf_commit_unref (commit);
    seaf_dirent_free (dent);
    g_free (fullpath);
    return ret;
}

gboolean
file_cache_mgr_is_file_opened (FileCacheMgr *mgr,
                               const char *repo_id,
                               const char *path)
{
    char *file_key;
    CachedFile *file;
    gboolean ret = FALSE;

    file_key = g_strconcat (repo_id, "/", path, NULL);

    pthread_mutex_lock (&mgr->priv->cache_lock);

    file = g_hash_table_lookup (mgr->priv->cached_files, file_key);
    if (!file)
        goto out;
    if (file->n_open > 1)
        ret = TRUE;

out:
    pthread_mutex_unlock (&mgr->priv->cache_lock);
    g_free (file_key);
    return ret;
}

gboolean
file_cache_mgr_is_file_changed (FileCacheMgr *mgr,
                                const char *repo_id,
                                const char *path,
                                gboolean print_msg)
{
    FileCacheStat st, attrs;
    char *fullpath;

    fullpath = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);

    if (!seaf_util_exists (fullpath)) {
        g_free (fullpath);
        return FALSE;
    }

    g_free (fullpath);

    /* Assume unchanged if failed to access file attributes. */
    if (file_cache_mgr_stat (mgr, repo_id, path, &st) < 0)
        return FALSE;

    if (file_cache_mgr_get_attrs (mgr, repo_id, path, &attrs) < 0)
        return FALSE;

    if (st.mtime != attrs.mtime || st.size != attrs.size) {
        if (print_msg) {
            seaf_message ("File %s in repo %s is changed in file cache."
                          "Cached file mtime is %"G_GINT64_FORMAT", size is %"G_GINT64_FORMAT". "
                          "Seafile mtime is %"G_GINT64_FORMAT", size is %"G_GINT64_FORMAT".\n",
                          path, repo_id,
                          st.mtime, st.size, attrs.mtime, attrs.size);
        }
        return TRUE;
    }

    return FALSE;
}

void
file_cache_mgr_get_file_info_from_handle (FileCacheMgr *mgr,
                                          CachedFileHandle *handle,
                                          char **repo_id,
                                          char **file_path)
{
    CachedFile *file = handle->cached_file;
    char **tokens = g_strsplit (file->file_key, "/", 2);
    *repo_id = g_strdup(tokens[0]);
    *file_path = g_strdup(tokens[1]);
    g_strfreev (tokens);
}

int
file_cache_mgr_index_file (FileCacheMgr *mgr,
                           const char *repo_id,
                           int repo_version,
                           const char *file_path,
                           SeafileCrypt *crypt,
                           gboolean write_data,
                           unsigned char sha1[],
                           gboolean *changed)
{
    char *fullpath = NULL;
    SeafStat st;
    char mtime_attr[64], size_attr[64];
    char mtime_str[64], size_str[64];
    char file_id[41];
    int ret = 0;

    fullpath = g_build_filename (mgr->priv->base_path, repo_id, file_path, NULL);

    if (seaf_stat (fullpath, &st) < 0) {
        seaf_warning ("Failed to stat %s: %s.\n", fullpath, strerror(errno));
        ret = -1;
        goto out;
    }

    snprintf (mtime_str, sizeof(mtime_str), "%"G_GINT64_FORMAT, st.st_mtime);

    if (seaf_getxattr (fullpath, SEAFILE_MTIME_ATTR, mtime_attr, sizeof(mtime_attr)) < 0) {
        seaf_warning ("Failed to get seafile-mtime attr from %s.\n", fullpath);
        ret = -1;
        goto out;
    }

    snprintf (size_str, sizeof(size_str), "%"G_GINT64_FORMAT, (gint64)st.st_size);

    if (seaf_getxattr (fullpath, SEAFILE_SIZE_ATTR, size_attr, sizeof(size_attr)) < 0) {
        seaf_warning ("Failed to get seafile-size attr from %s.\n", fullpath);
        ret = -1;
        goto out;
    }

    if (seaf_getxattr (fullpath, SEAFILE_FILE_ID_ATTR, file_id, sizeof(file_id)) < 0) {
        seaf_warning ("Failed to get file-id attr from %s.\n", fullpath);
        ret = -1;
        goto out;
    }

    if (strcmp(mtime_attr, mtime_str) == 0 && strcmp(size_attr, size_str) == 0) {
        /* File is not changed. */
        *changed = FALSE;
        hex_to_rawdata (file_id, sha1, 20);
        goto out;
    }

    *changed = TRUE;

    seaf_debug ("index %s\n", file_path);
    ret = seaf_fs_manager_index_blocks (seaf->fs_mgr,
                                        repo_id,
                                        repo_version,
                                        fullpath,
                                        crypt,
                                        write_data,
                                        sha1);

out:
    g_free (fullpath);
    return ret;
}

static int
traverse_dir_recursive (FileCacheMgr *mgr,
                        const char *repo_id,
                        const char *path,
                        FileCacheTraverseCB file_cb,
                        FileCacheTraverseCB dir_cb,
                        void *user_data)
{
    FileCacheMgrPriv *priv = mgr->priv;
    SeafStat st;
    char *dir_path = NULL, *subpath, *full_subpath;
    const char *dname;
    GDir *dir;
    GError *error = NULL;
    int ret = 0;

    dir_path = g_build_filename (priv->base_path, repo_id, path, NULL);
    dir = g_dir_open (dir_path, 0, &error);
    if (!dir) {
        seaf_warning ("Failed to open dir %s: %s.\n", dir_path, error->message);
        ret = -1;
        goto out;
    }

    while ((dname = g_dir_read_name (dir)) != NULL) {
        if (path[0] != '\0')
            subpath = g_build_filename (path, dname, NULL);
        else
            subpath = g_strdup(dname);
        full_subpath = g_build_filename (dir_path, dname, NULL);
        if (seaf_stat (full_subpath, &st) < 0) {
            seaf_warning ("Failed to stat %s: %s.\n", full_subpath, strerror(errno));
            g_free (subpath);
            g_free (full_subpath);
            continue;
        }
        if (S_ISDIR(st.st_mode)) {
            if (dir_cb)
                dir_cb (repo_id, subpath, &st, user_data);
            ret = traverse_dir_recursive (mgr, repo_id, subpath, file_cb, dir_cb, user_data);
        } else if (S_ISREG(st.st_mode)) {
            if (file_cb)
                file_cb (repo_id, subpath, &st, user_data);
        }
        g_free (full_subpath);
        g_free (subpath);
    }

    g_dir_close (dir);

out:
    g_free (dir_path);
    return ret;
}

int
file_cache_mgr_traverse_path (FileCacheMgr *mgr,
                              const char *repo_id,
                              const char *path,
                              FileCacheTraverseCB file_cb,
                              FileCacheTraverseCB dir_cb,
                              void *user_data)
{
    char *fullpath;
    SeafStat st;

    fullpath = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);

    if (!seaf_util_exists (fullpath)) {
        g_free (fullpath);
        return 0;
    }

    if (seaf_stat (fullpath, &st) < 0) {
        seaf_warning ("Failed to stat %s: %s.\n", fullpath, strerror(errno));
        g_free (fullpath);
        return -1;
    }

    if (S_ISREG(st.st_mode)) {
        if (file_cb)
            file_cb (repo_id, path, &st, user_data);
    } else if (S_ISDIR(st.st_mode)) {
        if (dir_cb)
            dir_cb (repo_id, path, &st, user_data);
        traverse_dir_recursive (mgr, repo_id, path, file_cb, dir_cb, user_data);
    }

    g_free (fullpath);
    return 0;
}

/* Clean up cached files. */

int
file_cache_mgr_set_clean_cache_interval (FileCacheMgr *mgr, int seconds)
{
    mgr->priv->clean_cache_interval = seconds;

    return seafile_session_config_set_int (seaf, KEY_CLEAN_CACHE_INTERVAL, seconds);
}

int
file_cache_mgr_get_clean_cache_interval (FileCacheMgr *mgr)
{
    return mgr->priv->clean_cache_interval;
}

int
file_cache_mgr_set_cache_size_limit (FileCacheMgr *mgr, gint64 limit)
{
    mgr->priv->cache_size_limit = limit;

    return seafile_session_config_set_int64 (seaf, KEY_CACHE_SIZE_LIMIT, limit);
}

gint64
file_cache_mgr_get_cache_size_limit (FileCacheMgr *mgr)
{
    return mgr->priv->cache_size_limit;
}

struct CachedFileInfo {
    char repo_id[37];
    char *path;
    gint64 ctime;
    gint64 size;
};
typedef struct CachedFileInfo CachedFileInfo;

static void
free_cached_file_info (CachedFileInfo *info)
{
    if (!info)
        return;
    g_free (info->path);
    g_free (info);
}

static gint
cached_file_compare_fn (gconstpointer a, gconstpointer b)
{
    const CachedFileInfo *file_a = a;
    const CachedFileInfo *file_b = b;

    return file_a->ctime - file_b->ctime;
}

static void
load_cached_file_info_cb (const char *repo_id, const char *file_path,
                          SeafStat *st, void *user_data)
{
    GList **pfiles = user_data;
    CachedFileInfo *info = g_new0 (CachedFileInfo, 1);

    memcpy (info->repo_id, repo_id, 36);
    info->path = g_strdup(file_path);
    info->ctime = (gint64)st->st_ctime;
    info->size = (gint64)st->st_size;

    *pfiles = g_list_prepend (*pfiles, info);
}

static GList *
load_cached_file_list (FileCacheMgr *mgr)
{
    FileCacheMgrPriv *priv = mgr->priv;
    GDir *dir;
    GError *error = NULL;
    const char *repo_id;
    GList *ret = NULL;

    dir = g_dir_open (priv->base_path, 0, &error);
    if (error) {
        seaf_warning ("Failed to open %s: %s.\n", priv->base_path, error->message);
        return NULL;
    }

    while ((repo_id = g_dir_read_name (dir)) != NULL) {
        if (!is_uuid_valid (repo_id))
            continue;
        file_cache_mgr_traverse_path (mgr, repo_id, "",
                                      load_cached_file_info_cb, NULL, &ret);
    }

    g_dir_close (dir);

    ret = g_list_sort (ret, cached_file_compare_fn);

    return ret;
}

static gboolean
file_can_be_cleaned (FileCacheMgr *mgr, CachedFileInfo *info)
{
    char *filename;
    FileCacheStat st, attrs;

    if (info->size == 0)
        return FALSE;

    filename = g_path_get_basename (info->path);
    if (seaf_repo_manager_ignored_on_commit (filename)) {
        seaf_debug ("[cache clean] Skip file %s/%s since it is ignored on commit.\n",
                    info->repo_id, info->path);
        g_free (filename);
        return FALSE;
    }
    g_free (filename);

    /* Don't remove opened files. */
    if (file_cache_mgr_is_file_opened (mgr, info->repo_id, info->path)) {
        seaf_debug ("[cache clean] Skip file %s/%s since it is opened.\n",
                    info->repo_id, info->path);
        return FALSE;
    }

    /* Don't remove changed files. */

    if (file_cache_mgr_stat (mgr, info->repo_id, info->path, &st) < 0) {
        seaf_debug ("[cache clean] Skip file %s/%s since cannot stat it: %s.\n",
                    info->repo_id, info->path, strerror(errno));
        return FALSE;
    }

    /* But remove a file that's partially cached. */
    if (file_cache_mgr_get_attrs (mgr, info->repo_id, info->path, &attrs) < 0) {
        seaf_debug ("[cache clean] Remove partially cached file %s/%s\n",
                    info->repo_id, info->path);
        return TRUE;
    }

    if (st.mtime != attrs.mtime || st.size != attrs.size) {
        seaf_debug ("[cache clean] Skip file %s/%s since it is changed in cache. "
                    "st.mtime = %"G_GINT64_FORMAT", st.size = %"G_GINT64_FORMAT", "
                    "attrs.mtime = %"G_GINT64_FORMAT", attrs.size = %"G_GINT64_FORMAT"\n",
                    info->repo_id, info->path,
                    st.mtime, st.size, attrs.mtime, attrs.size);
        return FALSE;
    }

    /* Don't remove not-uploaded files. */
    if (!file_cache_mgr_is_file_uploaded (mgr, info->repo_id, info->path, attrs.file_id)) {
        seaf_debug ("[cache clean] Skip file %s/%s since it is not uploaded.\n",
                    info->repo_id, info->path);
        return FALSE;
    }

    return TRUE;
}

static void
do_clean_cache (FileCacheMgr *mgr)
{
    GList *files = load_cached_file_list (mgr);
    GList *ptr;
    CachedFileInfo *info;
    gint64 total_size = 0, target, removed_size = 0;
    int n_files = 0, n_removed = 0;

    for (ptr = files; ptr; ptr = ptr->next) {
        info = (CachedFileInfo *)ptr->data;
        total_size += info->size;
        ++n_files;
    }

    seaf_message ("cache size limit is %"G_GINT64_FORMAT"\n", mgr->priv->cache_size_limit);

    if (total_size < mgr->priv->cache_size_limit) {
        goto out;
    }

    seaf_message ("Cleaning cache space.\n");
    seaf_message ("%d files in cache, total size is %"G_GINT64_FORMAT"\n",
                  n_files, total_size);

    /* Clean until the total cache size reduce to 70% of limit. */
    target = (total_size - mgr->priv->cache_size_limit * 0.7);

    for (ptr = files; ptr; ptr = ptr->next) {
        info = (CachedFileInfo *)ptr->data;

        if (file_can_be_cleaned (mgr, info)) {
            seaf_debug ("[cache clean] Removing %s/%s.\n", info->repo_id, info->path);
            file_cache_mgr_unlink (mgr, info->repo_id, info->path);
            seaf_sync_manager_delete_active_path (seaf->sync_mgr,
                                                  info->repo_id,
                                                  info->path);
            removed_size += info->size;
            ++n_removed;
            if (removed_size >= target)
                break;
        }
    }

    seaf_message ("Cache cleaning done.\n");
    seaf_message ("Removed %d files, cleaned up %"G_GINT64_FORMAT" MB\n",
                  n_removed, (gint64)(removed_size/1000000));

out:
    g_list_free_full (files, (GDestroyNotify)free_cached_file_info);
}

static void *
clean_cache_worker (void *data)
{
    FileCacheMgr *mgr = data;

    /* Delay 1 minute so that it doesn't conflict with repo tree loading. */
    seaf_sleep (60);

    while (1) {
        do_clean_cache (mgr);

        seaf_sleep (mgr->priv->clean_cache_interval);
    }

    return NULL;
}

static void
do_remove_deleted_cache (FileCacheMgr *mgr)
{
    GDir *dir;
    const char *dname;
    GError *error = NULL;
    char *top_dir_path;
    char *deleted_path;

    top_dir_path = mgr->priv->base_path;

    dir = g_dir_open (top_dir_path, 0, &error);
    if (error) {
        seaf_warning ("Failed to open top file-cache dir: %s\n", error->message);
        return;
    }

    while ((dname = g_dir_read_name (dir)) != NULL) {
        if (strncmp (dname, "deleted", strlen("deleted")) == 0) {
            seaf_message ("Removing file cache %s.\n", dname);
            deleted_path = g_build_filename (top_dir_path, dname, NULL);
            seaf_rm_recursive (deleted_path);
            g_free (deleted_path);
        }
    }

    g_dir_close (dir);
}

#define REMOVE_DELETED_CACHE_INTERVAL 30

static void *
remove_deleted_cache_worker (void *data)
{
    FileCacheMgr *mgr = data;

    while (1) {
        do_remove_deleted_cache (mgr);

        seaf_sleep (REMOVE_DELETED_CACHE_INTERVAL);
    }

    return NULL;
}

static char *
gen_deleted_cache_path (const char *base_path, const char *repo_id)
{
    int n = 1;
    char *path = NULL;
    char *name = NULL;

    name = g_strdup_printf ("deleted-%s", repo_id);
    path = g_build_filename (base_path, name, NULL);
    while (g_file_test(path, G_FILE_TEST_EXISTS) && n < 10) {
        g_free (path);
        name = g_strdup_printf ("deleted-%s(%d)", repo_id, n);
        path = g_build_filename (base_path, name, NULL);
        g_free (name);
        ++n;
    }

    if (n == 10) {
        g_free (path);
        return NULL;
    }

    return path;
}

int
file_cache_mgr_delete_repo_cache (FileCacheMgr *mgr, const char *repo_id)
{
    char *cache_path = NULL, *deleted_path = NULL;
    int ret;

    cache_path = g_build_filename (mgr->priv->base_path, repo_id, NULL);

    if (!seaf_util_exists (cache_path)) {
        g_free (cache_path);
        return 0;
    }

    deleted_path = gen_deleted_cache_path (mgr->priv->base_path, repo_id);
    if (!deleted_path) {
        seaf_warning ("Too many deleted cache dirs for repo %s.\n", repo_id);
        g_free (cache_path);
        return -1;
    }

    int n = 0;
    while (1) {
        ret = seaf_util_rename (cache_path, deleted_path);
        if (ret < 0) {
            seaf_warning ("Failed to rename file cache dir %s to %s: %s.\n",
                          cache_path, deleted_path, strerror(errno));
            if (errno == EACCES && ++n < 3) {
                seaf_warning ("Retry rename\n");
                g_usleep (100000);
            } else {
                break;
            }
        } else {
            break;
        }
    }

    g_free (cache_path);
    g_free (deleted_path);
    return ret;
}

gboolean
file_cache_mgr_is_fetching_file (FileCacheMgr *mgr)
{
    return (g_hash_table_size(mgr->priv->cache_tasks) != 0);
}

gssize
file_cache_mgr_read_file_in_root (FileCacheMgr *mgr, CachedFileHandle *handle,
                                  char *buf, size_t size, off_t offset)
{
    gssize ret = 0;

    pthread_mutex_lock (&handle->lock);

    gint64 rc = seaf_util_lseek (handle->fd, offset, SEEK_SET);
    if (rc < 0) {
        seaf_warning ("Failed to lseek file: %s.\n", strerror(errno));
        ret = (-errno);
        goto out;
    }

    ret = readn (handle->fd, buf, size);
    if (ret < 0) {
        seaf_warning ("Failed to read file: %s.\n", strerror (errno));
        ret = (-errno);
        goto out;
    }

out:
    pthread_mutex_unlock (&handle->lock);
    return ret;
}

gssize
file_cache_mgr_write_file_in_root (FileCacheMgr *mgr, CachedFileHandle *handle,
                                   const char *buf, size_t size, off_t offset)
{
    gssize ret = 0;

    pthread_mutex_lock (&handle->lock);

    gint64 rc = seaf_util_lseek (handle->fd, offset, SEEK_SET);
    if (rc < 0) {
        seaf_warning ("Failed to lseek file: %s.\n", strerror(errno));
        ret = (-errno);
        goto out;
    }

    ret = writen (handle->fd, buf, size);
    if (ret < 0) {
        seaf_warning ("Failed to write file: %s.\n", strerror (errno));
        ret = (-errno);
        goto out;
    }

out:
    pthread_mutex_unlock (&handle->lock);
    return ret;
}

int
file_cache_mgr_readdir_in_root (FileCacheMgr *mgr, const char *server, const char *user, const char *path, GHashTable *dirents)
{
    char *ondisk_path;
    SeafAccount *account;
    GError *error = NULL;
    GDir *dir;
    const char *dname;
    char *subpath;
    SeafStat file_st;
    RepoTreeStat *st;
    int ret = 0;

    account = seaf_repo_manager_get_account (seaf->repo_mgr, server, user);
    if (!account) {
        return (-ENOENT);
    }

    ondisk_path = g_build_filename (mgr->priv->base_path, "root",
                                    account->unique_id, path, NULL);

    dir = g_dir_open (ondisk_path, 0, &error);
    if (error) {
        ret = (-errno);
        goto out;
    }

    while ((dname = g_dir_read_name (dir)) != NULL) {
        subpath = g_build_filename (ondisk_path, dname, NULL);
        if (seaf_stat (subpath, &file_st) < 0) {
            g_free (subpath);
            ret = (-errno);
            goto out;
        }
        st = g_new0 (RepoTreeStat, 1);
        st->mode = file_st.st_mode;
        st->mtime = file_st.st_mtime;
        st->size = file_st.st_size;
        g_hash_table_insert (dirents, g_strdup(dname), st);
        g_free (subpath);
    }

out:
    g_free (ondisk_path);
    if (dir)
        g_dir_close (dir);
    seaf_account_free (account);
    return ret;
}

int
file_cache_mgr_utimen (FileCacheMgr *mgr, const char *repo_id,
                       const char *path, time_t mtime, time_t atime)
{
    int ret = 0;
    struct utimbuf times;
    char *ondisk_path = g_build_filename (mgr->priv->base_path, repo_id, path, NULL);

    times.actime = atime;
    times.modtime = mtime;
    ret = utime (ondisk_path, &times);
    if (ret < 0) {
        seaf_warning ("Failed to utime %s: %s\n", ondisk_path, strerror(errno));
        ret = (-errno);
    }

    g_free (ondisk_path);
    return ret;
}

static void
collect_downloading_files (gpointer key, gpointer value,
                           gpointer user_data)
{
    char *file_key = key;
    char **tokens = g_strsplit (file_key, "/", 2);
    char *path = tokens[1];
    FileCacheMgrPriv *priv = seaf->file_cache_mgr->priv;
    json_t *downloading = user_data;
    json_t *info_obj = json_object ();
    CachedFile *cached_file;

    cached_file = g_hash_table_lookup (priv->cached_files, file_key);

    char *abs_path = g_build_filename (cached_file->repo_uname, path, NULL);
    json_object_set_string_member (info_obj, "file_path", abs_path);
    json_object_set_int_member (info_obj, "downloaded", cached_file->downloaded);
    json_object_set_int_member (info_obj, "total_download", cached_file->total_download);
    json_array_append_new (downloading, info_obj);

    g_free (abs_path);
    g_strfreev (tokens);
}

json_t *
file_cache_mgr_get_download_progress (FileCacheMgr *mgr)
{
    FileCacheMgrPriv *priv = mgr->priv;
    int i = 0;
    char *file_path;
    json_t *downloaded = json_array ();
    json_t *downloading = json_array ();
    json_t *progress = json_object ();

    pthread_mutex_lock (&priv->task_lock);

    g_hash_table_foreach (priv->cache_tasks, collect_downloading_files,
                          downloading);

    pthread_mutex_unlock (&priv->task_lock);

    pthread_mutex_lock (&priv->downloaded_files_lock);

    while (i < MAX_GET_FINISHED_FILES) {
        file_path = g_queue_peek_nth (priv->downloaded_files, i);
        if (file_path) {
            json_array_append_new (downloaded, json_string (file_path));
        } else {
            break;
        }
        i++;
    }

    pthread_mutex_unlock (&priv->downloaded_files_lock);

    json_object_set_new (progress, "downloaded_files", downloaded);
    json_object_set_new (progress, "downloading_files", downloading);

    return progress;
}

#if 0

#define DOWNLOADED_FILE_LIST_NAME "downloaded.json"
#define SAVE_DOWNLOADED_FILE_LIST_INTERVAL 5
#define DOWNLOADED_FILE_LIST_VERSION 1

static void *
save_downloaded_file_list_worker (void *data)
{
    char *path = g_build_filename (seaf->seaf_dir, DOWNLOADED_FILE_LIST_NAME, NULL);
    json_t *progress = NULL, *downloaded, *list;
    char *txt = NULL;
    GError *error = NULL;

    while (1) {
        seaf_sleep (SAVE_DOWNLOADED_FILE_LIST_INTERVAL);

        progress = file_cache_mgr_get_download_progress (seaf->file_cache_mgr);
        downloaded = json_object_get (progress, "downloaded_files");
        if (json_array_size(downloaded) > 0) {
            list = json_object();
            json_object_set_new (list, "version",
                                 json_integer(DOWNLOADED_FILE_LIST_VERSION));
            json_object_set (list, "downloaded_files", downloaded);
            txt = json_dumps (list, 0);
            if (!g_file_set_contents (path, txt, -1, &error)) {
                seaf_warning ("Failed to save downloaded file list: %s\n",
                              error->message);
                g_clear_error (&error);
            }
            json_decref (list);
            free (txt);
        }
        json_decref (progress);
    }

    g_free (path);
    return NULL;
}

static void
load_downloaded_file_list (FileCacheMgr *mgr)
{
    char *path = g_build_filename (seaf->seaf_dir, DOWNLOADED_FILE_LIST_NAME, NULL);
    char *txt = NULL;
    gsize len;
    GError *error = NULL;
    json_t *list = NULL, *downloaded = NULL;
    json_error_t jerror;

    if (!g_file_test (path, G_FILE_TEST_IS_REGULAR))
        return;

    if (!g_file_get_contents (path, &txt, &len, &error)) {
        seaf_warning ("Failed to read downloaded file list: %s\n", error->message);
        g_clear_error (&error);
        g_free (path);
        return;
    }

    list = json_loadb (txt, len, 0, &jerror);
    if (!list) {
        seaf_warning ("Failed to load downloaded file list: %s\n", jerror.text);
        goto out;
    }
    if (json_typeof(list) != JSON_OBJECT) {
        seaf_warning ("Bad downloaded file list format.\n");
        goto out;
    }

    downloaded = json_object_get (list, "downloaded_files");
    if (!downloaded) {
        seaf_warning ("No downloaded_files in json object.\n");
        goto out;
    }
    if (json_typeof(downloaded) != JSON_ARRAY) {
        seaf_warning ("Bad downloaded file list format.\n");
        goto out;
    }

    json_t *iter;
    size_t n = json_array_size (downloaded), i;
    for (i = 0; i < n; ++i) {
        iter = json_array_get (downloaded, i);
        if (json_typeof(iter) != JSON_STRING) {
            seaf_warning ("Bad downloaded file list format.\n");
            goto out;
        }
        g_queue_push_tail (mgr->priv->downloaded_files,
                           g_strdup(json_string_value(iter)));
    }

out:
    if (list)
        json_decref (list);
    g_free (path);
    g_free (txt);
}

#endif

int
file_cache_mgr_cancel_download (FileCacheMgr *mgr,
                                const char *server,
                                const char *user,
                                const char *full_file_path,
                                GError **error)
{
    char **tokens = NULL;
    char *category = NULL, *repo_name = NULL, *file_path = NULL;
    char *display_name = NULL;
    char *repo_id = NULL;
    char *file_key = NULL;
    CachedFile *file;
    int ret = 0;

    tokens = g_strsplit (full_file_path, "/", 3);
    if (g_strv_length (tokens) != 3) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS,
                     "invalid file path format.");
        g_strfreev (tokens);
        return -1;
    }

    category = tokens[0];
    repo_name = tokens[1];
    file_path = tokens[2];
    display_name = g_build_path ("/", category, repo_name, NULL);
    repo_id = seaf_repo_manager_get_repo_id_by_name (seaf->repo_mgr, server, user, repo_name);
    g_free (display_name);
    if (!repo_id) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS,
                     "Repo not found.");
        ret = -1;
        goto out;
    }
    file_key = g_strconcat (repo_id, "/", file_path, NULL);

    pthread_mutex_lock (&mgr->priv->task_lock);
    if (!g_hash_table_lookup (mgr->priv->cache_tasks, file_key)) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS,
                     "%s is not downloading.", file_key);
        pthread_mutex_unlock (&mgr->priv->task_lock);
        ret = -1;
        goto out;
    }
    pthread_mutex_unlock (&mgr->priv->task_lock);

    pthread_mutex_lock (&mgr->priv->cache_lock);
    file = g_hash_table_lookup (mgr->priv->cached_files, file_key);
    if (file) {
        file->force_canceled = TRUE;
        file->last_cancel_time = (gint64)time(NULL);
    }
    pthread_mutex_unlock (&mgr->priv->cache_lock);

out:
    g_strfreev (tokens);
    g_free (repo_id);
    g_free (file_key);
    return ret;
}

#define BEFORE_NOTIFY_DOWNLOAD_START 5

static void *
check_download_file_time_worker (void *data)
{
    FileCacheMgrPriv *priv = data;
    gint64 now;
    GHashTableIter iter;
    gpointer key, value;
    char *file_key;
    CachedFileHandle *handle;
    char **tokens;
    char *repo_id, *file_path;

    while (1) {
        now = (gint64)time(NULL);

        pthread_mutex_lock (&priv->task_lock);

        g_hash_table_iter_init (&iter, priv->cache_tasks);
        while (g_hash_table_iter_next (&iter, &key, &value)) {
            file_key = key;
            handle = value;

            if (!handle->notified_download_start &&
                (!handle->fetch_canceled || handle->cached_file->n_open > 2) &&
                handle->start_download_time > 0 &&
                (now - handle->start_download_time >= BEFORE_NOTIFY_DOWNLOAD_START)) {
                tokens = g_strsplit (file_key, "/", 2);
                repo_id = tokens[0];
                file_path = tokens[1];
                send_file_download_notification ("file-download.start", repo_id, file_path);
                g_strfreev (tokens);
                handle->notified_download_start = TRUE;
            }
        }

        pthread_mutex_unlock (&priv->task_lock);

        seaf_sleep (1);
    }

    return NULL;
}

#ifndef WIN32
static int
rm_file_and_active_path_recursive (const char *repo_id, const char *path, const char *active_path)
{
    SeafStat st;
    int ret = 0;
    GDir *dir;
    const char *dname;
    char *subpath;
    char *sub_active_path;
    GError *error = NULL;

    if (seaf_stat (path, &st) < 0) {
        seaf_warning ("Failed to stat %s: %s\n", path, strerror(errno));
        return -1;
    }

    if (S_ISREG(st.st_mode)) {
        ret = seaf_util_unlink (path);
        seaf_sync_manager_delete_active_path (seaf->sync_mgr, repo_id, active_path);
        return ret;
    } else if (S_ISDIR (st.st_mode)) {
        dir = g_dir_open (path, 0, &error);
        if (error) {
            seaf_warning ("Failed to open dir %s: %s\n", path, error->message);
            return -1;
        }

        while ((dname = g_dir_read_name (dir)) != NULL) {
            subpath = g_build_filename (path, dname, NULL);
            sub_active_path = g_build_filename (active_path, dname, NULL);
            ret = rm_file_and_active_path_recursive (repo_id, subpath, sub_active_path);
            g_free (subpath);
            g_free (sub_active_path);
            if (ret < 0)
                break;
        }

        g_dir_close (dir);

        if (ret == 0) {
            ret = seaf_util_rmdir (path);
        }

        return ret;
    }

    return ret;

}
int
file_cache_mgr_uncache_path (const char *repo_id, const char *path)
{
    FileCacheMgr *mgr = seaf->file_cache_mgr;
    char *top_dir_path = mgr->priv->base_path;
    char *deleted_path;
    char *canon_path = NULL;

    if (strcmp (path, "") != 0) {
        canon_path = format_path (path);
    } else {
        canon_path = g_strdup(path);
    }

    deleted_path = g_build_filename (top_dir_path, repo_id, path, NULL);
    rm_file_and_active_path_recursive (repo_id, deleted_path, canon_path);

    g_free (deleted_path);
    g_free (canon_path);

    return 0;
}
#endif
