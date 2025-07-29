/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */


#include "common.h"

#include <pthread.h>

#include "db.h"
#include "seafile-session.h"
#include "seafile-config.h"
#include "sync-mgr.h"
#include "seafile-error.h"
#include "utils.h"
#include "timer.h"
#include "change-set.h"
#include "diff-simple.h"
#include "sync-status-tree.h"

#define DEBUG_FLAG SEAFILE_DEBUG_SYNC
#include "log.h"

#ifndef SEAFILE_CLIENT_VERSION
#define SEAFILE_CLIENT_VERSION PACKAGE_VERSION
#endif

#define DEFAULT_SYNC_INTERVAL 30 /* 30s */
#define CHECK_SYNC_INTERVAL  1000 /* 1s */
#define UPDATE_TX_STATE_INTERVAL 1000 /* 1s */
#define MAX_RUNNING_SYNC_TASKS 5
#define CHECK_SERVER_LOCKED_FILES_INTERVAL 10 /* 10s */
#define CHECK_LOCKED_FILES_INTERVAL 30 /* 30s */
#define CHECK_FOLDER_PERMS_INTERVAL 30 /* 30s */
#define MAX_RESYNC_COUNT 3
#define CHECK_REPO_LIST_INTERVAL 1     /* 1s */

#define JWT_TOKEN_EXPIRE_TIME 3*24*3600 /* 3 days */

struct _HttpServerState {
    int http_version;
    gboolean checking;
    gint64 last_http_check_time;
    char *testing_host;
    /* Can be server_url or server_url:8082, depends on which one works. */
    char *effective_host;
    gboolean use_fileserver_port;

    gboolean notif_server_checked;
    gboolean notif_server_alive;

    gboolean folder_perms_not_supported;
    gint64 last_check_perms_time;
    gboolean checking_folder_perms;

    gboolean locked_files_not_supported;
    gint64 last_check_locked_files_time;
    gboolean checking_locked_files;

    gboolean immediate_check_folder_perms;
    gboolean immediate_check_locked_files;

    gboolean is_fileserver_repo_list_api_disabled;

    gint64 n_jwt_token_request;
};
typedef struct _HttpServerState HttpServerState;

struct _SyncTask;

struct _SyncInfo {
    SeafSyncManager *manager;

    char       repo_id[37];     /* the repo */
    struct _SyncTask  *current_task;

    RepoInfo *repo_info;

    int resync_count;

    gint64     last_sync_time;

    gboolean   in_sync;         /* set to FALSE when sync state is DONE or ERROR */

    gint       err_cnt;
    gboolean   in_error;        /* set to TRUE if err_cnt >= 3 */

    gboolean del_confirmation_pending;
};
typedef struct _SyncInfo SyncInfo;

struct _RepoToken {
    char token[41];
    int repo_version;
};
typedef struct _RepoToken RepoToken;
enum {
    SYNC_STATE_DONE,
    SYNC_STATE_COMMIT,
    SYNC_STATE_INIT,
    SYNC_STATE_GET_TOKEN,
    SYNC_STATE_FETCH,
    SYNC_STATE_LOAD_REPO,
    SYNC_STATE_UPLOAD,
    SYNC_STATE_ERROR,
    SYNC_STATE_CANCELED,
    SYNC_STATE_CANCEL_PENDING,
    SYNC_STATE_NUM,
};

enum {
    SYNC_ERROR_NONE,
    SYNC_ERROR_ACCESS_DENIED,
    SYNC_ERROR_NO_WRITE_PERMISSION,
    SYNC_ERROR_PERM_NOT_SYNCABLE,
    SYNC_ERROR_QUOTA_FULL,
    SYNC_ERROR_DATA_CORRUPT,
    SYNC_ERROR_START_UPLOAD,
    SYNC_ERROR_UPLOAD,
    SYNC_ERROR_START_FETCH,
    SYNC_ERROR_FETCH,
    SYNC_ERROR_NOREPO,
    SYNC_ERROR_REPO_CORRUPT,
    SYNC_ERROR_COMMIT,
    SYNC_ERROR_FILES_LOCKED,
    SYNC_ERROR_GET_SYNC_INFO,
    SYNC_ERROR_FILES_LOCKED_BY_USER,
    SYNC_ERROR_FOLDER_PERM_DENIED,
    SYNC_ERROR_DEL_CONFIRMATION_PENDING,
    SYNC_ERROR_TOO_MANY_FILES,
    SYNC_ERROR_BLOCK_MISSING,
    SYNC_ERROR_UNKNOWN,
    SYNC_ERROR_NUM,
};

struct _SyncTask {
    SyncInfo        *info;
    HttpServerState *server_state;

    int              state;
    int              error;
    int              tx_error_code;

    gboolean         is_clone;
    gboolean         uploaded;
    char            *token;
    char            *password;

    SeafRepo        *repo;  /* for convenience, only valid when in_sync. */

    char *unsyncable_path;

    char *server;
    char *user;
};
typedef struct _SyncTask SyncTask;

struct _SyncError {
    char *repo_id;
    char *repo_name;
    char *path;
    int err_id;
    gint64 timestamp;
};
typedef struct _SyncError SyncError;

typedef struct DelConfirmationResult {
    gboolean resync;
} DelConfirmationResult;

struct _SeafSyncManagerPriv {
    GHashTable *sync_infos;
    pthread_mutex_t infos_lock;

    GHashTable *repo_tokens;
    int         n_running_tasks;

    GHashTable *http_server_states;
    pthread_mutex_t server_states_lock;

    struct SeafTimer *check_sync_timer;
    struct SeafTimer *update_repo_list_timer;
    struct SeafTimer *update_tx_state_timer;

    struct SeafTimer *update_sync_status_timer;
    GHashTable *active_paths;
    pthread_mutex_t paths_lock;

    GAsyncQueue *lock_file_job_queue;

    GList *sync_errors;
    pthread_mutex_t errors_lock;

    gint server_disconnected;

    GAsyncQueue *cache_file_task_queue;

    gboolean auto_sync_enabled;

    pthread_mutex_t del_confirmation_lock;
    GHashTable *del_confirmation_tasks;
};

struct _ActivePathsInfo {
    struct SyncStatusTree *syncing_tree;
    struct SyncStatusTree *synced_tree;
};
typedef struct _ActivePathsInfo ActivePathsInfo;

struct _CacheFileTask {
    char *repo_id;
    char *path;
};
typedef struct _CacheFileTask CacheFileTask;

typedef struct SyncErrorInfo {
    int error_id;
    int error_level;
} SyncErrorInfo;

static SyncErrorInfo sync_error_info_tbl[] = {
    {
        SYNC_ERROR_ID_FILE_LOCKED_BY_APP,
        SYNC_ERROR_LEVEL_FILE,
    }, // 0
    {
        SYNC_ERROR_ID_FOLDER_LOCKED_BY_APP,
        SYNC_ERROR_LEVEL_FILE,
    }, // 1
    {
        SYNC_ERROR_ID_FILE_LOCKED,
        SYNC_ERROR_LEVEL_FILE,
    }, // 2
    {
        SYNC_ERROR_ID_INVALID_PATH,
        SYNC_ERROR_LEVEL_FILE,
    }, // 3
    {
        SYNC_ERROR_ID_INDEX_ERROR,
        SYNC_ERROR_LEVEL_FILE,
    }, // 4
    {
        SYNC_ERROR_ID_ACCESS_DENIED,
        SYNC_ERROR_LEVEL_REPO,
    }, // 5
    {
        SYNC_ERROR_ID_QUOTA_FULL,
        SYNC_ERROR_LEVEL_REPO,
    }, // 6
    {
        SYNC_ERROR_ID_NETWORK,
        SYNC_ERROR_LEVEL_NETWORK,
    }, // 7
    {
        SYNC_ERROR_ID_RESOLVE_PROXY,
        SYNC_ERROR_LEVEL_NETWORK,
    }, // 8
    {
        SYNC_ERROR_ID_RESOLVE_HOST,
        SYNC_ERROR_LEVEL_NETWORK,
    }, // 9
    {
        SYNC_ERROR_ID_CONNECT,
        SYNC_ERROR_LEVEL_NETWORK,
    }, // 10
    {
        SYNC_ERROR_ID_SSL,
        SYNC_ERROR_LEVEL_NETWORK,
    }, // 11
    {
        SYNC_ERROR_ID_TX,
        SYNC_ERROR_LEVEL_NETWORK,
    }, // 12
    {
        SYNC_ERROR_ID_TX_TIMEOUT,
        SYNC_ERROR_LEVEL_NETWORK,
    }, // 13
    {
        SYNC_ERROR_ID_UNHANDLED_REDIRECT,
        SYNC_ERROR_LEVEL_NETWORK,
    }, // 14
    {
        SYNC_ERROR_ID_SERVER,
        SYNC_ERROR_LEVEL_REPO,
    }, // 15
    {
        SYNC_ERROR_ID_LOCAL_DATA_CORRUPT,
        SYNC_ERROR_LEVEL_REPO,
    }, // 16
    {
        SYNC_ERROR_ID_WRITE_LOCAL_DATA,
        SYNC_ERROR_LEVEL_REPO,
    }, // 17
    {
        SYNC_ERROR_ID_PERM_NOT_SYNCABLE,
        SYNC_ERROR_LEVEL_FILE,
    }, // 18
    {
        SYNC_ERROR_ID_NO_WRITE_PERMISSION,
        SYNC_ERROR_LEVEL_REPO,
    }, // 19
    {
        SYNC_ERROR_ID_FOLDER_PERM_DENIED,
        SYNC_ERROR_LEVEL_FILE,
    }, // 20
    {
        SYNC_ERROR_ID_PATH_END_SPACE_PERIOD,
        SYNC_ERROR_LEVEL_FILE,
    }, // 21
    {
        SYNC_ERROR_ID_PATH_INVALID_CHARACTER,
        SYNC_ERROR_LEVEL_FILE,
    }, // 22
    {
        SYNC_ERROR_ID_UPDATE_TO_READ_ONLY_REPO,
        SYNC_ERROR_LEVEL_FILE,
    }, // 23
    {
        SYNC_ERROR_ID_CONFLICT,
        SYNC_ERROR_LEVEL_FILE,
    }, // 24
    {
        SYNC_ERROR_ID_UPDATE_NOT_IN_REPO,
        SYNC_ERROR_LEVEL_FILE,
    }, // 25
    {
        SYNC_ERROR_ID_LIBRARY_TOO_LARGE,
        SYNC_ERROR_LEVEL_REPO,
    }, // 26
    {
        SYNC_ERROR_ID_MOVE_NOT_IN_REPO,
        SYNC_ERROR_LEVEL_FILE,
    }, // 27
    {
        SYNC_ERROR_ID_DEL_CONFIRMATION_PENDING,
        SYNC_ERROR_LEVEL_REPO,
    }, // 28
    {
        SYNC_ERROR_ID_INVALID_PATH_ON_WINDOWS,
        SYNC_ERROR_LEVEL_FILE,
    }, // 29
    {
        SYNC_ERROR_ID_TOO_MANY_FILES,
        SYNC_ERROR_LEVEL_REPO,
    }, // 30
    {
        SYNC_ERROR_ID_BLOCK_MISSING,
        SYNC_ERROR_LEVEL_REPO,
    }, // 31
    {
        SYNC_ERROR_ID_CHECKOUT_FILE,
        SYNC_ERROR_LEVEL_FILE,
    }, // 32
    {
        SYNC_ERROR_ID_CASE_CONFLICT,
        SYNC_ERROR_LEVEL_FILE,
    }, // 33
    {
        SYNC_ERROR_ID_GENERAL_ERROR,
        SYNC_ERROR_LEVEL_REPO,
    }, // 34
    {
        SYNC_ERROR_ID_NO_ERROR,
        SYNC_ERROR_LEVEL_REPO,
    }, // 35
};

int
sync_error_level (int error)
{
    g_return_val_if_fail ((error >= 0 && error < SYNC_ERROR_ID_NO_ERROR), -1);

    return sync_error_info_tbl[error].error_level;
}

static const char *ignore_table[] = {
    /* tmp files under Linux */
    "*~",
    /* Seafile's backup file */
    "*.sbak",
    /* Emacs tmp files */
    "#*#",
    /* windows image cache */
    "Thumbs.db",
    /* For Mac */
    ".DS_Store",
    "._*",
    ".fuse_hidden*",
    NULL,
};

static GPatternSpec **ignore_patterns;
static GPatternSpec* office_temp_ignore_patterns[5];

static int update_repo_list_pulse (void *vmanager);

static int auto_sync_pulse (void *vmanager);

static int update_tx_state_pulse (void *vmanager);

static SyncInfo*
get_sync_info (SeafSyncManager *manager, const char *repo_id)
{
    SyncInfo *info;

    pthread_mutex_lock (&manager->priv->infos_lock);

    info = g_hash_table_lookup (manager->priv->sync_infos, repo_id);
    if (info)
        goto out;

    info = g_new0 (SyncInfo, 1);
    info->manager = manager;
    memcpy (info->repo_id, repo_id, 36);
    g_hash_table_insert (manager->priv->sync_infos, info->repo_id, info);

out:
    pthread_mutex_unlock (&manager->priv->infos_lock);
    return info;
}

SyncInfo *
seaf_sync_manager_get_sync_info (SeafSyncManager *mgr,
                                 const char *repo_id)
{
    return g_hash_table_lookup (mgr->priv->sync_infos, repo_id);
}

void
seaf_sync_manager_set_last_sync_time (SeafSyncManager *mgr,
                                      const char *repo_id,
                                      gint64 last_sync_time)
{
    SyncInfo *info;
    info = get_sync_info (mgr, repo_id);
    info->last_sync_time = last_sync_time;
}

static inline gboolean
has_trailing_space_or_period (const char *path)
{
    int len = strlen(path);
    if (path[len - 1] == ' ' || path[len - 1] == '.') {
        return TRUE;
    }

    return FALSE;
}

static gboolean
seaf_sync_manager_ignored_on_checkout_on_windows (const char *file_path, IgnoreReason *ignore_reason)
{
    gboolean ret = FALSE;

    static char illegals[] = {'\\', ':', '*', '?', '"', '<', '>', '|', '\b', '\t'};
    char **components = g_strsplit (file_path, "/", -1);
    int n_comps = g_strv_length (components);
    int j = 0;
    char *file_name;
    int i;
    char c;

    for (; j < n_comps; ++j) {
        file_name = components[j];

        if (has_trailing_space_or_period (file_name)) {
            /* Ignore files/dir whose path has trailing spaces. It would cause
             * problem on windows. */
            /* g_debug ("ignore '%s' which contains trailing space in path\n", path); */
            ret = TRUE;
            if (ignore_reason)
                *ignore_reason = IGNORE_REASON_END_SPACE_PERIOD;
            goto out;
        }

        for (i = 0; i < G_N_ELEMENTS(illegals); i++) {
            if (strchr (file_name, illegals[i])) {
                ret = TRUE;
                if (ignore_reason)
                    *ignore_reason = IGNORE_REASON_INVALID_CHARACTER;
                goto out;
            }
        }

        for (c = 1; c <= 31; c++) {
            if (strchr (file_name, c)) {
                ret = TRUE;
                if (ignore_reason)
                    *ignore_reason = IGNORE_REASON_INVALID_CHARACTER;
                goto out;
            }
        }
    }

out:
    g_strfreev (components);

    return ret;
}

static HttpServerState *
get_http_server_state (SeafSyncManagerPriv *priv, const char *fileserver_addr)
{
    HttpServerState *state;

    pthread_mutex_lock (&priv->server_states_lock);
    state = g_hash_table_lookup (priv->http_server_states,
                                 fileserver_addr);
    pthread_mutex_unlock (&priv->server_states_lock);

    if (state && state->http_version == 0) {
        return NULL;
    }
    return state;
}

static const char *sync_state_str[] = {
    "synchronized",
    "committing",
    "get sync info",
    "get token",
    "downloading",
    "load repo",
    "uploading",
    "error",
    "canceled",
    "cancel pending"
};

static const char *sync_error_str[] = {
    "Success",
    "You do not have permission to access this library",
    "Do not have write permission to the library",
    "Do not have permission to sync the library",
    "The storage space of the library owner has been used up",
    "Internal data corrupted.",
    "Failed to start upload.",
    "Error occurred in upload.",
    "Failed to start download.",
    "Error occurred in download.",
    "No such library on server.",
    "Library is damaged on server.",
    "Failed to index files.",
    "Files are locked by other application",
    "Failed to get sync info from server.",
    "File is locked by another user.",
    "Update to file denied by folder permission setting.",
    "Waiting for confirmation to delete files.",
    "Too many files in library.",
    "Failed to upload file blocks.",
    "Unknown error.",
};

static void
sync_task_free (SyncTask *task)
{
    if (task->is_clone) {
        g_free (task->server);
        g_free (task->user);
    }
    g_free (task->token);
    g_free (task);
}

static SyncTask *
sync_task_new (SyncInfo *info,
               HttpServerState *server_state,
               const char *server,
               const char *user,
               const char *token,
               gboolean is_clone)
{
    SyncTask *task = g_new0 (SyncTask, 1);
    SeafRepo *repo = NULL;

    if (!is_clone) {
        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, info->repo_id);
        if (!repo) {
            seaf_warning ("Failed to get repo %s.\n", info->repo_id);
            return NULL;
        }
    }

    task->info = info;
    task->server_state = server_state;
    task->token = g_strdup(token);
    task->is_clone = is_clone;
    if (!is_clone)
        task->repo = repo;
    if (is_clone) {
        task->server = g_strdup (server);
        task->user = g_strdup (user);
    }

    info->in_sync = TRUE;
    info->last_sync_time = time(NULL);
    ++(info->manager->priv->n_running_tasks);

    /* Free the last task when a new task is started.
     * This way we can always get the state of the last task even
     * after it's done.
     */
    if (task->info->current_task)
        sync_task_free (task->info->current_task);
    task->info->current_task = task;

    return task;
}

static void
send_sync_error_notification (const char *repo_id,
                              const char *repo_name,
                              const char *path,
                              int err_id)
{
    json_t *msg;

    msg = json_object ();
    json_object_set_new (msg, "type", json_string("sync.error"));
    if (repo_id)
        json_object_set_new (msg, "repo_id", json_string(repo_id));
    if (repo_name)
        json_object_set_new (msg, "repo_name", json_string(repo_name));
    if (path)
        json_object_set_new (msg, "path", json_string(path));
    json_object_set_new (msg, "err_id", json_integer(err_id));

    mq_mgr_push_msg (seaf->mq_mgr, SEADRIVE_NOTIFY_CHAN, msg);
}

static void
record_sync_error (SeafSyncManager *mgr,
                   const char *repo_id, const char *repo_name,
                   const char *path, int err_id)
{
    GList *errors = mgr->priv->sync_errors, *ptr;
    SyncError *err, *new_err;
    gboolean found = FALSE;

    // record sync error to database. 
    seaf_repo_manager_record_sync_error (seaf->repo_mgr, repo_id, repo_name, path, err_id);

    pthread_mutex_lock (&mgr->priv->errors_lock);

    for (ptr = errors; ptr; ptr = ptr->next) {
        err = ptr->data;
        if (g_strcmp0 (err->repo_id, repo_id) == 0 &&
            g_strcmp0 (err->path, path) == 0) {
            found = TRUE;
            if (err->err_id != err_id) {
                err->err_id = err_id;
                send_sync_error_notification (repo_id, repo_name, path, err_id);
            }
            err->timestamp = (gint64)time(NULL);
            break;
        }
    }

    if (!found) {
        new_err = g_new0 (SyncError, 1);
        new_err->repo_id = g_strdup(repo_id);
        new_err->repo_name = g_strdup(repo_name);
        new_err->path = g_strdup(path);
        new_err->err_id = err_id;
        new_err->timestamp = (gint64)time(NULL);
        mgr->priv->sync_errors = g_list_prepend (mgr->priv->sync_errors, new_err);

        send_sync_error_notification (repo_id, repo_name, path, err_id);
    }

    pthread_mutex_unlock (&mgr->priv->errors_lock);
}

static void
remove_sync_error (SeafSyncManager *mgr, const char *repo_id, const char *path)
{
    GList *ptr;
    SyncError *err;
    int err_level = -1;

    pthread_mutex_lock (&mgr->priv->errors_lock);

    for (ptr = mgr->priv->sync_errors; ptr; ptr = ptr->next) {
        err = ptr->data;
        err_level = sync_error_level (err->err_id);
        // Only delete network error.
        if (err_level != SYNC_ERROR_LEVEL_NETWORK) {
            continue;
        }
        if (g_strcmp0 (err->repo_id, repo_id) == 0 &&
            g_strcmp0 (err->path, path) == 0) {
            mgr->priv->sync_errors = g_list_delete_link (mgr->priv->sync_errors, ptr);
            g_free (err->repo_id);
            g_free (err->repo_name);
            g_free (err->path);
            g_free (err);
            break;
        }
    }

    pthread_mutex_unlock (&mgr->priv->errors_lock);
}

json_t *
seaf_sync_manager_list_sync_errors (SeafSyncManager *mgr)
{
    GList *ptr;
    SyncError *err;
    json_t *array, *obj;

    array = json_array ();

    pthread_mutex_lock (&mgr->priv->errors_lock);

    for (ptr = mgr->priv->sync_errors; ptr; ptr = ptr->next) {
        err = ptr->data;
        obj = json_object ();
        if (err->repo_id)
            json_object_set_new (obj, "repo_id", json_string(err->repo_id));
        if (err->repo_name)
            json_object_set_new (obj, "repo_name", json_string(err->repo_name));
        if (err->path)
            json_object_set_new (obj, "path", json_string(err->path));
        json_object_set_new (obj, "err_id", json_integer(err->err_id));
        json_object_set_new (obj, "timestamp", json_integer(err->timestamp));
        json_array_append_new (array, obj);
    }

    pthread_mutex_unlock (&mgr->priv->errors_lock);

    return array;
}

static int
transfer_error_to_error_id (int http_tx_error)
{
    switch (http_tx_error) {
    case HTTP_TASK_ERR_NET:
        return SYNC_ERROR_ID_NETWORK;
    case HTTP_TASK_ERR_RESOLVE_PROXY:
        return SYNC_ERROR_ID_RESOLVE_PROXY;
    case HTTP_TASK_ERR_RESOLVE_HOST:
        return SYNC_ERROR_ID_RESOLVE_HOST;
    case HTTP_TASK_ERR_CONNECT:
        return SYNC_ERROR_ID_CONNECT;
    case HTTP_TASK_ERR_SSL:
        return SYNC_ERROR_ID_SSL;
    case HTTP_TASK_ERR_TX:
        return SYNC_ERROR_ID_TX;
    case HTTP_TASK_ERR_TX_TIMEOUT:
        return SYNC_ERROR_ID_TX_TIMEOUT;
    case HTTP_TASK_ERR_UNHANDLED_REDIRECT:
        return SYNC_ERROR_ID_UNHANDLED_REDIRECT;
    case HTTP_TASK_ERR_SERVER:
        return SYNC_ERROR_ID_SERVER;
    case HTTP_TASK_ERR_BAD_LOCAL_DATA:
        return SYNC_ERROR_ID_LOCAL_DATA_CORRUPT;
    case HTTP_TASK_ERR_WRITE_LOCAL_DATA:
        return SYNC_ERROR_ID_WRITE_LOCAL_DATA;
    case HTTP_TASK_ERR_LIBRARY_TOO_LARGE:
        return SYNC_ERROR_ID_LIBRARY_TOO_LARGE;
    default:
        return SYNC_ERROR_ID_GENERAL_ERROR;
    }
}

/* Check the notify setting by user.  */
static gboolean
need_notify_sync (SeafRepo *repo)
{
    char *notify_setting = seafile_session_config_get_string(seaf, "notify_sync");
    if (notify_setting == NULL) {
        seafile_session_config_set_string(seaf, "notify_sync", "on");
        return TRUE;
    }

    gboolean result = (g_strcmp0(notify_setting, "on") == 0);
    g_free (notify_setting);
    return result;
}

static gboolean
find_meaningful_commit (SeafCommit *commit, void *data, gboolean *stop)
{
    SeafCommit **p_head = data;

    if (commit->second_parent_id && commit->new_merge && !commit->conflict)
        return TRUE;

    *stop = TRUE;
    seaf_commit_ref (commit);
    *p_head = commit;
    return TRUE;
}

static void
notify_sync (SeafRepo *repo)
{
    SeafCommit *head = NULL;

    if (!seaf_commit_manager_traverse_commit_tree_truncated (seaf->commit_mgr,
                                                             repo->id, repo->version,
                                                             repo->head->commit_id,
                                                             find_meaningful_commit,
                                                             &head, FALSE)) {
        seaf_warning ("Failed to traverse commit tree of %.8s.\n", repo->id);
        return;
    }
    if (!head)
        return;

    json_t *msg = json_object ();
    if (!repo->partial_commit_mode)
        json_object_set_string_member (msg, "type", "sync.done");
    else
        json_object_set_string_member (msg, "type", "sync.multipart_upload");
    json_object_set_string_member (msg, "repo_id", repo->id);
    json_object_set_string_member (msg, "repo_name", repo->name);
    json_object_set_string_member (msg, "commit_id", head->commit_id);
    json_object_set_string_member (msg, "parent_commit_id", head->parent_id);
    json_object_set_string_member (msg, "commit_desc", head->desc);

    mq_mgr_push_msg (seaf->mq_mgr, SEADRIVE_NOTIFY_CHAN, msg);

    seaf_commit_unref (head);
}

#define IN_ERROR_THRESHOLD 3

static gboolean
is_permanent_error (SyncTask *task)
{
    if (task->error == SYNC_ERROR_ACCESS_DENIED ||
        task->error == SYNC_ERROR_NO_WRITE_PERMISSION ||
        task->error == SYNC_ERROR_FOLDER_PERM_DENIED ||
        task->error == SYNC_ERROR_PERM_NOT_SYNCABLE ||
        task->error == SYNC_ERROR_QUOTA_FULL ||
        task->error == SYNC_ERROR_TOO_MANY_FILES ||
        task->error == SYNC_ERROR_BLOCK_MISSING) {
        return TRUE;
    }
    return FALSE;
}

static void
update_sync_info_error_state (SyncTask *task, int new_state)
{
    SyncInfo *info = task->info;

    if (new_state == SYNC_STATE_ERROR && is_permanent_error (task)) {
        info->err_cnt++;
        if (info->err_cnt >= IN_ERROR_THRESHOLD) {
            info->in_error = TRUE;
            /* Only delete a local repo after 3 consecutive permission errors.
             * This prevents mistakenly re-syncing a repo on temporary database
             * errors in the server.
             */
            /* TODO: Currently, when there is a database error on the server when
             * checking permissions, HTTP_FORBIDDEN is returned. On massive database
             * errors, it'll cause all clients to un-sync all repos. To prevent
             * confusing users, we don't un-sync repos automatically for the mean
             * time.
             */
            /* seaf_repo_manager_mark_repo_deleted (seaf->repo_mgr, task->repo, FALSE); */
        }
    } else if (info->err_cnt > 0) {
        info->err_cnt = 0;
        info->in_error = FALSE;
    }

    if (task->tx_error_code == HTTP_TASK_ERR_LIBRARY_TOO_LARGE)
        info->in_error = TRUE;
}

static inline void
transition_sync_state (SyncTask *task, int new_state)
{
    g_return_if_fail (new_state >= 0 && new_state < SYNC_STATE_NUM);

    if (task->state != new_state) {
        seaf_message ("Repo '%s' sync state transition from '%s' to '%s'.\n",
                      task->info->repo_info->name,
                      sync_state_str[task->state],
                      sync_state_str[new_state]);

        if ((task->state == SYNC_STATE_INIT && task->uploaded &&
             new_state == SYNC_STATE_DONE &&
             need_notify_sync(task->repo)))
            notify_sync (task->repo);

        task->state = new_state;
        if (new_state == SYNC_STATE_DONE ||
            new_state == SYNC_STATE_CANCELED ||
            new_state == SYNC_STATE_ERROR) {
            task->info->in_sync = FALSE;
            --(task->info->manager->priv->n_running_tasks);
            update_sync_info_error_state (task, new_state);

            if (new_state == SYNC_STATE_DONE)
                remove_sync_error (seaf->sync_mgr, task->info->repo_info->id, NULL);

            if (task->repo)
                seaf_repo_unref (task->repo);
        }
    }
}

void
seaf_sync_manager_set_task_error (SyncTask *task, int error)
{
    g_return_if_fail (error >= 0 && error < SYNC_ERROR_NUM);

    if (task->state != SYNC_STATE_ERROR) {
        seaf_message ("Repo '%s' sync state transition from %s to '%s': '%s'.\n",
                      task->info->repo_info->name,
                      sync_state_str[task->state],
                      sync_state_str[SYNC_STATE_ERROR],
                      sync_error_str[error]);
        task->state = SYNC_STATE_ERROR;
        task->error = error;
        task->info->in_sync = FALSE;
        --(task->info->manager->priv->n_running_tasks);
        update_sync_info_error_state (task, SYNC_STATE_ERROR);

        int sync_error_id;
        if (task->error == SYNC_ERROR_ACCESS_DENIED)
            sync_error_id = SYNC_ERROR_ID_ACCESS_DENIED;
        else if (task->error == SYNC_ERROR_NO_WRITE_PERMISSION)
            sync_error_id = SYNC_ERROR_ID_NO_WRITE_PERMISSION;
        else if (task->error == SYNC_ERROR_PERM_NOT_SYNCABLE)
            sync_error_id = SYNC_ERROR_ID_PERM_NOT_SYNCABLE;
        else if (task->error == SYNC_ERROR_QUOTA_FULL)
            sync_error_id = SYNC_ERROR_ID_QUOTA_FULL;
        else if (task->error == SYNC_ERROR_DATA_CORRUPT)
            sync_error_id = SYNC_ERROR_ID_LOCAL_DATA_CORRUPT;
        else if (task->error == SYNC_ERROR_FILES_LOCKED_BY_USER)
            sync_error_id = SYNC_ERROR_ID_FILE_LOCKED;
        else if (task->error == SYNC_ERROR_FOLDER_PERM_DENIED)
            sync_error_id = SYNC_ERROR_ID_FOLDER_PERM_DENIED;
        else if (task->error == SYNC_ERROR_DEL_CONFIRMATION_PENDING)
            sync_error_id = SYNC_ERROR_ID_DEL_CONFIRMATION_PENDING;
        else if (task->error == SYNC_ERROR_TOO_MANY_FILES)
            sync_error_id = SYNC_ERROR_ID_TOO_MANY_FILES;
        else if (task->error == SYNC_ERROR_BLOCK_MISSING)
            sync_error_id = SYNC_ERROR_ID_BLOCK_MISSING;
        else if (task->tx_error_code > 0)
            sync_error_id = transfer_error_to_error_id (task->tx_error_code);
        else
            sync_error_id = SYNC_ERROR_ID_GENERAL_ERROR;
        record_sync_error (seaf->sync_mgr,
                           task->info->repo_info->id,
                           task->info->repo_info->name,
                           task->unsyncable_path,
                           sync_error_id);

        /* If local metadata is corrupted, remove local repo and resync later. */
        if (sync_error_id == SYNC_ERROR_ID_LOCAL_DATA_CORRUPT) {
            if (task->info->resync_count < MAX_RESYNC_COUNT) {
                seaf_message ("Repo %s(%s) local metadata is corrupted. Remove and resync later.\n",
                              task->repo->name, task->repo->id);
                seaf_repo_manager_mark_repo_deleted (seaf->repo_mgr, task->repo, FALSE);
                ++(task->info->resync_count);
            }
        }

        if (task->repo)
            seaf_repo_unref (task->repo);
    }
}

static void
active_paths_info_free (ActivePathsInfo *info);

SeafSyncManager*
seaf_sync_manager_new (SeafileSession *seaf)
{
    SeafSyncManager *mgr = g_new0 (SeafSyncManager, 1);
    mgr->priv = g_new0 (SeafSyncManagerPriv, 1);    
    mgr->seaf = seaf;

    mgr->priv->sync_infos = g_hash_table_new (g_str_hash, g_str_equal);
    pthread_mutex_init (&mgr->priv->infos_lock, NULL);

    mgr->priv->repo_tokens = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                    g_free, g_free);
    mgr->priv->http_server_states = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                           g_free, g_free);
    pthread_mutex_init (&mgr->priv->server_states_lock, NULL);

    gboolean exists;
    int download_limit = seafile_session_config_get_int (seaf,
                                                         KEY_DOWNLOAD_LIMIT,
                                                         &exists);
    if (exists)
        mgr->download_limit = download_limit;

    int upload_limit = seafile_session_config_get_int (seaf,
                                                       KEY_UPLOAD_LIMIT,
                                                       &exists);
    if (exists)
        mgr->upload_limit = upload_limit;

    mgr->priv->active_paths = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                     g_free,
                                                     (GDestroyNotify)active_paths_info_free);
    pthread_mutex_init (&mgr->priv->paths_lock, NULL);

    ignore_patterns = g_new0 (GPatternSpec *, G_N_ELEMENTS(ignore_table));
    int i;
    for (i = 0; ignore_table[i] != NULL; i++) {
        ignore_patterns[i] = g_pattern_spec_new (ignore_table[i]);
    }

    office_temp_ignore_patterns[0] = g_pattern_spec_new("~$*");
    /* for files like ~WRL0001.tmp for docx and *.tmp for xlsx and pptx */
    office_temp_ignore_patterns[1] = g_pattern_spec_new("*.tmp");
    office_temp_ignore_patterns[2] = g_pattern_spec_new(".~lock*#");
    /* for temporary files of WPS Office */
    office_temp_ignore_patterns[3] = g_pattern_spec_new(".~*");
    office_temp_ignore_patterns[4] = NULL;

    mgr->priv->del_confirmation_tasks = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                           g_free,
                                                           g_free);
    pthread_mutex_init (&mgr->priv->del_confirmation_lock, NULL);

    mgr->priv->lock_file_job_queue = g_async_queue_new ();

    pthread_mutex_init (&mgr->priv->errors_lock, NULL);

    mgr->priv->cache_file_task_queue = g_async_queue_new ();

    mgr->priv->auto_sync_enabled = TRUE;

    return mgr;
}

static void
load_sync_errors (SeafileSession *seaf, SeafSyncManager *mgr)
{
    json_t *array = seaf_repo_manager_list_sync_errors (seaf->repo_mgr, 0, 50);
    if (!array) {
        return;
    }

    int i;
    SyncError *err;
    json_t *object, *member;
    size_t n = json_array_size (array);
    for (i = 0; i < n; ++i) {
        err = g_new0 (SyncError, 1);
        object = json_array_get (array, i);
        member = json_object_get (object, "repo_id");
        if (member) {
            err->repo_id = g_strdup (json_string_value(member));
        }
        member = json_object_get (object, "repo_name");
        if (member) {
            err->repo_name = g_strdup (json_string_value(member));
        }
        member = json_object_get (object, "path");
        if (member) {
            err->path = g_strdup (json_string_value(member));
        }
        member = json_object_get (object, "err_id");
        if (member) {
            err->err_id = json_integer_value(member);
        }
        member = json_object_get (object, "timestamp");
        if (member) {
            err->timestamp = json_integer_value(member);
        }
        mgr->priv->sync_errors = g_list_prepend (mgr->priv->sync_errors, err);
    }

    json_decref (array);
}

int
seaf_sync_manager_init (SeafSyncManager *mgr)
{
    load_sync_errors (seaf, mgr);

    return 0;
}

static void
on_repo_http_fetched (SeafileSession *seaf,
                      HttpTxTask *tx_task,
                      SeafSyncManager *mgr);

static void
on_repo_http_uploaded (SeafileSession *seaf,
                       HttpTxTask *tx_task,
                       SeafSyncManager *mgr);

static void* check_space_usage_thread (void *data);
static void* lock_file_worker (void *data);
static void* cache_file_task_worker (void *data);

int
seaf_sync_manager_start (SeafSyncManager *mgr)
{
    mgr->priv->check_sync_timer = seaf_timer_new (
        auto_sync_pulse, mgr, CHECK_SYNC_INTERVAL);

    mgr->priv->update_repo_list_timer = seaf_timer_new (
           update_repo_list_pulse, mgr, CHECK_REPO_LIST_INTERVAL*1000);

    mgr->priv->update_tx_state_timer = seaf_timer_new (
           update_tx_state_pulse, mgr, UPDATE_TX_STATE_INTERVAL);

    g_signal_connect (seaf, "repo-http-fetched",
                      (GCallback)on_repo_http_fetched, mgr);
    g_signal_connect (seaf, "repo-http-uploaded",
                      (GCallback)on_repo_http_uploaded, mgr);

    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    int rc = pthread_create (&tid, &attr, check_space_usage_thread, NULL);
    if (rc != 0) {
        seaf_warning ("Failed to create check space thread: %s.\n", strerror(rc));
    }

    rc = pthread_create (&tid, &attr, lock_file_worker, mgr->priv->lock_file_job_queue);
    if (rc != 0) {
        seaf_warning ("Failed to create lock file thread: %s.\n", strerror(rc));
    }

    rc = pthread_create (&tid, NULL, cache_file_task_worker, mgr->priv->cache_file_task_queue);
    if (rc != 0) {
        seaf_warning ("Failed to create cache file task worker thread: %s.\n", strerror(rc));
    }

    return 0;
}

/* Synchronization framework. */

typedef struct _FolderPermInfo {
    HttpServerState *server_state;
    SyncTask *task;
    char *server;
    char *user;
} FolderPermInfo;

static void
folder_perm_info_free (FolderPermInfo *info)
{
    if (!info)
        return;
    g_free (info->server);
    g_free (info->user);
    g_free (info);
}

static void
check_account_space_usage (SeafAccount *account)
{
    gint64 total, used;

    if (http_tx_manager_api_get_space_usage (seaf->http_tx_mgr,
                                             account->server,
                                             account->token,
                                             &total, &used) < 0) {
        seaf_warning ("Failed to get space usage for account %s/%s\n",
                      account->server, account->username);
        return;
    }

    seaf_repo_manager_set_account_space (seaf->repo_mgr,
                                         account->server, account->username,
                                         total, used);
}

static void
check_accounts_space_usage ()
{
    GList *accounts = NULL, *ptr;
    SeafAccount *account;

    accounts = seaf_repo_manager_get_account_list (seaf->repo_mgr);
    if (!accounts)
        return;

    for (ptr = accounts; ptr; ptr = ptr->next) {
        account = ptr->data;
        check_account_space_usage ( account);
    }

    g_list_free_full (accounts, (GDestroyNotify)seaf_account_free);
}

static void*
check_space_usage_thread (void *data)
{
    seaf_sleep (5);

    while (1) {
        check_accounts_space_usage ();
        seaf_sleep (60);
    }

    return NULL;
}

static int
parse_repo_list (const char *rsp_content, int rsp_size, GHashTable *repos)
{
    json_t *array = NULL, *object, *member;
    json_error_t jerror;
    size_t n;
    int i;
    RepoInfo *repo;
    const char *repo_id, *head_commit_id, *name = NULL, *permission, *type_str, *owner;
    gint64 mtime;
    int version;
    gboolean is_readonly, is_corrupted = FALSE;
    RepoType type;
    int ret = 0;
    RepoInfo *repo_info = NULL;

    array = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!array) {
        seaf_warning ("Parse response failed: %s.\n", jerror.text);
        return -1;
    }

    n = json_array_size (array);
    for (i = 0; i < n; ++i) {
        object = json_array_get (array, i);

        member = json_object_get (object, "version");
        if (!member) {
            seaf_warning ("Invalid repo list response: no version.\n");
            ret = -1;
            goto out;
        }
        version = json_integer_value (member);
        if (version == 0) {
            continue;
        }

        member = json_object_get (object, "id");
        if (!member) {
            seaf_warning ("Invalid repo list response: no id.\n");
            ret = -1;
            goto out;
        }
        repo_id = json_string_value (member);
        if (!is_uuid_valid (repo_id)) {
            seaf_warning ("Invalid repo list response: invalid repo_id.\n");
            ret = -1;
            goto out;
        }

        member = json_object_get (object, "head_commit_id");
        if (!member) {
            seaf_warning ("Invalid repo list response: no head_commit_id.\n");
            ret = -1;
            goto out;
        }
        head_commit_id = json_string_value (member);
        if (!is_object_id_valid (head_commit_id)) {
            seaf_warning ("Invalid repo list response: invalid head_commit_id.\n");
            ret = -1;
            goto out;
        }

        member = json_object_get (object, "name");
        if (!member) {
            seaf_warning ("Invalid repo list response: no name.\n");
            ret = -1;
            goto out;
        }
        if (member == json_null()) {
            is_corrupted = TRUE;
        } else {
            name = json_string_value (member);
        }

        member = json_object_get (object, "mtime");
        if (!member) {
            seaf_warning ("Invalid repo list response: no mtime.\n");
            ret = -1;
            goto out;
        }
        mtime = json_integer_value (member);

        member = json_object_get (object, "permission");
        if (!member) {
            seaf_warning ("Invalid repo list response: no permission.\n");
            ret = -1;
            goto out;
        }
        permission = json_string_value (member);
        if (g_strcmp0 (permission, "rw") == 0)
            is_readonly = FALSE;
        else
            is_readonly = TRUE;

        member = json_object_get (object, "owner");
        if (!member) {
            seaf_warning ("Invalid repo list response: no owner.\n");
            ret = -1;
            goto out;
        }
        owner = json_string_value (member);

        member = json_object_get (object, "type");
        if (!member) {
            seaf_warning ("Invalid repo list response: no repo type.\n");
            ret = -1;
            goto out;
        }
        type_str = json_string_value (member);
        if (g_strcmp0 (type_str, "repo") == 0) {
            type = REPO_TYPE_MINE;
        } else if (g_strcmp0 (type_str, "srepo") == 0) {
            type = REPO_TYPE_SHARED;
        } else if (g_strcmp0 (type_str, "grepo") == 0) {
            if (g_strcmp0 (owner, "Organization") == 0)
                type = REPO_TYPE_PUBLIC;
            else
                type = REPO_TYPE_GROUP;
        } else {
            seaf_warning ("Unknown repo type: %s.\n", type_str);
            ret = -1;
            goto out;
        }

        if ((repo_info = g_hash_table_lookup (repos, repo_id))) {
            if (repo_info->type == REPO_TYPE_MINE ||
                (!repo_info->is_readonly && is_readonly))
                continue;
        }

        repo = repo_info_new (repo_id, head_commit_id, name, mtime, is_readonly);
        repo->is_corrupted = is_corrupted;
        repo->type = type;

        g_hash_table_insert (repos, g_strdup(repo_id), repo);
    }

out:
    json_decref (array);

    return ret;
}

#define HTTP_SERVERR_BAD_GATEWAY    502
#define HTTP_SERVERR_UNAVAILABLE    503
#define HTTP_SERVERR_TIMEOUT        504

static void update_current_repos(HttpAPIGetResult *result, void *user_data)
{
    SeafAccount *account = user_data;
    SeafAccount *curr_account;
    GHashTable *repo_hash;
    GList *added = NULL, *removed = NULL, *ptr;
    RepoInfo *info;
    SeafRepo *repo;

    if (!result->success) {
        if (result->http_status == HTTP_SERVERR_BAD_GATEWAY ||
            result->http_status == HTTP_SERVERR_UNAVAILABLE ||
            result->http_status == HTTP_SERVERR_TIMEOUT) {
            seaf_repo_manager_set_account_server_disconnected (seaf->repo_mgr, account->server, account->username, TRUE);
        }
        record_sync_error (seaf->sync_mgr, NULL, NULL, NULL,
                           transfer_error_to_error_id (result->error_code));
        g_atomic_int_set (&seaf->sync_mgr->priv->server_disconnected, 1);
        return;
    } else {
        //If the fileserver hangs up before sending events to the notification server,
        // the client will not receive the updates, and some updates will be lost.
        // Therefore, after the fileserver recovers, immediately checking locks and folder perms.
        if (account->server_disconnected) {
            seaf_sync_manager_check_locks_and_folder_perms (seaf->sync_mgr, account->fileserver_addr);
        }
        remove_sync_error (seaf->sync_mgr, NULL, NULL);
        g_atomic_int_set (&seaf->sync_mgr->priv->server_disconnected, 0);
        seaf_repo_manager_set_account_server_disconnected (seaf->repo_mgr, account->server, account->username, FALSE);
    }

    /* If the get repo list request was sent around account deleting,
     * this callback may be called after the account has been deleted.
     */
    curr_account = seaf_repo_manager_get_account (seaf->repo_mgr, account->server, account->username);
    if (!curr_account)
        return;
    seaf_account_free (curr_account);

    repo_hash = g_hash_table_new_full (g_str_hash, g_str_equal,
                                       g_free, (GDestroyNotify)repo_info_free);

    if (parse_repo_list (result->rsp_content, result->rsp_size, repo_hash) < 0) {
        goto out;
    }

    seaf_repo_manager_set_repo_list_fetched (seaf->repo_mgr, account->server, account->username);

    seaf_repo_manager_update_account_repos (seaf->repo_mgr, account->server, account->username, repo_hash, &added, &removed);

    /* Mark repos removed on the server as delete-pending. */
    for (ptr = removed; ptr; ptr = ptr->next) {
        info = ptr->data;
        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, info->id);
        if (repo) {
            seaf_message ("Repo %s(%.8s) was deleted on server. Remove local repo.\n",
                          repo->name, repo->id);
            seaf_repo_manager_mark_repo_deleted (seaf->repo_mgr, repo, TRUE);
            http_tx_manager_cancel_task (seaf->http_tx_mgr, info->id,
                                         HTTP_TASK_TYPE_DOWNLOAD);
            http_tx_manager_cancel_task (seaf->http_tx_mgr, info->id,
                                         HTTP_TASK_TYPE_UPLOAD);
            seaf_repo_unref (repo);
        }
    }

out:
    g_list_free_full (added, (GDestroyNotify)repo_info_free);
    g_list_free_full (removed, (GDestroyNotify)repo_info_free);
    g_hash_table_destroy (repo_hash);
    seaf_account_free (account);
}

static void
get_repo_list_cb (HttpAPIGetResult *result, void *user_data)
{
    update_current_repos (result, user_data);
}

#define HTTP_FORBIDDEN 403
#define HTTP_NOT_FOUND 404

static void
fileserver_get_repo_list_cb (HttpAPIGetResult *result, void *user_data)
{
    SeafAccount *account = user_data;

    if (result->http_status == HTTP_NOT_FOUND ||
        result->http_status == HTTP_FORBIDDEN) {
        char *url = NULL;
        if (result->http_status == HTTP_NOT_FOUND) {
            HttpServerState *state;
            state = get_http_server_state (seaf->sync_mgr->priv, account->fileserver_addr);
            if (state)
                state->is_fileserver_repo_list_api_disabled = TRUE;
        }
        // use seahub API
        url = g_strdup_printf ("%s/api2/repos/", account->server);
        if (http_tx_manager_api_get (seaf->http_tx_mgr,
                                     account->server,
                                     url,
                                     account->token,
                                     get_repo_list_cb,
                                     account) < 0) {
            seaf_account_free (account);
        }
        g_free (url);
        return;
    }

    update_current_repos (result, user_data);
}

static int
update_account_repo_list (SeafSyncManager *manager,
                          const char *server,
                          const char *username)
{
    HttpServerState *state;
    char *url = NULL;
    char *repo_id = NULL;
    char *repo_token = NULL;

    SeafAccount *account = seaf_repo_manager_get_account (seaf->repo_mgr, server, username);
    if (!account)
        return TRUE;

    //get the first repo_id and repo_token from curr_repos which will be used to validate the user
    repo_id = seaf_repo_manager_get_first_repo_token_from_account_repos (seaf->repo_mgr, server, username, &repo_token);
    state = get_http_server_state (manager->priv, account->fileserver_addr);
    if (!state || !repo_id || state->is_fileserver_repo_list_api_disabled) {
        url = g_strdup_printf ("%s/api2/repos/", account->server);

        if (http_tx_manager_api_get (seaf->http_tx_mgr,
                                     account->server,
                                     url,
                                     account->token,
                                     get_repo_list_cb,
                                     account) < 0) {
            seaf_warning ("Failed to start get repo list from server %s\n",
                          account->server);
            seaf_account_free (account);
        }

        g_free (repo_id);
        g_free (repo_token);
        g_free (url);
        return TRUE;
    }

    if (!state->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/accessible-repos/?repo_id=%s", state->effective_host, repo_id);
    else
        url = g_strdup_printf ("%s/accessible-repos/?repo_id=%s", state->effective_host, repo_id);


    if (http_tx_manager_fileserver_api_get  (seaf->http_tx_mgr,
                                             state->effective_host,
                                             url,
                                             repo_token,
                                             fileserver_get_repo_list_cb,
                                             account) < 0) {
        seaf_warning ("Failed to start get repo list from server %s\n",
                      account->server);
        seaf_account_free (account);
    }

    g_free (repo_id);
    g_free (repo_token);
    g_free (url);

    return TRUE;
}

static int
update_repo_list_pulse (void *vmanager)
{
    SeafSyncManager *manager = vmanager;
    GList *accounts = NULL, *ptr;
    SeafAccount *account;

    accounts = seaf_repo_manager_get_account_list (seaf->repo_mgr);
    if (!accounts)
        return TRUE;

    for (ptr = accounts; ptr; ptr = ptr->next) {
        account = ptr->data;
        update_account_repo_list (manager, account->server, account->username);
    }

    g_list_free_full (accounts, (GDestroyNotify)seaf_account_free);

    return TRUE;
}

int
seaf_sync_manager_update_account_repo_list (SeafSyncManager *mgr,
                                            const char *server,
                                            const char *user)
{
    return update_account_repo_list (mgr, server, user);
}

static void
check_folder_perms_done (HttpFolderPerms *result, void *user_data)
{
    FolderPermInfo *perm_info = user_data;
    HttpServerState *server_state = perm_info->server_state;
    GList *ptr;
    HttpFolderPermRes *res;
    gint64 now = (gint64)time(NULL);

    server_state->checking_folder_perms = FALSE;

    if (!result->success) {
        /* If on star-up we find that checking folder perms fails,
         * we assume the server doesn't support it.
         */
        if (server_state->last_check_perms_time == 0)
            server_state->folder_perms_not_supported = TRUE;
        server_state->last_check_perms_time = now;
        folder_perm_info_free (perm_info);
        return;
    }

    SyncInfo *info;
    for (ptr = result->results; ptr; ptr = ptr->next) {
        res = ptr->data;

        info = get_sync_info (seaf->sync_mgr, res->repo_id);
        // Getting folder perms before clone repo will set task, at this point we need to update the folder perms.
        if (info->in_sync && !perm_info->task)
            continue;

        seaf_repo_manager_update_folder_perms (seaf->repo_mgr, res->repo_id,
                                               FOLDER_PERM_TYPE_USER,
                                               res->user_perms);
        seaf_repo_manager_update_folder_perms (seaf->repo_mgr, res->repo_id,
                                               FOLDER_PERM_TYPE_GROUP,
                                               res->group_perms);
        seaf_repo_manager_update_folder_perm_timestamp (seaf->repo_mgr,
                                                        res->repo_id,
                                                        res->timestamp);
    }

    server_state->last_check_perms_time = now;
    folder_perm_info_free (perm_info);
}

static void
check_folder_permissions_immediately  (SeafSyncManager *mgr,
                                       HttpServerState *server_state,
                                       const char *server,
                                       const char *user,
                                       gboolean force)
{
    GList *repo_ids;
    GList *ptr;
    char *repo_id;
    SeafRepo *repo;
    gint64 timestamp;
    HttpFolderPermReq *req;
    GList *requests = NULL;

    repo_ids = seaf_repo_manager_get_account_repo_ids (seaf->repo_mgr, server, user);

    for (ptr = repo_ids; ptr; ptr = ptr->next) {
        repo_id = ptr->data;

        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
        if (!repo)
            continue;

#ifdef COMPILE_WS
        // Don't need to check folder perms regularly when we get folder perms from notification server.
        if (!force && seaf_notif_manager_is_repo_subscribed (seaf->notif_mgr, repo)) {
            seaf_repo_unref (repo);
            continue;
        }
#endif

        if (!repo->token) {
            seaf_repo_unref (repo);
            continue;
        }

        timestamp = seaf_repo_manager_get_folder_perm_timestamp (seaf->repo_mgr,
                                                                 repo->id);
        if (timestamp < 0)
            timestamp = 0;

        req = g_new0 (HttpFolderPermReq, 1);
        memcpy (req->repo_id, repo->id, 36);
        req->token = g_strdup(repo->token);
        req->timestamp = timestamp;

        requests = g_list_append (requests, req);

        seaf_repo_unref (repo);
    }

    g_list_free_full (repo_ids, g_free);

    if (!requests)
        return;

    server_state->checking_folder_perms = TRUE;

    FolderPermInfo *info = g_new0 (FolderPermInfo, 1);
    info->server_state = server_state;
    info->server = g_strdup (server);
    info->user = g_strdup (user);

    /* The requests list will be freed in http tx manager. */
    if (http_tx_manager_get_folder_perms (seaf->http_tx_mgr,
                                          server_state->effective_host,
                                          server_state->use_fileserver_port,
                                          requests,
                                          check_folder_perms_done,
                                          info) < 0) {
        seaf_warning ("Failed to schedule check folder permissions\n");
        server_state->checking_folder_perms = FALSE;
        folder_perm_info_free (info);
    }
}

static void
check_folder_permissions (SeafSyncManager *mgr,
                          HttpServerState *server_state,
                          const char *server,
                          const char *user)
{
    gint64 now = (gint64)time(NULL);

    if (server_state->http_version == 0 ||
        server_state->folder_perms_not_supported ||
        server_state->checking_folder_perms)
        return;

    if (server_state->immediate_check_folder_perms) {
        server_state->immediate_check_folder_perms = FALSE;
        check_folder_permissions_immediately (mgr, server_state, server, user, TRUE);
        return;
    }

    if (server_state->last_check_perms_time > 0 &&
        now - server_state->last_check_perms_time < CHECK_FOLDER_PERMS_INTERVAL)
        return;

    check_folder_permissions_immediately (mgr, server_state, server, user, FALSE);
}

static void
check_server_locked_files_done (HttpLockedFiles *result, void *user_data)
{
    HttpServerState *server_state = user_data;
    GList *ptr;
    HttpLockedFilesRes *locked_res;
    gint64 now = (gint64)time(NULL);

    server_state->checking_locked_files = FALSE;

    if (!result->success) {
        /* If on star-up we find that checking locked files fails,
         * we assume the server doesn't support it.
         */
        if (server_state->last_check_locked_files_time == 0)
            server_state->locked_files_not_supported = TRUE;
        server_state->last_check_locked_files_time = now;
        return;
    }

    SyncInfo *info;
    for (ptr = result->results; ptr; ptr = ptr->next) {
        locked_res = ptr->data;

        info = get_sync_info (seaf->sync_mgr, locked_res->repo_id);
        if (info->in_sync)
            continue;

        seaf_filelock_manager_update (seaf->filelock_mgr,
                                      locked_res->repo_id,
                                      locked_res->locked_files);

        seaf_filelock_manager_update_timestamp (seaf->filelock_mgr,
                                                locked_res->repo_id,
                                                locked_res->timestamp);
    }

    server_state->last_check_locked_files_time = now;
}

static void
check_locked_files_immediately (SeafSyncManager *mgr, HttpServerState *server_state,
                                const char *server, const char *user, gboolean force)
{
    GList *repo_ids;
    GList *ptr;
    char *repo_id;
    SeafRepo *repo;
    gint64 timestamp;
    HttpLockedFilesReq *req;
    GList *requests = NULL;


    repo_ids = seaf_repo_manager_get_account_repo_ids (seaf->repo_mgr, server, user);

    for (ptr = repo_ids; ptr; ptr = ptr->next) {
        repo_id = ptr->data;

        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
        if (!repo)
            continue;

#ifdef COMPILE_WS
        // Don't need to check locked files regularly when we get locked files from notification server.
        if (!force && seaf_notif_manager_is_repo_subscribed (seaf->notif_mgr, repo)) {
            seaf_repo_unref (repo);
            continue;
        }
#endif

        if (!repo->token) {
            seaf_repo_unref (repo);
            continue;
        }

        timestamp = seaf_filelock_manager_get_timestamp (seaf->filelock_mgr,
                                                         repo->id);
        if (timestamp < 0)
            timestamp = 0;

        req = g_new0 (HttpLockedFilesReq, 1);
        memcpy (req->repo_id, repo->id, 36);
        req->token = g_strdup(repo->token);
        req->timestamp = timestamp;

        requests = g_list_append (requests, req);

        seaf_repo_unref (repo);
    }

    g_list_free_full (repo_ids, g_free);

    if (!requests)
        return;

    server_state->checking_locked_files = TRUE;

    /* The requests list will be freed in http tx manager. */
    if (http_tx_manager_get_locked_files (seaf->http_tx_mgr,
                                          server_state->effective_host,
                                          server_state->use_fileserver_port,
                                          requests,
                                          check_server_locked_files_done,
                                          server_state) < 0) {
        seaf_warning ("Failed to schedule check server locked files\n");
        server_state->checking_locked_files = FALSE;
    }
}

static void
check_locked_files (SeafSyncManager *mgr, HttpServerState *server_state,
                    const char *server, const char *user)
{
    gint64 now = (gint64)time(NULL);

    if (server_state->http_version == 0 ||
        server_state->locked_files_not_supported ||
        server_state->checking_locked_files)
        return;
    
    if (server_state->immediate_check_locked_files) {
        server_state->immediate_check_locked_files = FALSE;
        check_locked_files_immediately (mgr, server_state, server, user, TRUE);
        return;
    }

    if (server_state->last_check_locked_files_time > 0 &&
        now - server_state->last_check_locked_files_time < CHECK_SERVER_LOCKED_FILES_INTERVAL)
        return;

    check_locked_files_immediately (mgr, server_state, server, user, FALSE);
}

static char *
http_fileserver_url (const char *url)
{
    const char *host;
    char *colon;
    char *url_no_port;
    char *ret = NULL;

    /* Just return the url itself if it's invalid. */
    if (strlen(url) <= strlen("http://"))
        return g_strdup(url);

    /* Skip protocol schem. */
    host = url + strlen("http://");

    colon = strrchr (host, ':');
    if (colon) {
        url_no_port = g_strndup(url, colon - url);
        ret = g_strconcat(url_no_port, ":8082", NULL);
        g_free (url_no_port);
    } else {
        ret = g_strconcat(url, ":8082", NULL);
    }

    return ret;
}

static void
check_http_fileserver_protocol_done (HttpProtocolVersion *result, void *user_data)
{
    HttpServerState *state = user_data;

    state->checking = FALSE;

    if (result->check_success && !result->not_supported) {
        state->http_version = result->version;
        state->effective_host = http_fileserver_url(state->testing_host);
        state->use_fileserver_port = TRUE;
    }
}

static void
check_http_protocol_done (HttpProtocolVersion *result, void *user_data)
{
    HttpServerState *state = user_data;

    if (result->check_success && !result->not_supported) {
        state->http_version = result->version;
        state->effective_host = g_strdup(state->testing_host);
        state->checking = FALSE;
    } else if (strncmp(state->testing_host, "https", 5) != 0) {
        char *host_fileserver = http_fileserver_url(state->testing_host);
        if (http_tx_manager_check_protocol_version (seaf->http_tx_mgr,
                                                    host_fileserver,
                                                    TRUE,
                                                    check_http_fileserver_protocol_done,
                                                    state) < 0)
            state->checking = FALSE;
        g_free (host_fileserver);
    } else {
        state->checking = FALSE;
    }
}

#define CHECK_HTTP_INTERVAL 10

/*
 * Returns TRUE if we're ready to use http-sync; otherwise FALSE.
 */
static gboolean
check_http_protocol (SeafSyncManager *mgr, const char *server_url)
{
    pthread_mutex_lock (&mgr->priv->server_states_lock);

    HttpServerState *state = g_hash_table_lookup (mgr->priv->http_server_states,
                                                  server_url);
    if (!state) {
        state = g_new0 (HttpServerState, 1);
        g_hash_table_insert (mgr->priv->http_server_states,
                             g_strdup(server_url), state);
    }

    pthread_mutex_unlock (&mgr->priv->server_states_lock);

    if (state->checking) {
        return FALSE;
    }

    if (state->http_version > 0) {
        return TRUE;
    }

    /* If we haven't detected the server url successfully, retry every 10 seconds. */
    gint64 now = time(NULL);
    if (now - state->last_http_check_time < CHECK_HTTP_INTERVAL)
        return FALSE;

    /* First try server_url.
     * If it fails and https is not used, try server_url:8082 instead.
     */
    g_free (state->testing_host);
    state->testing_host = g_strdup(server_url);

    state->last_http_check_time = (gint64)time(NULL);

    if (http_tx_manager_check_protocol_version (seaf->http_tx_mgr,
                                                server_url,
                                                FALSE,
                                                check_http_protocol_done,
                                                state) < 0)
        return FALSE;

    state->checking = TRUE;

    return FALSE;
}

static void
check_notif_server_done (gboolean is_alive, void *user_data)
{
    HttpServerState *state = user_data;
    
    if (is_alive) {
        state->notif_server_alive = TRUE;
        seaf_message ("Notification server is enabled on the remote server %s.\n", state->effective_host);
    }
}

static char *
http_notification_url (const char *url)
{
    const char *host;
    char *colon;
    char *url_no_port;
    char *ret = NULL;

    /* Just return the url itself if it's invalid. */
    if (strlen(url) <= strlen("http://"))
        return g_strdup(url);

    /* Skip protocol schem. */
    host = url + strlen("http://");

    colon = strrchr (host, ':');
    if (colon) {
        url_no_port = g_strndup(url, colon - url);
        ret = g_strconcat(url_no_port, ":8083", NULL);
        g_free (url_no_port);
    } else {
        ret = g_strconcat(url, ":8083", NULL);
    }

    return ret;
}

#ifdef COMPILE_WS
// Returns TRUE if notification server is alive; otherwise FALSE.
// We only check notification server once.
static gboolean
check_notif_server (SeafSyncManager *mgr, const char *server_url)
{
    pthread_mutex_lock (&mgr->priv->server_states_lock);

    HttpServerState *state = g_hash_table_lookup (mgr->priv->http_server_states,
                                                  server_url);

    pthread_mutex_unlock (&mgr->priv->server_states_lock);
    if (!state) {
        return FALSE;
    }

    if (state->notif_server_alive) {
        return TRUE;
    }

    if (state->notif_server_checked) {
        return FALSE;
    }

    char *notif_url = NULL;
    if (state->use_fileserver_port) {
        notif_url = http_notification_url (server_url);
    } else {
        notif_url = g_strdup (server_url);
    }

    if (http_tx_manager_check_notif_server (seaf->http_tx_mgr,
                                            notif_url,
                                            state->use_fileserver_port,
                                            check_notif_server_done,
                                            state) < 0) {
        g_free (notif_url);
        return FALSE;
    }

    state->notif_server_checked = TRUE;

    g_free (notif_url);
    return FALSE;
}
#endif

gint
cmp_sync_info_by_sync_time (gconstpointer a, gconstpointer b, gpointer user_data)
{
    const SyncInfo *info_a = a;
    const SyncInfo *info_b = b;

    return (info_a->last_sync_time - info_b->last_sync_time);
}

inline static gboolean
exceed_max_tasks (SeafSyncManager *manager)
{
    if (manager->priv->n_running_tasks >= MAX_RUNNING_SYNC_TASKS)
        return TRUE;
    return FALSE;
}

static void
notify_fs_loaded ()
{
    json_t *msg = json_object ();
    json_object_set_string_member (msg, "type", "fs-loaded");
    mq_mgr_push_msg (seaf->mq_mgr, SEADRIVE_NOTIFY_CHAN, msg);
    seaf_message ("All repo fs trees are loaded.\n");
}

static char *
parse_jwt_token (const char *rsp_content, gint64 rsp_size)
{
    json_t *object = NULL;
    json_error_t jerror;
    const char *member = NULL;
    char *jwt_token = NULL;

    object = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!object) {
        return NULL;
    }

    if (json_object_has_member (object, "jwt_token")) {
        member = json_object_get_string_member (object, "jwt_token");
        if (member)
            jwt_token = g_strdup (member);
    } else {
        json_decref (object);
        return NULL;
    }

    json_decref (object);
    return jwt_token;
}

#define HTTP_FORBIDDEN 403
#define HTTP_NOT_FOUND 404
#define HTTP_SERVERR   500

typedef struct _GetJwtTokenAux {
    HttpServerState *state; 
    char *repo_id;
} GetJwtTokenAux;

static void
fileserver_get_jwt_token_cb (HttpAPIGetResult *result, void *user_data)
{
    GetJwtTokenAux *aux = user_data;
    HttpServerState *state = aux->state;
    char *repo_id = aux->repo_id;
    SeafRepo *repo = NULL;
    char *jwt_token = NULL;

    state->n_jwt_token_request--;

    if (result->http_status == HTTP_NOT_FOUND ||
        result->http_status == HTTP_FORBIDDEN ||
        result->http_status == HTTP_SERVERR) {
        goto out;
    }

    if (!result->success) {
        goto out;
    }

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo)
        goto out;

    jwt_token = parse_jwt_token (result->rsp_content,result->rsp_size);
    if (!jwt_token) {
        seaf_warning ("Failed to parse jwt token for repo %s\n", repo->id);
        goto out;
    }
    g_free (repo->jwt_token);
    repo->jwt_token = jwt_token;

out:
    g_free (aux->repo_id);
    g_free (aux);
    seaf_repo_unref (repo);
    return;
}

#ifdef COMPILE_WS
static int
check_and_subscribe_repo (HttpServerState *state, SeafRepo *repo)
{
    char *url = NULL;

    if (!state->notif_server_alive) {
        return 0;
    }

    if (state->n_jwt_token_request > 10) {
        return 0;
    }

    gint64 now = (gint64)time(NULL);
    if (now - repo->last_check_jwt_token > JWT_TOKEN_EXPIRE_TIME) {
        repo->last_check_jwt_token = now;
        if (!state->use_fileserver_port)
            url = g_strdup_printf ("%s/seafhttp/repo/%s/jwt-token", state->effective_host, repo->id);
        else
            url = g_strdup_printf ("%s/repo/%s/jwt-token", state->effective_host, repo->id);

        state->n_jwt_token_request++;
        GetJwtTokenAux *aux = g_new0 (GetJwtTokenAux, 1);
        aux->repo_id = g_strdup (repo->id);
        aux->state = state;
        if (http_tx_manager_fileserver_api_get (seaf->http_tx_mgr,
                                            state->effective_host,
                                            url,
                                            repo->token,
                                            fileserver_get_jwt_token_cb,
                                            aux) < 0) {
            g_free (aux->repo_id);
            g_free (aux);
            state->n_jwt_token_request--;
        }
        g_free (url);
        return 0;
    }
    if (!seaf_notif_manager_is_repo_subscribed (seaf->notif_mgr, repo)) {
        if (repo->jwt_token)
            seaf_notif_manager_subscribe_repo (seaf->notif_mgr, repo);
    }

    return 0;
}
#endif

static void
clone_repo (SeafSyncManager *manager,
            SeafAccount *account,
            HttpServerState *state,
            SyncInfo *sync_info);

static int
sync_repo (SeafSyncManager *manager,
           HttpServerState *state,
           SyncInfo *sync_info,
           SeafRepo *repo);

static int
auto_sync_account_repos (SeafSyncManager *manager, const char *server, const char *user)
{
    SeafAccount *account;
    GList *repo_info_list, *ptr, *sync_info_list = NULL;
    RepoInfo *repo_info;
    SyncInfo *sync_info;
    SeafRepo *repo;
    HttpServerState *state;
    gboolean all_repos_loaded = TRUE;

    if (!manager->priv->auto_sync_enabled)
        return TRUE;

    account = seaf_repo_manager_get_account (seaf->repo_mgr, server, user);
    if (!account)
        return TRUE;

    if (!check_http_protocol (manager, account->fileserver_addr)) {
        seaf_account_free (account);
        return TRUE;
    }

    state = get_http_server_state (manager->priv, account->fileserver_addr);
    if (!state) {
        seaf_account_free (account);
        return TRUE;
    }

#ifdef COMPILE_WS
    if (check_notif_server (manager, account->fileserver_addr)) {
        seaf_notif_manager_connect_server (seaf->notif_mgr, account->fileserver_addr,
                                           state->use_fileserver_port);
    }
#endif

    if (account->is_pro) {
        check_folder_permissions (manager, state, account->server, account->username);
        check_locked_files (manager, state, account->server, account->username);
    }

    /* Find sync_infos coresponded to repos in the current account. */
    repo_info_list = seaf_repo_manager_get_account_repos (seaf->repo_mgr, server, user);
    for (ptr = repo_info_list; ptr; ptr = ptr->next) {
        repo_info = (RepoInfo *)ptr->data;

        if (!account->all_repos_loaded) {
            repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_info->id);
            if (!repo)
                all_repos_loaded = FALSE;
            else if (!repo->fs_ready) {
                if (repo->encrypted && !repo->is_passwd_set) {
                    seaf_repo_unref (repo);
                    continue;
                }
                all_repos_loaded = FALSE;
            }
            seaf_repo_unref (repo);
        }

        /* The head commit id of the repo is fetched when getting repo list.
         * If this is not set yet, we don't have enough information to sync
         * the repo.
         */
        if (!repo_info->head_commit_id) {
            repo_info_free (repo_info);
            continue;
        }

        if (repo_info->is_corrupted) {
            repo_info_free (repo_info);
            continue;
        }

        sync_info = get_sync_info (manager, repo_info->id);

        if (sync_info->in_sync) {
            repo_info_free (repo_info);
            continue;
        }

        repo_info_free (sync_info->repo_info);
        sync_info->repo_info = repo_info;

        sync_info_list = g_list_prepend (sync_info_list, sync_info);
    }

    if (account->repo_list_fetched && !account->all_repos_loaded && all_repos_loaded) {
        seaf_repo_manager_set_account_all_repos_loaded (seaf->repo_mgr, server, user);
        notify_fs_loaded ();
    }

    /* Sort sync_infos by last_sync_time, so that we don't "starve" any repo. */
    sync_info_list = g_list_sort_with_data (sync_info_list,
                                            cmp_sync_info_by_sync_time,
                                            NULL);

    for (ptr = sync_info_list; ptr != NULL; ptr = ptr->next) {
        sync_info = (SyncInfo *)ptr->data;

        if (sync_info->in_error) {
            continue;
        }

        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, sync_info->repo_id);
        if (!repo) {
            if (exceed_max_tasks (manager))
                continue;

            /* Don't re-download a repo that's still marked as delete-pending.
             * It should be downloaded after the local repo is removed.
             */
            if (seaf_repo_manager_is_repo_delete_pending (seaf->repo_mgr,
                                                          sync_info->repo_id))
                continue;

            seaf_message ("Cloning repo %s(%s).\n",
                          sync_info->repo_info->name, sync_info->repo_id);
            clone_repo (manager, account, state, sync_info);

            continue;
        }

#ifdef USE_GPL_CRYPTO
        if (repo->version == 0 || (repo->encrypted && repo->enc_version < 2)) {
            seaf_repo_unref (repo);
            continue;
        }
#endif

        if (!repo->token) {
            /* If the user has logged out of the account, the repo token would
             * be null */
            seaf_repo_unref (repo);
            continue;
        }

        /* If repo tree is not loaded yet, we should wait until it's loaded. */
        if (!repo->fs_ready) {
            seaf_repo_unref (repo);
            continue;
        }

        if (exceed_max_tasks (manager) && !repo->force_sync_pending) {
            seaf_repo_unref (repo);
            continue;
        }

        if (repo->encrypted && !repo->is_passwd_set) {
            seaf_repo_unref (repo);
            continue;
        }

        sync_repo (manager, state, sync_info, repo);

#ifdef COMPILE_WS
        check_and_subscribe_repo (state, repo);
#endif

        seaf_repo_unref (repo);
    }
    
    seaf_account_free (account);
    g_list_free (repo_info_list);
    g_list_free (sync_info_list);
    return TRUE;
}

static int
auto_sync_pulse (void *vmanager)
{
    SeafSyncManager *manager = vmanager;
    GList *accounts = NULL, *ptr;
    SeafAccount *account;

    accounts = seaf_repo_manager_get_account_list (seaf->repo_mgr);
    if (!accounts)
        return TRUE;

    for (ptr = accounts; ptr; ptr = ptr->next) {
        account = ptr->data;
        auto_sync_account_repos (manager, account->server, account->username);
    }

    g_list_free_full (accounts, (GDestroyNotify)seaf_account_free);

    return TRUE;
}

static void
check_repo_folder_perms_done (HttpFolderPerms *result, void *user_data)
{
    FolderPermInfo *perm_info = user_data;
    HttpServerState *server_state = perm_info->server_state;
    SyncTask *task = perm_info->task;
    SyncInfo *sync_info = task->info;
    check_folder_perms_done (result, user_data);
    if (!result->success) {
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        return;
    }
    RepoToken *repo_token = g_hash_table_lookup (seaf->sync_mgr->priv->repo_tokens,
                                                 sync_info->repo_id);
    if (!repo_token) {
        seaf_warning ("Failed to get reop sync token fro %s.\n", sync_info->repo_id);
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        return;
    }
    int rc = http_tx_manager_add_download (seaf->http_tx_mgr,
                                           sync_info->repo_id,
                                           repo_token->repo_version,
                                           task->server,
                                           task->user,
                                           server_state->effective_host,
                                           repo_token->token,
                                           sync_info->repo_info->head_commit_id,
                                           TRUE,
                                           server_state->http_version,
                                           server_state->use_fileserver_port,
                                           NULL);

    if (rc < 0) {
        seaf_warning ("Failed to add download task for repo %s from %s.\n",
                      sync_info->repo_id, server_state->effective_host);
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        return;
    }

    transition_sync_state (task, SYNC_STATE_FETCH);
}

static void
check_folder_permissions_by_repo (HttpServerState *server_state,
                                  const char *server, const char *user,
                                  const char *repo_id, const char *token,
                                  SyncTask *task)
{
    HttpFolderPermReq *req;
    GList *requests = NULL;

    req = g_new0 (HttpFolderPermReq, 1);
    memcpy (req->repo_id, repo_id, 36);
    req->token = g_strdup(token);
    req->timestamp = 0;

    requests = g_list_append (requests, req);

    server_state->checking_folder_perms = TRUE;

    FolderPermInfo *info = g_new0 (FolderPermInfo, 1);
    info->server_state = server_state;
    info->task = task;
    info->server = g_strdup (server);
    info->user = g_strdup (user);

    /* The requests list will be freed in http tx manager. */
    if (http_tx_manager_get_folder_perms (seaf->http_tx_mgr,
                                          server_state->effective_host,
                                          server_state->use_fileserver_port,
                                          requests,
                                          check_repo_folder_perms_done,
                                          info) < 0) {
        seaf_warning ("Failed to schedule check repo %s folder permissions\n", repo_id);
        server_state->checking_folder_perms = FALSE;
        folder_perm_info_free (info);
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
    }
}

static void
get_repo_sync_token_cb (HttpAPIGetResult *result, void *user_data)
{
    SyncTask *task = user_data;
    SyncInfo *sync_info = task->info;
    HttpServerState *state = task->server_state;
    json_t *object, *member;
    json_error_t jerror;
    const char *token;
    int repo_version;

    if (!result->success) {
        task->tx_error_code = result->error_code;
        if (task->tx_error_code == HTTP_TASK_ERR_FORBIDDEN)
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_ACCESS_DENIED);
        else
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        return;
    }

    object = json_loadb (result->rsp_content, result->rsp_size, 0, &jerror);
    if (!object) {
        seaf_warning ("Parse response failed: %s.\n", jerror.text);
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        return;
    }

    member = json_object_get (object, "token");
    if (!member) {
        seaf_warning ("Invalid download info response: no token.\n");
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        goto out;
    }
    token = json_string_value (member);
    if (!token) {
        seaf_warning ("Invalid download info response: no token.\n");
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        goto out;
    }

    member = json_object_get (object, "repo_version");
    if (!member) {
        seaf_warning ("Invalid download info response: no repo_version.\n");
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        goto out;
    }
    repo_version = json_integer_value (member);

    task->token = g_strdup (token);

    // save repo token
    RepoToken *repo_token = g_new0 (RepoToken, 1);
    memcpy (repo_token->token, token, 40);
    repo_token->token[40] = '\0';
    repo_token->repo_version = repo_version;
    g_hash_table_insert (seaf->sync_mgr->priv->repo_tokens, g_strdup (sync_info->repo_id), repo_token);

    if (seaf_repo_manager_account_is_pro (seaf->repo_mgr, task->server, task->user)) {
        int timestamp = seaf_repo_manager_get_folder_perm_timestamp (seaf->repo_mgr,
                                                                     sync_info->repo_id);
        if (timestamp <= 0) {
            check_folder_permissions_by_repo (state, task->server, task->user, sync_info->repo_id, token, task);
            goto out;
        }
    }

    int rc = http_tx_manager_add_download (seaf->http_tx_mgr,
                                           sync_info->repo_id,
                                           repo_version,
                                           task->server,
                                           task->user,
                                           state->effective_host,
                                           token,
                                           sync_info->repo_info->head_commit_id,
                                           TRUE,
                                           state->http_version,
                                           state->use_fileserver_port,
                                           NULL);
    if (rc < 0) {
        seaf_warning ("Failed to add download task for repo %s from %s.\n",
                      sync_info->repo_id, state->effective_host);
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        goto out;
    }

    transition_sync_state (task, SYNC_STATE_FETCH);

out:
    json_decref (object);
}

static void
get_repo_sync_token (SeafSyncManager *manager,
                     SeafAccount *account,
                     HttpServerState *state,
                     SyncInfo *sync_info)
{
    SyncTask *task;
    task = sync_task_new (sync_info, state, account->server, account->username, NULL, TRUE);
    
    RepoToken *repo_token = g_hash_table_lookup (seaf->sync_mgr->priv->repo_tokens,
                                                 sync_info->repo_id);
    if(repo_token == NULL) {
        char *url = g_strdup_printf ("%s/api2/repos/%s/download-info/",
                                     account->server, sync_info->repo_id);

        if (http_tx_manager_api_get (seaf->http_tx_mgr,
                                    account->server,
                                    url,
                                    account->token,
                                    get_repo_sync_token_cb,
                                    task) < 0) {
            seaf_warning ("Failed to start get repo sync token for %s from server %s\n",
                        sync_info->repo_id, account->server);
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
            goto out;
        }
        transition_sync_state (task, SYNC_STATE_GET_TOKEN);
    out:
        g_free (url);
    } else {
        task->token = g_strdup(repo_token->token);
        // In order to use folder perms when cloning repo, we nned to get folder perms once immediately after getting rep token.
        if (account->is_pro) {
            int timestamp = seaf_repo_manager_get_folder_perm_timestamp (seaf->repo_mgr,
                                                                         sync_info->repo_id);
            if (timestamp <= 0) {
                check_folder_permissions_by_repo (state, account->server, account->username, sync_info->repo_id, repo_token->token, task);
                return;
            }
        }
        int rc = http_tx_manager_add_download (seaf->http_tx_mgr,
                                               sync_info->repo_id,
                                               repo_token->repo_version,
                                               account->server,
                                               account->username,
                                               state->effective_host,
                                               repo_token->token,
                                               sync_info->repo_info->head_commit_id,
                                               TRUE,
                                               state->http_version,
                                               state->use_fileserver_port,
                                               NULL);
        if (rc < 0) {
            seaf_warning ("Failed to add download task for repo %s from %s.\n",
                          sync_info->repo_id, state->effective_host);
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
            return;
        }
        transition_sync_state (task, SYNC_STATE_FETCH);
    }
}

static void
clone_repo (SeafSyncManager *manager,
            SeafAccount *account,
            HttpServerState *state,
            SyncInfo *sync_info)
{
    get_repo_sync_token (manager, account, state, sync_info);
}

static gboolean
can_schedule_repo (SyncInfo *info)
{
    gint64 now = (gint64)time(NULL);

    return (info->last_sync_time == 0 ||
            info->last_sync_time < now - DEFAULT_SYNC_INTERVAL);
}

static gboolean
create_commit_from_journal (SyncInfo *info, HttpServerState *state, SeafRepo *repo);

static int
check_head_commit_http (SyncTask *task);

inline static char *
get_basename (char *path)
{
    char *slash;
    slash = strrchr (path, '/');
    if (!slash)
        return path;
    return (slash + 1);
}

static char *
exceed_max_deleted_files (SeafRepo *repo)
{
    SeafBranch *master = NULL, *local = NULL;
    SeafCommit *local_head = NULL, *master_head = NULL;
    GList *diff_results = NULL;
    char *deleted_file = NULL;
    GString *desc = NULL;
    char *ret = NULL;

    local = seaf_branch_manager_get_branch (seaf->branch_mgr, repo->id, "local");
    if (!local) {
        seaf_warning ("No local branch found for repo %s(%.8s).\n",
                      repo->name, repo->id);
        goto out;
    }

    master = seaf_branch_manager_get_branch (seaf->branch_mgr, repo->id, "master");
    if (!master) {
        seaf_warning ("No master branch found for repo %s(%.8s).\n",
                      repo->name, repo->id);
        goto out;
    }

    local_head = seaf_commit_manager_get_commit (seaf->commit_mgr, repo->id, repo->version,
                                                 local->commit_id);
    if (!local_head) {
        seaf_warning ("Failed to get head of local branch for repo %s.\n", repo->id);
        goto out;
    }

    master_head = seaf_commit_manager_get_commit (seaf->commit_mgr, repo->id, repo->version,
                                                  master->commit_id);
    if (!master_head) {
        seaf_warning ("Failed to get head of master branch for repo %s.\n", repo->id);
        goto out;
    }

    diff_commit_roots (repo->id, repo->version, master_head->root_id, local_head->root_id, &diff_results, TRUE);
    if (!diff_results) {
        goto out;
    }
    GList *p;
    DiffEntry *de;
    int n_deleted = 0;

    for (p = diff_results; p != NULL; p = p->next) {
        de = p->data;
        switch (de->status) {
        case DIFF_STATUS_DELETED:
            if (n_deleted == 0)
                deleted_file = get_basename(de->name);
            n_deleted++;
            break;
        }
    }

    if (n_deleted >= seaf->delete_confirm_threshold) {
        desc = g_string_new ("");
        g_string_append_printf (desc, "Deleted \"%s\" and %d more files.\n",
                                deleted_file, n_deleted - 1);
        ret = g_string_free (desc, FALSE);
    }

out:
    seaf_branch_unref (local);
    seaf_branch_unref (master);
    seaf_commit_unref (local_head);
    seaf_commit_unref (master_head);
    g_list_free_full (diff_results, (GDestroyNotify)diff_entry_free);

    return ret;
}

static void
notify_delete_confirmation (const char *repo_name, const char *desc, const char *confirmation_id)
{
    json_t *msg = json_object ();
    json_object_set_string_member (msg, "type", "del_confirmation");
    json_object_set_string_member (msg, "repo_name", repo_name);
    json_object_set_string_member (msg, "delete_files", desc);
    json_object_set_string_member (msg, "confirmation_id", confirmation_id);
    mq_mgr_push_msg (seaf->mq_mgr, SEADRIVE_NOTIFY_CHAN, msg);
}

int
seaf_sync_manager_add_del_confirmation (SeafSyncManager *mgr,
                                        const char *confirmation_id,
                                        gboolean resync)
{
    SeafSyncManagerPriv *priv = seaf->sync_mgr->priv;
    DelConfirmationResult *result = NULL;

    result = g_new0 (DelConfirmationResult, 1);
    result->resync = resync;

    pthread_mutex_lock (&priv->del_confirmation_lock);
    g_hash_table_insert (priv->del_confirmation_tasks, g_strdup (confirmation_id), result);
    pthread_mutex_unlock (&priv->del_confirmation_lock);

    return 0;
}

static DelConfirmationResult *
get_del_confirmation_result (const char *confirmation_id)
{
    SeafSyncManagerPriv *priv = seaf->sync_mgr->priv;
    DelConfirmationResult *result, *copy = NULL;


    pthread_mutex_lock (&priv->del_confirmation_lock);
    result = g_hash_table_lookup (priv->del_confirmation_tasks, confirmation_id);
    if (result) {
        copy = g_new0 (DelConfirmationResult, 1);
        copy->resync = result->resync;
        g_hash_table_remove (priv->del_confirmation_tasks, confirmation_id); 
    }
    pthread_mutex_unlock (&priv->del_confirmation_lock);

    return copy;
}


static int
sync_repo (SeafSyncManager *manager,
           HttpServerState *state,
           SyncInfo *sync_info,
           SeafRepo *repo)
{
    SeafBranch *master = NULL, *local = NULL;
    SyncTask *task;
    int ret = 0;

    master = seaf_branch_manager_get_branch (seaf->branch_mgr, repo->id, "master");
    if (!master) {
        seaf_warning ("No master branch found for repo %s(%.8s).\n",
                      repo->name, repo->id);
        ret = -1;
        goto out;
    }
    local = seaf_branch_manager_get_branch (seaf->branch_mgr, repo->id, "local");
    if (!local) {
        seaf_warning ("No local branch found for repo %s(%.8s).\n",
                      repo->name, repo->id);
        ret = -1;
        goto out;
    }

    if (strcmp (local->commit_id, master->commit_id) != 0) {
        if (can_schedule_repo (sync_info) || repo->force_sync_pending || sync_info->del_confirmation_pending) {
            if (repo->force_sync_pending)
                repo->force_sync_pending = FALSE;
            task = sync_task_new (sync_info, state, NULL, NULL, repo->token, FALSE);

            if (!sync_info->del_confirmation_pending) {
                char *desc = NULL;
                desc = exceed_max_deleted_files (repo);
                if (desc) {
                    notify_delete_confirmation (repo->name, desc, local->commit_id);
                    seaf_warning ("Delete more than %d files, add delete confirmation.\n", seaf->delete_confirm_threshold);
                    sync_info->del_confirmation_pending = TRUE;
                    seaf_sync_manager_set_task_error (task, SYNC_ERROR_DEL_CONFIRMATION_PENDING);
                    g_free (desc);
                    goto out;
                }
            } else {
                DelConfirmationResult *result = get_del_confirmation_result (local->commit_id);
                if (!result) {
                    // User has not confirmed whether to continue syncing.
                    seaf_sync_manager_set_task_error (task, SYNC_ERROR_DEL_CONFIRMATION_PENDING);
                    goto out;
                } else if (result->resync) {
                    // User chooses to resync.
                    g_free (result);
                    seaf_sync_manager_set_task_error (task, SYNC_ERROR_DEL_CONFIRMATION_PENDING);
                    // Delete this repo. It'll be re-synced when checking repo list next time.
                    seaf_repo_manager_mark_repo_deleted (seaf->repo_mgr, repo, FALSE);
                    goto out;
                }
                // User chooes to continue syncing.
                g_free (result);
                sync_info->del_confirmation_pending = FALSE;
            }

            int rc = http_tx_manager_add_upload (seaf->http_tx_mgr,
                                                 repo->id,
                                                 repo->version,
                                                 repo->server,
                                                 repo->user,
                                                 repo->repo_uname,
                                                 state->effective_host,
                                                 repo->token,
                                                 state->http_version,
                                                 state->use_fileserver_port,
                                                 NULL);

            if (rc < 0) {
                seaf_warning ("Failed to add upload task for repo %s to %s.\n",
                              sync_info->repo_id, state->effective_host);
                seaf_sync_manager_set_task_error (task, SYNC_ERROR_START_UPLOAD);
                ret = -1;
                goto out;
            }

            transition_sync_state (task, SYNC_STATE_UPLOAD);
        }

        goto out;
    }

    if (create_commit_from_journal (sync_info, state, repo)) {
        if (repo->force_sync_pending)
            repo->force_sync_pending = FALSE;
        goto out;
    }

    if ((strcmp (sync_info->repo_info->head_commit_id, master->commit_id) != 0 &&
         can_schedule_repo (sync_info)) ||
         repo->force_sync_pending) {

        if (repo->force_sync_pending)
            repo->force_sync_pending = FALSE;

        task = sync_task_new (sync_info, state, NULL, NULL, repo->token, FALSE);

        /* In some cases, sync_info->repo_info->head_commit_id may be outdated.
         * To avoid mistakenly download a repo, we check server head commit
         * before starting download.
         */
        check_head_commit_http (task);
    }

out:
    seaf_branch_unref (local);
    seaf_branch_unref (master);
    return ret;
}

static void
check_head_commit_done (HttpHeadCommit *result, void *user_data)
{
    SyncTask *task = user_data;
    SyncInfo *info = task->info;
    SeafRepo *repo = task->repo;
    HttpServerState *state = task->server_state;
    SeafBranch *master = NULL;

    if (!result->check_success) {
        task->tx_error_code = result->error_code;
        if (result->perm_denied)
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_ACCESS_DENIED);
        else
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_GET_SYNC_INFO);
        /* if (result->perm_denied) */
        /*     seaf_repo_manager_mark_repo_deleted (seaf->repo_mgr, repo, FALSE); */
        return;
    }

    if (result->is_deleted) {
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_NOREPO);
        return;
    }

    if (result->is_corrupt) {
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_REPO_CORRUPT);
        return;
    }

    /* The cached head commit id may be outdated. */
    if (strcmp (result->head_commit, info->repo_info->head_commit_id) != 0) {
        seaf_repo_manager_set_repo_info_head_commit (seaf->repo_mgr,
                                                     repo->id,
                                                     result->head_commit);
    }

    master = seaf_branch_manager_get_branch (seaf->branch_mgr, repo->id, "master");
    if (!master) {
        seaf_warning ("master branch not found for repo %s.\n", repo->id);
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_DATA_CORRUPT);
        return;
    }

    if (strcmp (result->head_commit, master->commit_id) != 0) {
        int rc = http_tx_manager_add_download (seaf->http_tx_mgr,
                                               repo->id,
                                               repo->version,
                                               repo->server,
                                               repo->user,
                                               state->effective_host,
                                               repo->token,
                                               result->head_commit,
                                               FALSE,
                                               state->http_version,
                                               state->use_fileserver_port,
                                               NULL);
        if (rc < 0) {
            seaf_warning ("Failed to add download task for repo %s from %s.\n",
                          info->repo_id, state->effective_host);
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_START_FETCH);
            goto out;
        }

        transition_sync_state (task, SYNC_STATE_FETCH);
    } else {
        transition_sync_state (task, SYNC_STATE_DONE);
    }

out:
    seaf_branch_unref (master);
}

static int
check_head_commit_http (SyncTask *task)
{
    SeafRepo *repo = task->repo;
    HttpServerState *state = task->server_state;

    int ret = http_tx_manager_check_head_commit (seaf->http_tx_mgr,
                                                 repo->id, repo->version,
                                                 state->effective_host,
                                                 repo->token,
                                                 state->use_fileserver_port,
                                                 check_head_commit_done,
                                                 task);
    if (ret == 0)
        transition_sync_state (task, SYNC_STATE_INIT);
    else if (ret < 0)
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_GET_SYNC_INFO);

    return ret;
}

static gboolean
server_is_pro (const char *server_url) {
    GList *accounts = NULL, *ptr;
    SeafAccount *account;
    gboolean is_pro = FALSE;
    accounts = seaf_repo_manager_get_account_list (seaf->repo_mgr);
    if (!accounts)
        return FALSE;

    for (ptr = accounts; ptr; ptr = ptr->next) {
        account = ptr->data;
        if (g_strcmp0 (account->fileserver_addr, server_url) == 0) {
            is_pro = account->is_pro;
            break;
        }
    }

    g_list_free_full (accounts, (GDestroyNotify)seaf_account_free);

    return is_pro;
}

void
seaf_sync_manager_check_locks_and_folder_perms (SeafSyncManager *manager, const char *server_url)
{
    HttpServerState *state;

    state = get_http_server_state (manager->priv, server_url);
    if (!state) {
        return;
    }

    if (server_is_pro (server_url)) {
        state->immediate_check_folder_perms = TRUE;
        state->immediate_check_locked_files = TRUE;
    }

    return;
}

gboolean
seaf_sync_manager_ignored_on_checkout (const char *file_path, IgnoreReason *ignore_reason)
{
    gboolean ret = FALSE;

    return ret;
}

gboolean
seaf_repo_manager_ignored_on_commit (const char *filename)
{
    GPatternSpec **spec = ignore_patterns;

    if (!g_utf8_validate (filename, -1, NULL)) {
        seaf_warning ("File name %s contains non-UTF8 characters, skip.\n", filename);
        return TRUE;
    }

    /* Ignore file/dir if its name is too long. */
    if (strlen(filename) >= SEAF_DIR_NAME_LEN)
        return TRUE;

    if (strchr (filename, '/'))
        return TRUE;

    while (*spec) {
        if (g_pattern_match_string(*spec, filename))
            return TRUE;
        spec++;
    }

    if (!seaf->sync_extra_temp_file) {
        spec = office_temp_ignore_patterns;
        while (*spec) {
            if (g_pattern_match_string(*spec, filename))
                return TRUE;
            spec++;
        }
    }

    return FALSE;
}

#if 0
static int
copy_file_to_conflict_file (const char *repo_id,
                            const char *path,
                            const char *conflict_path)
{
    SeafRepo *repo = NULL;
    RepoTreeStat st;
    JournalOp *op;
    int ret = 0;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        ret = -1;
        goto out;
    }

    if (repo_tree_stat_path (repo->tree, path, &st) < 0) {
        ret = -1;
        goto out;
    }

    repo_tree_create_file (repo->tree, conflict_path, st.id, st.mode, st.mtime, st.size);

    op = journal_op_new (OP_TYPE_CREATE_FILE, conflict_path, NULL, st.size, st.mtime, st.mode);
    if (journal_append_op (repo->journal, op) < 0) {
        journal_op_free (op);
        ret = -1;
        goto out;
    }

    file_cache_mgr_rename (seaf->file_cache_mgr, repo_id, path, repo_id, conflict_path);

out:
    seaf_repo_unref (repo);
    return ret;
}
#endif

static int
merge_remote_head_to_repo_tree (SeafRepo *repo, const char *new_head_id)
{
    SeafBranch *master = NULL;
    SeafCommit *local_head = NULL, *remote_head = NULL;
    GList *diff_results = NULL, *ptr;
    DiffEntry *de;
    char id[41];
    /* gboolean conflicted; */
    int ret = 0;

    master = seaf_branch_manager_get_branch (seaf->branch_mgr,
                                             repo->id,
                                             "master");
    if (!master) {
        seaf_warning ("Failed to get master branch of repo %s.\n", repo->id);
        ret = -1;
        goto out;
    }

    local_head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                                 repo->id, repo->version,
                                                 repo->head->commit_id);
    if (!local_head) {
        seaf_warning ("Failed to get head commit of local branch for repo %s.\n",
                      repo->id);
        ret = -1;
        goto out;
    }

    remote_head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                                  repo->id, repo->version,
                                                  new_head_id);
    if (!remote_head) {
        seaf_warning ("Failed to get remote head commit %s for repo %s.\n",
                      new_head_id, repo->id);
        ret = -1;
        goto out;
    }

    if (diff_commits (local_head, remote_head, &diff_results, TRUE) < 0) {
        seaf_warning ("Failed to diff for repo %s.\n", repo->id);
        ret = -1;
        goto out;
    }

    /* Process delete/rename before add/update. Since rename of empty files/dirs
     * are interpreted as delete+create, we need to be sure about the order.
     */
    for (ptr = diff_results; ptr; ptr = ptr->next) {
        de = (DiffEntry *)ptr->data;
        switch (de->status) {
        case DIFF_STATUS_DELETED:
            /* If local file was changed, don't delete it. */
            if (file_cache_mgr_is_file_changed (seaf->file_cache_mgr,
                                                repo->id, de->name, TRUE))
                break;
            repo_tree_unlink (repo->tree, de->name);
            file_cache_mgr_unlink (seaf->file_cache_mgr, repo->id, de->name);
            break;
        case DIFF_STATUS_DIR_DELETED:
            repo_tree_remove_subtree (repo->tree, de->name);
            break;
        case DIFF_STATUS_RENAMED:
        case DIFF_STATUS_DIR_RENAMED:
            if (seaf_sync_manager_ignored_on_checkout(de->new_name, NULL)) {
                seaf_message ("Path %s is invalid on Windows, skip rename.\n",
                              de->new_name);
                /* send_sync_error_notification (repo->id, repo->name, de->new_name, */
                /*                               SYNC_ERROR_ID_INVALID_PATH); */
                break;
            } else if (seaf_sync_manager_ignored_on_checkout(de->name, NULL)) {
                /* If the server renames an invalid path to a valid path,
                 * directly checkout the valid path.
                 */
                rawdata_to_hex (de->sha1, id, 20);
                repo_tree_add_subtree (repo->tree,
                                       de->new_name,
                                       id,
                                       de->mtime);
                break;
            }

            repo_tree_rename (repo->tree, de->name, de->new_name, TRUE);

            file_cache_mgr_rename (seaf->file_cache_mgr,
                                   repo->id, de->name,
                                   repo->id, de->new_name);

            break;
        }
    }

    for (ptr = diff_results; ptr; ptr = ptr->next) {
        de = (DiffEntry *)ptr->data;
        switch (de->status) {
        case DIFF_STATUS_ADDED:
            if (seaf_sync_manager_ignored_on_checkout(de->name, NULL)) {
                seaf_message ("Path %s is invalid on Windows, skip creating.\n",
                              de->name);
                /* send_sync_error_notification (repo->id, repo->name, de->name, */
                /*                               SYNC_ERROR_ID_INVALID_PATH); */
                break;
            }

            /* handle_file_conflict (repo->id, de->name, &conflicted); */

            rawdata_to_hex (de->sha1, id, 20);
            /* if (conflicted) { */
            /*     repo_tree_set_file_mtime (repo->tree, */
            /*                               de->name, */
            /*                               de->mtime); */
            /*     repo_tree_set_file_size (repo->tree, */
            /*                              de->name, */
            /*                              de->size); */
            /*     repo_tree_set_file_id (repo->tree, de->name, id); */
            /* } else { */
            repo_tree_create_file (repo->tree,
                                   de->name,
                                   id,
                                   de->mode,
                                   de->mtime,
                                   de->size);
            /* } */

            break;
        case DIFF_STATUS_DIR_ADDED:
            if (seaf_sync_manager_ignored_on_checkout(de->name, NULL)) {
                seaf_message ("Path %s is invalid on Windows, skip creating.\n",
                              de->name);
                /* send_sync_error_notification (repo->id, repo->name, de->name, */
                /*                               SYNC_ERROR_ID_INVALID_PATH); */
                break;
            }

            rawdata_to_hex (de->sha1, id, 20);
            repo_tree_add_subtree (repo->tree,
                                   de->name,
                                   id,
                                   de->mtime);
            break;
        case DIFF_STATUS_MODIFIED:
            if (seaf_sync_manager_ignored_on_checkout(de->name, NULL)) {
                seaf_message ("Path %s is invalid on Windows, skip update.\n",
                              de->name);
                /* send_sync_error_notification (repo->id, repo->name, de->name, */
                /*                               SYNC_ERROR_ID_INVALID_PATH); */
                break;
            }

            /* handle_file_conflict (repo->id, de->name, &conflicted); */

            rawdata_to_hex (de->sha1, id, 20);
            repo_tree_set_file_mtime (repo->tree,
                                      de->name,
                                      de->mtime);
            repo_tree_set_file_size (repo->tree,
                                     de->name,
                                     de->size);
            repo_tree_set_file_id (repo->tree, de->name, id);

            break;
        }
    }

    seaf_branch_set_commit (master, new_head_id);
    master->opid = repo->head->opid;
    seaf_branch_set_commit (repo->head, new_head_id);

    /* Update both branches in db in a single operatoin, to prevent race conditions. */
    seaf_branch_manager_update_repo_branches (seaf->branch_mgr, repo->head);

    /* Update repo name if it's changed on server. */
    if (g_strcmp0 (local_head->repo_name, remote_head->repo_name) != 0) {
        seaf_repo_manager_rename_repo (seaf->repo_mgr, repo->id, remote_head->repo_name);
    }

out:
    seaf_branch_unref (master);
    seaf_commit_unref (local_head);
    seaf_commit_unref (remote_head);
    g_list_free_full (diff_results, (GDestroyNotify)diff_entry_free);
    return ret;
}

typedef struct _LoadRepoTreeAux {
    SyncTask *task;
    char new_head_id[41];
    gboolean success;
} LoadRepoTreeAux;

static void *
load_repo_tree_job (void *vdata)
{
    LoadRepoTreeAux *aux = vdata;
    SyncTask *task = aux->task;
    SeafRepo *repo = task->repo;

    if (task->is_clone) {
        if (seaf_repo_load_fs (repo, TRUE) < 0) {
            aux->success = FALSE;
        } else {
            aux->success = TRUE;
        }
    } else {
        if (merge_remote_head_to_repo_tree (repo, aux->new_head_id) < 0) {
            aux->success = FALSE;
        } else {
            aux->success = TRUE;
        }
    }

    return aux;
}

static void
load_repo_tree_done (void *vresult)
{
    LoadRepoTreeAux *aux = vresult;
    SyncTask *task = aux->task;

    SeafSyncManagerPriv *priv = seaf->sync_mgr->priv;

    if (aux->success) {
        g_hash_table_remove (priv->repo_tokens, task->repo->id);
        transition_sync_state (task, SYNC_STATE_DONE);
    } else {
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_DATA_CORRUPT);
    }

    g_free (aux);
}

static void
handle_repo_fetched_clone (SeafileSession *seaf,
                           HttpTxTask *tx_task,
                           SeafSyncManager *mgr)
{
    SyncInfo *info = get_sync_info (mgr, tx_task->repo_id);
    SyncTask *task = info->current_task;

    if (!task) {
        seaf_warning ("BUG: sync task not found after fetch is done.\n");
        return;
    }

    if (tx_task->state == HTTP_TASK_STATE_CANCELED) {
        transition_sync_state (task, SYNC_STATE_CANCELED);
        return;
    } else if (tx_task->state == HTTP_TASK_STATE_ERROR) {
        task->tx_error_code = tx_task->error;
        if (tx_task->error == HTTP_TASK_ERR_FORBIDDEN) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_ACCESS_DENIED);
        } else if (tx_task->error == HTTP_TASK_ERR_FILE_LOCKED) {
            task->unsyncable_path = tx_task->unsyncable_path;
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FILES_LOCKED_BY_USER);
        }
        else if (tx_task->error == HTTP_TASK_ERR_FOLDER_PERM_DENIED) {
            task->unsyncable_path = tx_task->unsyncable_path;
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FOLDER_PERM_DENIED);
        } else if (tx_task->error == HTTP_TASK_ERR_NO_PERMISSION_TO_SYNC) {
            task->unsyncable_path = tx_task->unsyncable_path;
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_PERM_NOT_SYNCABLE);
        } else if (tx_task->error == HTTP_TASK_ERR_NO_WRITE_PERMISSION) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_NO_WRITE_PERMISSION);
        } else
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        return;
    }

    SeafRepo *repo = seaf_repo_manager_get_repo (seaf->repo_mgr,
                                                 tx_task->repo_id);
    if (repo == NULL) {
        seaf_warning ("Cannot find repo %s after fetched.\n", 
                      tx_task->repo_id);
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_DATA_CORRUPT);
        return;
    }

    seaf_repo_manager_set_repo_token (seaf->repo_mgr, repo, task->token);
    if (info->repo_info->is_readonly)
        seaf_repo_set_readonly (repo);
    if (!repo->worktree)
        seaf_repo_set_worktree (repo, info->repo_info->display_name);

    /* Set task->repo since the repo exists now. */
    task->repo = repo;

    LoadRepoTreeAux *aux = g_new(LoadRepoTreeAux, 1);
    aux->task = task;

    if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                       load_repo_tree_job,
                                       load_repo_tree_done,
                                       aux) < 0) {
        seaf_warning ("Failed to start load repo tree job.\n");
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_UNKNOWN);
        g_free (aux);
        return;
    }

    transition_sync_state (task, SYNC_STATE_LOAD_REPO);
}

static void
handle_repo_fetched_sync (SeafileSession *seaf,
                          HttpTxTask *tx_task,
                          SeafSyncManager *mgr)
{
    SyncInfo *info = get_sync_info (mgr, tx_task->repo_id);
    SyncTask *task = info->current_task;
    SeafRepo *repo;

    if (!task) {
        seaf_warning ("BUG: sync task not found after fetch is done.\n");
        return;
    }

    repo = task->repo;
    if (repo->delete_pending) {
        transition_sync_state (task, SYNC_STATE_CANCELED);
        return;
    }

    if (tx_task->state == HTTP_TASK_STATE_CANCELED) {
        transition_sync_state (task, SYNC_STATE_CANCELED);
        return;
    } else if (tx_task->state == HTTP_TASK_STATE_ERROR) {
        task->tx_error_code = tx_task->error;
        if (tx_task->error == HTTP_TASK_ERR_FORBIDDEN) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_ACCESS_DENIED);
        } else if (tx_task->error == HTTP_TASK_ERR_FILE_LOCKED) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FILES_LOCKED_BY_USER);
        } else if (tx_task->error == HTTP_TASK_ERR_FOLDER_PERM_DENIED) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FOLDER_PERM_DENIED);
        } else if (tx_task->error == HTTP_TASK_ERR_NO_PERMISSION_TO_SYNC) {
            task->unsyncable_path = tx_task->unsyncable_path;
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_PERM_NOT_SYNCABLE);
        } else if (tx_task->error == HTTP_TASK_ERR_NO_WRITE_PERMISSION) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_NO_WRITE_PERMISSION);
        } else
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FETCH);
        return;
    }

    LoadRepoTreeAux *aux = g_new0 (LoadRepoTreeAux, 1);
    aux->task = task;
    memcpy (aux->new_head_id, tx_task->head, 40);

    if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                       load_repo_tree_job,
                                       load_repo_tree_done,
                                       aux) < 0) {
        seaf_warning ("Failed to start load repo tree job.\n");
        seaf_sync_manager_set_task_error (task, SYNC_ERROR_UNKNOWN);
        g_free (aux);
        return;
    }

    transition_sync_state (task, SYNC_STATE_LOAD_REPO);
}

static void
on_repo_http_fetched (SeafileSession *seaf,
                      HttpTxTask *tx_task,
                      SeafSyncManager *mgr)
{
    if (tx_task->is_clone)
        handle_repo_fetched_clone (seaf, tx_task, mgr);
    else
        handle_repo_fetched_sync (seaf, tx_task, mgr);
}

static void
print_upload_corrupt_debug_info (SeafRepo *repo)
{
    SeafBranch *local = NULL, *master = NULL;
    SeafCommit *local_head = NULL, *master_head = NULL;

    seaf_message ("Repo %s(%s) local metadata is found corrupted when upload. "
                  "Printing debug information and removing the local repo. "
                  "It will be resynced later.\n", repo->name, repo->id);

    local = seaf_branch_manager_get_branch (seaf->branch_mgr, repo->id, "local");
    if (!local) {
        seaf_warning ("Failed to get local branch of repo %s.\n", repo->id);
        goto out;
    }

    master = seaf_branch_manager_get_branch (seaf->branch_mgr, repo->id, "master");
    if (!master) {
        seaf_warning ("Failed to get master branch of repo %s.\n", repo->id);
        goto out;
    }

    GList *ops = NULL, *ptr;
    JournalOp *op;
    gboolean error = FALSE;

    seaf_message ("Operations in journal used to create the commit "
                  "(op id %"G_GINT64_FORMAT" to %"G_GINT64_FORMAT"):\n",
                  master->opid + 1, local->opid);

    ops = journal_read_ops (repo->journal, master->opid + 1, local->opid, &error);
    for (ptr = ops; ptr; ptr = ptr->next) {
        op = ptr->data;
        seaf_message ("%"G_GINT64_FORMAT", %d, %s, %s, %"
                      G_GINT64_FORMAT", %"G_GINT64_FORMAT", %u\n",
                      op->opid, op->type, op->path, op->new_path, op->size, op->mtime, op->mode);
    }
    g_list_free_full (ops, (GDestroyNotify)journal_op_free);

    local_head = seaf_commit_manager_get_commit (seaf->commit_mgr, repo->id, repo->version,
                                                 local->commit_id);
    if (!local_head) {
        seaf_warning ("Failed to get head of local branch for repo %s.\n", repo->id);
        goto out;
    }

    master_head = seaf_commit_manager_get_commit (seaf->commit_mgr, repo->id, repo->version,
                                                  master->commit_id);
    if (!master_head) {
        seaf_warning ("Failed to get head of master branch for repo %s.\n", repo->id);
        goto out;
    }

    GList *results = NULL;
    DiffEntry *de;
    if (diff_commits (master_head, local_head, &results, TRUE) < 0) {
        seaf_warning ("Failed to diff repo %s.\n", repo->id);
        goto out;
    }

    seaf_message ("Diff results:\n");

    for (ptr = results; ptr; ptr = ptr->next) {
        de = ptr->data;
        seaf_message ("%c %s %s\n", de->status, de->name, de->new_name);
    }
    g_list_free_full (results, (GDestroyNotify)diff_entry_free);

out:
    seaf_branch_unref (local);
    seaf_branch_unref (master);
    seaf_commit_unref (local_head);
    seaf_commit_unref (master_head);

    return;
}
    
static void
on_repo_http_uploaded (SeafileSession *seaf,
                       HttpTxTask *tx_task,
                       SeafSyncManager *manager)
{
    SyncInfo *info = get_sync_info (manager, tx_task->repo_id);
    SyncTask *task = info->current_task;

    if (task->repo->delete_pending) {
        transition_sync_state (task, SYNC_STATE_CANCELED);
        return;
    }

    if (tx_task->state == HTTP_TASK_STATE_FINISHED) {
        seaf_message ("removing blocks for repo %s\n", tx_task->repo_id);
        /* Since the sync loop is a background thread, it should be no problem
         * to block it a bit.
         */
        seaf_block_manager_remove_store (seaf->block_mgr, task->repo->id);

        task->uploaded = TRUE;
        check_head_commit_http (task);
    } else if (tx_task->state == HTTP_TASK_STATE_CANCELED) {
        transition_sync_state (task, SYNC_STATE_CANCELED);
    } else if (tx_task->state == HTTP_TASK_STATE_ERROR) {
        task->tx_error_code = tx_task->error;
        if (tx_task->error == HTTP_TASK_ERR_FORBIDDEN) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_ACCESS_DENIED);
        } else if (tx_task->error == HTTP_TASK_ERR_FILE_LOCKED) {
            task->unsyncable_path = tx_task->unsyncable_path;
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FILES_LOCKED_BY_USER);
        } else if (tx_task->error == HTTP_TASK_ERR_FOLDER_PERM_DENIED) {
            task->unsyncable_path = tx_task->unsyncable_path;
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_FOLDER_PERM_DENIED);
        } else if (tx_task->error == HTTP_TASK_ERR_TOO_MANY_FILES) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_TOO_MANY_FILES);
        } else if (tx_task->error == HTTP_TASK_ERR_NO_WRITE_PERMISSION) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_NO_WRITE_PERMISSION);
        } else if (tx_task->error == HTTP_TASK_ERR_NO_PERMISSION_TO_SYNC) {
            task->unsyncable_path = tx_task->unsyncable_path;
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_PERM_NOT_SYNCABLE);
        } else if (tx_task->error == HTTP_TASK_ERR_NO_QUOTA) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_QUOTA_FULL);
            /* Only notify "quota full" once. */
            /* if (!task->repo->quota_full_notified) { */
            /*     send_sync_error_notification (repo->id, repo->name, NULL, */
            /*                                   SYNC_ERROR_ID_QUOTA_FULL); */
            /*     task->repo->quota_full_notified = 1; */
            /* } */
        } else if (tx_task->error == HTTP_TASK_ERR_BLOCK_MISSING) {
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_BLOCK_MISSING);
        } else {
            if (tx_task->error == HTTP_TASK_ERR_BAD_LOCAL_DATA)
                print_upload_corrupt_debug_info (task->repo);
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_UPLOAD);
        }
    }
}

#define MAX_COMMIT_SIZE 100 * (1 << 20) /* 100MB */

static gboolean
is_repo_writable (SeafRepo *repo)
{
    if (repo->is_readonly && seaf_repo_manager_get_folder_perm_timestamp (seaf->repo_mgr, repo->id) < 0)
        return FALSE;

    return TRUE;
}

static void
handle_update_file_op (SeafRepo *repo, ChangeSet *changeset, const char *username,
                       JournalOp *op, gboolean renamed_from_ignored,
                       GHashTable *updated_files, gint64 *total_size,
                       SeafileCrypt *crypt)
{
    unsigned char sha1[20] = {0};
    SeafStat st;
    FileCacheStat cache_st, *file_info;
    gboolean changed = FALSE;

    /* If index doesn't complete, st will be all-zero. */
    memset (&st, 0, sizeof(st));

    if (file_cache_mgr_index_file (seaf->file_cache_mgr,
                                   repo->id,
                                   repo->version,
                                   op->path,
                                   crypt,
                                   TRUE,
                                   sha1,
                                   &changed) < 0) {
        seaf_warning ("Failed to index file %s in repo %s, skip.\n",
                      op->path, repo->id);
        goto out;
    }

    if (file_cache_mgr_stat (seaf->file_cache_mgr,
                             repo->id, op->path,
                             &cache_st) == 0) {
        st.st_size = cache_st.size;
        st.st_mtime = cache_st.mtime;
        st.st_mode = op->mode;
    } else {
        st.st_size = op->size;
        st.st_mtime = op->mtime;
        st.st_mode = op->mode;
    }

    add_to_changeset (changeset,
                      DIFF_STATUS_MODIFIED,
                      sha1,
                      &st,
                      username,
                      op->path,
                      NULL);

    if (changed)
        *total_size += (gint64)st.st_size;

out:
    file_info = g_new0 (FileCacheStat, 1);
    rawdata_to_hex (sha1, file_info->file_id, 20);
    file_info->mtime = st.st_mtime;
    file_info->size = st.st_size;

    /* Keep the last information if there was one in the hash table. */
    g_hash_table_replace (updated_files, g_strdup(op->path), file_info);
}

typedef struct CheckRenameAux {
    SeafRepo *repo;
    ChangeSet *changeset;
    const char *username;
    GHashTable *updated_files;
    gint64 *total_size;
    gboolean renamed_from_ignored;
} CheckRenameAux;

static void
check_renamed_file_cb (const char *repo_id, const char *file_path,
                       SeafStat *st, void *user_data)
{
    char *filename = g_path_get_basename (file_path);
    gboolean file_ignored = seaf_repo_manager_ignored_on_commit (filename);
    if (file_ignored)
        return;

    CheckRenameAux *aux = (CheckRenameAux *)user_data;
    JournalOp *op;
    SeafRepo *repo = NULL;
    SeafileCrypt *crypt = NULL;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo)
        return;

    if (repo->encrypted)
        crypt = seafile_crypt_new (repo->enc_version, repo->enc_key, repo->enc_iv);

    /* Create a faked journal op. */
    op = journal_op_new (OP_TYPE_UPDATE_FILE, file_path, NULL,
                         (gint64)st->st_size, (gint64)st->st_mtime, (guint32)st->st_mode);
    handle_update_file_op (aux->repo, aux->changeset, aux->username,
                           op, aux->renamed_from_ignored,
                           aux->updated_files, aux->total_size,
                           crypt);
    g_free (crypt);
    journal_op_free (op);
    seaf_repo_unref (repo);
}

/* Excel first writes update to a temporary file and then rename the file to
 * xlsx/xls. Unfortunately the temp file dosen't have specific pattern.
 * We can only ignore renaming from non xlsx file to xlsx file.
 */
static gboolean
ignore_xlsx_update (const char *src_path, const char *dst_path)
{
    GPatternSpec *pattern_xlsx = g_pattern_spec_new ("*.xlsx");
    GPatternSpec *pattern_xls = g_pattern_spec_new ("*.xls");
    int ret = FALSE;

    if (!g_pattern_match_string(pattern_xlsx, src_path) &&
        g_pattern_match_string(pattern_xlsx, dst_path))
        ret = TRUE;

    if (!g_pattern_match_string(pattern_xls, src_path) &&
        g_pattern_match_string(pattern_xls, dst_path))
        ret = TRUE;

    g_pattern_spec_free (pattern_xlsx);
    g_pattern_spec_free (pattern_xls);
    return ret;
}

static void
handle_rename_op (SeafRepo *repo, ChangeSet *changeset, const char *username,
                  GHashTable *updated_files, JournalOp *op, gint64 *total_size)
{
    char *src_filename, *dst_filename;
    gboolean src_ignored, dst_ignored;

    src_filename = g_path_get_basename (op->path);
    dst_filename = g_path_get_basename (op->new_path);

    src_ignored = (seaf_repo_manager_ignored_on_commit (src_filename) ||
                   ignore_xlsx_update (src_filename, dst_filename));
    dst_ignored = seaf_repo_manager_ignored_on_commit (dst_filename);

    /* If destination path is ignored, just remove the src path. */
    if (dst_ignored) {
        remove_from_changeset (changeset,
                               op->path,
                               FALSE,
                               NULL);
        goto out;
    }

    /* Now destination is not ignored. */

    if (!src_ignored) {
        add_to_changeset (changeset,
                          DIFF_STATUS_RENAMED,
                          NULL,
                          NULL,
                          username,
                          op->path,
                          op->new_path);
    }

    /* We should always scan the destination to check whether files are
     * changed. For example, in the following case:
     * 1. file a.txt is updated;
     * 2. a.txt is moved to test/a.txt;
     * If the two operations are executed in a batch, the updated content
     * of a.txt won't be committed if we don't scan the destination, because
     * when we process the update operation, a.txt is already not in its
     * original place.
     */
    CheckRenameAux aux;
    aux.repo = repo;
    aux.changeset = changeset;
    aux.username = username;
    aux.updated_files = updated_files;
    aux.total_size = total_size;
    aux.renamed_from_ignored = src_ignored;
    file_cache_mgr_traverse_path (seaf->file_cache_mgr,
                                  repo->id,
                                  op->new_path,
                                  check_renamed_file_cb,
                                  NULL,
                                  &aux);

out:
    g_free (src_filename);
    g_free (dst_filename);
}

static int
apply_journal_ops_to_changeset (SeafRepo *repo, ChangeSet *changeset,
                                const char *username, gint64 *last_opid,
                                GHashTable *updated_files)
{
    GList *ops, *ptr;
    JournalOp *op, *next_op;
    SeafStat st;
    gboolean error;
    unsigned char allzero[20] = {0};
    gint64 total_size = 0;
    char *filename;
    RepoTreeStat tree_st;
    SeafileCrypt *crypt = NULL;

    ops = journal_read_ops (repo->journal, *last_opid + 1, G_MAXINT64, &error);
    if (error) {
        seaf_warning ("Failed to read operations from journal for repo %s.\n",
                      repo->id);
        return -1;
    }

    if (!ops) {
        seaf_message ("All operations of repo %s(%.8s) have been processed.\n",
                      repo->name, repo->id);
        repo->partial_commit_mode = FALSE;
        return 0;
    }

    if (repo->encrypted)
        crypt = seafile_crypt_new (repo->enc_version, repo->enc_key, repo->enc_iv);

    for (ptr = ops; ptr; ptr = ptr->next) {
        op = (JournalOp *)ptr->data;
        if (ptr->next)
            next_op = (JournalOp *)(ptr->next->data);
        else
            next_op = NULL;
        filename = g_path_get_basename (op->path);

        seaf_debug ("Processing event %d %s\n", op->type, op->path);

        switch (op->type) {
        case OP_TYPE_CREATE_FILE:
            /* If the next op is update the same file, skip this one. */
            if (next_op &&
                next_op->type == OP_TYPE_UPDATE_FILE &&
                strcmp (next_op->path, op->path) == 0)
                break;

            if (seaf_repo_manager_ignored_on_commit (filename))
                break;

            st.st_size = op->size;
            st.st_mtime = op->mtime;
            st.st_mode = op->mode;
            add_to_changeset (changeset,
                              DIFF_STATUS_ADDED,
                              allzero,
                              &st,
                              username,
                              op->path,
                              NULL);
            break;
        case OP_TYPE_DELETE_FILE:
            if (seaf_repo_manager_ignored_on_commit (filename))
                break;

            remove_from_changeset (changeset,
                                   op->path,
                                   FALSE,
                                   NULL);
            g_hash_table_remove (updated_files, op->path);
            break;
        case OP_TYPE_UPDATE_FILE:
            /* If the next op is update the same file, skip this one. */
            if (next_op &&
                next_op->type == OP_TYPE_UPDATE_FILE &&
                strcmp (next_op->path, op->path) == 0)
                break;

            if (seaf_repo_manager_ignored_on_commit (filename))
                break;

            handle_update_file_op (repo, changeset, username, op, FALSE,
                                   updated_files, &total_size, crypt);
            if (total_size >= MAX_COMMIT_SIZE) {
                seaf_message ("Creating partial commit after adding %s in repo %s(%.8s).\n",
                              op->path, repo->name, repo->id);
                repo->partial_commit_mode = TRUE;
                *last_opid = op->opid;
                g_free (filename);
                goto out;
            }

            break;
        case OP_TYPE_RENAME:
            handle_rename_op (repo, changeset, username,
                              updated_files, op, &total_size);
            if (total_size >= MAX_COMMIT_SIZE) {
                seaf_message ("Creating partial commit after rename %s in repo %s(%.8s).\n",
                              op->path, repo->name, repo->id);
                repo->partial_commit_mode = TRUE;
                *last_opid = op->opid;
                g_free (filename);
                goto out;
            }

            break;
        case OP_TYPE_MKDIR:
            if (seaf_repo_manager_ignored_on_commit (filename))
                break;

            st.st_size = op->size;
            st.st_mtime = op->mtime;
            st.st_mode = S_IFDIR;
            add_to_changeset (changeset,
                              DIFF_STATUS_DIR_ADDED,
                              allzero,
                              &st,
                              username,
                              op->path,
                              NULL);
            break;
        case OP_TYPE_RMDIR:
            if (seaf_repo_manager_ignored_on_commit (filename))
                break;

            remove_from_changeset (changeset,
                                   op->path,
                                   FALSE,
                                   NULL);
            break;
        case OP_TYPE_UPDATE_ATTR:
            if (seaf_repo_manager_ignored_on_commit (filename))
                break;

            /* Don't update the file if it doesn't exist in the current
             * repo tree.
             */
            if (repo_tree_stat_path (repo->tree, op->path, &tree_st) < 0) {
                break;
            }

            st.st_size = op->size;
            st.st_mtime = op->mtime;
            st.st_mode = op->mode;
            add_to_changeset (changeset,
                              DIFF_STATUS_MODIFIED,
                              NULL,
                              &st,
                              username,
                              op->path,
                              NULL);
            break;
        default:
            seaf_warning ("Unknwon op type %d, skipped.\n", op->type);
        }

        g_free (filename);

        if (!ptr->next) {
            seaf_message ("All operations of repo %s(%.8s) have been processed.\n",
                          repo->name, repo->id);
            repo->partial_commit_mode = FALSE;
            *last_opid = op->opid;
        }
    }

out:
    g_free (crypt);
    g_list_free_full (ops, (GDestroyNotify)journal_op_free);

    return 0;
}

static int
update_head_commit (SeafRepo *repo, const char *root_id,
                    const char *desc, const char *username,
                    gint64 last_opid)
{
    SeafCommit *commit;
    int ret = 0;

    commit = seaf_commit_new (NULL, repo->id, root_id,
                              username,
                              seaf->client_id,
                              desc, 0);

    commit->parent_id = g_strdup (repo->head->commit_id);

    /* Add this computer's name to commit. */
    commit->device_name = g_strdup(seaf->client_name);
    commit->client_version = g_strdup("seadrive_"SEAFILE_CLIENT_VERSION);

    seaf_repo_to_commit (repo, commit);

    if (seaf_commit_manager_add_commit (seaf->commit_mgr, commit) < 0) {
        ret = -1;
        goto out;
    }

    seaf_branch_set_commit (repo->head, commit->commit_id);
    repo->head->opid = last_opid;

    if (seaf_branch_manager_update_branch (seaf->branch_mgr, repo->head) < 0) {
        ret = -1;
    }

out:
    seaf_commit_unref (commit);
    return ret;
}

static int
update_head_opid (SeafRepo *repo, gint64 opid)
{
    repo->head->opid = opid;
    if (seaf_branch_manager_update_opid (seaf->branch_mgr,
                                         repo->id,
                                         "local",
                                         opid) < 0)
        return -1;

    if (seaf_branch_manager_update_opid (seaf->branch_mgr,
                                         repo->id,
                                         "master",
                                         opid) < 0)
        return -1;
    return 0;
}

static int
create_commit_from_changeset (SeafRepo *repo, ChangeSet *changeset,
                              gint64 last_opid, const char *username,
                              gboolean *changed)
{
    char *desc = NULL;
    char *root_id = NULL;
    SeafCommit *head = NULL;
    GList *diff_results = NULL;
    int ret = 0;

    *changed = TRUE;

    root_id = commit_tree_from_changeset (changeset);
    if (!root_id) {
        seaf_warning ("Failed to create commit tree for repo %s.\n", repo->id);
        ret = -1;
        goto out;
    }

    head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                           repo->id, repo->version,
                                           repo->head->commit_id);
    if (!head) {
        seaf_warning ("Head commit %s for repo %s not found\n",
                      repo->head->commit_id, repo->id);
        ret = -1;
        goto out;
    }

    if (strcmp (head->root_id, root_id) != 0) {
        if (!is_repo_writable (repo)) {
            seaf_warning ("Skip creating commit for read-only repo: %s.\n", repo->id);
            ret = -1;
            goto out;
        }
        /* Calculate diff for more accurate commit descriptions. */
        diff_commit_roots (repo->id, repo->version, head->root_id, root_id, &diff_results, TRUE);
        desc = diff_results_to_description (diff_results);
        if (!desc)
            desc = g_strdup("");

        if (update_head_commit (repo, root_id, desc, username, last_opid) < 0) {
            seaf_warning ("Failed to update head commit for repo %s.\n", repo->id);
            ret = -1;
            goto out;
        }
    } else {
        *changed = FALSE;
        if (update_head_opid (repo, last_opid) < 0) {
            seaf_warning ("Failed to update head opid for repo %s.\n", repo->id);
            ret = -1;
            goto out;
        }
    }

out:
    g_free (desc);
    g_free (root_id);
    seaf_commit_unref (head);
    g_list_free_full (diff_results, (GDestroyNotify)diff_entry_free);
    return ret;
}

static int
commit_repo (SeafRepo *repo, gboolean *changed)
{
    ChangeSet *changeset;
    gint64 last_opid;
    SeafAccount *account;
    GHashTable *updated_files = NULL;
    GHashTableIter iter;
    gpointer key, value;
    int ret = 0;

    changeset = changeset_new (repo->id);
    if (!changeset) {
        seaf_warning ("Failed to create changeset for repo %s.\n", repo->id);
        return -1;
    }

    account = seaf_repo_manager_get_account (seaf->repo_mgr, repo->server, repo->user);
    if (!account) {
        seaf_warning ("No current account found.\n");
        ret = -1;
        goto out;
    }

    last_opid = repo->head->opid;

    updated_files = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);

    if (apply_journal_ops_to_changeset (repo, changeset, account->username,
                                        &last_opid, updated_files) < 0) {
        seaf_warning ("Failed to apply journal operations to changeset for repo %s.\n",
                      repo->id);
        ret = -1;
        goto out;
    }

    /* No new operations. */
    if (last_opid == repo->head->opid) {
        goto out;
    }

    if (create_commit_from_changeset (repo, changeset, last_opid,
                                      account->username, changed) < 0) {
        ret = -1;
        goto out;
    }

    /* Update cached file attrs after commit is done. */

    char *path;
    FileCacheStat *st;
    g_hash_table_iter_init (&iter, updated_files);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        path = (char *)key;
        st = (FileCacheStat *)value;
        /* If mtime is 0, the file was actually not indexed. Just skip this. */
        if (st->mtime != 0) {
            /* Mark file content as not-yet-uploaded to server, so that
             * they won't be removed by cache cleaning routine.
             * They'll be marked as uploaded again when upload finishes.
             */
            file_cache_mgr_set_file_uploaded (seaf->file_cache_mgr,
                                              repo->id, path, FALSE);

            repo_tree_set_file_id (repo->tree, path, st->file_id);

            file_cache_mgr_set_attrs (seaf->file_cache_mgr, repo->id, path,
                                      st->mtime, st->size, st->file_id);
        }
    }

out:
    seaf_account_free (account);
    changeset_free (changeset);
    if (updated_files)
        g_hash_table_destroy (updated_files);
    return ret;
}



struct CommitResult {
    SyncTask *task;
    gboolean changed;
    gboolean success;
};

static void *
commit_job (void *vtask)
{
    SyncTask *task = vtask;
    struct CommitResult *res = g_new0 (struct CommitResult, 1);

    res->task = task;
    res->success = TRUE;

    if (commit_repo (task->repo, &res->changed) < 0) {
        res->success = FALSE;
    }

    return res;
}

static void
commit_job_done (void *vres)
{
    struct CommitResult *res = (struct CommitResult *)vres;
    SyncTask *task = res->task;
    SeafRepo *repo = task->repo;
    HttpServerState *state = task->server_state;

    if (repo->delete_pending) {
        transition_sync_state (task, SYNC_STATE_CANCELED);
        g_free (res);
        return;
    }

    if (!res->success) {
        seaf_sync_manager_set_task_error (res->task, SYNC_ERROR_COMMIT);
        g_free (res);
        return;
    }

    if (res->changed) {
        char *desc = NULL;
        desc = exceed_max_deleted_files (repo);
        if (desc) {
            notify_delete_confirmation (repo->name, desc, repo->head->commit_id);
            seaf_warning ("Delete more than %d files, add delete confirmation.\n", seaf->delete_confirm_threshold);
            task->info->del_confirmation_pending = TRUE;
            seaf_sync_manager_set_task_error (res->task, SYNC_ERROR_DEL_CONFIRMATION_PENDING);
            g_free (desc);
            g_free (res);
            return;
        }
        int rc = http_tx_manager_add_upload (seaf->http_tx_mgr,
                                             repo->id,
                                             repo->version,
                                             repo->server,
                                             repo->user,
                                             repo->repo_uname,
                                             state->effective_host,
                                             repo->token,
                                             state->http_version,
                                             state->use_fileserver_port,
                                             NULL);
        if (rc < 0) {
            seaf_warning ("Failed to add upload task for repo %s to server %s.\n",
                          repo->id, state->effective_host);
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_START_UPLOAD);
        } else {
            transition_sync_state (task, SYNC_STATE_UPLOAD);
        }
    } else {
        transition_sync_state (res->task, SYNC_STATE_DONE);
    }

    g_free (res);
}

static gboolean
create_commit_from_journal (SyncInfo *info, HttpServerState *state, SeafRepo *repo)
{
    JournalStat st;
    gint now = (gint)time(NULL);

    journal_get_stat (repo->journal, &st);

    if (st.last_commit_time == 0 ||
        (st.last_change_time >= st.last_commit_time &&
         now - st.last_change_time >= 2) ||
        repo->partial_commit_mode) {

        if (!repo->partial_commit_mode)
            journal_set_last_commit_time (repo->journal, now);

        SyncTask *task = sync_task_new (info, state, NULL, NULL, repo->token, FALSE);

        if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                           commit_job,
                                           commit_job_done,
                                           task) < 0) {
            seaf_warning ("Failed to schedule commit task for repo %s.\n",
                          repo->id);
            seaf_sync_manager_set_task_error (task, SYNC_ERROR_COMMIT);
            return TRUE;
        }

        transition_sync_state (task, SYNC_STATE_COMMIT);
        return TRUE;
    }

    return FALSE;
}



HttpServerInfo *
seaf_sync_manager_get_server_info (SeafSyncManager *mgr,
                                   const char *server,
                                   const char *user)
{
    SeafAccount *account;
    HttpServerState *state;
    HttpServerInfo *info = NULL;

    account = seaf_repo_manager_get_account (seaf->repo_mgr, server, user);
    if (!account) {
        return NULL;
    }

    state = get_http_server_state (mgr->priv, account->fileserver_addr);

    seaf_account_free (account);
    if (!state) {
        return NULL;
    }

    info = g_new0 (HttpServerInfo, 1);
    info->host = g_strdup (state->effective_host);
    info->use_fileserver_port = state->use_fileserver_port;

    return info;
}

void
seaf_sync_manager_free_server_info (HttpServerInfo *info)
{
    if (!info)
        return;

    g_free (info->host);
    g_free (info);
}

/* Path sync status. */

static ActivePathsInfo *
active_paths_info_new (SeafRepo *repo)
{
    ActivePathsInfo *info = g_new0 (ActivePathsInfo, 1);

    info->syncing_tree = sync_status_tree_new (repo->worktree);
    info->synced_tree = sync_status_tree_new (repo->worktree);

    return info;
}

static void
active_paths_info_free (ActivePathsInfo *info)
{
    if (!info)
        return;
    sync_status_tree_free (info->syncing_tree);
    sync_status_tree_free (info->synced_tree);
    g_free (info);
}

void
seaf_sync_manager_update_active_path (SeafSyncManager *mgr,
                                      const char *repo_id,
                                      const char *path,
                                      int mode,
                                      SyncStatus status)
{
    ActivePathsInfo *info;
    SeafRepo *repo = NULL;
    char *mount_path = NULL;

    if (!repo_id || !path) {
        seaf_warning ("BUG: empty repo_id or path.\n");
        return;
    }

    if (status <= SYNC_STATUS_NONE || status >= N_SYNC_STATUS) {
        seaf_warning ("BUG: invalid sync status %d.\n", status);
        return;
    }

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo)
        return;

    // By setting extended attributes on files in the mounted path, Nautilus can be prompted to refresh their status.
    mount_path = seaf_repo_manager_get_mount_path (seaf->repo_mgr, repo, path);

    pthread_mutex_lock (&mgr->priv->paths_lock);

    info = g_hash_table_lookup (mgr->priv->active_paths, repo_id);
    if (!info) {
        info = active_paths_info_new (repo);
        g_hash_table_insert (mgr->priv->active_paths, g_strdup(repo_id), info);
    }

    if (status == SYNC_STATUS_SYNCING) {
        sync_status_tree_del (info->synced_tree, path);
        sync_status_tree_add (info->syncing_tree, path, mode);
    } else if (status == SYNC_STATUS_SYNCED) {
        sync_status_tree_del (info->syncing_tree, path);
        sync_status_tree_add (info->synced_tree, path, mode);
    }

    pthread_mutex_unlock (&mgr->priv->paths_lock);

    if (mount_path) {
        if (status == SYNC_STATUS_SYNCING)
            seaf_setxattr (mount_path, "user.seafile-status", "syncing", 8);
        else
            seaf_setxattr (mount_path, "user.seafile-status", "cached", 7);
    }

    seaf_repo_unref (repo);
    g_free (mount_path);
}

void
seaf_sync_manager_delete_active_path (SeafSyncManager *mgr,
                                      const char *repo_id,
                                      const char *path)
{
    ActivePathsInfo *info;

    if (!repo_id || !path) {
        seaf_warning ("BUG: empty repo_id or path.\n");
        return;
    }

    pthread_mutex_lock (&mgr->priv->paths_lock);

    info = g_hash_table_lookup (mgr->priv->active_paths, repo_id);
    if (!info) {
        pthread_mutex_unlock (&mgr->priv->paths_lock);
        return;
    }

    sync_status_tree_del (info->syncing_tree, path);
    sync_status_tree_del (info->synced_tree, path);

    pthread_mutex_unlock (&mgr->priv->paths_lock);
}

static char *path_status_tbl[] = {
    "none",
    "syncing",
    "error",
    "synced",
    "partial_synced",
    "cloud",
    "readonly",
    "locked",
    "locked_by_me",
    NULL,
};

static SyncStatus
get_repo_sync_status (SeafSyncManager *mgr, const char *repo_id)
{
    ActivePathsInfo *info;
    SyncStatus status = SYNC_STATUS_CLOUD;

    pthread_mutex_lock (&mgr->priv->paths_lock);

    info = g_hash_table_lookup (mgr->priv->active_paths, repo_id);
    if (!info)
        goto out;

    if (sync_status_tree_has_file (info->syncing_tree))
        status = SYNC_STATUS_SYNCING;
    else if (sync_status_tree_has_file (info->synced_tree))
        status = SYNC_STATUS_PARTIAL_SYNCED;

out:
    pthread_mutex_unlock (&mgr->priv->paths_lock);

    return status;
}

char *
seaf_sync_manager_get_path_sync_status (SeafSyncManager *mgr,
                                        const char *repo_id,
                                        const char *path)
{
    ActivePathsInfo *info;
    SyncStatus ret = SYNC_STATUS_NONE;

    if (!repo_id || !path) {
        seaf_warning ("BUG: empty repo_id or path.\n");
        return NULL;
    }

    if (path[0] == 0) {
        ret = get_repo_sync_status (mgr, repo_id);
        goto check_special_states;
    }

    pthread_mutex_lock (&mgr->priv->paths_lock);

    info = g_hash_table_lookup (mgr->priv->active_paths, repo_id);
    if (!info) {
        pthread_mutex_unlock (&mgr->priv->paths_lock);
        ret = SYNC_STATUS_CLOUD;
        goto check_special_states;
    }

    gboolean is_dir = FALSE;
    if (sync_status_tree_exists (info->syncing_tree, path, &is_dir)) {
        ret = SYNC_STATUS_SYNCING;
    } else if (sync_status_tree_exists (info->synced_tree, path, &is_dir)) {
        if (is_dir)
            ret = SYNC_STATUS_PARTIAL_SYNCED;
        else if (!file_cache_mgr_is_file_outdated (seaf->file_cache_mgr, repo_id, path))
            ret = SYNC_STATUS_SYNCED;
        else
            ret = SYNC_STATUS_CLOUD;
    } else {
        SeafRepo *repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
        if (!repo) {
            pthread_mutex_unlock (&mgr->priv->paths_lock);
            goto out;
        }

        RepoTreeStat st;
        if (repo_tree_stat_path (repo->tree, path, &st) < 0) {
            pthread_mutex_unlock (&mgr->priv->paths_lock);
            seaf_repo_unref (repo);
            goto out;
        }

        if (S_ISDIR(st.mode) ||
            !file_cache_mgr_is_file_cached (seaf->file_cache_mgr, repo_id, path)) {
            ret = SYNC_STATUS_CLOUD;
        }

        seaf_repo_unref (repo);
    }

    pthread_mutex_unlock (&mgr->priv->paths_lock);

check_special_states:
    if (ret == SYNC_STATUS_SYNCED || ret == SYNC_STATUS_PARTIAL_SYNCED ||
        ret == SYNC_STATUS_CLOUD || ret == SYNC_STATUS_NONE)
    {
        if (!seaf_repo_manager_is_path_writable(seaf->repo_mgr, repo_id, path))
            ret = SYNC_STATUS_READONLY;
        else if (seaf_filelock_manager_is_file_locked_by_me (seaf->filelock_mgr,
                                                             repo_id, path))
            ret = SYNC_STATUS_LOCKED_BY_ME;
        else if (seaf_filelock_manager_is_file_locked (seaf->filelock_mgr,
                                                       repo_id, path))
            ret = SYNC_STATUS_LOCKED;
    }

out:
    return g_strdup(path_status_tbl[ret]);
}

void
seaf_sync_manager_remove_active_path_info (SeafSyncManager *mgr, const char *repo_id)
{
    pthread_mutex_lock (&mgr->priv->paths_lock);

    g_hash_table_remove (mgr->priv->active_paths, repo_id);

    pthread_mutex_unlock (&mgr->priv->paths_lock);
}

int
seaf_sync_manager_is_syncing (SeafSyncManager *mgr)
{
    GHashTableIter iter;
    gpointer key, value;
    SyncInfo *info;
    gboolean is_syncing = FALSE;

    pthread_mutex_lock (&mgr->priv->infos_lock);

    g_hash_table_iter_init (&iter, mgr->priv->sync_infos);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        info = (SyncInfo *)value;
        if (!info->in_sync || !info->current_task)
            continue;
        if (info->current_task->state == SYNC_STATE_UPLOAD) {
            is_syncing = TRUE;
            break;
        }
    }

    pthread_mutex_unlock (&mgr->priv->infos_lock);

    if (!is_syncing) {
        is_syncing = file_cache_mgr_is_fetching_file (seaf->file_cache_mgr);
    }

    return is_syncing;
}

static int
update_tx_state_pulse (void *vmanager)
{
    SeafSyncManager *mgr = vmanager;

    mgr->last_sent_bytes = g_atomic_int_get (&mgr->sent_bytes);
    g_atomic_int_set (&mgr->sent_bytes, 0);
    mgr->last_recv_bytes = g_atomic_int_get (&mgr->recv_bytes);
    g_atomic_int_set (&mgr->recv_bytes, 0);

    return TRUE;
}

/* Lock/unlock files on server */

typedef struct LockFileJob {
    char repo_id[37];
    char *path;
    gboolean lock;              /* False if unlock */
} LockFileJob;

static void
lock_file_job_free (LockFileJob *job)
{
    if (!job)
        return;
    g_free (job->path);
    g_free (job);
}

static void
do_lock_file (LockFileJob *job)
{
    SeafRepo *repo = NULL;
    HttpServerInfo *server = NULL;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, job->repo_id);
    if (!repo)
        return;

    seaf_message ("Auto lock file %s/%s\n", repo->name, job->path);

    int status = seaf_filelock_manager_get_lock_status (seaf->filelock_mgr,
                                                        repo->id, job->path);
    if (status != FILE_NOT_LOCKED) {
        goto out;
    }

    server = seaf_sync_manager_get_server_info (seaf->sync_mgr, repo->server, repo->user);
    if (!server)
        goto out;

    if (http_tx_manager_lock_file (seaf->http_tx_mgr,
                                   server->host,
                                   server->use_fileserver_port,
                                   repo->token,
                                   repo->id,
                                   job->path) < 0) {
        seaf_warning ("Failed to lock %s in repo %.8s on server.\n",
                      job->path, repo->id);
        goto out;
    }

    /* Mark file as locked locally so that the user can see the effect immediately. */
    seaf_filelock_manager_mark_file_locked (seaf->filelock_mgr, repo->id, job->path, LOCKED_AUTO);

out:
    seaf_repo_unref (repo);
    seaf_sync_manager_free_server_info (server);
}

static void
do_unlock_file (LockFileJob *job)
{
    SeafRepo *repo = NULL;
    HttpServerInfo *server = NULL;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, job->repo_id);
    if (!repo)
        return;

    seaf_message ("Auto unlock file %s/%s\n", repo->name, job->path);

    int status = seaf_filelock_manager_get_lock_status (seaf->filelock_mgr,
                                                        repo->id, job->path);
    if (status != FILE_LOCKED_BY_ME_AUTO) {
        goto out;
    }

    server = seaf_sync_manager_get_server_info (seaf->sync_mgr, repo->server, repo->user);
    if (!server)
        goto out;

    if (http_tx_manager_unlock_file (seaf->http_tx_mgr,
                                     server->host,
                                     server->use_fileserver_port,
                                     repo->token,
                                     repo->id,
                                     job->path) < 0) {
        seaf_warning ("Failed to unlock %s in repo %.8s on server.\n",
                      job->path, repo->id);
        goto out;
    }

    /* Mark file as unlocked locally so that the user can see the effect immediately. */
    seaf_filelock_manager_mark_file_unlocked (seaf->filelock_mgr, repo->id, job->path);

out:
    seaf_repo_unref (repo);
    seaf_sync_manager_free_server_info (server);
}

static void *
lock_file_worker (void *vdata)
{
    GAsyncQueue *queue = (GAsyncQueue *)vdata;
    LockFileJob *job;

    while (1) {
        job = g_async_queue_pop (queue);
        if (!job)
            break;

        if (job->lock)
            do_lock_file (job);
        else
            do_unlock_file (job);

        lock_file_job_free (job);
    }

    return NULL;
}

void
seaf_sync_manager_lock_file_on_server (SeafSyncManager *mgr,
                                       const char *server,
                                       const char *user,
                                       const char *repo_id,
                                       const char *path)
{
    LockFileJob *job;
    GAsyncQueue *queue = mgr->priv->lock_file_job_queue;

    if (!seaf_repo_manager_account_is_pro (seaf->repo_mgr, server, user))
        return;

    job = g_new0 (LockFileJob, 1);
    memcpy (job->repo_id, repo_id, 36);
    job->path = g_strdup(path);
    job->lock = TRUE;

    g_async_queue_push (queue, job);
}

void
seaf_sync_manager_unlock_file_on_server (SeafSyncManager *mgr,
                                         const char *server,
                                         const char *user,
                                         const char *repo_id,
                                         const char *path)
{
    LockFileJob *job;
    GAsyncQueue *queue = mgr->priv->lock_file_job_queue;

    if (!seaf_repo_manager_account_is_pro (seaf->repo_mgr, server, user))
        return;

    job = g_new0 (LockFileJob, 1);
    memcpy (job->repo_id, repo_id, 36);
    job->path = g_strdup(path);
    job->lock = FALSE;

    g_async_queue_push (queue, job);
}

/* Repo operations on server. */

// Create repo when mkdir in root, it contain follow procedures:
// 1. Invoke api to create repo
// 2. Get head commit from seaf-server
// 3. Create and init repo to make it can sync in local
int
seaf_sync_manager_create_repo (SeafSyncManager *mgr,
                               const char *server,
                               const char *user,
                               const char *repo_name)
{
    SeafAccount *account;
    char *resp = NULL;
    gint64 resp_size;
    json_t *info = NULL;
    json_error_t jerror;
    const char *repo_id;
    const char *head_commit_id;
    const char *sync_token;
    HttpServerState *state;
    SeafCommit *head_commit = NULL;
    int ret;
    char *display_name;

    account = seaf_repo_manager_get_account (seaf->repo_mgr, server ,user);
    if (!account)
        return -1;

    ret = http_tx_manager_api_create_repo (seaf->http_tx_mgr, account->server,
                                           account->token, repo_name, &resp, &resp_size);
    if (ret < 0) {
        ret = -1;
        goto out;
    }

    info = json_loadb (resp, resp_size, 0, &jerror);
    if (!info) {
        seaf_warning ("Invalid resp from create repo api: %s.\n", jerror.text);
        ret = -1;
        goto out;
    }

    repo_id = json_object_get_string_member (info, "repo_id");
    head_commit_id = json_object_get_string_member (info, "head_commit_id");
    sync_token = json_object_get_string_member (info, "token");

    if (!repo_id || !head_commit_id || !sync_token) {
        seaf_warning ("Invalid resp from create repo api.\n");
        ret = -1;
        goto out;
    }
    g_free (resp);
    resp = NULL;

    state = get_http_server_state (mgr->priv, account->fileserver_addr);
    if (!state) {
        seaf_warning ("Invalid http server state.\n");
        ret = -1;
        goto out;
    }

    ret = http_tx_manager_get_commit (seaf->http_tx_mgr, state->effective_host,
                                      state->use_fileserver_port, sync_token,
                                      repo_id, head_commit_id, &resp, &resp_size);
    if (ret < 0) {
        goto out;
    }

    head_commit = seaf_commit_from_data (head_commit_id, resp, resp_size);
    if (!head_commit) {
        seaf_warning ("Invalid commit info returned from server.\n");
        ret = -1;
        goto out;
    }

    if (seaf_obj_store_write_obj (seaf->commit_mgr->obj_store,
                                  repo_id, head_commit->version,
                                  head_commit_id, resp, (int)resp_size, TRUE) < 0) {
        ret = -1;
        goto out;
    }

    SeafRepo *repo = seaf_repo_new (repo_id, NULL, NULL);
    seaf_repo_from_commit (repo, head_commit);

    SeafBranch *branch = seaf_branch_new ("local", repo_id, head_commit_id, 0);
    seaf_branch_manager_add_branch (seaf->branch_mgr, branch);
    seaf_repo_set_head (repo, branch);
    seaf_branch_unref (branch);

    branch = seaf_branch_new ("master", repo_id, head_commit_id, 0);
    seaf_branch_manager_add_branch (seaf->branch_mgr, branch);
    seaf_branch_unref (branch);

    seaf_repo_manager_set_repo_token (seaf->repo_mgr, repo, sync_token);
    display_name = seaf_repo_manager_get_display_name_by_repo_name (seaf->repo_mgr, server, user, repo_name);
    seaf_repo_set_worktree (repo, display_name);
    g_free (display_name);

    if (seaf_repo_load_fs (repo, FALSE) < 0) {
        seaf_repo_free (repo);
        ret = -1;
        goto out;
    }

    repo->server = g_strdup (server);
    repo->user = g_strdup (user);

    seaf_repo_manager_add_repo (seaf->repo_mgr, repo);

    ret = seaf_repo_manager_add_repo_to_account (seaf->repo_mgr,
                                                 account->server,
                                                 account->username,
                                                 repo);

out:
    seaf_account_free (account);
    if (resp)
        g_free (resp);
    if (info)
        json_decref (info);
    if (head_commit)
        seaf_commit_unref (head_commit);
    return ret;
}

// Rename repo when rename dir in root, it contain follow procedures:
// 1. Invoke api to rename repo
// 2. Change some internal mapping
int
seaf_sync_manager_rename_repo (SeafSyncManager *mgr,
                               const char *server,
                               const char *user,
                               const char *repo_id,
                               const char *new_name)
{
    SeafAccount *account;
    int ret;

    account = seaf_repo_manager_get_account (seaf->repo_mgr, server, user);
    if (!account)
        return -1;

    ret = http_tx_manager_api_rename_repo (seaf->http_tx_mgr, account->server,
                                           account->token, repo_id, new_name);
    if (ret < 0)
        goto out;

    ret = seaf_repo_manager_rename_repo_on_account (seaf->repo_mgr,
                                                    account->server,
                                                    account->username,
                                                    repo_id,
                                                    new_name);
    if (ret < 0)
        goto out;

    seaf_repo_manager_rename_repo (seaf->repo_mgr, repo_id, new_name);

out:
    seaf_account_free (account);
    return ret;
}

int
seaf_sync_manager_delete_repo (SeafSyncManager *mgr,
                               const char *server,
                               const char *user,
                               const char *repo_id)
{
    SeafAccount *account;
    int ret;

    account = seaf_repo_manager_get_account (seaf->repo_mgr, server, user);
    if (!account)
        return -1;

    ret = http_tx_manager_api_delete_repo (seaf->http_tx_mgr, account->server,
                                           account->token, repo_id);
    if (ret < 0)
        goto out;

    seaf_repo_manager_remove_account_repo (seaf->repo_mgr, repo_id, server, user);

out:
    seaf_account_free (account);
    return ret;
}

gboolean
seaf_sync_manager_is_server_disconnected (SeafSyncManager *mgr)
{
    gint disconnected;

    disconnected = g_atomic_int_get (&mgr->priv->server_disconnected);

    if (disconnected == 0)
        return FALSE;
    else
        return TRUE;
}

void
seaf_sync_manager_reset_server_connected_state (SeafSyncManager *mgr)
{
    g_atomic_int_set (&mgr->priv->server_disconnected, 0);
}

static void
schedule_cache_file (const char *repo_id, const char *path, RepoTreeStat *st)
{
    if (S_ISREG(st->mode))
        file_cache_mgr_cache_file (seaf->file_cache_mgr, repo_id, path, st);
    else
        file_cache_mgr_mkdir (seaf->file_cache_mgr, repo_id, path);
}

static void
check_server_connectivity (const char *server, const char *user)
{
    HttpServerInfo *server_info = NULL;

    while (1) {
        server_info = seaf_sync_manager_get_server_info (seaf->sync_mgr, server, user);
        if (server_info)
            break;
        seaf_sleep (1);
    }

    seaf_sync_manager_free_server_info (server_info);
}

static void *
cache_file_task_worker (void *vdata)
{
    GAsyncQueue *task_queue = vdata;
    CacheFileTask *task;
    SeafRepo *repo = NULL;

    while (1) {
        task = g_async_queue_pop (task_queue);

        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, task->repo_id);
        if (!repo) {
            seaf_warning ("Failed to find repo %s.\n", task->repo_id);
            goto next;
        }

        check_server_connectivity (repo->server, repo->user);
        seaf_message ("Start to cache %s/%s\n", repo->name, task->path);
        repo_tree_traverse (repo->tree, task->path, schedule_cache_file);

        seaf_repo_unref (repo);
    next:
        g_free (task->repo_id);
        g_free (task->path);
        g_free (task);
    }

    return NULL;
}

void
seaf_sync_manager_cache_path (SeafSyncManager *mgr, const char *repo_id, const char *path)
{
    CacheFileTask *task = g_new0 (CacheFileTask, 1);
    task->repo_id = g_strdup(repo_id);
    task->path = g_strdup(path);

    g_async_queue_push (mgr->priv->cache_file_task_queue, task);
}

/* static void */
/* disable_auto_sync_for_repos (SeafSyncManager *mgr) */
/* { */
/*     GList *repos; */
/*     GList *ptr; */
/*     SeafRepo *repo; */

/*     repos = seaf_repo_manager_get_repo_list (seaf->repo_mgr, -1, -1); */
/*     for (ptr = repos; ptr; ptr = ptr->next) { */
/*         repo = ptr->data; */
/*         seaf_sync_manager_cancel_sync_task (mgr, repo->id); */
/*     } */

/*     g_list_free (repos); */
/* } */

int
seaf_sync_manager_disable_auto_sync (SeafSyncManager *mgr)
{
    if (!seaf->started) {
        seaf_message ("sync manager is not started, skip disable auto sync.\n");
        return -1;
    }

    seaf_message ("Disabled auto sync.\n");

    mgr->priv->auto_sync_enabled = FALSE;
    return 0;
}

int
seaf_sync_manager_enable_auto_sync (SeafSyncManager *mgr)
{
    if (!seaf->started) {
        seaf_message ("sync manager is not started, skip enable auto sync.\n");
        return -1;
    }

    seaf_message ("Enabled auto sync.\n");

    mgr->priv->auto_sync_enabled = TRUE;
    return 0;
}

int
seaf_sync_manager_is_auto_sync_enabled (SeafSyncManager *mgr)
{
    if (mgr->priv->auto_sync_enabled)
        return 1;
    else
        return 0;
}
