/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef SYNC_MGR_H
#define SYNC_MGR_H

#include <jansson.h>

enum {
    SYNC_ERROR_LEVEL_REPO,
    SYNC_ERROR_LEVEL_FILE,
    SYNC_ERROR_LEVEL_NETWORK,
};

#define SYNC_ERROR_ID_FILE_LOCKED_BY_APP        0
#define SYNC_ERROR_ID_FOLDER_LOCKED_BY_APP      1
#define SYNC_ERROR_ID_FILE_LOCKED               2
#define SYNC_ERROR_ID_INVALID_PATH              3
#define SYNC_ERROR_ID_INDEX_ERROR               4
#define SYNC_ERROR_ID_ACCESS_DENIED             5
#define SYNC_ERROR_ID_QUOTA_FULL                6
#define SYNC_ERROR_ID_NETWORK                   7
#define SYNC_ERROR_ID_RESOLVE_PROXY             8
#define SYNC_ERROR_ID_RESOLVE_HOST              9
#define SYNC_ERROR_ID_CONNECT                   10
#define SYNC_ERROR_ID_SSL                       11
#define SYNC_ERROR_ID_TX                        12
#define SYNC_ERROR_ID_TX_TIMEOUT                13
#define SYNC_ERROR_ID_UNHANDLED_REDIRECT        14
#define SYNC_ERROR_ID_SERVER                    15
#define SYNC_ERROR_ID_LOCAL_DATA_CORRUPT        16
#define SYNC_ERROR_ID_WRITE_LOCAL_DATA          17
#define SYNC_ERROR_ID_PERM_NOT_SYNCABLE         18
#define SYNC_ERROR_ID_NO_WRITE_PERMISSION       19
#define SYNC_ERROR_ID_FOLDER_PERM_DENIED        20
#define SYNC_ERROR_ID_PATH_END_SPACE_PERIOD     21
#define SYNC_ERROR_ID_PATH_INVALID_CHARACTER    22
#define SYNC_ERROR_ID_UPDATE_TO_READ_ONLY_REPO  23
#define SYNC_ERROR_ID_CONFLICT                  24
#define SYNC_ERROR_ID_UPDATE_NOT_IN_REPO        25
#define SYNC_ERROR_ID_LIBRARY_TOO_LARGE         26
#define SYNC_ERROR_ID_MOVE_NOT_IN_REPO          27
#define SYNC_ERROR_ID_DEL_CONFIRMATION_PENDING  28
#define SYNC_ERROR_ID_INVALID_PATH_ON_WINDOWS   29
#define SYNC_ERROR_ID_TOO_MANY_FILES            30
#define SYNC_ERROR_ID_BLOCK_MISSING             31
#define SYNC_ERROR_ID_CHECKOUT_FILE             32
#define SYNC_ERROR_ID_CASE_CONFLICT             33
#define SYNC_ERROR_ID_GENERAL_ERROR             34

#define SYNC_ERROR_ID_NO_ERROR                  35

int
sync_error_level (int error);

typedef struct _SeafSyncManager SeafSyncManager;
typedef struct _SeafSyncManagerPriv SeafSyncManagerPriv;

struct _SeafileSession;

struct _SeafSyncManager {
    struct _SeafileSession   *seaf;

    /* Sent/recv bytes from all transfer tasks in this second.
     */
    gint             sent_bytes;
    gint             recv_bytes;
    gint             last_sent_bytes;
    gint             last_recv_bytes;
    /* Upload/download rate limits. */
    gint             upload_limit;
    gint             download_limit;

    SeafSyncManagerPriv *priv;
};

SeafSyncManager* seaf_sync_manager_new (struct _SeafileSession *seaf);

int seaf_sync_manager_init (SeafSyncManager *mgr);
int seaf_sync_manager_start (SeafSyncManager *mgr);

const char *
sync_error_to_str (int error);

const char *
sync_state_to_str (int state);

typedef struct HttpServerInfo {
    char *host;
    gboolean use_fileserver_port;
} HttpServerInfo;

HttpServerInfo *
seaf_sync_manager_get_server_info (SeafSyncManager *mgr,
                                   const char *server,
                                   const char *user);

void
seaf_sync_manager_free_server_info (HttpServerInfo *info);

int
seaf_sync_manager_update_account_repo_list (SeafSyncManager *mgr,
                                            const char *server,
                                            const char *username);

int
seaf_sync_manager_is_syncing (SeafSyncManager *mgr);

int
seaf_handle_file_conflict (const char *repo_id,
                           const char *path,
                           gboolean *conflicted);

/* Path sync status */

enum _SyncStatus {
    SYNC_STATUS_NONE = 0,
    SYNC_STATUS_SYNCING,
    SYNC_STATUS_ERROR,
    SYNC_STATUS_SYNCED,
    SYNC_STATUS_PARTIAL_SYNCED,
    SYNC_STATUS_CLOUD,
    SYNC_STATUS_READONLY,
    SYNC_STATUS_LOCKED,
    SYNC_STATUS_LOCKED_BY_ME,
    N_SYNC_STATUS,
};
typedef enum _SyncStatus SyncStatus;

void
seaf_sync_manager_update_active_path (SeafSyncManager *mgr,
                                      const char *repo_id,
                                      const char *path,
                                      int mode,
                                      SyncStatus status);

void
seaf_sync_manager_delete_active_path (SeafSyncManager *mgr,
                                      const char *repo_id,
                                      const char *path);

char *
seaf_sync_manager_get_path_sync_status (SeafSyncManager *mgr,
                                        const char *repo_id,
                                        const char *path);

char *
seaf_sync_manager_list_active_paths_json (SeafSyncManager *mgr);

int
seaf_sync_manager_active_paths_number (SeafSyncManager *mgr);

void
seaf_sync_manager_remove_active_path_info (SeafSyncManager *mgr, const char *repo_id);

int
seaf_sync_manager_create_repo (SeafSyncManager *mgr,
                               const char *server,
                               const char *user,
                               const char *repo_name);

int
seaf_sync_manager_rename_repo (SeafSyncManager *mgr,
                               const char *server,
                               const char *user,
                               const char *repo_id,
                               const char *new_name);

int
seaf_sync_manager_delete_repo (SeafSyncManager *mgr,
                               const char *server,
                               const char *user,
                               const char *repo_id);

/* Lock/unlock files on server. */

void
seaf_sync_manager_lock_file_on_server (SeafSyncManager *mgr,
                                       const char *server,
                                       const char *user,
                                       const char *repo_id,
                                       const char *path);

void
seaf_sync_manager_unlock_file_on_server (SeafSyncManager *mgr,
                                         const char *server,
                                         const char *user,
                                         const char *repo_id,
                                         const char *path);

/* Sync errors */

json_t *
seaf_sync_manager_list_sync_errors (SeafSyncManager *mgr);

gboolean
seaf_sync_manager_is_server_disconnected (SeafSyncManager *mgr);

void
seaf_sync_manager_reset_server_connected_state (SeafSyncManager *mgr);

void
seaf_sync_manager_cache_path (SeafSyncManager *mgr, const char *repo_id, const char *path);

int
seaf_sync_manager_disable_auto_sync (SeafSyncManager *mgr);

int
seaf_sync_manager_enable_auto_sync (SeafSyncManager *mgr);

int
seaf_sync_manager_is_auto_sync_enabled (SeafSyncManager *mgr);

// File Cache Entry Management

typedef struct _CacheEntry {
    gint64 mtime;
    gint64 size;
} CacheEntry;

CacheEntry*
seaf_sync_manager_find_cache_entry (SeafSyncManager* mgr, const char* repo_id, const char* path);

int
seaf_sync_manager_handle_file_conflict (CacheEntry *entry,
                                        const char *repo_id,
                                        const char *fullpath,
                                        const char *path,
                                        gboolean *conflicted);

int
seaf_sync_manager_add_cache_entry (SeafSyncManager* mgr,
                                   const char* repo_id, const char* path,
                                   gint64 mtime, gint64 size);

int
seaf_sync_manager_update_cache_entry (SeafSyncManager* mgr,
                                      const char* repo_id, const char* path,
                                      gint64 mtime, gint64 size);

int
seaf_sync_manager_clean_cache_entries (SeafSyncManager* mgr, const char* repo_id);

int
seaf_sync_manager_del_cached_entry (SeafSyncManager* mgr, const char* repo_id, const char* path);

void
seaf_sync_manager_lock_cache_db();

void
seaf_sync_manager_unlock_cache_db();


// Rename or move entries in repo.
// old_prefix is the prefix of existing entries. They will be renamed to start with new_prefix.
// Can be used to rename a file or a folder.
int
seaf_sync_manager_rename_cache_entries (SeafSyncManager* mgr, const char* repo_id,
                                        const char* old_prefix, const char* new_prefix);

typedef enum IgnoreReason {
    IGNORE_REASON_END_SPACE_PERIOD = 0,
    IGNORE_REASON_INVALID_CHARACTER = 1,
} IgnoreReason;

gboolean
seaf_sync_manager_ignored_on_checkout (const char *file_path, IgnoreReason *ignore_reason);

gboolean
seaf_repo_manager_ignored_on_commit (const char *filename);

void
seaf_sync_manager_check_locks_and_folder_perms (SeafSyncManager *manager, const char *server_url);

void
seaf_sync_manager_set_last_sync_time (SeafSyncManager *mgr,
                                      const char *repo_id,
                                      gint64 last_sync_time);

int
seaf_sync_manager_add_del_confirmation (SeafSyncManager *mgr,
                                        const char *confirmation_id,
                                        gboolean resync);

#endif
