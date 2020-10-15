/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef SYNC_MGR_H
#define SYNC_MGR_H

#include <jansson.h>

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
seaf_sync_manager_get_server_info (SeafSyncManager *mgr);

void
seaf_sync_manager_free_server_info (HttpServerInfo *info);

int
seaf_sync_manager_is_syncing (SeafSyncManager *mgr);

int
seaf_handle_file_conflict (const char *repo_id,
                           const char *path,
                           gboolean *conflicted);

int
seaf_sync_manager_update_repo_list (SeafSyncManager *mgr);

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
seaf_sync_manager_get_category_sync_status (SeafSyncManager *mgr,
                                            const char *category);

char *
seaf_sync_manager_list_active_paths_json (SeafSyncManager *mgr);

int
seaf_sync_manager_active_paths_number (SeafSyncManager *mgr);

void
seaf_sync_manager_remove_active_path_info (SeafSyncManager *mgr, const char *repo_id);

int
seaf_sync_manager_create_repo (SeafSyncManager *mgr,
                               const char *repo_name);

int
seaf_sync_manager_rename_repo (SeafSyncManager *mgr,
                               const char *repo_id,
                               const char *new_name);

int
seaf_sync_manager_delete_repo (SeafSyncManager *mgr,
                               const char *repo_id);

/* Lock/unlock files on server. */

void
seaf_sync_manager_lock_file_on_server (SeafSyncManager *mgr,
                                       const char *repo_id,
                                       const char *path);

void
seaf_sync_manager_unlock_file_on_server (SeafSyncManager *mgr,
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

#endif
