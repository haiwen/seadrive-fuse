/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#include "common.h"
#include <glib/gstdio.h>

#include <pthread.h>

#include "utils.h"
#define DEBUG_FLAG SEAFILE_DEBUG_SYNC
#include "log.h"

#include "seafile-session.h"
#include "seafile-config.h"
#include "commit-mgr.h"
#include "branch-mgr.h"
#include "repo-mgr.h"
#include "fs-mgr.h"
#include "seafile-error.h"
#include "seafile-crypt.h"

#include "db.h"

struct _SeafRepoManagerPriv {
    GHashTable *repo_hash;
    pthread_rwlock_t lock;

    /* Cache of repos for current account. */
    // The key is 'server_username', the value is server and user of account.
    GHashTable *accounts;
    // The key is repo id, the value is repo info.
    GHashTable *repo_infos;
    // The key is 'server_username', the value a hash table of name_to_repo.
    GHashTable *repo_names;
    pthread_rwlock_t account_lock;

    sqlite3    *db;
    pthread_mutex_t db_lock;

    GHashTable *user_perms;     /* repo_id -> folder user perms */
    GHashTable *group_perms;    /* repo_id -> folder group perms */
    pthread_mutex_t perm_lock;
};

static SeafRepo *
load_repo (SeafRepoManager *manager, const char *repo_id);

static void load_repos (SeafRepoManager *manager, const char *seaf_dir);
static void seaf_repo_manager_del_repo_property (SeafRepoManager *manager,
                                                 const char *repo_id);

static int save_branch_repo_map (SeafRepoManager *manager, SeafBranch *branch);
static void save_repo_property (SeafRepoManager *manager,
                                const char *repo_id,
                                const char *key, const char *value);

static void
locked_file_free (LockedFile *file)
{
    if (!file)
        return;
    g_free (file->operation);
    g_free (file);
}

static gboolean
load_locked_file (sqlite3_stmt *stmt, void *data)
{
    GHashTable *ret = data;
    LockedFile *file;
    const char *path, *operation, *file_id;
    gint64 old_mtime;

    path = (const char *)sqlite3_column_text (stmt, 0);
    operation = (const char *)sqlite3_column_text (stmt, 1);
    old_mtime = sqlite3_column_int64 (stmt, 2);
    file_id = (const char *)sqlite3_column_text (stmt, 3);

    file = g_new0 (LockedFile, 1);
    file->operation = g_strdup(operation);
    file->old_mtime = old_mtime;
    if (file_id)
        memcpy (file->file_id, file_id, 40);

    g_hash_table_insert (ret, g_strdup(path), file);

    return TRUE;
}

LockedFileSet *
seaf_repo_manager_get_locked_file_set (SeafRepoManager *mgr, const char *repo_id)
{
    GHashTable *locked_files = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                      g_free,
                                                      (GDestroyNotify)locked_file_free);
    char sql[256];

    sqlite3_snprintf (sizeof(sql), sql,
                      "SELECT path, operation, old_mtime, file_id FROM LockedFiles "
                      "WHERE repo_id = '%q'",
                      repo_id);

    pthread_mutex_lock (&mgr->priv->db_lock);

    /* Ingore database error. We return an empty set on error. */
    sqlite_foreach_selected_row (mgr->priv->db, sql,
                                 load_locked_file, locked_files);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    LockedFileSet *ret = g_new0 (LockedFileSet, 1);
    ret->mgr = mgr;
    memcpy (ret->repo_id, repo_id, 36);
    ret->locked_files = locked_files;

    return ret;
}

void
locked_file_set_free (LockedFileSet *fset)
{
    if (!fset)
        return;
    g_hash_table_destroy (fset->locked_files);
    g_free (fset);
}

int
locked_file_set_add_update (LockedFileSet *fset,
                            const char *path,
                            const char *operation,
                            gint64 old_mtime,
                            const char *file_id)
{
    SeafRepoManager *mgr = fset->mgr;
    char *sql;
    sqlite3_stmt *stmt;
    LockedFile *file;
    gboolean exists;

    exists = (g_hash_table_lookup (fset->locked_files, path) != NULL);

    pthread_mutex_lock (&mgr->priv->db_lock);

    if (!exists) {
        seaf_debug ("New locked file record %.8s, %s, %s, %"
                    G_GINT64_FORMAT".\n",
                    fset->repo_id, path, operation, old_mtime);

        sql = "INSERT INTO LockedFiles VALUES (?, ?, ?, ?, ?, NULL)";
        stmt = sqlite_query_prepare (mgr->priv->db, sql);
        sqlite3_bind_text (stmt, 1, fset->repo_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text (stmt, 2, path, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text (stmt, 3, operation, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64 (stmt, 4, old_mtime);
        sqlite3_bind_text (stmt, 5, file_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step (stmt) != SQLITE_DONE) {
            seaf_warning ("Failed to insert locked file %s to db: %s.\n",
                          path, sqlite3_errmsg (mgr->priv->db));
            sqlite3_finalize (stmt);
            pthread_mutex_unlock (&mgr->priv->db_lock);
            return -1;
        }
        sqlite3_finalize (stmt);

        file = g_new0 (LockedFile, 1);
        file->operation = g_strdup(operation);
        file->old_mtime = old_mtime;
        if (file_id)
            memcpy (file->file_id, file_id, 40);

        g_hash_table_insert (fset->locked_files, g_strdup(path), file);
    } else {
        seaf_debug ("Update locked file record %.8s, %s, %s.\n",
                    fset->repo_id, path, operation);

        /* If a UPDATE record exists, don't update the old_mtime.
         * We need to keep the old mtime when the locked file was first detected.
         */

        sql = "UPDATE LockedFiles SET operation = ?, file_id = ? "
            "WHERE repo_id = ? AND path = ?";
        stmt = sqlite_query_prepare (mgr->priv->db, sql);
        sqlite3_bind_text (stmt, 1, operation, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text (stmt, 2, file_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text (stmt, 3, fset->repo_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text (stmt, 4, path, -1, SQLITE_TRANSIENT);
        if (sqlite3_step (stmt) != SQLITE_DONE) {
            seaf_warning ("Failed to update locked file %s to db: %s.\n",
                          path, sqlite3_errmsg (mgr->priv->db));
            sqlite3_finalize (stmt);
            pthread_mutex_unlock (&mgr->priv->db_lock);
            return -1;
        }
        sqlite3_finalize (stmt);

        file = g_hash_table_lookup (fset->locked_files, path);
        g_free (file->operation);
        file->operation = g_strdup(operation);
        if (file_id)
            memcpy (file->file_id, file_id, 40);
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    return 0;
}

int
locked_file_set_remove (LockedFileSet *fset, const char *path, gboolean db_only)
{
    SeafRepoManager *mgr = fset->mgr;
    char *sql;
    sqlite3_stmt *stmt;

    if (g_hash_table_lookup (fset->locked_files, path) == NULL)
        return 0;

    seaf_debug ("Remove locked file record %.8s, %s.\n",
                fset->repo_id, path);

    pthread_mutex_lock (&mgr->priv->db_lock);

    sql = "DELETE FROM LockedFiles WHERE repo_id = ? AND path = ?";
    stmt = sqlite_query_prepare (mgr->priv->db, sql);
    sqlite3_bind_text (stmt, 1, fset->repo_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text (stmt, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step (stmt) != SQLITE_DONE) {
        seaf_warning ("Failed to remove locked file %s from db: %s.\n",
                      path, sqlite3_errmsg (mgr->priv->db));
        sqlite3_finalize (stmt);
        pthread_mutex_unlock (&mgr->priv->db_lock);
        return -1;
    }
    sqlite3_finalize (stmt);
    pthread_mutex_unlock (&mgr->priv->db_lock);

    if (!db_only)
        g_hash_table_remove (fset->locked_files, path);

    return 0;
}

LockedFile *
locked_file_set_lookup (LockedFileSet *fset, const char *path)
{
    return (LockedFile *) g_hash_table_lookup (fset->locked_files, path);
}

/* Folder permissions. */

FolderPerm *
folder_perm_new (const char *path, const char *permission)
{
    FolderPerm *perm = g_new0 (FolderPerm, 1);

    perm->path = g_strdup(path);
    perm->permission = g_strdup(permission);

    return perm;
}

void
folder_perm_free (FolderPerm *perm)
{
    if (!perm)
        return;

    g_free (perm->path);
    g_free (perm->permission);
    g_free (perm);
}

static GList *
folder_perm_list_copy (GList *perms)
{
    GList *ret = NULL, *ptr;
    FolderPerm *perm, *new_perm;

    for (ptr = perms; ptr; ptr = ptr->next) {
        perm = ptr->data;
        new_perm = folder_perm_new (perm->path, perm->permission);
        ret = g_list_append (ret, new_perm);
    }

    return ret;
}

static gint
comp_folder_perms (gconstpointer a, gconstpointer b)
{
    const FolderPerm *perm_a = a, *perm_b = b;

    return (strcmp (perm_b->path, perm_a->path));
}

static void
delete_folder_perm (SeafRepoManager *mgr, const char *repo_id, FolderPermType type, FolderPerm *perm)
{
    GList *folder_perms = NULL;
    if (type == FOLDER_PERM_TYPE_USER) {
        folder_perms = g_hash_table_lookup (mgr->priv->user_perms, repo_id);
        if (!folder_perms)
            return;

        // if path is empty string, delete all folder perms in this repo.
        if (g_strcmp0 (perm->path, "") == 0) {
            g_list_free_full (folder_perms, (GDestroyNotify)folder_perm_free);
            g_hash_table_remove (mgr->priv->user_perms, repo_id);
            return;
        }

        GList *existing = g_list_find_custom (folder_perms,
                                              perm,
                                              comp_folder_perms);
        if (existing) {
            FolderPerm *old_perm = existing->data;
            folder_perms = g_list_remove (folder_perms, old_perm);
            g_hash_table_insert (mgr->priv->user_perms, g_strdup(repo_id), folder_perms);
            folder_perm_free (old_perm);
        }
    } else if (type == FOLDER_PERM_TYPE_GROUP) {
        folder_perms = g_hash_table_lookup (mgr->priv->group_perms, repo_id);
        if (!folder_perms)
            return;

        if (g_strcmp0 (perm->path, "") == 0) {
            g_list_free_full (folder_perms, (GDestroyNotify)folder_perm_free);
            g_hash_table_remove (mgr->priv->group_perms, repo_id);
            return;
        }

        GList *existing = g_list_find_custom (folder_perms,
                                              perm,
                                              comp_folder_perms);
        if (existing) {
            FolderPerm *old_perm = existing->data;
            folder_perms = g_list_remove (folder_perms, old_perm);
            g_hash_table_insert (mgr->priv->group_perms, g_strdup(repo_id), folder_perms);
            folder_perm_free (old_perm);
        }
    }
}

int
seaf_repo_manager_delete_folder_perm (SeafRepoManager *mgr,
                                      const char *repo_id,
                                      FolderPermType type,
                                      FolderPerm *perm)
{
    char *sql;
    sqlite3_stmt *stmt;

    g_return_val_if_fail ((type == FOLDER_PERM_TYPE_USER ||
                           type == FOLDER_PERM_TYPE_GROUP),
                          -1);

    if (!perm) {
        return 0;
    }

    /* Update db. */
    pthread_mutex_lock (&mgr->priv->db_lock);

    if (g_strcmp0 (perm->path, "") == 0) {
        if (type == FOLDER_PERM_TYPE_USER)
            sql = "DELETE FROM FolderUserPerms WHERE repo_id = ?";
        else
            sql = "DELETE FROM FolderGroupPerms WHERE repo_id = ?";
    } else {
        if (type == FOLDER_PERM_TYPE_USER)
            sql = "DELETE FROM FolderUserPerms WHERE repo_id = ? and path = ?";
        else
            sql = "DELETE FROM FolderGroupPerms WHERE repo_id = ? and path = ?";
    }

    stmt = sqlite_query_prepare (mgr->priv->db, sql);
    sqlite3_bind_text (stmt, 1, repo_id, -1, SQLITE_TRANSIENT);
    if (g_strcmp0 (perm->path, "") != 0)
        sqlite3_bind_text (stmt, 2, perm->path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step (stmt) != SQLITE_DONE) {
        seaf_warning ("Failed to remove folder perm for %.8s: %s.\n",
                      repo_id, sqlite3_errmsg (mgr->priv->db));
        sqlite3_finalize (stmt);
        pthread_mutex_unlock (&mgr->priv->db_lock);
        return -1;
    }
    sqlite3_finalize (stmt);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    /* Update in memory */
    pthread_mutex_lock (&mgr->priv->perm_lock);
    delete_folder_perm (mgr, repo_id, type, perm);
    pthread_mutex_unlock (&mgr->priv->perm_lock);

    return 0;
}

int
seaf_repo_manager_update_folder_perm (SeafRepoManager *mgr,
                                      const char *repo_id,
                                      FolderPermType type,
                                      FolderPerm *perm)
{
    char *sql;
    sqlite3_stmt *stmt;

    g_return_val_if_fail ((type == FOLDER_PERM_TYPE_USER ||
                           type == FOLDER_PERM_TYPE_GROUP),
                          -1);

    if (!perm) {
        return 0;
    }

    /* Update db. */
    pthread_mutex_lock (&mgr->priv->db_lock);

    if (type == FOLDER_PERM_TYPE_USER)
        sql = "DELETE FROM FolderUserPerms WHERE repo_id = ? and path = ?";
    else
        sql = "DELETE FROM FolderGroupPerms WHERE repo_id = ? and path = ?";
    stmt = sqlite_query_prepare (mgr->priv->db, sql);
    sqlite3_bind_text (stmt, 1, repo_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text (stmt, 2, perm->path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step (stmt) != SQLITE_DONE) {
        seaf_warning ("Failed to remove folder perm for %.8s(%s): %s.\n",
                      repo_id, perm->path, sqlite3_errmsg (mgr->priv->db));
        sqlite3_finalize (stmt);
        pthread_mutex_unlock (&mgr->priv->db_lock);
        return -1;
    }
    sqlite3_finalize (stmt);

    if (type == FOLDER_PERM_TYPE_USER)
        sql = "INSERT INTO FolderUserPerms VALUES (?, ?, ?)";
    else
        sql = "INSERT INTO FolderGroupPerms VALUES (?, ?, ?)";
    stmt = sqlite_query_prepare (mgr->priv->db, sql);

    sqlite3_bind_text (stmt, 1, repo_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text (stmt, 2, perm->path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text (stmt, 3, perm->permission, -1, SQLITE_TRANSIENT);

    if (sqlite3_step (stmt) != SQLITE_DONE) {
        seaf_warning ("Failed to insert folder perm for %.8s(%s): %s.\n",
                      repo_id, perm->path, sqlite3_errmsg (mgr->priv->db));
        sqlite3_finalize (stmt);
        pthread_mutex_unlock (&mgr->priv->db_lock);
        return -1;
    }

    sqlite3_finalize (stmt);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    /* Update in memory */
    GList *folder_perms;

    pthread_mutex_lock (&mgr->priv->perm_lock);

    if (type == FOLDER_PERM_TYPE_USER) {
        folder_perms = g_hash_table_lookup (mgr->priv->user_perms, repo_id);
        if (folder_perms) {
            GList *existing = g_list_find_custom (folder_perms,
                                                  perm,
                                                  comp_folder_perms);
            if (existing) {
                FolderPerm *old_perm = existing->data;
                g_free (old_perm->permission);
                old_perm->permission = g_strdup (perm->permission);
            } else {
                FolderPerm *new_perm = folder_perm_new (perm->path, perm->permission);
                folder_perms = g_list_insert_sorted (folder_perms, new_perm,
                                                     comp_folder_perms);
            }
        } else {
                FolderPerm *new_perm = folder_perm_new (perm->path, perm->permission);
                folder_perms = g_list_insert_sorted (folder_perms, new_perm,
                                                     comp_folder_perms);
        }
        g_hash_table_insert (mgr->priv->user_perms, g_strdup(repo_id), folder_perms);
    } else if (type == FOLDER_PERM_TYPE_GROUP) {
        folder_perms = g_hash_table_lookup (mgr->priv->group_perms, repo_id);
        if (folder_perms) {
            GList *existing = g_list_find_custom (folder_perms,
                                                  perm,
                                                  comp_folder_perms);
            if (existing) {
                FolderPerm *old_perm = existing->data;
                g_free (old_perm->permission);
                old_perm->permission = g_strdup (perm->permission);
            } else {
                FolderPerm *new_perm = folder_perm_new (perm->path, perm->permission);
                folder_perms = g_list_insert_sorted (folder_perms, new_perm,
                                                     comp_folder_perms);
            }
        } else {
                FolderPerm *new_perm = folder_perm_new (perm->path, perm->permission);
                folder_perms = g_list_insert_sorted (folder_perms, new_perm,
                                                     comp_folder_perms);
        }
        g_hash_table_insert (mgr->priv->group_perms, g_strdup(repo_id), folder_perms);
    }
    pthread_mutex_unlock (&mgr->priv->perm_lock);

    return 0;
}

int
seaf_repo_manager_update_folder_perms (SeafRepoManager *mgr,
                                       const char *repo_id,
                                       FolderPermType type,
                                       GList *folder_perms)
{
    char *sql;
    sqlite3_stmt *stmt;
    GList *ptr;
    FolderPerm *perm;
    GList *new, *old;

    g_return_val_if_fail ((type == FOLDER_PERM_TYPE_USER ||
                           type == FOLDER_PERM_TYPE_GROUP),
                          -1);

    /* Update db. */

    pthread_mutex_lock (&mgr->priv->db_lock);

    if (type == FOLDER_PERM_TYPE_USER)
        sql = "DELETE FROM FolderUserPerms WHERE repo_id = ?";
    else
        sql = "DELETE FROM FolderGroupPerms WHERE repo_id = ?";
    stmt = sqlite_query_prepare (mgr->priv->db, sql);
    sqlite3_bind_text (stmt, 1, repo_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step (stmt) != SQLITE_DONE) {
        seaf_warning ("Failed to remove folder perms for %.8s: %s.\n",
                      repo_id, sqlite3_errmsg (mgr->priv->db));
        sqlite3_finalize (stmt);
        pthread_mutex_unlock (&mgr->priv->db_lock);
        return -1;
    }
    sqlite3_finalize (stmt);

    if (!folder_perms) {
        pthread_mutex_unlock (&mgr->priv->db_lock);

        pthread_mutex_lock (&mgr->priv->perm_lock);
        if (type == FOLDER_PERM_TYPE_USER) {
            old = g_hash_table_lookup (mgr->priv->user_perms, repo_id);
             if (old) {
                g_list_free_full (old, (GDestroyNotify)folder_perm_free);
            }
            g_hash_table_insert (mgr->priv->user_perms, g_strdup(repo_id), NULL);
        } else if (type == FOLDER_PERM_TYPE_GROUP) {
            old = g_hash_table_lookup (mgr->priv->group_perms, repo_id);
            if (old) {
                g_list_free_full (old, (GDestroyNotify)folder_perm_free);
            }
            g_hash_table_insert (mgr->priv->group_perms, g_strdup(repo_id), NULL);
        }
        pthread_mutex_unlock (&mgr->priv->perm_lock);
        return 0;
    }

    if (type == FOLDER_PERM_TYPE_USER)
        sql = "INSERT INTO FolderUserPerms VALUES (?, ?, ?)";
    else
        sql = "INSERT INTO FolderGroupPerms VALUES (?, ?, ?)";
    stmt = sqlite_query_prepare (mgr->priv->db, sql);

    for (ptr = folder_perms; ptr; ptr = ptr->next) {
        perm = ptr->data;

        sqlite3_bind_text (stmt, 1, repo_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text (stmt, 2, perm->path, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text (stmt, 3, perm->permission, -1, SQLITE_TRANSIENT);

        if (sqlite3_step (stmt) != SQLITE_DONE) {
            seaf_warning ("Failed to insert folder perms for %.8s: %s.\n",
                          repo_id, sqlite3_errmsg (mgr->priv->db));
            sqlite3_finalize (stmt);
            pthread_mutex_unlock (&mgr->priv->db_lock);
            return -1;
        }

        sqlite3_reset (stmt);
        sqlite3_clear_bindings (stmt);
    }

    sqlite3_finalize (stmt);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    /* Update in memory */
    new = folder_perm_list_copy (folder_perms);
    new = g_list_sort (new, comp_folder_perms);

    pthread_mutex_lock (&mgr->priv->perm_lock);
    if (type == FOLDER_PERM_TYPE_USER) {
        old = g_hash_table_lookup (mgr->priv->user_perms, repo_id);
        if (old)
            g_list_free_full (old, (GDestroyNotify)folder_perm_free);
        g_hash_table_insert (mgr->priv->user_perms, g_strdup(repo_id), new);
    } else if (type == FOLDER_PERM_TYPE_GROUP) {
        old = g_hash_table_lookup (mgr->priv->group_perms, repo_id);
        if (old)
            g_list_free_full (old, (GDestroyNotify)folder_perm_free);
        g_hash_table_insert (mgr->priv->group_perms, g_strdup(repo_id), new);
    }
    pthread_mutex_unlock (&mgr->priv->perm_lock);

    return 0;
}

static gboolean
load_folder_perm (sqlite3_stmt *stmt, void *data)
{
    GList **p_perms = data;
    const char *path, *permission;

    path = (const char *)sqlite3_column_text (stmt, 0);
    permission = (const char *)sqlite3_column_text (stmt, 1);

    FolderPerm *perm = folder_perm_new (path, permission);
    *p_perms = g_list_prepend (*p_perms, perm);

    return TRUE;
}

static GList *
load_folder_perms_for_repo (SeafRepoManager *mgr,
                            const char *repo_id,
                            FolderPermType type)
{
    GList *perms = NULL;
    char sql[256];

    g_return_val_if_fail ((type == FOLDER_PERM_TYPE_USER ||
                           type == FOLDER_PERM_TYPE_GROUP),
                          NULL);

    if (type == FOLDER_PERM_TYPE_USER)
        sqlite3_snprintf (sizeof(sql), sql,
                          "SELECT path, permission FROM FolderUserPerms "
                          "WHERE repo_id = '%q'",
                          repo_id);
    else
        sqlite3_snprintf (sizeof(sql), sql,
                          "SELECT path, permission FROM FolderGroupPerms "
                          "WHERE repo_id = '%q'",
                          repo_id);

    pthread_mutex_lock (&mgr->priv->db_lock);

    if (sqlite_foreach_selected_row (mgr->priv->db, sql,
                                     load_folder_perm, &perms) < 0) {
        pthread_mutex_unlock (&mgr->priv->db_lock);
        GList *ptr;
        for (ptr = perms; ptr; ptr = ptr->next)
            folder_perm_free ((FolderPerm *)ptr->data);
        g_list_free (perms);
        return NULL;
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    /* Sort list in descending order by perm->path (longer path first). */
    perms = g_list_sort (perms, comp_folder_perms);

    return perms;
}

static void
init_folder_perms (SeafRepoManager *mgr)
{
    SeafRepoManagerPriv *priv = mgr->priv;
    GList *repo_ids = g_hash_table_get_keys (priv->repo_hash);
    GList *ptr;
    GList *perms;
    char *repo_id;

    for (ptr = repo_ids; ptr; ptr = ptr->next) {
        repo_id = ptr->data;
        perms = load_folder_perms_for_repo (mgr, repo_id, FOLDER_PERM_TYPE_USER);
        if (perms) {
            pthread_mutex_lock (&priv->perm_lock);
            g_hash_table_insert (priv->user_perms, g_strdup(repo_id), perms);
            pthread_mutex_unlock (&priv->perm_lock);
        }
        perms = load_folder_perms_for_repo (mgr, repo_id, FOLDER_PERM_TYPE_GROUP);
        if (perms) {
            pthread_mutex_lock (&priv->perm_lock);
            g_hash_table_insert (priv->group_perms, g_strdup(repo_id), perms);
            pthread_mutex_unlock (&priv->perm_lock);
        }
    }

    g_list_free (repo_ids);
}

static void
remove_folder_perms (SeafRepoManager *mgr, const char *repo_id)
{
    GList *perms = NULL;

    pthread_mutex_lock (&mgr->priv->perm_lock);

    perms = g_hash_table_lookup (mgr->priv->user_perms, repo_id);
    if (perms) {
        g_list_free_full (perms, (GDestroyNotify)folder_perm_free);
        g_hash_table_remove (mgr->priv->user_perms, repo_id);
    }

    perms = g_hash_table_lookup (mgr->priv->group_perms, repo_id);
    if (perms) {
        g_list_free_full (perms, (GDestroyNotify)folder_perm_free);
        g_hash_table_remove (mgr->priv->group_perms, repo_id);
    }

    pthread_mutex_unlock (&mgr->priv->perm_lock);
}

int
seaf_repo_manager_update_folder_perm_timestamp (SeafRepoManager *mgr,
                                                const char *repo_id,
                                                gint64 timestamp)
{
    char sql[256];
    int ret;

    snprintf (sql, sizeof(sql),
              "REPLACE INTO FolderPermTimestamp VALUES ('%s', %"G_GINT64_FORMAT")",
              repo_id, timestamp);

    pthread_mutex_lock (&mgr->priv->db_lock);

    ret = sqlite_query_exec (mgr->priv->db, sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    return ret;
}

gint64
seaf_repo_manager_get_folder_perm_timestamp (SeafRepoManager *mgr,
                                             const char *repo_id)
{
    char sql[256];
    gint64 ret;

    sqlite3_snprintf (sizeof(sql), sql,
                      "SELECT timestamp FROM FolderPermTimestamp WHERE repo_id = '%q'",
                      repo_id);

    pthread_mutex_lock (&mgr->priv->db_lock);

    ret = sqlite_get_int64 (mgr->priv->db, sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    return ret;
}

static char *
lookup_folder_perm (GList *perms, const char *path)
{
    GList *ptr;
    FolderPerm *perm;
    char *folder;
    int len;
    char *permission = NULL;

    for (ptr = perms; ptr; ptr = ptr->next) {
        perm = ptr->data;

        if (strcmp (perm->path, "/") != 0)
            folder = g_strconcat (perm->path, "/", NULL);
        else
            folder = g_strdup(perm->path);

        len = strlen(folder);
        if (strcmp (perm->path, path) == 0 || strncmp(folder, path, len) == 0) {
            permission = perm->permission;
            g_free (folder);
            break;
        }
        g_free (folder);
    }

    return permission;
}

char *
get_folder_perm_by_path (const char *repo_id, const char *path)
{
    SeafRepoManager *mgr = seaf->repo_mgr;
    GList *user_perms = NULL, *group_perms = NULL;
    char *permission = NULL;
    char *abs_path = NULL;

    pthread_mutex_lock (&mgr->priv->perm_lock);

    user_perms = g_hash_table_lookup (mgr->priv->user_perms, repo_id);
    group_perms = g_hash_table_lookup (mgr->priv->group_perms, repo_id);

    if (user_perms || group_perms) {
        if (path[0] != '/') {
            abs_path = g_strconcat ("/", path, NULL);
        } else {
            abs_path = g_strdup (path);
        }
    }

    if (user_perms)
        permission = lookup_folder_perm (user_perms, abs_path);
    if (!permission && group_perms)
        permission = lookup_folder_perm (group_perms, abs_path);

    pthread_mutex_unlock (&mgr->priv->perm_lock);

    g_free (abs_path);

    return permission;
}

static gboolean
is_path_writable (const char *repo_id,
                  gboolean is_repo_readonly,
                  const char *path)
{
    char *permission = NULL;

    permission = get_folder_perm_by_path (repo_id, path);

    if (!permission)
        return !is_repo_readonly;

    if (strcmp (permission, "rw") == 0)
        return TRUE;
    else
        return FALSE;
}

gboolean
seaf_repo_manager_is_path_writable (SeafRepoManager *mgr,
                                    const char *repo_id,
                                    const char *path)
{
    SeafRepo *repo;
    gboolean ret;

    repo = seaf_repo_manager_get_repo (mgr, repo_id);
    if (!repo) {
        return FALSE;
    }

    ret = is_path_writable (repo_id, repo->is_readonly, path);

    seaf_repo_unref (repo);
    return ret;
}

// When checking whether a path is visible, we need to consider whether the parent folder is visible or not.
// If the parent folder is not visible, then even if the subfolder has visible permissions, the subfolder is not visible.
gboolean
is_path_invisible_recursive (const char *repo_id, const char *path)
{
    char *permission = NULL;

    if (!path || g_strcmp0 (path, ".") == 0) {
        return FALSE;
    }

    permission = get_folder_perm_by_path (repo_id, path);

    // Check if path is / after get folder perm.
    if (g_strcmp0 (path, "/") == 0) {
        if (!permission) {
            return FALSE;
        }

        if (strcmp (permission, "rw") == 0 ||
            strcmp (permission, "r") == 0) {
            return FALSE;    
        }
        return TRUE;
    }

    if (!permission) {
        char *folder = g_path_get_dirname (path);
        gboolean is_invisible = is_path_invisible_recursive(repo_id, folder);
        g_free (folder);
        return is_invisible;

    }
    if (strcmp (permission, "rw") == 0 ||
        strcmp (permission, "r") == 0) {
        char *folder = g_path_get_dirname (path);
        gboolean is_invisible = is_path_invisible_recursive(repo_id, folder);
        g_free (folder);
        return is_invisible;
    } else {
        return TRUE;
    }
}

gboolean
seaf_repo_manager_is_path_invisible (SeafRepoManager *mgr,
                                     const char *repo_id,
                                     const char *path)
{
    SeafRepo *repo;
    gboolean ret;

    repo = seaf_repo_manager_get_repo (mgr, repo_id);
    if (!repo) {
        return TRUE;
    }

    ret = is_path_invisible_recursive (repo_id, path);

    seaf_repo_unref (repo);
    return ret;
}

static gboolean
include_invisible_perm (GList *perms)
{
    GList *ptr;
    FolderPerm *perm;

    for (ptr = perms; ptr; ptr = ptr->next) {
        perm = ptr->data;
        if (strcmp (perm->permission, "rw") != 0 && 
            strcmp (perm->permission, "r") != 0)
            return TRUE;
    }

    return FALSE;
}

gboolean
seaf_repo_manager_include_invisible_perm (SeafRepoManager *mgr, const char *repo_id)
{
    GList *user_perms = NULL, *group_perms = NULL;

    pthread_mutex_lock (&mgr->priv->perm_lock);
    user_perms = g_hash_table_lookup (mgr->priv->user_perms, repo_id);
    if (user_perms && include_invisible_perm (user_perms)) {
        pthread_mutex_unlock (&mgr->priv->perm_lock);
        return TRUE;
    }

    group_perms = g_hash_table_lookup (mgr->priv->group_perms, repo_id);
    if (group_perms && include_invisible_perm (group_perms)) {
        pthread_mutex_unlock (&mgr->priv->perm_lock);
        return TRUE;
    }
    pthread_mutex_unlock (&mgr->priv->perm_lock);

    return FALSE;
}

gboolean
is_repo_id_valid (const char *id)
{
    if (!id)
        return FALSE;

    return is_uuid_valid (id);
}

/*
 * Repo object life cycle management:
 *
 * Each repo object has a refcnt. A repo object is inserted into an internal
 * hash table in repo manager when it's created or loaded from db on startup.
 * The initial refcnt is 1, for the reference by the internal hash table.
 *
 * A repo object can only be obtained from outside by calling
 * seaf_repo_manager_get_repo(), which increase refcnt by 1.
 * Once the caller is done with the object, it must call seaf_repo_unref() to
 * decrease refcnt.
 *
 * When a repo needs to be deleted, call seaf_repo_manager_mark_repo_deleted().
 * The function sets the "delete_pending" flag of the repo object.
 * Repo manager won't return the repo to callers once it's marked as deleted.
 * Repo manager regularly checks every repo in the hash table.
 * If a repo is marked as delete pending and refcnt is 1, it removes the repo
 * on disk. After the repo is removed on disk, repo manager removes the repo
 * object from internal hash table.
 *
 * Sync manager must make sure a repo won't be downloaded when it's marked as
 * delete pending. Otherwise repo manager may remove the downloaded metadata
 * as it cleans up the old repo on disk.
 */

SeafRepo*
seaf_repo_new (const char *id, const char *name, const char *desc)
{
    SeafRepo* repo;

    repo = g_new0 (SeafRepo, 1);
    memcpy (repo->id, id, 36);
    repo->id[36] = '\0';

    repo->name = g_strdup(name);
    repo->desc = g_strdup(desc);
    repo->tree = repo_tree_new (id);

    return repo;
}

void
seaf_repo_free (SeafRepo *repo)
{
    if (repo->head) seaf_branch_unref (repo->head);

    repo_tree_free (repo->tree);

    if (repo->journal)
        journal_close (repo->journal, TRUE);

    g_free (repo->name);
    g_free (repo->desc);
    g_free (repo->category);
    g_free (repo->token);
    g_free (repo->jwt_token);
    if (repo->repo_uname)
        g_free (repo->repo_uname);
    if (repo->worktree)
        g_free (repo->worktree);
    g_free (repo->server);
    g_free (repo->user);
    g_free (repo->fileserver_addr);
    g_free (repo->pwd_hash_algo);
    g_free (repo->pwd_hash_params);
    g_free (repo);
}

void
seaf_repo_ref (SeafRepo *repo)
{
    g_atomic_int_inc (&repo->refcnt);
}

void
seaf_repo_unref (SeafRepo *repo)
{
    if (!repo)
        return;

    pthread_rwlock_wrlock (&seaf->repo_mgr->priv->lock);

    if (g_atomic_int_dec_and_test (&repo->refcnt))
        seaf_repo_free (repo);

    pthread_rwlock_unlock (&seaf->repo_mgr->priv->lock);
}

static void
set_head_common (SeafRepo *repo, SeafBranch *branch)
{
    if (repo->head)
        seaf_branch_unref (repo->head);
    repo->head = branch;
    seaf_branch_ref(branch);
}

int
seaf_repo_set_head (SeafRepo *repo, SeafBranch *branch)
{
    if (save_branch_repo_map (seaf->repo_mgr, branch) < 0)
        return -1;
    set_head_common (repo, branch);
    return 0;
}

SeafCommit *
seaf_repo_get_head_commit (const char *repo_id)
{
    SeafRepo *repo = NULL;
    SeafCommit *head = NULL;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to get repo %s.\n", repo_id);
        return NULL;
    }

    head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                           repo_id, repo->version,
                                           repo->head->commit_id);
    if (!head) {
        seaf_warning ("Failed to get head for repo %s.\n", repo_id);
    }

    seaf_repo_unref (repo);
    return head;
}

void
seaf_repo_from_commit (SeafRepo *repo, SeafCommit *commit)
{
    repo->name = g_strdup (commit->repo_name);
    repo->desc = g_strdup (commit->repo_desc);
    repo->encrypted = commit->encrypted;
    repo->last_modify = commit->ctime;
    memcpy (repo->root_id, commit->root_id, 40);
    if (repo->encrypted) {
        repo->enc_version = commit->enc_version;
        if (repo->enc_version == 1 && !commit->pwd_hash_algo)
            memcpy (repo->magic, commit->magic, 32);
        else if (repo->enc_version == 2) {
            memcpy (repo->random_key, commit->random_key, 96);
        }
        else if (repo->enc_version == 3) {
            memcpy (repo->salt, commit->salt, 64);
            memcpy (repo->random_key, commit->random_key, 96);
        }
        else if (repo->enc_version == 4) {
            memcpy (repo->salt, commit->salt, 64);
            memcpy (repo->random_key, commit->random_key, 96);
        }
        if (repo->enc_version >= 2 && !commit->pwd_hash_algo) {
            memcpy (repo->magic, commit->magic, 64);
        }
        if (commit->pwd_hash_algo) {
            memcpy (repo->pwd_hash, commit->pwd_hash, 64);
            repo->pwd_hash_algo = g_strdup (commit->pwd_hash_algo);
            repo->pwd_hash_params = g_strdup (commit->pwd_hash_params);
        }
    }
    repo->version = commit->version;
}

void
seaf_repo_to_commit (SeafRepo *repo, SeafCommit *commit)
{
    commit->repo_name = g_strdup (repo->name);
    commit->repo_desc = g_strdup (repo->desc);
    commit->encrypted = repo->encrypted;
    if (commit->encrypted) {
        commit->enc_version = repo->enc_version;
        if (commit->enc_version == 1 && !repo->pwd_hash_algo)
            commit->magic = g_strdup (repo->magic);
        else if (commit->enc_version == 2) {
            commit->random_key = g_strdup (repo->random_key);
        }
        else if (commit->enc_version == 3) {
            commit->salt = g_strdup (repo->salt);
            commit->random_key = g_strdup (repo->random_key);
        }
        else if (commit->enc_version == 4) {
            commit->salt = g_strdup (repo->salt);
            commit->random_key = g_strdup (repo->random_key);
        }
        if (commit->enc_version >= 2 && !repo->pwd_hash_algo) {
            commit->magic = g_strdup (repo->magic);
        }
        if (repo->pwd_hash_algo) {
            commit->pwd_hash = g_strdup (repo->pwd_hash);
            commit->pwd_hash_algo = g_strdup (repo->pwd_hash_algo);
            commit->pwd_hash_params = g_strdup (repo->pwd_hash_params);
        }
    }
    commit->version = repo->version;
}

void
seaf_repo_set_readonly (SeafRepo *repo)
{
    repo->is_readonly = TRUE;
    save_repo_property (repo->manager, repo->id, REPO_PROP_IS_READONLY, "true");
}

void
seaf_repo_unset_readonly (SeafRepo *repo)
{
    repo->is_readonly = FALSE;
    save_repo_property (repo->manager, repo->id, REPO_PROP_IS_READONLY, "false");
}

void
seaf_repo_set_worktree (SeafRepo *repo, const char *repo_uname)
{
    if (repo->repo_uname)
        g_free (repo->repo_uname);
    repo->repo_uname = g_strdup (repo_uname);

    if (repo->worktree)
        g_free (repo->worktree);
    repo->worktree = g_build_path ("/", seaf->mount_point, repo_uname, NULL);
}

static void
print_apply_op_error (const char *repo_id, JournalOp *op, int err)
{
    seaf_warning ("Failed to apply op to repo tree of %s: %s.\n"
                  "Op details: %d %s %s %"G_GINT64_FORMAT" %"G_GINT64_FORMAT" %u\n",
                  repo_id, strerror(err),
                  op->type, op->path, op->new_path, op->size, op->mtime, op->mode);
}

static int
apply_journal_ops_to_repo_tree (SeafRepo *repo)
{
    GList *ops, *ptr;
    gboolean error;
    JournalOp *op;
    RepoTree *tree = repo->tree;
    int rc;
    int ret = 0;

    ops = journal_read_ops (repo->journal, repo->head->opid + 1, G_MAXINT64, &error);
    if (error) {
        seaf_warning ("Failed to read operations from journal for repo %s.\n",
                      repo->id);
        return -1;
    }

    for (ptr = ops; ptr; ptr = ptr->next) {
        op = (JournalOp *)ptr->data;
        switch (op->type) {
        case OP_TYPE_CREATE_FILE:
            rc = repo_tree_create_file (tree, op->path, EMPTY_SHA1, op->mode, op->mtime, op->size);
            if (rc < 0) {
                print_apply_op_error (repo->id, op, -rc);
                ret = -1;
                goto out;
            }
            break;
        case OP_TYPE_DELETE_FILE:
            rc = repo_tree_unlink (tree, op->path);
            if (rc < 0) {
                print_apply_op_error (repo->id, op, -rc);
                ret = -1;
                goto out;
            }
            break;
        case OP_TYPE_UPDATE_FILE:
            rc = repo_tree_set_file_mtime (tree, op->path, op->mtime);
            if (rc < 0) {
                print_apply_op_error (repo->id, op, -rc);
                ret = -1;
                goto out;
            }
            rc = repo_tree_set_file_size (tree, op->path, op->size);
            if (rc < 0) {
                print_apply_op_error (repo->id, op, -rc);
                ret = -1;
                goto out;
            }
            break;
        case OP_TYPE_RENAME:
            rc = repo_tree_rename (tree, op->path, op->new_path, TRUE);
            if (rc < 0) {
                print_apply_op_error (repo->id, op, -rc);
                ret = -1;
                goto out;
            }
            break;
        case OP_TYPE_MKDIR:
            rc = repo_tree_mkdir (tree, op->path, op->mtime);
            if (rc < 0) {
                print_apply_op_error (repo->id, op, -rc);
                ret = -1;
                goto out;
            }
            break;
        case OP_TYPE_RMDIR:
            rc = repo_tree_rmdir (tree, op->path);
            if (rc < 0) {
                print_apply_op_error (repo->id, op, -rc);
                ret = -1;
                goto out;
            }
            break;
        case OP_TYPE_UPDATE_ATTR:
            rc = repo_tree_set_file_mtime (tree, op->path, op->mtime);
            if (rc < 0) {
                print_apply_op_error (repo->id, op, -rc);
                ret = -1;
                goto out;
            }
            break;
        default:
            seaf_warning ("Unknown op type %d, skipped.\n", op->type);
        }
    }

out:
    g_list_free_full (ops, (GDestroyNotify)journal_op_free);
    return ret;
}

static char *
build_conflict_path (const char *user, const char *path, time_t t)
{
    char time_buf[64];
    char *copy = g_strdup (path);
    GString *conflict_path = g_string_new (NULL);
    char *dot, *ext;

    strftime(time_buf, 64, "%Y-%m-%d-%H-%M-%S", localtime(&t));

    dot = strrchr (copy, '.');

    if (dot != NULL) {
        *dot = '\0';
        ext = dot + 1;
        g_string_printf (conflict_path, "%s (SFConflict %s %s).%s",
                         copy, user, time_buf, ext);
    } else {
        g_string_printf (conflict_path, "%s (SFConflict %s %s)",
                         copy, user, time_buf);
    }

    g_free (copy);
    return g_string_free (conflict_path, FALSE);
}

static int
copy_file_to_conflict_file (SeafRepo *repo,
                            SeafStat *st,
                            const char *path,
                            const char *conflict_path)
{
    JournalOp *op;
    int ret = 0;

    repo_tree_create_file (repo->tree, conflict_path,
                           EMPTY_SHA1, st->st_mode, st->st_mtime, st->st_size);

    op = journal_op_new (OP_TYPE_CREATE_FILE, conflict_path, NULL, 0, 0, st->st_mode);
    if (journal_append_op (repo->journal, op) < 0) {
        journal_op_free (op);
        ret = -1;
        goto out;
    }

    op = journal_op_new (OP_TYPE_UPDATE_FILE, conflict_path, NULL,
                         st->st_size, st->st_mtime, st->st_mode);
    if (journal_append_op (repo->journal, op) < 0) {
        journal_op_free (op);
        ret = -1;
        goto out;
    }

    file_cache_mgr_rename (seaf->file_cache_mgr, repo->id, path, repo->id, conflict_path);

    /* Reset extended attrs so that the file will be indexed on commit. */
    file_cache_mgr_set_attrs (seaf->file_cache_mgr, repo->id, conflict_path,
                              0, 0, EMPTY_SHA1);

out:
    return ret;
}

static int
create_parent_dirs (SeafRepo *repo, const char *parent_path)
{
    char **parts = g_strsplit (parent_path, "/", 0);
    guint len = g_strv_length (parts);
    guint i;
    GString *dir_path = NULL;
    int rc;
    RepoTreeStat tree_st;
    JournalOp *op;
    int ret = 0;

    if (len == 0)
        return 0;

    dir_path = g_string_new ("");

    for (i = 0; i < len; ++i) {
        if (i == 0)
            g_string_append (dir_path, parts[0]);
        else
            g_string_append_printf (dir_path, "/%s", parts[i]);

        if (repo_tree_stat_path (repo->tree, dir_path->str, &tree_st) == 0) {
            continue;
        }

        rc = repo_tree_mkdir (repo->tree, dir_path->str, (gint64)time(NULL));
        if (rc < 0) {
            seaf_warning ("Failed to create dir %s/%s in repo tree: %s.\n",
                          repo->id, dir_path->str, strerror(rc));
            ret = -1;
            break;
        }

        op = journal_op_new (OP_TYPE_MKDIR, dir_path->str, NULL, 0, (gint64)time(NULL), 0);
        if (journal_append_op (repo->journal, op) < 0) {
            seaf_warning ("Failed to append operation to journal of repo %s.\n",
                          repo->id);
            journal_op_free (op);
            ret = -1;
            break;
        }
    }

    g_string_free (dir_path, TRUE);
    g_strfreev (parts);
    return ret;
}

static int
add_cached_file_to_tree (SeafRepo *repo, SeafStat *st, const char *path)
{
    JournalOp *op;
    char *parent_path;

    parent_path = g_path_get_dirname (path);
    if (g_strcmp0 (parent_path, ".") != 0) {
        if (create_parent_dirs (repo, parent_path) < 0) {
            g_free (parent_path);
            return -1;
        }
    }
    g_free (parent_path);

    repo_tree_create_file (repo->tree, path,
                           EMPTY_SHA1, st->st_mode, st->st_mtime, st->st_size);

    op = journal_op_new (OP_TYPE_CREATE_FILE, path, NULL, 0, 0, st->st_mode);
    if (journal_append_op (repo->journal, op) < 0) {
        journal_op_free (op);
        return -1;
    }

    op = journal_op_new (OP_TYPE_UPDATE_FILE, path, NULL,
                         st->st_size, st->st_mtime, st->st_mode);
    if (journal_append_op (repo->journal, op) < 0) {
        journal_op_free (op);
        return -1;
    }

    /* Reset extended attrs so that the file will be indexed on commit. */
    file_cache_mgr_set_attrs (seaf->file_cache_mgr, repo->id, path,
                              0, 0, EMPTY_SHA1);

    return 0;
}

static int
update_cached_file_in_tree (SeafRepo *repo, SeafStat *st, const char *path)
{
    JournalOp *op;

    repo_tree_set_file_mtime (repo->tree, path, (gint64)st->st_mtime);
    repo_tree_set_file_size (repo->tree, path, (gint64)st->st_size);

    op = journal_op_new (OP_TYPE_UPDATE_FILE, path, NULL,
                         st->st_size, st->st_mtime, st->st_mode);
    if (journal_append_op (repo->journal, op) < 0) {
        journal_op_free (op);
        return -1;
    }

    return 0;
}

typedef struct TraverseCachedFileAux {
    SeafRepo *repo;
    gboolean after_clone;
    char *nickname;
} TraverseCachedFileAux;

static void
check_cached_file_status_cb (const char *repo_id,
                             const char *file_path,
                             SeafStat *st,
                             void *user_data)
{
    TraverseCachedFileAux *aux = (TraverseCachedFileAux *)user_data;
    SeafRepo *repo = aux->repo;
    RepoTreeStat tree_st;
    char *filename;
    FileCacheStat attrs;

    /* If file is not completely cached yet, don't rely on its status. */
    if (!file_cache_mgr_is_file_cached (seaf->file_cache_mgr,
                                        repo_id, file_path))
        return;

    if (file_cache_mgr_get_attrs (seaf->file_cache_mgr, repo_id, file_path, &attrs) < 0) {
        seaf_warning ("Failed to get cached attrs for file %s/%s\n",
                      repo_id, file_path);
        return;
    }

    filename = g_path_get_basename (file_path);
    if (repo_tree_stat_path (repo->tree, file_path, &tree_st) < 0) {
        if (seaf_repo_manager_ignored_on_commit(filename)) {
            /* Internally ignored files are not committed to the tree.
             * So after restart, these files are not loaded.
             * We can pick them up in the cache files folder.
             */
            repo_tree_create_file (repo->tree, file_path,
                                   attrs.file_id,
                                   (guint32)st->st_mode,
                                   (gint64)st->st_mtime,
                                   (gint64)st->st_size);
        } else if (aux->after_clone ||
                   attrs.mtime != (gint64)st->st_mtime ||
                   attrs.size != (gint64)st->st_size) {
            /* If the program was hard shutdown, some operations were not flushed
             * into journal db. In this case, there could be some files missing from
             * journal and repo tree while present in the file cache.
             */
            seaf_message ("Adding cached file %s/%s to tree.\n", repo->id, file_path);
            add_cached_file_to_tree (repo, st, file_path);
        }
        g_free (filename);
        return;
    }

    if (aux->after_clone) {
        /* Merge existing but not-yet committed cached files into repo after clone. */
        if (((gint64)st->st_mtime != tree_st.mtime || (gint64)st->st_size != tree_st.size) &&
            ((gint64)st->st_mtime != attrs.mtime || (gint64)st->st_size != attrs.size || !attrs.is_uploaded) &&
            !seaf_repo_manager_ignored_on_commit(filename)) {
            seaf_message ("Cached file %s of repo %s(%.8s) is changed and different from repo tree status.\n"
                          "Cached file: mtime %"G_GINT64_FORMAT", size %"G_GINT64_FORMAT"\n"
                          "Extended attrs: mtime: %"G_GINT64_FORMAT", size %"G_GINT64_FORMAT"\n"
                          "Repo tree: mtime %"G_GINT64_FORMAT", size %"G_GINT64_FORMAT"\n",
                          file_path, repo->name, repo->id,
                          (gint64)st->st_mtime, (gint64)st->st_size,
                          attrs.mtime, attrs.size,
                          tree_st.mtime, tree_st.size);
            char *conflict_path = build_conflict_path (aux->nickname, file_path, st->st_mtime);
            if (conflict_path) {
                seaf_message ("Generating conflict file %s in repo %s.\n",
                              conflict_path, repo_id);
                copy_file_to_conflict_file (repo, st, file_path, conflict_path);
                g_free (conflict_path);
            }
        }
    } else {
        /* On start-up, if a cached file is not committed and its timestamp or size is
         * different from the repo tree, some updates to it was not flushed into journal
         * on the last shutdown.
         */
        if (((gint64)st->st_mtime != tree_st.mtime || (gint64)st->st_size != tree_st.size) &&
            ((gint64)st->st_mtime != attrs.mtime || (gint64)st->st_size != attrs.size) &&
            !seaf_repo_manager_ignored_on_commit(filename)) {
            seaf_message ("Updating cached file %s/%s in tree.\n", repo->id, file_path);
            seaf_message ("Cached file: mtime %"G_GINT64_FORMAT", size %"G_GINT64_FORMAT"\n"
                          "Extended attrs: mtime: %"G_GINT64_FORMAT", size %"G_GINT64_FORMAT"\n"
                          "Repo tree: mtime: %"G_GINT64_FORMAT", size %"G_GINT64_FORMAT"\n",
                          (gint64)st->st_mtime, (gint64)st->st_size,
                          attrs.mtime, attrs.size,
                          tree_st.mtime, tree_st.size);
            update_cached_file_in_tree (repo, st, file_path);
        }
    }

    g_free (filename);

    if ((gint64)st->st_mtime == attrs.mtime && (gint64)st->st_size == attrs.size) {
        seaf_sync_manager_update_active_path (seaf->sync_mgr, repo_id, file_path,
                                              st->st_mode, SYNC_STATUS_SYNCED);
    }
}

/* static void */
/* check_cached_dir_cb (const char *repo_id, */
/*                      const char *dir_path, */
/*                      SeafStat *st, */
/*                      void *user_data) */
/* { */
/*     TraverseCachedFileAux *aux = (TraverseCachedFileAux *)user_data; */
/*     SeafRepo *repo = aux->repo; */
/*     RepoTreeStat tree_st; */
/*     JournalOp *op; */

/*     if (repo_tree_stat_path (repo->tree, dir_path, &tree_st) == 0) { */
/*         return; */
/*     } */

/*     int rc = repo_tree_mkdir (repo->tree, dir_path, (gint64)st->st_mtime); */
/*     if (rc < 0) { */
/*         return; */
/*     } */

/*     op = journal_op_new (OP_TYPE_MKDIR, dir_path, NULL, 0, (gint64)st->st_mtime, 0); */
/*     if (journal_append_op (repo->journal, op) < 0) { */
/*         seaf_warning ("Failed to append operation to journal of repo %s.\n", */
/*                       repo_id); */
/*         journal_op_free (op); */
/*     } */
/* } */

int
seaf_repo_load_fs (SeafRepo *repo, gboolean after_clone)
{
    if (repo->fs_ready)
        return 0;

    if (!repo->journal) {
        repo->journal = journal_manager_open_journal (seaf->journal_mgr,
                                                      repo->id);
        if (!repo->journal) {
            seaf_warning ("Failed to open journal for repo %s(%s).\n",
                          repo->name, repo->id);
            return -1;
        }
    }

    if (repo_tree_load_commit (repo->tree, repo->head->commit_id) < 0) {
        seaf_warning ("Failed to load tree for repo %s.\n", repo->id);
        journal_close (repo->journal, TRUE);
        repo->journal = NULL;
        return -1;
    }

    /* Ignore errors when replaying the journal.
     * There can sometimes be edge case that fails an operation replay, like deleting
     * a non-existing file. These cases shouldn't harm.
     */
    apply_journal_ops_to_repo_tree (repo);

    TraverseCachedFileAux aux;
    aux.repo = repo;
    aux.after_clone = after_clone;

    SeafAccount *account = seaf_repo_manager_get_account (seaf->repo_mgr, repo->server, repo->user);
    if (account && account->nickname) {
        aux.nickname = g_strdup (account->nickname);
    } else {
        aux.nickname = g_strdup (repo->user);
    }
    seaf_account_free (account);


    file_cache_mgr_traverse_path (seaf->file_cache_mgr,
                                  repo->id,
                                  "",
                                  check_cached_file_status_cb,
                                  NULL,
                                  &aux);

    g_free (aux.nickname);
    repo->fs_ready = TRUE;

    return 0;
}

SeafRepoManager*
seaf_repo_manager_new (SeafileSession *seaf)
{
    SeafRepoManager *mgr = g_new0 (SeafRepoManager, 1);

    mgr->priv = g_new0 (SeafRepoManagerPriv, 1);
    mgr->seaf = seaf;

    pthread_mutex_init (&mgr->priv->db_lock, NULL);

    mgr->priv->repo_hash = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);
    pthread_rwlock_init (&mgr->priv->lock, NULL);

    mgr->priv->user_perms = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);
    mgr->priv->group_perms = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);
    pthread_mutex_init (&mgr->priv->perm_lock, NULL);

    mgr->priv->accounts = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                 g_free,
                                                 (GDestroyNotify)seaf_account_free);
    pthread_rwlock_init (&mgr->priv->account_lock, NULL);

    mgr->priv->repo_infos = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                    g_free,
                                                    (GDestroyNotify)repo_info_free);

    mgr->priv->repo_names = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                   g_free, (GDestroyNotify)g_hash_table_destroy);

    return mgr;
}

int
seaf_repo_manager_init (SeafRepoManager *mgr)
{
    /* Load all the repos into memory on the client side. */
    load_repos (mgr, mgr->seaf->seaf_dir);

    /* Load folder permissions from db. */
    init_folder_perms (mgr);

    return 0;
}

#define REMOVE_OBJECTS_BATCH 1000

static int
remove_store (const char *top_store_dir, const char *store_id, int *count)
{
    char *obj_dir = NULL;
    GDir *dir1, *dir2;
    const char *dname1, *dname2;
    char *path1, *path2;

    obj_dir = g_build_filename (top_store_dir, store_id, NULL);

    dir1 = g_dir_open (obj_dir, 0, NULL);
    if (!dir1) {
        g_free (obj_dir);
        return 0;
    }

    seaf_message ("Removing store %s\n", obj_dir);

    while ((dname1 = g_dir_read_name(dir1)) != NULL) {
        path1 = g_build_filename (obj_dir, dname1, NULL);

        dir2 = g_dir_open (path1, 0, NULL);
        if (!dir2) {
            seaf_warning ("Failed to open obj dir %s.\n", path1);
            g_dir_close (dir1);
            g_free (path1);
            g_free (obj_dir);
            return -1;
        }

        while ((dname2 = g_dir_read_name(dir2)) != NULL) {
            path2 = g_build_filename (path1, dname2, NULL);
            g_unlink (path2);

            /* To prevent using too much IO, only remove 1000 objects per 5 seconds.
             */
            if (++(*count) > REMOVE_OBJECTS_BATCH) {
                g_usleep (5 * G_USEC_PER_SEC);
                *count = 0;
            }

            g_free (path2);
        }
        g_dir_close (dir2);

        g_rmdir (path1);
        g_free (path1);
    }

    g_dir_close (dir1);
    g_rmdir (obj_dir);
    g_free (obj_dir);

    return 0;
}

static void
cleanup_deleted_stores_by_type (const char *type)
{
    char *top_store_dir;
    const char *repo_id;

    top_store_dir = g_build_filename (seaf->seaf_dir, "deleted_store", type, NULL);

    GError *error = NULL;
    GDir *dir = g_dir_open (top_store_dir, 0, &error);
    if (!dir) {
        seaf_warning ("Failed to open store dir %s: %s.\n",
                      top_store_dir, error->message);
        g_free (top_store_dir);
        return;
    }

    int count = 0;
    while ((repo_id = g_dir_read_name(dir)) != NULL) {
        remove_store (top_store_dir, repo_id, &count);
    }

    g_free (top_store_dir);
    g_dir_close (dir);
}

static void *
cleanup_deleted_stores (void *vdata)
{
    while (1) {
        cleanup_deleted_stores_by_type ("commits");
        cleanup_deleted_stores_by_type ("fs");
        cleanup_deleted_stores_by_type ("blocks");
        g_usleep (60 * G_USEC_PER_SEC);
    }
    return NULL;
}

int
del_repo (SeafRepoManager *mgr, SeafRepo *repo)
{
    char *repo_id = g_strdup (repo->id);

    gboolean remove_cache = repo->remove_cache;

    seaf_sync_manager_remove_active_path_info (seaf->sync_mgr, repo->id);

    seaf_repo_manager_remove_repo_ondisk (mgr, repo->id, remove_cache);

    /* Only remove the repo object from hash table after it's removed on disk.
     * Otherwise the sync manager may find that the repo doesn't exist and clone it
     * while the on-disk removal is still in progress.
     */
    pthread_rwlock_wrlock (&mgr->priv->lock);
    g_hash_table_remove (mgr->priv->repo_hash, repo->id);
    pthread_rwlock_unlock (&mgr->priv->lock);

    seaf_repo_unref (repo);

    //Journal database is closed when object is released.So we have to delete
    //database file after repo object is release,otherwise the file is still
    //opened and not possible to delete on windows.
    journal_manager_delete_journal (seaf->journal_mgr, repo_id);

    g_free (repo_id);
    return 0;
}

static void *
cleanup_deleted_repos (void *vdata)
{
    SeafRepoManager *mgr = vdata;
    GHashTableIter iter;
    gpointer key, value;
    SeafRepo *repo;
    GList *deleted = NULL;
    GList *ptr;

    while (1) {
        pthread_rwlock_wrlock (&mgr->priv->lock);

        g_hash_table_iter_init (&iter, mgr->priv->repo_hash);
        while (g_hash_table_iter_next (&iter, &key, &value)) {
            repo = value;
            /* Delete a repo if it's marked as delete pending and
             * it's only referenced by the internal hash table.
             */
            if (repo->delete_pending && repo->refcnt == 1) {
                deleted = g_list_prepend (deleted, repo);
            }
        }

        pthread_rwlock_unlock (&mgr->priv->lock);

        for (ptr = deleted; ptr; ptr = ptr->next) {
            repo = ptr->data;
            del_repo (mgr, repo);
        }

        g_list_free (deleted);
        deleted = NULL;

        g_usleep (G_USEC_PER_SEC);
    }
    return NULL;
}

int
seaf_repo_manager_start (SeafRepoManager *mgr)
{
    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    int rc = pthread_create (&tid, &attr, cleanup_deleted_stores, NULL);
    if (rc != 0) {
        seaf_warning ("Failed to start cleanup thread: %s\n", strerror(rc));
        return -1;
    }

    rc = pthread_create (&tid, &attr, cleanup_deleted_repos, mgr);
    if (rc != 0) {
        seaf_warning ("Failed to start cleanup deleted repo thread: %s\n", strerror(rc));
        return -1;
    }

    return 0;
}

int
seaf_repo_manager_add_repo (SeafRepoManager *manager,
                            SeafRepo *repo)
{
    char sql[256];
    sqlite3 *db = manager->priv->db;

    pthread_mutex_lock (&manager->priv->db_lock);

    snprintf (sql, sizeof(sql), "REPLACE INTO Repo VALUES ('%s');", repo->id);
    sqlite_query_exec (db, sql);

    pthread_mutex_unlock (&manager->priv->db_lock);

    repo->manager = manager;

    if (pthread_rwlock_wrlock (&manager->priv->lock) < 0) {
        seaf_warning ("[repo mgr] failed to lock repo cache.\n");
        return -1;
    }

    g_hash_table_insert (manager->priv->repo_hash, g_strdup(repo->id), repo);
    seaf_repo_ref (repo);

    pthread_rwlock_unlock (&manager->priv->lock);

    return 0;
}

int
seaf_repo_manager_mark_repo_deleted (SeafRepoManager *mgr,
                                     SeafRepo *repo,
                                     gboolean remove_cache)
{
    char sql[256];

    seaf_message ("Mark repo %s(%.8s) as deleted.\n", repo->name, repo->id);

    pthread_mutex_lock (&mgr->priv->db_lock);

    snprintf (sql, sizeof(sql), "INSERT INTO DeletedRepo VALUES ('%s', %d)",
              repo->id, remove_cache);
    if (sqlite_query_exec (mgr->priv->db, sql) < 0) {
        pthread_mutex_unlock (&mgr->priv->db_lock);
        return -1;
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    repo->delete_pending = TRUE;
    repo->remove_cache = remove_cache;

#ifdef COMPILE_WS
    seaf_notif_manager_unsubscribe_repo (seaf->notif_mgr, repo);
#endif

    return 0;
}

static char *
gen_deleted_store_path (const char *type, const char *repo_id)
{
    int n = 1;
    char *path = NULL;
    char *name = NULL;

    path = g_build_filename (seaf->deleted_store, type, repo_id, NULL);
    while (g_file_test(path, G_FILE_TEST_EXISTS) && n < 10) {
        g_free (path);
        name = g_strdup_printf ("%s(%d)", repo_id, n);
        path = g_build_filename (seaf->deleted_store, type, name, NULL);
        g_free (name);
        ++n;
    }

    if (n == 10) {
        g_free (path);
        return NULL;
    }

    return path;
}

void
seaf_repo_manager_move_repo_store (SeafRepoManager *mgr,
                                   const char *type,
                                   const char *repo_id)
{
    char *src = NULL;
    char *dst = NULL;

    src = g_build_filename (seaf->seaf_dir, "storage", type, repo_id, NULL);
    if (!seaf_util_exists (src)) {
        return;
    }

    dst = gen_deleted_store_path (type, repo_id);
    if (dst) {
        g_rename (src, dst);
    }
    g_free (src);
    g_free (dst);
}

/* Move commits, fs stores into "deleted_store" directory. */
static void
move_repo_stores (SeafRepoManager *mgr, const char *repo_id)
{
    seaf_repo_manager_move_repo_store (mgr, "commits", repo_id);
    seaf_repo_manager_move_repo_store (mgr, "fs", repo_id);
}

void
seaf_repo_manager_remove_repo_ondisk (SeafRepoManager *mgr,
                                      const char *repo_id,
                                      gboolean remove_cache)
{
    char sql[256];

    seaf_message ("Removing repo %s on disk.\n", repo_id);

    pthread_mutex_lock (&mgr->priv->db_lock);

    snprintf (sql, sizeof(sql), "DELETE FROM Repo WHERE repo_id = '%s'", repo_id);
    if (sqlite_query_exec (mgr->priv->db, sql) < 0) {
        pthread_mutex_unlock (&mgr->priv->db_lock);
        return;
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    /* remove branch */
    GList *p;
    GList *branch_list = 
        seaf_branch_manager_get_branch_list (seaf->branch_mgr, repo_id);
    for (p = branch_list; p; p = p->next) {
        SeafBranch *b = (SeafBranch *)p->data;
        seaf_repo_manager_branch_repo_unmap (mgr, b);
        seaf_branch_manager_del_branch (seaf->branch_mgr, repo_id, b->name);
    }
    seaf_branch_list_free (branch_list);

    /* delete repo property firstly */
    seaf_repo_manager_del_repo_property (mgr, repo_id);

    pthread_mutex_lock (&mgr->priv->db_lock);

    snprintf (sql, sizeof(sql), "DELETE FROM RepoKeys WHERE repo_id = '%s'", 
              repo_id);
    sqlite_query_exec (mgr->priv->db, sql);

    snprintf (sql, sizeof(sql), "DELETE FROM FolderUserPerms WHERE repo_id = '%s'", 
              repo_id);
    sqlite_query_exec (mgr->priv->db, sql);

    snprintf (sql, sizeof(sql), "DELETE FROM FolderGroupPerms WHERE repo_id = '%s'", 
              repo_id);
    sqlite_query_exec (mgr->priv->db, sql);

    snprintf (sql, sizeof(sql), "DELETE FROM FolderPermTimestamp WHERE repo_id = '%s'", 
              repo_id);
    sqlite_query_exec (mgr->priv->db, sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    seaf_filelock_manager_remove (seaf->filelock_mgr, repo_id);

    remove_folder_perms (mgr, repo_id);

    move_repo_stores (mgr, repo_id);

    if (remove_cache)
        file_cache_mgr_delete_repo_cache (seaf->file_cache_mgr, repo_id);

    /* At last remove the deleted record from db. If the above procedure is
     * interrupted, we can still redo the operations on next start.
     */

    pthread_mutex_lock (&mgr->priv->db_lock);

    snprintf (sql, sizeof(sql), 
              "DELETE FROM DeletedRepo WHERE repo_id = '%s'", repo_id);
    sqlite_query_exec (mgr->priv->db, sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);
}

SeafRepo*
seaf_repo_manager_get_repo (SeafRepoManager *manager, const gchar *id)
{
    SeafRepo *repo, *res = NULL;

    if (!id) {
        return NULL;
    }

    if (pthread_rwlock_rdlock (&manager->priv->lock) < 0) {
        seaf_warning ("[repo mgr] failed to lock repo cache.\n");
        return NULL;
    }

    repo = g_hash_table_lookup (manager->priv->repo_hash, id);
    if (repo && !repo->delete_pending) {
        seaf_repo_ref (repo);
        res = repo;
    }

    pthread_rwlock_unlock (&manager->priv->lock);

    return res;
}

gboolean
seaf_repo_manager_repo_exists (SeafRepoManager *manager, const gchar *id)
{
    SeafRepo *repo;
    gboolean res = FALSE;

    if (pthread_rwlock_rdlock (&manager->priv->lock) < 0) {
        seaf_warning ("[repo mgr] failed to lock repo cache.\n");
        return FALSE;
    }

    repo = g_hash_table_lookup (manager->priv->repo_hash, id);
    if (repo && !repo->delete_pending)
        res = TRUE;

    pthread_rwlock_unlock (&manager->priv->lock);

    return res;
}

static int
save_branch_repo_map (SeafRepoManager *manager, SeafBranch *branch)
{
    char *sql;
    sqlite3 *db = manager->priv->db;

    pthread_mutex_lock (&manager->priv->db_lock);

    sql = sqlite3_mprintf ("REPLACE INTO RepoBranch VALUES (%Q, %Q)",
                           branch->repo_id, branch->name);
    sqlite_query_exec (db, sql);
    sqlite3_free (sql);

    pthread_mutex_unlock (&manager->priv->db_lock);

    return 0;
}

int
seaf_repo_manager_branch_repo_unmap (SeafRepoManager *manager, SeafBranch *branch)
{
    char *sql;
    sqlite3 *db = manager->priv->db;

    pthread_mutex_lock (&manager->priv->db_lock);

    sql = sqlite3_mprintf ("DELETE FROM RepoBranch WHERE branch_name = %Q"
                           " AND repo_id = %Q",
                           branch->name, branch->repo_id);
    if (sqlite_query_exec (db, sql) < 0) {
        seaf_warning ("Unmap branch repo failed\n");
        pthread_mutex_unlock (&manager->priv->db_lock);
        sqlite3_free (sql);
        return -1;
    }

    sqlite3_free (sql);
    pthread_mutex_unlock (&manager->priv->db_lock);

    return 0;
}

static void
load_repo_commit (SeafRepoManager *manager,
                  SeafRepo *repo,
                  SeafBranch *branch)
{
    SeafCommit *commit;

    commit = seaf_commit_manager_get_commit_compatible (manager->seaf->commit_mgr,
                                                        repo->id,
                                                        branch->commit_id);
    if (!commit) {
        seaf_warning ("Commit %s/%s is missing\n",
                      repo->id, branch->commit_id);
        repo->is_corrupted = TRUE;
        return;
    }

    set_head_common (repo, branch);
    seaf_repo_from_commit (repo, commit);

    seaf_commit_unref (commit);
}

static gboolean
load_keys_cb (sqlite3_stmt *stmt, void *vrepo)
{
    SeafRepo *repo = vrepo;
    const char *key, *iv;

    key = (const char *)sqlite3_column_text(stmt, 0);
    iv = (const char *)sqlite3_column_text(stmt, 1);

    if (repo->enc_version == 1) {
        hex_to_rawdata (key, repo->enc_key, 16);
        hex_to_rawdata (iv, repo->enc_iv, 16);
    } else if (repo->enc_version >= 2) {
        hex_to_rawdata (key, repo->enc_key, 32);
        hex_to_rawdata (iv, repo->enc_iv, 16);
    }

    repo->is_passwd_set = TRUE;

    return FALSE;
}

static int
load_repo_passwd (SeafRepoManager *manager, SeafRepo *repo)
{
    sqlite3 *db = manager->priv->db;
    char sql[256];
    int n;

    pthread_mutex_lock (&manager->priv->db_lock);

    snprintf (sql, sizeof(sql), 
              "SELECT key, iv FROM RepoKeys WHERE repo_id='%s'",
              repo->id);
    n = sqlite_foreach_selected_row (db, sql, load_keys_cb, repo);
    if (n < 0) {
        pthread_mutex_unlock (&manager->priv->db_lock);
        return -1;
    }

    pthread_mutex_unlock (&manager->priv->db_lock);

    return 0;
    
}

static gboolean
load_property_cb (sqlite3_stmt *stmt, void *pvalue)
{
    char **value = pvalue;

    char *v = (char *) sqlite3_column_text (stmt, 0);
    *value = g_strdup (v);

    /* Only one result. */
    return FALSE;
}

static char *
load_repo_property (SeafRepoManager *manager,
                    const char *repo_id,
                    const char *key)
{
    sqlite3 *db = manager->priv->db;
    char sql[256];
    char *value = NULL;

    pthread_mutex_lock (&manager->priv->db_lock);

    snprintf(sql, 256, "SELECT value FROM RepoProperty WHERE "
             "repo_id='%s' and key='%s'", repo_id, key);
    if (sqlite_foreach_selected_row (db, sql, load_property_cb, &value) < 0) {
        seaf_warning ("Error read property %s for repo %s.\n", key, repo_id);
        pthread_mutex_unlock (&manager->priv->db_lock);
        return NULL;
    }

    pthread_mutex_unlock (&manager->priv->db_lock);

    return value;
}

static gboolean
load_branch_cb (sqlite3_stmt *stmt, void *vrepo)
{
    SeafRepo *repo = vrepo;
    SeafRepoManager *manager = repo->manager;

    char *branch_name = (char *) sqlite3_column_text (stmt, 0);
    SeafBranch *branch =
        seaf_branch_manager_get_branch (manager->seaf->branch_mgr,
                                        repo->id, branch_name);
    if (branch == NULL) {
        seaf_warning ("Broken branch name for repo %s\n", repo->id); 
        repo->is_corrupted = TRUE;
        return FALSE;
    }
    load_repo_commit (manager, repo, branch);
    seaf_branch_unref (branch);

    /* Only one result. */
    return FALSE;
}

static gboolean
load_account_info_cb (sqlite3_stmt *stmt, void *vrepo)
{
    SeafRepo *repo = vrepo;
    const char *server, *user;

    server = (const char *)sqlite3_column_text(stmt, 0);
    user = (const char *)sqlite3_column_text(stmt, 1);

    repo->server = g_strdup (server);
    repo->user = g_strdup (user);

    return FALSE;
}

static int
load_repo_account_info (SeafRepoManager *manager, SeafRepo *repo)
{
    sqlite3 *db = manager->priv->db;
    char sql[256];
    int n;

    pthread_mutex_lock (&manager->priv->db_lock);

    snprintf (sql, sizeof(sql),
              "SELECT server, username FROM AccountRepos WHERE repo_id='%s'",
              repo->id);
    n = sqlite_foreach_selected_row (db, sql, load_account_info_cb, repo);
    if (n < 0) {
        pthread_mutex_unlock (&manager->priv->db_lock);
        return -1;
    }

    pthread_mutex_unlock (&manager->priv->db_lock);

    return 0;

}

static SeafRepo *
load_repo (SeafRepoManager *manager, const char *repo_id)
{
    char sql[256];

    SeafRepo *repo = seaf_repo_new(repo_id, NULL, NULL);
    if (!repo) {
        seaf_warning ("[repo mgr] failed to alloc repo.\n");
        return NULL;
    }

    repo->manager = manager;

    snprintf(sql, 256, "SELECT branch_name FROM RepoBranch WHERE repo_id='%s'",
             repo->id);
    if (sqlite_foreach_selected_row (manager->priv->db, sql, 
                                     load_branch_cb, repo) <= 0) {
        seaf_warning ("Error read branch for repo %s.\n", repo->id);
        seaf_repo_free (repo);
        return NULL;
    }

    /* If repo head is set but failed to load branch or commit. */
    if (repo->is_corrupted) {
        seaf_repo_free (repo);
        return NULL;
    }

    load_repo_passwd (manager, repo);

    load_repo_account_info (manager, repo);

    char *value;

    repo->token = load_repo_property (manager, repo->id, REPO_PROP_TOKEN);

    /* load readonly property */
    value = load_repo_property (manager, repo->id, REPO_PROP_IS_READONLY);
    if (g_strcmp0(value, "true") == 0)
        repo->is_readonly = TRUE;
    else
        repo->is_readonly = FALSE;
    g_free (value);

    g_hash_table_insert (manager->priv->repo_hash, g_strdup(repo->id), repo);

    seaf_repo_ref (repo);

    return repo;
}

static sqlite3*
open_db (SeafRepoManager *manager, const char *seaf_dir)
{
    sqlite3 *db;
    char *db_path;

    db_path = g_build_filename (seaf_dir, "repo.db", NULL);
    if (sqlite_open_db (db_path, &db) < 0)
        return NULL;
    g_free (db_path);
    manager->priv->db = db;

    char *sql = "CREATE TABLE IF NOT EXISTS Repo (repo_id TEXT PRIMARY KEY);";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS DeletedRepo (repo_id TEXT PRIMARY KEY, remove_cache INTEGER);";
    sqlite_query_exec (db, sql);

    /* Locally cache repo list for each account. */
    sql = "CREATE TABLE IF NOT EXISTS AccountRepos "
        "(server TEXT, username TEXT, repo_id TEXT, "
        "name TEXT, display_name TEXT, mtime INTEGER);";
    sqlite_query_exec (db, sql);

    sql = "CREATE UNIQUE INDEX IF NOT EXISTS AccountRepoIdIdx on AccountRepos "
        "(server, username, repo_id);";
    sqlite_query_exec (db, sql);

    sql = "CREATE UNIQUE INDEX IF NOT EXISTS AccountRepoDisplayNameIdx on AccountRepos "
        "(server, username, display_name);";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS AccountSpace "
        "(server TEXT, username TEXT, total INTEGER, used INTEGER, "
        "PRIMARY KEY (server, username));";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS RepoBranch ("
        "repo_id TEXT PRIMARY KEY, branch_name TEXT);";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS RepoKeys "
        "(repo_id TEXT PRIMARY KEY, key TEXT NOT NULL, iv TEXT NOT NULL);";
    sqlite_query_exec (db, sql);
    
    sql = "CREATE TABLE IF NOT EXISTS RepoProperty ("
        "repo_id TEXT, key TEXT, value TEXT);";
    sqlite_query_exec (db, sql);

    sql = "CREATE INDEX IF NOT EXISTS RepoIndex ON RepoProperty (repo_id);";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS FolderUserPerms ("
        "repo_id TEXT, path TEXT, permission TEXT);";
    sqlite_query_exec (db, sql);

    sql = "CREATE INDEX IF NOT EXISTS folder_user_perms_repo_id_idx "
        "ON FolderUserPerms (repo_id);";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS FolderGroupPerms ("
        "repo_id TEXT, path TEXT, permission TEXT);";
    sqlite_query_exec (db, sql);

    sql = "CREATE INDEX IF NOT EXISTS folder_group_perms_repo_id_idx "
        "ON FolderGroupPerms (repo_id);";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS FolderPermTimestamp ("
        "repo_id TEXT, timestamp INTEGER, PRIMARY KEY (repo_id));";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS LockedFiles (repo_id TEXT, path TEXT, "
        "operation TEXT, old_mtime INTEGER, file_id TEXT, new_path TEXT, "
        "PRIMARY KEY (repo_id, path));";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS RepoOldHead ("
          "repo_id TEXT, head TEXT);";
    sqlite_query_exec (db, sql);

    sql = "CREATE TABLE IF NOT EXISTS FileSyncError ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, repo_id TEXT, repo_name TEXT, "
        "path TEXT, err_id INTEGER, timestamp INTEGER);";
    sqlite_query_exec (db, sql);

    sql = "CREATE INDEX IF NOT EXISTS FileSyncErrorIndex ON FileSyncError (repo_id, path)";
    sqlite_query_exec (db, sql);

    return db;
}

static gboolean
load_repo_cb (sqlite3_stmt *stmt, void *vmanager)
{
    SeafRepoManager *manager = vmanager;
    const char *repo_id;

    repo_id = (const char *) sqlite3_column_text (stmt, 0);

    load_repo (manager, repo_id);

    return TRUE;
}

static gboolean
mark_repo_deleted (sqlite3_stmt *stmt, void *vmanager)
{
    SeafRepoManager *manager = vmanager;
    const char *repo_id;
    int remove_cache;
    SeafRepo *repo;

    repo_id = (const char *) sqlite3_column_text (stmt, 0);
    remove_cache = sqlite3_column_int (stmt, 1);

    repo = seaf_repo_manager_get_repo (manager, repo_id);
    if (!repo)
        return TRUE;

    repo->delete_pending = TRUE;
    repo->remove_cache = remove_cache;

    seaf_repo_unref (repo);

    return TRUE;
}

static void
load_repos (SeafRepoManager *manager, const char *seaf_dir)
{
    sqlite3 *db = open_db(manager, seaf_dir);
    if (!db) return;

    char *sql;

    sql = "SELECT repo_id FROM Repo;";
    if (sqlite_foreach_selected_row (db, sql, load_repo_cb, manager) < 0) {
        seaf_warning ("Error read repo db.\n");
        return;
    }

    sql = "SELECT repo_id, remove_cache FROM DeletedRepo";
    if (sqlite_foreach_selected_row (db, sql, mark_repo_deleted, manager) < 0) {
        seaf_warning ("Error mark repos deleted.\n");
        return;
    }
}

static void
save_repo_property (SeafRepoManager *manager,
                    const char *repo_id,
                    const char *key, const char *value)
{
    char *sql;
    sqlite3 *db = manager->priv->db;

    pthread_mutex_lock (&manager->priv->db_lock);

    sql = sqlite3_mprintf ("SELECT repo_id FROM RepoProperty WHERE repo_id=%Q AND key=%Q",
                           repo_id, key);
    if (sqlite_check_for_existence(db, sql)) {
        sqlite3_free (sql);
        sql = sqlite3_mprintf ("UPDATE RepoProperty SET value=%Q"
                               "WHERE repo_id=%Q and key=%Q",
                               value, repo_id, key);
        sqlite_query_exec (db, sql);
        sqlite3_free (sql);
    } else {
        sqlite3_free (sql);
        sql = sqlite3_mprintf ("INSERT INTO RepoProperty VALUES (%Q, %Q, %Q)",
                               repo_id, key, value);
        sqlite_query_exec (db, sql);
        sqlite3_free (sql);
    }

    pthread_mutex_unlock (&manager->priv->db_lock);
}

int
seaf_repo_manager_set_repo_property (SeafRepoManager *manager, 
                                     const char *repo_id,
                                     const char *key,
                                     const char *value)
{
    if (!seaf_repo_manager_repo_exists (manager, repo_id))
        return -1;

    save_repo_property (manager, repo_id, key, value);
    return 0;
}

char *
seaf_repo_manager_get_repo_property (SeafRepoManager *manager, 
                                     const char *repo_id,
                                     const char *key)
{
    return load_repo_property (manager, repo_id, key);
}

static void
seaf_repo_manager_del_repo_property (SeafRepoManager *manager, 
                                     const char *repo_id)
{
    char *sql;
    sqlite3 *db = manager->priv->db;

    pthread_mutex_lock (&manager->priv->db_lock);

    sql = sqlite3_mprintf ("DELETE FROM RepoProperty WHERE repo_id = %Q", repo_id);
    sqlite_query_exec (db, sql);
    sqlite3_free (sql);

    pthread_mutex_unlock (&manager->priv->db_lock);
}

static void
seaf_repo_manager_del_repo_property_by_key (SeafRepoManager *manager,
                                            const char *repo_id,
                                            const char *key)
{
    char *sql;
    sqlite3 *db = manager->priv->db;

    pthread_mutex_lock (&manager->priv->db_lock);

    sql = sqlite3_mprintf ("DELETE FROM RepoProperty "
                           "WHERE repo_id = %Q "
                           "  AND key = %Q", repo_id, key);
    sqlite_query_exec (db, sql);
    sqlite3_free (sql);

    pthread_mutex_unlock (&manager->priv->db_lock);
}

static int
save_repo_enc_info (SeafRepoManager *manager,
                    SeafRepo *repo)
{
    sqlite3 *db = manager->priv->db;
    char sql[512];
    char key[65], iv[33];

    if (repo->enc_version == 1) {
        rawdata_to_hex (repo->enc_key, key, 16);
        rawdata_to_hex (repo->enc_iv, iv, 16);
    } else if (repo->enc_version >= 2) {
        rawdata_to_hex (repo->enc_key, key, 32);
        rawdata_to_hex (repo->enc_iv, iv, 16);
    }

    snprintf (sql, sizeof(sql), "REPLACE INTO RepoKeys VALUES ('%s', '%s', '%s')",
              repo->id, key, iv);
    if (sqlite_query_exec (db, sql) < 0)
        return -1;

    return 0;
}

GList*
seaf_repo_manager_get_repo_list (SeafRepoManager *manager, int start, int limit)
{
    GList *repo_list = NULL;
    GHashTableIter iter;
    SeafRepo *repo;
    gpointer key, value;

    if (pthread_rwlock_rdlock (&manager->priv->lock) < 0) {
        seaf_warning ("[repo mgr] failed to lock repo cache.\n");
        return NULL;
    }
    g_hash_table_iter_init (&iter, manager->priv->repo_hash);

    while (g_hash_table_iter_next (&iter, &key, &value)) {
        repo = value;
        if (!repo->delete_pending) {
            repo_list = g_list_prepend (repo_list, repo);
            seaf_repo_ref (repo);
        }
    }

    pthread_rwlock_unlock (&manager->priv->lock);

    return repo_list;
}

GList*
seaf_repo_manager_get_enc_repo_list (SeafRepoManager *manager, int start, int limit)
{
    GList *repo_list = NULL;
    GHashTableIter iter;
    SeafRepo *repo;
    gpointer key, value;

    if (pthread_rwlock_rdlock (&manager->priv->lock) < 0) {
        seaf_warning ("[repo mgr] failed to lock repo cache.\n");
        return NULL;
    }
    g_hash_table_iter_init (&iter, manager->priv->repo_hash);

    while (g_hash_table_iter_next (&iter, &key, &value)) {
        repo = value;
        if (!repo->delete_pending && repo->encrypted) {
            repo_list = g_list_prepend (repo_list, repo);
            seaf_repo_ref (repo);
        }
    }

    pthread_rwlock_unlock (&manager->priv->lock);

    return repo_list;
}

gboolean
seaf_repo_manager_is_repo_delete_pending (SeafRepoManager *manager, const char *id)
{
    SeafRepo *repo;
    gboolean ret = FALSE;

    pthread_rwlock_rdlock (&manager->priv->lock);

    repo = g_hash_table_lookup (manager->priv->repo_hash, id);
    if (repo && repo->delete_pending)
        ret = TRUE;

    pthread_rwlock_unlock (&manager->priv->lock);

    return ret;
}

int
seaf_repo_manager_set_repo_token (SeafRepoManager *manager, 
                                  SeafRepo *repo,
                                  const char *token)
{
    g_free (repo->token);
    repo->token = g_strdup(token);

    save_repo_property (manager, repo->id, REPO_PROP_TOKEN, token);
    return 0;
}


int
seaf_repo_manager_remove_repo_token (SeafRepoManager *manager,
                                     SeafRepo *repo)
{
    g_free (repo->token);
    repo->token = NULL;
    seaf_repo_manager_del_repo_property_by_key(manager, repo->id, REPO_PROP_TOKEN);
    return 0;
}

void
seaf_repo_manager_rename_repo (SeafRepoManager *manager,
                               const char *repo_id,
                               const char *new_name)
{
    SeafRepo *repo;

    pthread_rwlock_wrlock (&manager->priv->lock);

    repo = g_hash_table_lookup (manager->priv->repo_hash, repo_id);
    if (repo) {
        g_free (repo->name);
        repo->name = g_strdup (new_name);
    }

    pthread_rwlock_unlock (&manager->priv->lock);
}

/* Account cache related functions. */

void
seaf_account_free (SeafAccount *account)
{
    if (!account)
        return;
    g_free (account->server);
    g_free (account->username);
    g_free (account->nickname);
    g_free (account->token);
    g_free (account->fileserver_addr);
    g_free (account->unique_id);
    g_free (account);
}

static const char *repo_type_strings[] = {
    "",
    "My Libraries",
    "Shared with me",
    "Shared with groups",
    "Shared with all",
    NULL
};

static const char *repo_type_strings_zh[] = {
    "",
    "我的资料库",
    "共享资料库",
    "群组资料库",
    "公共资料库",
    NULL
};

static const char *repo_type_strings_fr[] = {
    "",
    "Mes bibliothèques",
    "Partagé avec moi",
    "Partagé avec des groupes",
    "Partagé avec tout le monde",
    NULL
};

static const char *repo_type_strings_de[] = {
    "",
    "Meine Bibliotheken",
    "Für mich freigegeben",
    "Für meine Gruppen",
    "Für alle freigegeben",
    NULL
};

static const char **
get_repo_type_string_table ()
{
    const char **string_table;

    if (seaf->language == SEAF_LANG_ZH_CN)
        string_table = repo_type_strings_zh;
    else if (seaf->language == SEAF_LANG_FR_FR)
        string_table = repo_type_strings_fr;
    else if (seaf->language == SEAF_LANG_DE_DE)
        string_table = repo_type_strings_de;
    else
        string_table = repo_type_strings;

    return string_table;
}

RepoType
repo_type_from_string (const char *type_str)
{
    const char *str;
    int i;
    RepoType type = REPO_TYPE_UNKNOWN;
    const char **string_table = get_repo_type_string_table ();

    for (i = 0; i < N_REPO_TYPE; ++i) {
        str = string_table[i];
        if (g_strcmp0 (type_str, str) == 0) {
            type = i;
            break;
        }
    }

    return type;
}

GList *
repo_type_string_list ()
{
    GList *ret = NULL;
    const char *str;
    int i = 0;
    const char **string_table = get_repo_type_string_table ();

    for (i = 1; i < N_REPO_TYPE; ++i) {
        str = string_table[i];
        ret = g_list_append (ret, g_strdup(str));
    }

    return ret;
}

RepoInfo *
repo_info_new (const char *id, const char *head_commit_id,
               const char *name, gint64 mtime, gboolean is_readonly)
{
    RepoInfo *ret = g_new0 (RepoInfo, 1);
    memcpy (ret->id, id, 36);
    ret->head_commit_id = g_strdup(head_commit_id);
    ret->name = g_strdup(name);
    ret->mtime = mtime;
    ret->is_readonly = is_readonly;
    return ret;
}

RepoInfo *
repo_info_copy (RepoInfo *info)
{
    RepoInfo *ret = g_new0 (RepoInfo, 1);
    memcpy (ret->id, info->id, 36);
    ret->head_commit_id = g_strdup(info->head_commit_id);
    ret->name = g_strdup(info->name);
    ret->display_name = g_strdup(info->display_name);
    ret->mtime = info->mtime;
    ret->is_readonly = info->is_readonly;
    ret->is_corrupted = info->is_corrupted;
    ret->type = info->type;
    return ret;
}

void
repo_info_free (RepoInfo *info)
{
    if (!info)
        return;
    g_free (info->head_commit_id);
    g_free (info->name);
    g_free (info->display_name);
    g_free (info);
}

typedef struct _LoadRepoInfoRes {
    GHashTable *repos;
    GHashTable *name_to_repo;
} LoadRepoInfoRes;

static RepoType
repo_type_from_display_name (const char *display_name)
{
    char **tokens;
    char *type_str;
    RepoType type = REPO_TYPE_UNKNOWN;
    const char **string_table = get_repo_type_string_table ();
    int i;

    tokens = g_strsplit (display_name, "/", 0);
    if (g_strv_length (tokens) > 1) {
        type_str = tokens[0];
        for (i = 1; i < N_REPO_TYPE; ++i) {
            if (strcmp (type_str, string_table[i]) == 0)
                type = i;
        }
    }

    g_strfreev (tokens);
    return type;
}

static gboolean
load_repo_info_cb (sqlite3_stmt *stmt, void *data)
{
    LoadRepoInfoRes *res = data;
    GHashTable *repos = res->repos;
    const char *repo_id, *name, *display_name;
    gint64 mtime;
    SeafRepo* repo = NULL;

    repo_id = (const char *)sqlite3_column_text (stmt, 0);
    name = (const char *)sqlite3_column_text (stmt, 1);
    display_name = (const char *)sqlite3_column_text (stmt, 2);
    mtime = (gint64)sqlite3_column_int64 (stmt, 3);

    RepoInfo *info = g_new0 (RepoInfo, 1);
    memcpy (info->id, repo_id, 36);
    info->name = g_strdup(name);
    info->display_name = g_strdup(display_name);
    info->mtime = mtime;
    info->type = repo_type_from_display_name (display_name);

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
       seaf_warning ("Failed to find repo %s.\n", repo_id);
    } else {
       info->is_readonly = repo->is_readonly;
       seaf_repo_unref(repo);
    }

    g_hash_table_insert (repos, g_strdup(repo_id), info);

    return TRUE;
}

static int
load_current_account_repo_info (SeafRepoManager *mgr,
                                const char *server,
                                const char *username,
                                GHashTable *repos)
{
    LoadRepoInfoRes res;
    res.repos = repos;

    pthread_mutex_lock (&mgr->priv->db_lock);

    char *sql = sqlite3_mprintf ("SELECT repo_id, name, display_name, mtime "
                                 "FROM AccountRepos WHERE "
                                 "server='%q' AND username='%q'",
                                 server, username);
    if (sqlite_foreach_selected_row (mgr->priv->db, sql,
                                     load_repo_info_cb, &res) < 0) {
        sqlite3_free (sql);
        pthread_mutex_unlock (&mgr->priv->db_lock);
        return -1;
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    sqlite3_free (sql);

    return 0;
}

/* static void */
/* update_file_sync_status_cb (const char *repo_id, */
/*                             const char *file_path, */
/*                             SeafStat *st, */
/*                             void *user_data) */
/* { */
/*     if (file_cache_mgr_is_file_cached (seaf->file_cache_mgr, */
/*                                        repo_id, file_path) && */
/*         !file_cache_mgr_is_file_changed (seaf->file_cache_mgr, */
/*                                          repo_id, file_path, FALSE)) { */
/*         seaf_sync_manager_update_active_path (seaf->sync_mgr, repo_id, */
/*                                               file_path, st->st_mode, SYNC_STATUS_SYNCED); */
/*     } */
/* } */

static void
load_repo_fs_worker (gpointer data, gpointer user_data)
{
    SeafRepo *repo = (SeafRepo *)data;

    seaf_repo_load_fs (repo, FALSE);

    seaf_repo_unref (repo);
}

#define MAX_LOAD_REPO_TREE_THREADS 5

static void *
load_repo_file_systems_thread (void *vdata)
{
    AccountInfo *account_info = vdata;
    GList *info_list, *ptr;
    RepoInfo *info;
    SeafRepo *repo;
    GThreadPool *pool;

    pool = g_thread_pool_new (load_repo_fs_worker,
                              NULL,
                              MAX_LOAD_REPO_TREE_THREADS,
                              FALSE,
                              NULL);
    if (!pool) {
        seaf_warning ("Failed to create thread pool.\n");
        goto out;
    }

    info_list = seaf_repo_manager_get_account_repos (seaf->repo_mgr, account_info->server, account_info->username);
    for (ptr = info_list; ptr; ptr = ptr->next) {
        info = (RepoInfo *)ptr->data;

        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, info->id);
        if (!repo) {
            continue;
        }

        seaf_repo_set_worktree (repo, info->display_name);

        g_thread_pool_push (pool, repo, NULL);
    }

    /* Wait until all tasks being done. */
    g_thread_pool_free (pool, FALSE, TRUE);
    g_list_free_full (info_list, (GDestroyNotify)repo_info_free);

    /* for (ptr = info_list; ptr; ptr = ptr->next) { */
    /*     info = (RepoInfo *)ptr->data; */
    /*     file_cache_mgr_traverse_path (seaf->file_cache_mgr, info->id, "", */
    /*                                   update_file_sync_status_cb, NULL, NULL); */
    /* } */

out:
    account_info_free (account_info);
    return NULL;
}

static char *
create_unique_id (const char *server, const char *username)
{
    char *id = g_strconcat (server, "_", username, NULL);
    char *p;

    for (p = id; *p != '\0'; ++p) {
        if (*p == ':' || *p == '/')
            *p = '_';
    }

    return id;
}

void
account_info_free (AccountInfo *info)
{
    if (!info)
        return;
    g_free (info->server);
    g_free (info->username);
    g_free (info);
}

AccountInfo *
seaf_repo_manager_get_account_info_by_name  (SeafRepoManager *mgr,
                                             const char *name)
{
    GHashTableIter iter;
    gpointer key, value;
    SeafAccount *account = NULL;
    AccountInfo *account_info = NULL;

    pthread_rwlock_rdlock (&mgr->priv->account_lock);

    g_hash_table_iter_init (&iter, mgr->priv->accounts);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        account = (SeafAccount *)value;
        if (g_strcmp0 (account->name, name) == 0) {
            account_info = g_new0 (AccountInfo, 1);
            account_info->server = g_strdup (account->server);
            account_info->username = g_strdup (account->username);
            break;
        }
    }

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    return account_info;
}

static void
add_repo_info (SeafRepoManager *mgr, char *account_key, GHashTable *new_repos)
{
    GHashTable *new_name_to_repo;
    RepoInfo *info, *copy;
    GHashTableIter iter;
    gpointer key, value;
    new_name_to_repo = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);

    g_hash_table_iter_init (&iter, new_repos);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        info = value;
        copy = repo_info_copy (info);
        g_hash_table_insert (mgr->priv->repo_infos, g_strdup(copy->id), copy);
        g_hash_table_insert (new_name_to_repo, g_strdup(copy->display_name), copy);
    }

    g_hash_table_insert (mgr->priv->repo_names, g_strdup(account_key), new_name_to_repo);

    return;
}

int
seaf_repo_manager_add_account (SeafRepoManager *mgr,
                               const char *server,
                               const char *username,
                               const char *nickname,
                               const char *token,
                               const char *name,
                               gboolean is_pro)
{
    SeafAccount *account = NULL;
    SeafAccount *new_account = NULL;
    GHashTable *new_repos;

    seaf_message ("adding account %s %s %s.\n", server, username, name);

    account = seaf_repo_manager_get_account (seaf->repo_mgr, server, username);
    if (account) {
        seaf_account_free (account);
        return 0;
    }

    new_repos = g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
                                       (GDestroyNotify)repo_info_free);

    if (load_current_account_repo_info (mgr, server, username,
                                        new_repos ) < 0) {
        g_hash_table_destroy (new_repos);
        return -1;
    }

    char *account_key = g_strconcat (server, "_", username, NULL);

    new_account = g_new0 (SeafAccount, 1);
    new_account->server = g_strdup(server);
    new_account->username = g_strdup(username);
    new_account->nickname = g_strdup(nickname);
    new_account->token = g_strdup(token);
    new_account->name = g_strdup(name);
    new_account->fileserver_addr = parse_fileserver_addr(server);
    new_account->is_pro = is_pro;
    new_account->unique_id = create_unique_id (server, username);
    new_account->repo_list_fetched = FALSE;
    new_account->all_repos_loaded = FALSE;

    pthread_rwlock_wrlock (&mgr->priv->account_lock);
    add_repo_info (mgr, account_key, new_repos);
    g_hash_table_insert (mgr->priv->accounts, g_strdup(account_key), new_account);
    pthread_rwlock_unlock (&mgr->priv->account_lock);
    g_hash_table_destroy (new_repos);
    g_free (account_key);

    seaf->last_check_repo_list_time = 0;
    seaf->last_access_fs_time = 0;

    /* Update current repo list immediately after account switch. */
    seaf_sync_manager_update_account_repo_list (seaf->sync_mgr, server, username);

    pthread_t tid;
    AccountInfo *account_info = g_new0(AccountInfo, 1);
    account_info->server = g_strdup (server);
    account_info->username = g_strdup (username);
    int rc = pthread_create (&tid, NULL, load_repo_file_systems_thread, account_info);
    if (rc != 0) {
        seaf_warning ("Failed to start load repo fs thread: %s\n", strerror(rc));
    }

    return 0;
}

static
SeafAccount *
copy_account (SeafAccount *account)
{
    SeafAccount *ret = g_new0 (SeafAccount, 1);
    ret->server = g_strdup(account->server);
    ret->username = g_strdup(account->username);
    ret->nickname = g_strdup(account->nickname);
    ret->token = g_strdup(account->token);
    ret->name = g_strdup (account->name);
    ret->fileserver_addr = g_strdup(account->fileserver_addr);
    ret->is_pro = account->is_pro;
    ret->unique_id = g_strdup(account->unique_id);
    ret->repo_list_fetched = account->repo_list_fetched;
    ret->all_repos_loaded = account->all_repos_loaded;
    ret->server_disconnected = account->server_disconnected;

    return ret;
}

SeafAccount *
seaf_repo_manager_get_account (SeafRepoManager *mgr,
                               const char *server,
                               const char *username)
{
    SeafAccount *account = NULL, *ret = NULL;

    if (!server && !username) {
        return NULL;
    }

    char *key = g_strconcat (server, "_", username, NULL);

    pthread_rwlock_rdlock (&mgr->priv->account_lock);

    account =  g_hash_table_lookup (mgr->priv->accounts, key);
    if (!account) {
        goto out;
    }

    ret = copy_account (account);

out:
    pthread_rwlock_unlock (&mgr->priv->account_lock);
    g_free (key);

    return ret;
}

gboolean
seaf_repo_manager_account_exists (SeafRepoManager *mgr,
                                  const char *server,
                                  const char *username)
{
    SeafAccount *account = NULL;
    gboolean exists = TRUE;

    char *key = g_strconcat (server, "_", username, NULL);

    pthread_rwlock_rdlock (&mgr->priv->account_lock);

    account =  g_hash_table_lookup (mgr->priv->accounts, key);
    if (!account) {
        exists = FALSE;
        goto out;
    }

out:
    pthread_rwlock_unlock (&mgr->priv->account_lock);
    g_free (key);

    return exists;
}

GList *
seaf_repo_manager_get_account_list (SeafRepoManager *mgr)
{
    GList *ret = NULL;
    GHashTableIter iter;
    gpointer key, value;
    SeafAccount *account = NULL;

    pthread_rwlock_rdlock (&mgr->priv->account_lock);

    g_hash_table_iter_init (&iter, mgr->priv->accounts);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        account = copy_account ((SeafAccount *)value);
        ret = g_list_prepend (ret, account);
    }

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    return ret;
}

gboolean
seaf_repo_manager_account_is_pro (SeafRepoManager *mgr,
                                  const char *server,
                                  const char *user)
{
    gboolean ret = FALSE;

    SeafAccount *account = seaf_repo_manager_get_account (mgr, server, user);
    if (account) {
        ret = account->is_pro;

    }
    seaf_account_free (account);

    return ret;
}

void
seaf_repo_manager_set_repo_list_fetched (SeafRepoManager *mgr,
                                         const char *server,
                                         const char *username)
{
    SeafAccount *account = NULL;

    char *key = g_strconcat (server, "_", username, NULL);

    pthread_rwlock_wrlock (&mgr->priv->account_lock);

    account =  g_hash_table_lookup (mgr->priv->accounts, key);
    if (!account) {
        goto out;
    }

    account->repo_list_fetched = TRUE;

out:
    pthread_rwlock_unlock (&mgr->priv->account_lock);
    g_free (key);
}

void
seaf_repo_manager_set_account_all_repos_loaded (SeafRepoManager *mgr,
                                                const char *server,
                                                const char *username)
{
    SeafAccount *account = NULL;

    char *key = g_strconcat (server, "_", username, NULL);

    pthread_rwlock_wrlock (&mgr->priv->account_lock);

    account =  g_hash_table_lookup (mgr->priv->accounts, key);
    if (!account) {
        goto out;
    }

    account->all_repos_loaded = TRUE;

out:
    pthread_rwlock_unlock (&mgr->priv->account_lock);
    g_free (key);
}

void
seaf_repo_manager_set_account_server_disconnected (SeafRepoManager *mgr,
                                                   const char *server,
                                                   const char *username,
                                                   gboolean server_disconnected)
{
    SeafAccount *account = NULL;

    char *key = g_strconcat (server, "_", username, NULL);

    pthread_rwlock_wrlock (&mgr->priv->account_lock);

    account =  g_hash_table_lookup (mgr->priv->accounts, key);
    if (!account) {
        goto out;
    }

    account->server_disconnected = server_disconnected;

out:
    pthread_rwlock_unlock (&mgr->priv->account_lock);
    g_free (key);
}

static int
delete_account_repos_from_db (SeafRepoManager *mgr,
                              const char *server,
                              const char *username)
{
    char *sql;
    int ret;

    pthread_mutex_lock (&mgr->priv->db_lock);

    sql = sqlite3_mprintf ("DELETE FROM AccountRepos WHERE server='%q' AND username='%q'",
                           server, username);

    ret = sqlite_query_exec (mgr->priv->db, sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    sqlite3_free (sql);
    return ret;
}

static gboolean
collect_account_repo_cb (sqlite3_stmt *stmt, void *data)
{
    GList **prepos = data;
    const char *repo_id;

    repo_id = (const char *)sqlite3_column_text (stmt, 0);

    *prepos = g_list_prepend (*prepos, g_strdup(repo_id));

    return TRUE;
}

static int
delete_account_repos (SeafRepoManager *mgr, const char *server, const char *username,
                      gboolean remove_cache)
{
    char *sql;
    GList *repos = NULL, *ptr;
    char *repo_id;
    SeafRepo *repo;
    int ret = 0;

    pthread_mutex_lock (&mgr->priv->db_lock);

    sql = sqlite3_mprintf ("SELECT repo_id FROM AccountRepos WHERE "
                           "server='%q' AND username='%q'",
                           server, username);
    if (sqlite_foreach_selected_row (mgr->priv->db, sql, collect_account_repo_cb, &repos) < 0) {
        ret = -1;
        goto out;
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    for (ptr = repos; ptr; ptr = ptr->next) {
        repo_id = ptr->data;
        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
        if (repo) {
            seaf_repo_manager_mark_repo_deleted (seaf->repo_mgr, repo, remove_cache);
            http_tx_manager_cancel_task (seaf->http_tx_mgr, repo->id,
                                         HTTP_TASK_TYPE_DOWNLOAD);
            http_tx_manager_cancel_task (seaf->http_tx_mgr, repo->id,
                                         HTTP_TASK_TYPE_UPLOAD);
        }
        seaf_repo_unref (repo);
    }

    g_list_free_full (repos, g_free);

    delete_account_repos_from_db (mgr, server, username);

out:
    sqlite3_free (sql);
    return ret;
}

int
seaf_repo_manager_delete_account (SeafRepoManager *mgr,
                                  const char *server,
                                  const char *username,
                                  gboolean remove_cache,
                                  GError **error)
{
    SeafAccount *account = NULL;
    GHashTable *name_to_repo = NULL;
    GHashTableIter iter;
    gpointer key, value;
    RepoInfo *info;
    int ret = 0;

    char *account_key = g_strconcat (server, "_", username, NULL);

    pthread_rwlock_wrlock (&mgr->priv->account_lock);

    account =  g_hash_table_lookup (mgr->priv->accounts, account_key);
    if  (!account) {
        pthread_rwlock_unlock (&mgr->priv->account_lock);
        g_free (account_key);
        return 0;
    }

    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);

    g_hash_table_iter_init (&iter, name_to_repo);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        info = value;
        g_hash_table_remove (mgr->priv->repo_infos, info->id);
    }

    g_hash_table_remove (mgr->priv->repo_names, account_key);

    g_hash_table_remove (mgr->priv->accounts, account_key);

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    if (delete_account_repos (mgr, server, username, remove_cache) < 0) {
        ret = -1;
    }

    g_free (account_key);
    return ret;
}

GList *
seaf_repo_manager_get_account_repos (SeafRepoManager *mgr,
                                     const char *server,
                                     const char *user)
{
    GList *ret = NULL;
    GHashTable *name_to_repo = NULL;
    GHashTableIter iter;
    gpointer key, value;
    RepoInfo *info, *copy;
    char *account_key = NULL;

    account_key = g_strconcat (server, "_", user, NULL);

    pthread_rwlock_rdlock (&mgr->priv->account_lock);

    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);
    if (!name_to_repo) {
        pthread_rwlock_unlock (&mgr->priv->account_lock);
        g_free (account_key);
        return NULL;
    }

    g_hash_table_iter_init (&iter, name_to_repo);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        info = value;
        copy = repo_info_copy (info);
        ret = g_list_prepend (ret, copy);
    }

    ret = g_list_reverse (ret);

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    return ret;
}

GList *
seaf_repo_manager_get_account_repo_ids (SeafRepoManager *mgr,
                                        const char *server,
                                        const char *user)
{
    GList *ret = NULL;
    GHashTable *name_to_repo = NULL;
    GHashTableIter iter;
    gpointer key, value;
    RepoInfo *info;
    char *account_key = NULL;

    account_key = g_strconcat (server, "_", user, NULL);

    pthread_rwlock_rdlock (&mgr->priv->account_lock);

    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);
    if (!name_to_repo) {
        pthread_rwlock_unlock (&mgr->priv->account_lock);
        g_free (account_key);
        return NULL;
    }

    g_hash_table_iter_init (&iter, name_to_repo);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        info = value;
        ret = g_list_prepend (ret, g_strdup (info->id));
    }

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    return ret;
}

RepoInfo *
seaf_repo_manager_get_repo_info_by_display_name (SeafRepoManager *mgr,
                                                 const char *server,
                                                 const char *user,
                                                 const char *name)
{
    GHashTable *name_to_repo = NULL;
    RepoInfo *info, *ret = NULL;
    char *account_key = NULL;

    if (!seaf_repo_manager_account_exists (mgr, server, user)) {
        return NULL;
    }

    account_key = g_strconcat (server, "_", user, NULL);

    pthread_rwlock_rdlock (&mgr->priv->account_lock);

    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);

    info = g_hash_table_lookup (name_to_repo, name);
    if (info)
        ret = repo_info_copy (info);

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    g_free (account_key);

    return ret;
}

char *
seaf_repo_manager_get_repo_id_by_display_name (SeafRepoManager *mgr,
                                               const char *server,
                                               const char *user,
                                               const char *name)
{
    GHashTable *name_to_repo = NULL;
    RepoInfo *info;
    char *repo_id = NULL;
    char *account_key = NULL;

    if (!seaf_repo_manager_account_exists (mgr, server, user)) {
        return NULL;
    }

    account_key = g_strconcat (server, "_", user, NULL);

    pthread_rwlock_rdlock (&mgr->priv->account_lock);

    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);
    info = g_hash_table_lookup (name_to_repo, name);
    if (info)
        repo_id = g_strdup (info->id);

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    g_free (account_key);

    return repo_id;
}

char *
seaf_repo_manager_get_repo_display_name (SeafRepoManager *mgr,
                                         const char *id)
{
    RepoInfo *info;
    char *display_name = NULL;

    pthread_rwlock_rdlock (&mgr->priv->account_lock);

    info = g_hash_table_lookup (mgr->priv->repo_infos, id);
    if (info)
        display_name = g_strdup(info->display_name);

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    return display_name;
}

static char *
find_unique_display_name (GHashTable *name_to_repo, RepoType type, const char *name)
{
    char *base_display_name, *display_name;
    int n = 0;
    const char **string_table = get_repo_type_string_table ();

    if (type <= REPO_TYPE_UNKNOWN || type >= N_REPO_TYPE) {
        base_display_name = g_strdup(name);
    } else {
        base_display_name = g_build_path ("/", string_table[type], name, NULL);
    }

    display_name = g_strdup(base_display_name);
    while (g_hash_table_lookup (name_to_repo, display_name) != NULL) {
        ++n;
        g_free (display_name);
        display_name = g_strdup_printf ("%s (%d)", base_display_name, n);
    }

    g_free (base_display_name);
    return display_name;
}

static int
remove_repo_from_account (sqlite3 *db,
                          const char *server,
                          const char *username,
                          const char *repo_id)
{
    char *sql;
    int ret = 0;

    sql = sqlite3_mprintf ("DELETE FROM AccountRepos WHERE "
                           "server='%q' AND username='%q' AND repo_id='%q'",
                           server, username, repo_id);
    ret = sqlite_query_exec (db, sql);

    sqlite3_free (sql);

    return ret;
}

static int
add_repo_to_account (sqlite3 *db,
                     const char *server,
                     const char *username,
                     RepoInfo *info)
{
    char *sql;
    int ret = 0;

    sql = sqlite3_mprintf ("INSERT INTO AccountRepos VALUES "
                           "('%q', '%q', '%q', '%q', '%q', %lld)",
                           server, username, info->id,
                           info->name, info->display_name, info->mtime);
    ret = sqlite_query_exec (db, sql);

    sqlite3_free (sql);

    return ret;
}

static int
update_repo_to_account (sqlite3 *db,
                        const char *server,
                        const char *username,
                        RepoInfo *info)
{
    char *sql;
    int ret = 0;

    sql = sqlite3_mprintf ("UPDATE AccountRepos SET name = '%q', display_name = '%q', "
                           "mtime = %lld"
                           " WHERE server = '%q' AND username = '%q' AND repo_id = '%q'",
                           info->name, info->display_name, info->mtime,
                           server, username, info->id);
    ret = sqlite_query_exec (db, sql);

    sqlite3_free (sql);

    return ret;
}

void
seaf_repo_manager_remove_account_repo (SeafRepoManager *mgr,
                                       const char *repo_id,
                                       const char *server,
                                       const char *user)
{
    GHashTable *repo_infos = NULL, *name_to_repo = NULL;
    char *account_key = NULL;
    RepoInfo *info = NULL, *copy = NULL;
    SeafRepo *repo = NULL;

    if (!seaf_repo_manager_account_exists (mgr, server, user)) {
        return;
    }

    account_key = g_strconcat (server, "_", user, NULL);

    pthread_rwlock_wrlock (&mgr->priv->account_lock);

    repo_infos = mgr->priv->repo_infos;
    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);

    info = g_hash_table_lookup (repo_infos, repo_id);
    if (info) {
        copy = repo_info_copy (info);
        g_hash_table_remove (repo_infos, repo_id);
        g_hash_table_remove (name_to_repo, copy->display_name);
    }

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    pthread_mutex_lock (&mgr->priv->db_lock);

    remove_repo_from_account (mgr->priv->db, server, user, repo_id);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (repo) {
        seaf_message ("Repo %s(%.8s) was deleted on server. Remove local repo.\n",
                      repo->name, repo->id);

        seaf_repo_manager_mark_repo_deleted (seaf->repo_mgr, repo, TRUE);
        http_tx_manager_cancel_task (seaf->http_tx_mgr, repo_id,
                                     HTTP_TASK_TYPE_DOWNLOAD);
        http_tx_manager_cancel_task (seaf->http_tx_mgr, repo_id,
                                     HTTP_TASK_TYPE_UPLOAD);
        seaf_repo_unref (repo);
    }
    g_free (account_key);
    repo_info_free (copy);

    return;
}

int
seaf_repo_manager_update_account_repos (SeafRepoManager *mgr,
                                        const char *server,
                                        const char *username,
                                        GHashTable *server_repos,
                                        GList **added,
                                        GList **removed)
{
    GHashTable *repo_infos, *name_to_repo;
    GList *account_repos = NULL;
    GHashTableIter iter;
    gpointer key, value;
    char *repo_id;
    RepoInfo *info, *copy, *server_info;
    GList *ptr;
    char *display_name;
    char *account_key = NULL;
    SeafRepo *repo;
    gboolean need_update;
    GList *updated = NULL;

    if (!seaf_repo_manager_account_exists (mgr, server, username)) {
        return 0;
    }

    account_key = g_strconcat (server, "_", username, NULL);

    pthread_rwlock_wrlock (&mgr->priv->account_lock);

    repo_infos = mgr->priv->repo_infos;
    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);

    g_hash_table_iter_init (&iter, name_to_repo);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        info = value;
        account_repos = g_list_prepend (account_repos, info);
    }

    for (ptr = account_repos; ptr; ptr = ptr->next) {
        info = ptr->data;
        repo_id = info->id;

        server_info = g_hash_table_lookup (server_repos, repo_id);
        if (!server_info) {
            /* Remove local repos that don't exist in server_repos. */
            copy = repo_info_copy (info);
            *removed = g_list_prepend (*removed, copy);
            g_hash_table_remove (repo_infos, repo_id);
            g_hash_table_remove (name_to_repo, copy->display_name);
        } else {
            /* Update existing local repos. */
            if (g_strcmp0 (info->head_commit_id, server_info->head_commit_id) != 0) {
                g_free (info->head_commit_id);
                info->head_commit_id = g_strdup(server_info->head_commit_id);
            }

            if (info->is_readonly != server_info->is_readonly) {
                info->is_readonly = server_info->is_readonly;
                repo = seaf_repo_manager_get_repo (seaf->repo_mgr, info->id);
                if (repo) {
                    if (info->is_readonly)
                        seaf_repo_set_readonly (repo);
                    else
                        seaf_repo_unset_readonly (repo);
                    seaf_repo_unref (repo);
                }
            }

            need_update = FALSE;
            if (info->type != server_info->type ||
                g_strcmp0 (info->name, server_info->name) != 0) {

                repo = seaf_repo_manager_get_repo (seaf->repo_mgr, info->id);
                if (repo && !repo->fs_ready) {
                    seaf_repo_unref (repo);
                    continue;
                }
                g_hash_table_remove (name_to_repo, info->name);
                g_free (info->name);
                info->name = g_strdup(server_info->name);
                display_name = find_unique_display_name (name_to_repo,
                                                         server_info->type,
                                                         server_info->name);
                info->display_name = display_name;
                info->type = server_info->type;
                g_hash_table_insert (name_to_repo, g_strdup(display_name), info);

                if (repo) {
                    char *worktree = g_strdup (repo->worktree);
                    seaf_repo_set_worktree (repo, display_name);
                    g_free (worktree);
                    seaf_repo_unref (repo);
                }

                need_update = TRUE;
            }

            if (info->mtime != server_info->mtime) {
                info->mtime = server_info->mtime;
                need_update = TRUE;
            }

            if (need_update)
                updated = g_list_append (updated, repo_info_copy (info));

            if (server_info->is_corrupted)
                info->is_corrupted = TRUE;
            }
    }

    /* Add new repos from server_repos to local cache. */
    g_hash_table_iter_init (&iter, server_repos);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        repo_id = key;
        if (!g_hash_table_lookup (repo_infos, repo_id)) {
            info = value;
            copy = repo_info_copy (info);
            *added = g_list_prepend (*added, copy);
        }
    }

    for (ptr = *added; ptr; ptr = ptr->next) {
        info = ptr->data;
        display_name = find_unique_display_name (name_to_repo, info->type, info->name);
        info->display_name = display_name;
        copy = repo_info_copy (info);
        g_hash_table_insert (repo_infos, g_strdup(info->id), copy);
        g_hash_table_insert (name_to_repo, g_strdup(display_name), copy);
    }

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    pthread_mutex_lock (&mgr->priv->db_lock);

    for (ptr = *removed; ptr; ptr = ptr->next) {
        info = ptr->data;
        remove_repo_from_account (mgr->priv->db, server, username, info->id);
    }

    for (ptr = *added; ptr; ptr = ptr->next) {
        info = ptr->data;
        add_repo_to_account (mgr->priv->db, server, username, info);
    }

    for (ptr = updated; ptr; ptr = ptr->next) {
        info = ptr->data;
        update_repo_to_account (mgr->priv->db, server, username, info);
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    g_free (account_key);
    g_list_free (account_repos);
    g_list_free_full (updated, (GDestroyNotify)repo_info_free);

    return 0;
}

int
seaf_repo_manager_add_repo_to_account (SeafRepoManager *mgr,
                                       const char *server,
                                       const char *username,
                                       SeafRepo *repo)
{
    int ret = 0;
    RepoInfo *info = NULL;
    GHashTable *name_to_repo = NULL;
    char *account_key = NULL;

    if (!seaf_repo_manager_account_exists (mgr, server, username)) {
        return -1;
    }

    account_key = g_strconcat (server, "_", username, NULL);

    pthread_rwlock_wrlock (&mgr->priv->account_lock);

    if (g_hash_table_lookup (mgr->priv->repo_infos, repo->id) != NULL) {
        pthread_rwlock_unlock (&mgr->priv->account_lock);
        goto out;
    }

    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);

    info = repo_info_new (repo->id, repo->head->commit_id, repo->name,
                          repo->last_modify, FALSE);
    info->type = REPO_TYPE_MINE;
    info->display_name = find_unique_display_name (name_to_repo,
                                                   REPO_TYPE_MINE,
                                                   repo->name);

    g_hash_table_replace (mgr->priv->repo_infos, g_strdup (info->id), info);
    g_hash_table_replace (name_to_repo, g_strdup (info->display_name), info);

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    pthread_mutex_lock (&mgr->priv->db_lock);

    ret = add_repo_to_account (mgr->priv->db, server, username, info);
    if (ret < 0) {
        pthread_mutex_unlock (&mgr->priv->db_lock);
        g_free (account_key);
        repo_info_free (info);
        return -1;
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

out:
    g_free (account_key);
    return ret;
}

int
seaf_repo_manager_rename_repo_on_account (SeafRepoManager *mgr,
                                          const char *server,
                                          const char *username,
                                          const char *repo_id,
                                          const char *new_name)
{
    GHashTable *name_to_repo = NULL;
    RepoInfo *info;
    SeafRepo *repo;
    char *orig_repo_name;
    char *orig_display_name;
    char *display_name;
    char *account_key = NULL;
    int ret = 0;

    if (!seaf_repo_manager_account_exists (mgr, server, username)) {
        ret = -1;
        goto out;
    }

    account_key = g_strconcat (server, "_", username, NULL);

    display_name = seaf_repo_manager_get_repo_display_name (mgr, repo_id);

    pthread_rwlock_wrlock (&mgr->priv->account_lock);

    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);
    info = g_hash_table_lookup (name_to_repo, display_name);
    g_free (display_name);
    if (!info) {
        ret = -1;
        goto out;
    }

    g_hash_table_remove (name_to_repo, info->display_name);

    orig_repo_name = info->name;
    orig_display_name = info->display_name;
    info->name = g_strdup (new_name);
    info->display_name = find_unique_display_name (name_to_repo,
                                                   info->type,
                                                   new_name);

    g_hash_table_replace (name_to_repo, g_strdup (info->display_name), info);

    pthread_rwlock_unlock (&mgr->priv->account_lock);

    pthread_mutex_lock (&mgr->priv->db_lock);

    ret = update_repo_to_account (mgr->priv->db, server, username, info);
    if (ret < 0) {
        pthread_mutex_unlock (&mgr->priv->db_lock);
        g_free (info->name);
        g_free (info->display_name);
        info->name = orig_repo_name;
        info->display_name = orig_display_name;
        g_hash_table_replace (name_to_repo,
                              g_strdup(info->display_name),
                              info);
        goto out;
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    repo = seaf_repo_manager_get_repo (mgr, repo_id);
    if (repo)
        seaf_repo_set_worktree (repo, info->display_name);
    seaf_repo_unref (repo);

    g_free (orig_repo_name);
    g_free (orig_display_name);

out:
    g_free (account_key);
    return ret;
}

void
seaf_repo_manager_set_repo_info_head_commit (SeafRepoManager *mgr,
                                             const char *repo_id,
                                             const char *commit_id)
{
    RepoInfo *info;

    pthread_rwlock_wrlock (&mgr->priv->account_lock);

    info = g_hash_table_lookup (mgr->priv->repo_infos, repo_id);
    if (!info)
        goto out;

    g_free (info->head_commit_id);
    info->head_commit_id = g_strdup(commit_id);

out:
    pthread_rwlock_unlock (&mgr->priv->account_lock);
}

#define JOURNAL_FLUSH_TIMEOUT 5

void
seaf_repo_manager_flush_account_repo_journals (SeafRepoManager *mgr,
                                               const char *server,
                                               const char *user)
{
    GList *repo_ids, *ptr;
    char *repo_id;
    SeafRepo *repo;

    repo_ids = seaf_repo_manager_get_account_repo_ids (seaf->repo_mgr, server, user);
    for (ptr = repo_ids; ptr; ptr = ptr->next) {
        repo_id = ptr->data;
        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
        if (repo) {
            if (repo->journal)
                journal_flush (repo->journal, JOURNAL_FLUSH_TIMEOUT);
            seaf_repo_unref (repo);
        }
    }

    g_list_free_full (repo_ids, g_free);
}

static gboolean
get_account_space_cb (sqlite3_stmt *stmt, void *data)
{
    SeafAccountSpace *space = data;
    gint64 total, used;

    total = (gint64)sqlite3_column_int64 (stmt, 0);
    used = (gint64)sqlite3_column_int64 (stmt, 1);

    space->total = total;
    space->used = used;

    return FALSE;
}

#define DEFAULT_SPACE_USED (1000000000LL) /* 1GB */
#define DEFAULT_SPACE_TOTAL (100000000000000LL) /* 100TB */
#define MINIMAL_SPACE_TOTAL (10000000LL)        /* 10MB */
#define UNLIMITED_SPACE (-2LL)

SeafAccountSpace *
seaf_repo_manager_get_account_space (SeafRepoManager *mgr,
                                     const char *server,
                                     const char *username)
{
    char *sql;
    SeafAccountSpace *space = g_new0 (SeafAccountSpace, 1);

    sql = sqlite3_mprintf ("SELECT total, used FROM AccountSpace WHERE "
                           "server='%q' AND username='%q'",
                           server, username);

    pthread_mutex_lock (&mgr->priv->db_lock);

    if (sqlite_foreach_selected_row (mgr->priv->db, sql,
                                     get_account_space_cb, space) <= 0) {
        space->total = DEFAULT_SPACE_TOTAL;
        space->used = DEFAULT_SPACE_USED;
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    if (space->total == UNLIMITED_SPACE)
        space->total = (DEFAULT_SPACE_TOTAL > space->used ?
                        DEFAULT_SPACE_TOTAL : (10*space->used));
    else if (space->total == 0)
        /* If the user's quota is set to 0 on server, set it to a default quota so that
         * the user can still write to shared libraries. Otherwise the OS may prevent
         * writing data to the virtual drive.
         */
        space->total = DEFAULT_SPACE_TOTAL;

    sqlite3_free (sql);
    return space;
}

int
seaf_repo_manager_set_account_space (SeafRepoManager *mgr,
                                     const char *server, const char *username,
                                     gint64 total, gint64 used)
{
    char *sql;
    int ret = 0;

    sql = sqlite3_mprintf ("REPLACE INTO AccountSpace (server, username, total, used) "
                           "VALUES ('%q', '%q', %lld, %lld)",
                           server, username, total, used);

    pthread_mutex_lock (&mgr->priv->db_lock);

    ret = sqlite_query_exec (mgr->priv->db, sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    sqlite3_free (sql);

    return ret;
}

int
seaf_repo_manager_check_delete_repo (const char *repo_id, RepoType repo_type)
{
    SeafRepo *repo = NULL;
    int ret = 0;

    if (repo_type != REPO_TYPE_MINE) {
        seaf_warning ("rm repo: repo %s is not belong to the user.\n", repo_id);
        ret = -EACCES;
        goto out;
    }

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo || !repo->fs_ready) {
        ret = -ENOENT;
        goto out;
    }

    if (!repo_tree_is_empty_repo(repo->tree)) {
        ret = -ENOTEMPTY;
    }

out:
    seaf_repo_unref (repo);
    return ret;
}

char *
seaf_repo_manager_get_display_name_by_repo_name (SeafRepoManager *mgr,
                                                 const char *server,
                                                 const char *user,
                                                 const char *repo_name)
{
    GHashTable *name_to_repo = NULL;
    char *account_key = NULL;

    if (!seaf_repo_manager_account_exists (mgr, server, user)) {
        return NULL;

    }

    account_key = g_strconcat (server, "_", user, NULL);

    pthread_rwlock_rdlock (&mgr->priv->account_lock);
    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);
    pthread_rwlock_unlock (&mgr->priv->account_lock);

    g_free (account_key);

    return find_unique_display_name (name_to_repo,
                                     REPO_TYPE_MINE,
                                     repo_name);
}

static int
count_of_repo_id_in_account_repos (SeafRepoManager *mgr, const char *repo_id)
{
    char sql[256];
    gint64 ret;

    sqlite3_snprintf (sizeof(sql), sql,
                      "SELECT count(*) AS count FROM AccountRepos WHERE repo_id = '%q'",
                      repo_id);

    pthread_mutex_lock (&mgr->priv->db_lock);

    ret = sqlite_get_int64 (mgr->priv->db, sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    return ret;
}

char *
seaf_repo_manager_get_first_repo_token_from_account_repos (SeafRepoManager *mgr,
                                                           const char *server,
                                                           const char *username,
                                                           char **repo_token)
{
    GHashTable *name_to_repo;
    GHashTableIter iter;
    gpointer key, value;
    RepoInfo *info = NULL;
    char *repo_id = NULL;
    char *account_key = NULL;
    SeafRepo *repo;

    account_key = g_strconcat (server, "_", username, NULL);

    name_to_repo = g_hash_table_lookup (mgr->priv->repo_names, account_key);
    if (!name_to_repo) {
        goto out;
    }

    g_hash_table_iter_init (&iter, name_to_repo);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        info = value;
        repo = seaf_repo_manager_get_repo (mgr, info->id);
        if (!repo || repo->delete_pending || !repo->token) {
            seaf_repo_unref (repo);
            continue;
        }
        //if the repo appears twice in AccountRepos, it may be shared
        //to different users, skip it.
        if (count_of_repo_id_in_account_repos (mgr, repo->id) >= 2) {
            seaf_repo_unref (repo);
            continue;
        }
        repo_id = g_strdup (repo->id);
        *repo_token = g_strdup (repo->token);
        seaf_repo_unref (repo);
        break;
    }

out:
    g_free (account_key);
    return repo_id;
}

int
seaf_repo_manager_set_repo_passwd (SeafRepoManager *manager,
                                   SeafRepo *repo,
                                   const char *passwd)
{
    int ret;

    if (seafile_decrypt_repo_enc_key (repo->enc_version, passwd, repo->random_key,
                                      repo->salt,
                                      repo->enc_key, repo->enc_iv) < 0)
        return -1;

    pthread_mutex_lock (&manager->priv->db_lock);

    ret = save_repo_enc_info (manager, repo);

    pthread_mutex_unlock (&manager->priv->db_lock);

    if (ret != 0)
        return ret;

    repo->is_passwd_set = TRUE;

    return ret;
}

int
seaf_repo_manager_clear_enc_repo_passwd (const char *repo_id)
{
    SeafRepo *repo;
    sqlite3 *db = seaf->repo_mgr->priv->db;
    char sql[512];

    sqlite3_snprintf(sizeof(sql), sql,  "DELETE FROM RepoKeys WHERE repo_id = %Q",
                     repo_id);

    pthread_mutex_lock (&seaf->repo_mgr->priv->db_lock);
    if (sqlite_query_exec (db, sql) < 0) {
        pthread_mutex_unlock (&seaf->repo_mgr->priv->db_lock);
        return -1;
    }
    pthread_mutex_unlock (&seaf->repo_mgr->priv->db_lock);

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (repo) {
        repo->is_passwd_set = FALSE;
        memset (repo->enc_key, 0, 32);
        memset (repo->enc_iv, 0, 16);
    }

    seaf_repo_unref (repo);

    return 0;
}

json_t *
seaf_repo_manager_get_account_by_repo_id (SeafRepoManager *mgr, const char *repo_id)
{
    json_t *object = NULL;

    SeafRepo *repo = seaf_repo_manager_get_repo (mgr, repo_id);
    if (!repo) {
        return NULL;
    }

    object = json_object();
    json_object_set (object, "server", json_string(repo->server));
    json_object_set (object, "username", json_string(repo->user));

    seaf_repo_unref (repo);

    return object;
}

char *
seaf_repo_manager_get_mount_path (SeafRepoManager *mgr, SeafRepo *repo, const char *file_path)
{
    char *mount_path = NULL;
    GList *accounts = NULL;
    accounts = seaf_repo_manager_get_account_list (mgr);
    if (!accounts) {
        goto out;
    }

    if (g_list_length (accounts) <= 1) {
        mount_path = g_build_filename (seaf->mount_point, repo->repo_uname, file_path, NULL);
    } else {
        SeafAccount *account = seaf_repo_manager_get_account (mgr,
                                                              repo->server,
                                                              repo->user);
        if (account) {
            mount_path = g_build_filename (seaf->mount_point, account->name, repo->repo_uname, file_path, NULL);

        }
        seaf_account_free (account);
    }
out:
    if (accounts)
        g_list_free_full (accounts, (GDestroyNotify)seaf_account_free);
    return mount_path;
}

// Sync error related. These functions should belong to the sync-mgr module.
// But since we have to store the errors in repo database, we have to put the code here.
int
seaf_repo_manager_record_sync_error (SeafRepoManager *mgr,
                                     const char *repo_id,
                                     const char *repo_name,
                                     const char *path,
                                     int error_id)
{
    char *sql;
    int ret;

    // We only record network error in memory.
    if (sync_error_level(error_id) == SYNC_ERROR_LEVEL_NETWORK) {
        return 0;
    }

    // record empty string to database.
    if (!repo_id)
        repo_id = "";
    if (!repo_name)
        repo_name = "";
    if (!path)
        path = "";

    pthread_mutex_lock (&mgr->priv->db_lock);

    if (path != NULL)
        sql = sqlite3_mprintf ("DELETE FROM FileSyncError WHERE repo_id='%q' AND path='%q'",
                               repo_id, path);
    else
        sql = sqlite3_mprintf ("DELETE FROM FileSyncError WHERE repo_id='%q' AND path IS NULL",
                               repo_id);
    ret = sqlite_query_exec (mgr->priv->db, sql);
    sqlite3_free (sql);
    if (ret < 0)
        goto out;

    /* REPLACE INTO will update the primary key id automatically.
     * So new errors are always on top.
     */
    if (path != NULL)
        sql = sqlite3_mprintf ("INSERT INTO FileSyncError "
                               "(repo_id, repo_name, path, err_id, timestamp) "
                               "VALUES ('%q', '%q', '%q', %d, %lld)",
                               repo_id, repo_name, path, error_id, (gint64)time(NULL));
    else
        sql = sqlite3_mprintf ("INSERT INTO FileSyncError "
                               "(repo_id, repo_name, err_id, timestamp) "
                               "VALUES ('%q', '%q', %d, %lld)",
                               repo_id, repo_name, error_id, (gint64)time(NULL));
        
    ret = sqlite_query_exec (mgr->priv->db, sql);
    sqlite3_free (sql);

out:
    pthread_mutex_unlock (&mgr->priv->db_lock);
    return ret;
}

static gboolean
collect_file_sync_errors (sqlite3_stmt *stmt, void *data)
{
    json_t *array = data;
    const char *repo_id, *repo_name, *path;
    int id, err_id;
    gint64 timestamp;
    json_t *obj;

    id = sqlite3_column_int (stmt, 0);
    repo_id = (const char *)sqlite3_column_text (stmt, 1);
    repo_name = (const char *)sqlite3_column_text (stmt, 2);
    path = (const char *)sqlite3_column_text (stmt, 3);
    err_id = sqlite3_column_int (stmt, 4);
    timestamp = sqlite3_column_int64 (stmt, 5);

    if (sync_error_level (err_id) == SYNC_ERROR_LEVEL_NETWORK) {
        return TRUE;
    }

    obj = json_object ();
    if (repo_id)
        json_object_set_new (obj, "repo_id", json_string(repo_id));
    if (repo_name)
        json_object_set_new (obj, "repo_name", json_string(repo_name));
    if (path)
        json_object_set_new (obj, "path", json_string(path));
    json_object_set_new (obj, "err_id", json_integer(err_id));
    json_object_set_new (obj, "timestamp", json_integer(timestamp));
    json_array_append_new (array, obj);

    return TRUE;
}

json_t *
seaf_repo_manager_list_sync_errors (SeafRepoManager *mgr, int offset, int limit)
{
    json_t *array;
    char *sql;

    array = json_array ();
    sql = sqlite3_mprintf ("SELECT id, repo_id, repo_name, path, err_id, timestamp FROM "
                         "FileSyncError ORDER BY id DESC LIMIT %d OFFSET %d",
                         limit, offset);

    pthread_mutex_lock (&mgr->priv->db_lock);
    sqlite_foreach_selected_row (mgr->priv->db, sql,
                               collect_file_sync_errors, array);
    pthread_mutex_unlock (&mgr->priv->db_lock);

    sqlite3_free (sql);
    return array;
}
