/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef SEAF_REPO_MGR_H
#define SEAF_REPO_MGR_H

#include "common.h"

#include <pthread.h>

#include "commit-mgr.h"
#include "branch-mgr.h"
#include "repo-tree.h"
#include "journal-mgr.h"

#define REPO_PROP_TOKEN       "token"
#define REPO_PROP_IS_READONLY "is-readonly"

struct _SeafRepoManager;
typedef struct _SeafRepo SeafRepo;

/* The caller can use the properties directly. But the caller should
 * always write on repos via the API. 
 */
struct _SeafRepo {
    struct _SeafRepoManager *manager;

    gchar       id[37];
    gchar      *name;
    gchar      *desc;
    gchar      *category;       /* not used yet */
    gboolean    encrypted;
    gboolean    is_passwd_set;
    int         enc_version;
    gchar       magic[65];       /* hash(repo_id + passwd), key stretched. */
    gchar       random_key[97];  /* key length is 48 after encryption */
    gchar       salt[65];
    gint64 last_modify;

    SeafBranch *head;
    gchar root_id[41];

    gboolean    is_corrupted;
    gboolean    delete_pending;
    gboolean    remove_cache;

    int         last_sync_time;

    /* Last time check locked files. */
    int         last_check_locked_time;
    gboolean    checking_locked_files;

    unsigned char enc_key[32];   /* 256-bit encryption key */
    unsigned char enc_iv[16];

    gchar      *token;          /* token for access this repo on server */

    gboolean      is_readonly;

    char *repo_uname;
    char *worktree;

    unsigned int  quota_full_notified : 1;
    unsigned int  access_denied_notified : 1;

    int version;

    gint refcnt;

    /* Is this repo ready for file system operations. */
    gboolean fs_ready;

    /* In-memory representation of the repo directory hierarchy. */
    struct _RepoTree *tree;

    /* Journal for this repo */
    Journal *journal;

    /* Marks the repo into "partial-commit" mode. Once in this mode,
     * we'll create commits for about every 100MB data. Commits are created
     * and uploaded until all operations in the journal are processd.
     */
    gboolean partial_commit_mode;

    gboolean force_sync_pending;
};


gboolean is_repo_id_valid (const char *id);

SeafRepo* 
seaf_repo_new (const char *id, const char *name, const char *desc);

void
seaf_repo_free (SeafRepo *repo);

void
seaf_repo_ref (SeafRepo *repo);

void
seaf_repo_unref (SeafRepo *repo);

int
seaf_repo_set_head (SeafRepo *repo, SeafBranch *branch);

SeafCommit *
seaf_repo_get_head_commit (const char *repo_id);

void
seaf_repo_set_readonly (SeafRepo *repo);

void
seaf_repo_unset_readonly (SeafRepo *repo);

void
seaf_repo_set_worktree (SeafRepo *repo, const char *repo_uname);

/* Update repo name, desc, magic etc from commit.
 */
void
seaf_repo_from_commit (SeafRepo *repo, SeafCommit *commit);

/* Update repo-related fields to commit. 
 */
void
seaf_repo_to_commit (SeafRepo *repo, SeafCommit *commit);

int
seaf_repo_load_fs (SeafRepo *repo, gboolean after_clone);

GList *
seaf_repo_diff (SeafRepo *repo, const char *old, const char *new, int fold_dir_diff, char **error);

typedef struct _SeafRepoManager SeafRepoManager;
typedef struct _SeafRepoManagerPriv SeafRepoManagerPriv;

struct _SeafRepoManager {
    struct _SeafileSession *seaf;

    SeafRepoManagerPriv *priv;
};

SeafRepoManager* 
seaf_repo_manager_new (struct _SeafileSession *seaf);

int
seaf_repo_manager_init (SeafRepoManager *mgr);

int
seaf_repo_manager_start (SeafRepoManager *mgr);

int
seaf_repo_manager_add_repo (SeafRepoManager *mgr, SeafRepo *repo);

int
seaf_repo_manager_mark_repo_deleted (SeafRepoManager *mgr, SeafRepo *repo, gboolean remove_cache);

int
seaf_repo_manager_del_repo (SeafRepoManager *mgr, SeafRepo *repo);

void
seaf_repo_manager_move_repo_store (SeafRepoManager *mgr,
                                   const char *type,
                                   const char *repo_id);

void
seaf_repo_manager_remove_repo_ondisk (SeafRepoManager *mgr, const char *repo_id, gboolean remove_cache);

SeafRepo* 
seaf_repo_manager_get_repo (SeafRepoManager *manager, const gchar *id);

gboolean
seaf_repo_manager_repo_exists (SeafRepoManager *manager, const gchar *id);

GList* 
seaf_repo_manager_get_repo_list (SeafRepoManager *mgr, int start, int limit);

GList *
seaf_repo_manager_get_enc_repo_list (SeafRepoManager *mgr, int start, int limit);

gboolean
seaf_repo_manager_is_repo_delete_pending (SeafRepoManager *manager, const char *id);

int
seaf_repo_manager_set_repo_token (SeafRepoManager *manager, 
                                  SeafRepo *repo,
                                  const char *token);

int
seaf_repo_manager_remove_repo_token (SeafRepoManager *manager,
                                     SeafRepo *repo);

void
seaf_repo_manager_rename_repo (SeafRepoManager *manager,
                               const char *repo_id,
                               const char *new_name);

int
seaf_repo_manager_branch_repo_unmap (SeafRepoManager *manager, SeafBranch *branch);

int
seaf_repo_manager_set_repo_property (SeafRepoManager *manager,
                                     const char *repo_id,
                                     const char *key,
                                     const char *value);

char *
seaf_repo_manager_get_repo_property (SeafRepoManager *manager,
                                     const char *repo_id,
                                     const char *key);

/* Locked files. */

#define LOCKED_OP_UPDATE "update"
#define LOCKED_OP_DELETE "delete"

typedef struct _LockedFile {
    char *operation;
    gint64 old_mtime;
    char file_id[41];
} LockedFile;

typedef struct _LockedFileSet {
    SeafRepoManager *mgr;
    char repo_id[37];
    GHashTable *locked_files;
} LockedFileSet;

LockedFileSet *
seaf_repo_manager_get_locked_file_set (SeafRepoManager *mgr, const char *repo_id);

void
locked_file_set_free (LockedFileSet *fset);

int
locked_file_set_add_update (LockedFileSet *fset,
                            const char *path,
                            const char *operation,
                            gint64 old_mtime,
                            const char *file_id);

int
locked_file_set_remove (LockedFileSet *fset, const char *path, gboolean db_only);

LockedFile *
locked_file_set_lookup (LockedFileSet *fset, const char *path);

/* Folder Permissions. */

typedef enum FolderPermType {
    FOLDER_PERM_TYPE_USER = 0,
    FOLDER_PERM_TYPE_GROUP,
} FolderPermType;

typedef struct _FolderPerm {
    char *path;
    char *permission;
} FolderPerm;

void
folder_perm_free (FolderPerm *perm);

int
seaf_repo_manager_update_folder_perms (SeafRepoManager *mgr,
                                       const char *repo_id,
                                       FolderPermType type,
                                       GList *folder_perms);

int
seaf_repo_manager_update_folder_perm_timestamp (SeafRepoManager *mgr,
                                                const char *repo_id,
                                                gint64 timestamp);

gint64
seaf_repo_manager_get_folder_perm_timestamp (SeafRepoManager *mgr,
                                             const char *repo_id);

gboolean
seaf_repo_manager_is_path_writable (SeafRepoManager *mgr,
                                    const char *repo_id,
                                    const char *path);

/* Account repo list cache management. */

struct _SeafAccount {
    char *server;
    char *username;
    char *token;
    char *fileserver_addr;
    gboolean is_pro;
    char *unique_id;            /* server + username */
    gboolean repo_list_fetched;
    gboolean all_repos_loaded;
};
typedef struct _SeafAccount SeafAccount;

void seaf_account_free (SeafAccount *account);

int
seaf_repo_manager_switch_current_account (SeafRepoManager *mgr,
                                          const char *server,
                                          const char *username,
                                          const char *token,
                                          gboolean is_pro);

SeafAccount *
seaf_repo_manager_get_current_account (SeafRepoManager *mgr);

gboolean
seaf_repo_manager_current_account_is_pro (SeafRepoManager *mgr);

int
seaf_repo_manager_update_current_account (SeafRepoManager *mgr,
                                          const char *server,
                                          const char *username,
                                          const char *token);

int
seaf_repo_manager_delete_account (SeafRepoManager *mgr,
                                  const char *server,
                                  const char *username,
                                  gboolean remove_cache);

typedef enum _RepoType {
    REPO_TYPE_UNKNOWN = 0,
    REPO_TYPE_MINE,
    REPO_TYPE_SHARED,
    REPO_TYPE_GROUP,
    REPO_TYPE_PUBLIC,
    N_REPO_TYPE,
} RepoType;

struct _RepoInfo {
    char id[37];
    char *head_commit_id;    /* in-memory only. */
    gboolean is_corrupted;      /* in-memory only. */
    gboolean is_readonly;
    char *name;
    /* Unique name to used in file system. */
    char *display_name;
    RepoType type;
    gint64 mtime;
    gboolean perm_unsyncable;
};
typedef struct _RepoInfo RepoInfo;

RepoInfo *
repo_info_new (const char *id, const char *head_commit_id,
               const char *name, gint64 mtime, gboolean is_readonly);

RepoInfo *
repo_info_copy (RepoInfo *info);

void
repo_info_free (RepoInfo *info);

RepoType
repo_type_from_string (const char *type_str);

GList *
repo_type_string_list ();

GList *
seaf_repo_manager_get_current_repos (SeafRepoManager *mgr);

GList *
seaf_repo_manager_get_current_repo_ids (SeafRepoManager *mgr);

RepoInfo *
seaf_repo_manager_get_repo_info_by_name (SeafRepoManager *mgr,
                                         const char *name);

char *
seaf_repo_manager_get_repo_id_by_name (SeafRepoManager *mgr,
                                       const char *name);

char *
seaf_repo_manager_get_repo_display_name (SeafRepoManager *mgr,
                                         const char *id);

int
seaf_repo_manager_set_if_current_repo_unsyncable (SeafRepoManager *mgr,
                                                  const char *repo_id,
                                                  gboolean perm_unsyncable);

void
seaf_repo_manager_remove_current_repo (SeafRepoManager *mgr,
                                       const char *repo_id);

int
seaf_repo_manager_update_current_repos (SeafRepoManager *mgr,
                                        GHashTable *server_repos,
                                        GList **added,
                                        GList **removed);

void
seaf_repo_manager_remove_current_repo (SeafRepoManager *mgr,
                                       const char *repo_id);

void
seaf_repo_manager_set_repo_info_head_commit (SeafRepoManager *mgr,
                                             const char *repo_id,
                                             const char *commit_id);

int
seaf_repo_manager_add_repo_to_current_account (SeafRepoManager *mgr,
                                               const char *server,
                                               const char *username,
                                               SeafRepo *repo);

int
seaf_repo_manager_rename_repo_on_current_account (SeafRepoManager *mgr,
                                                  const char *server,
                                                  const char *username,
                                                  const char *repo_id,
                                                  const char *new_name);

void
seaf_repo_manager_flush_current_repo_journals (SeafRepoManager *mgr);

void
seaf_repo_manager_set_current_repo_list_fetched (SeafRepoManager *mgr);

void
seaf_repo_manager_set_current_account_all_repos_loaded (SeafRepoManager *mgr);

struct _SeafAccountSpace {
    gint64 total;
    gint64 used;
};
typedef struct _SeafAccountSpace SeafAccountSpace;

SeafAccountSpace *
seaf_repo_manager_get_account_space (SeafRepoManager *mgr,
                                     const char *server,
                                     const char *username);

int
seaf_repo_manager_set_account_space (SeafRepoManager *mgr,
                                     const char *server, const char *username,
                                     gint64 total, gint64 used);
int
seaf_repo_manager_check_delete_repo (const char *repo_id, RepoType repo_type);

int
seaf_repo_manager_create_placeholders (SeafRepo* repo);

char *
seaf_repo_manager_get_display_name_by_repo_name (SeafRepoManager *mgr,
                                                 const char *repo_name);

void
seaf_repo_manager_create_dir_placeholder_recursive (const char *repo_id, const char *dir_id, const char *fullpath);

gboolean
seaf_repo_manager_is_deleted_repo (SeafRepoManager *mgr, const char *display_name);

void
seaf_repo_manager_del_deleted_repo (SeafRepoManager *mgr, const char *display_name);

int
seaf_repo_manager_set_repo_passwd (SeafRepoManager *manager,
                                   SeafRepo *repo,
                                   const char *passwd);

int
seaf_repo_manager_clear_enc_repo_passwd (const char *repo_id);

char *
seaf_repo_manager_get_first_repo_token_from_curr_repos (SeafRepoManager *mgr, char **repo_token);

#endif
