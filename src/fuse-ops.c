#include "common.h"

#if defined __linux__ || defined __APPLE__

#define FUSE_USE_VERSION  26
#include <fuse.h>

#ifdef __APPLE__
#include <libproc.h>
#endif

#include "seafile-session.h"
#include "fuse-ops.h"

#define DEBUG_FLAG SEAFILE_DEBUG_FS
#include "log.h"

struct _FusePathComps {
    RepoType repo_type;
    RepoInfo *repo_info;
    char *repo_path;
    char *root_path;
    // account_info will not be set only if there are multiple accounts and it is the root directory.
    AccountInfo *account_info;
    gboolean multi_account;
};
typedef struct _FusePathComps FusePathComps;

/*
 * Input path and return result can be:
 * 1. root dir      -> return 0, repo_type == 0
 * 2. category dir  -> return 0, repo_type == TYPE, repo_info == NULL, repo_path == NULL
 * 3. new category dir -> return -ENOENT, repo_type == 0
 * 4. repo dir      -> return 0, repo_type == TYPE, repo_info != NULL, repo_path == NULL
 * 5. new repo dir && repo_path
 *                  -> return -ENOENT, repo_type == TYPE, repo_info == NULL, repo_path == NULL
 * 6. new repo dir && !repo_path
 *                  -> return 0, repo_type == TYPE, repo_info == NULL, repo_path == NULL, root_path == new repo dir
 * 7. repo path     -> return 0, repo_type == TYPE, repo_info != NULL, repo_path != NULL
 *
 * Note that case 2 and 6 are quite similar. The only difference is whether root_path is set.
 * If root_path is set, repo_info and repo_path are always NULL.
 */
static int
parse_fuse_path_single_account (SeafAccount *account, const char *path, FusePathComps *path_comps)
{
    char *path_nfc = NULL;
    char **tokens;
    int n;
    char *repo_name, *display_name;
    RepoInfo *info;
    int ret = 0;

    path_comps->repo_info = NULL;
    path_comps->repo_path = NULL;

    path_nfc = g_utf8_normalize (path, -1, G_NORMALIZE_NFC);
    if (!path_nfc)
        return -ENOENT;

    if (*path_nfc == '/')
        path = path_nfc + 1;
    else
        path = path_nfc;

    tokens = g_strsplit (path, "/", 3);
    n = g_strv_length (tokens);

    switch (n) {
    case 0:
        break;
    case 1:
        path_comps->repo_type = repo_type_from_string (tokens[0]);
        if (path_comps->repo_type == REPO_TYPE_UNKNOWN) {
            ret = -ENOENT;
            goto out;
        }
        break;
    case 2:
        path_comps->repo_type = repo_type_from_string (tokens[0]);
        if (path_comps->repo_type == REPO_TYPE_UNKNOWN) {
            ret = -ENOENT;
            goto out;
        }

        repo_name = tokens[1];
        display_name = g_strconcat (tokens[0], "/", tokens[1], NULL);
        info = seaf_repo_manager_get_repo_info_by_display_name (seaf->repo_mgr, account->server, account->username, display_name);
        g_free (display_name);
        if (!info) {
            path_comps->root_path = g_strdup (repo_name);
            goto out;
        }

        path_comps->repo_info = info;
        break;
    case 3:
        path_comps->repo_type = repo_type_from_string (tokens[0]);
        if (path_comps->repo_type == REPO_TYPE_UNKNOWN) {
            ret = -ENOENT;
            goto out;
        }

        repo_name = tokens[1];
        display_name = g_strconcat (tokens[0], "/", tokens[1], NULL);
        info = seaf_repo_manager_get_repo_info_by_display_name (seaf->repo_mgr, account->server, account->username, display_name);
        g_free (display_name);
        if (!info) {
            ret = -ENOENT;
            goto out;
        }

        path_comps->repo_info = info;

        path_comps->repo_path = g_strdup(tokens[2]);
        break;
    }

out:
    g_free (path_nfc);
    g_strfreev (tokens);
    return ret;
}

/*
 * Input path and return result can be:
 * 1. root dir      -> return 0, repo_type == 0, account_info == NULL
 * 2. accounts dir      -> return 0, repo_type == 0, account_info != NULL
 * 3. category dir  -> return 0, repo_type == TYPE, repo_info == NULL, repo_path == NULL
 * 4. new category dir -> return -ENOENT, repo_type == 0
 * 5. repo dir      -> return 0, repo_type == TYPE, repo_info != NULL, repo_path == NULL
 * 6. new repo dir && repo_path
 *                  -> return -ENOENT, repo_type == TYPE, repo_info == NULL, repo_path == NULL
 * 7. new repo dir && !repo_path
 *                  -> return 0, repo_type == TYPE, repo_info == NULL, repo_path == NULL, root_path == new repo dir
 * 8. repo path     -> return 0, repo_type == TYPE, repo_info != NULL, repo_path != NULL
 *
 * Note that case 3 and 7 are quite similar. The only difference is whether root_path is set.
 * If root_path is set, repo_info and repo_path are always NULL.
 */
static int
parse_fuse_path_multi_account (const char *path, FusePathComps *path_comps)
{
    char *path_nfc = NULL;
    char **tokens;
    int n;
    char *repo_name, *display_name;
    RepoInfo *info;
    int ret = 0;
    AccountInfo *account_info = NULL;

    path_comps->repo_info = NULL;
    path_comps->repo_path = NULL;

    path_nfc = g_utf8_normalize (path, -1, G_NORMALIZE_NFC);
    if (!path_nfc)
        return -ENOENT;

    if (*path_nfc == '/')
        path = path_nfc + 1;
    else
        path = path_nfc;

    tokens = g_strsplit (path, "/", 4);
    n = g_strv_length (tokens);

    switch (n) {
    case 0:
        break;
    case 1:
        account_info = seaf_repo_manager_get_account_info_by_name (seaf->repo_mgr, tokens[0]);
        if (!account_info) {
            ret = -ENOENT;
            goto out;
        }
        path_comps->account_info = account_info;
        break;
    case 2:
        account_info = seaf_repo_manager_get_account_info_by_name (seaf->repo_mgr, tokens[0]);
        if (!account_info) {
            ret = -ENOENT;
            goto out;
        }
        path_comps->repo_type = repo_type_from_string (tokens[1]);
        if (path_comps->repo_type == REPO_TYPE_UNKNOWN) {
            ret = -ENOENT;
            goto out;
        }
        path_comps->account_info = account_info;
        break;
    case 3:
        account_info = seaf_repo_manager_get_account_info_by_name (seaf->repo_mgr, tokens[0]);
        if (!account_info) {
            ret = -ENOENT;
            goto out;
        }
        path_comps->repo_type = repo_type_from_string (tokens[1]);
        if (path_comps->repo_type == REPO_TYPE_UNKNOWN) {
            ret = -ENOENT;
            goto out;
        }
        path_comps->account_info = account_info;

        repo_name = tokens[2];
        display_name = g_strconcat (tokens[1], "/", tokens[2], NULL);
        info = seaf_repo_manager_get_repo_info_by_display_name (seaf->repo_mgr, account_info->server, account_info->username, display_name);
        g_free (display_name);
        if (!info) {
            path_comps->root_path = g_strdup (repo_name);
            goto out;
        }

        path_comps->repo_info = info;
        break;
    case 4:
        account_info = seaf_repo_manager_get_account_info_by_name (seaf->repo_mgr, tokens[0]);
        if (!account_info) {
            ret = -ENOENT;
            goto out;
        }
        path_comps->repo_type = repo_type_from_string (tokens[1]);
        if (path_comps->repo_type == REPO_TYPE_UNKNOWN) {
            ret = -ENOENT;
            goto out;
        }

        repo_name = tokens[2];
        display_name = g_strconcat (tokens[1], "/", tokens[2], NULL);
        info = seaf_repo_manager_get_repo_info_by_display_name (seaf->repo_mgr, account_info->server, account_info->username, display_name);
        g_free (display_name);
        if (!info) {
            ret = -ENOENT;
            goto out;
        }

        path_comps->account_info = account_info;
        path_comps->repo_info = info;

        path_comps->repo_path = g_strdup(tokens[3]);
        break;
    }

out:
    if (ret != 0 && account_info) {
        account_info_free (account_info);
    }
    g_free (path_nfc);
    g_strfreev (tokens);
    return ret;
}

static int
parse_fuse_path (const char *path, FusePathComps *path_comps)
{
    int ret = 0;
    GList *accounts = NULL;
    SeafAccount *account;
    accounts = seaf_repo_manager_get_account_list (seaf->repo_mgr);
    if (!accounts) {
        ret = -ENOENT;
        goto out;
    }

    if (g_list_length (accounts) <= 1) {
        path_comps->multi_account = FALSE;
        account = accounts->data;
        ret = parse_fuse_path_single_account (account, path, path_comps);
        if (ret == 0) {
            AccountInfo *account_info = g_new0 (AccountInfo, 1);
            account_info->server = g_strdup (account->server);
            account_info->username = g_strdup (account->username);
            path_comps->account_info = account_info;
        }
    } else {
        path_comps->multi_account = TRUE;
        ret = parse_fuse_path_multi_account (path, path_comps);
    }

out:
    if (accounts)
        g_list_free_full (accounts, (GDestroyNotify)seaf_account_free);
    return ret;
}

static void
path_comps_free (FusePathComps *comps)
{
    if (!comps)
        return;
    if (comps->root_path) {
        g_free (comps->root_path);
    } else {
        repo_info_free (comps->repo_info);
        g_free (comps->repo_path);
    }
    if (comps->account_info)
        account_info_free (comps->account_info);
}

static void
notify_fs_op_error (const char *type, const char *path)
{
    json_t *msg = json_object();
    json_object_set_string_member (msg, "type", type);
    json_object_set_string_member (msg, "path", path);
    mq_mgr_push_msg (seaf->mq_mgr, SEADRIVE_EVENT_CHAN, msg);
}

static gint64
get_category_dir_mtime (const char *server, const char *user, RepoType type)
{
    GList *infos, *ptr;
    RepoInfo *info;
    gint64 mtime = 0;

    infos = seaf_repo_manager_get_account_repos (seaf->repo_mgr, server, user);
    for (ptr = infos; ptr; ptr = ptr->next) {
        info = ptr->data;
        if (type != REPO_TYPE_UNKNOWN && info->type != type)
            continue;
        if (info->mtime > mtime)
            mtime = info->mtime;
    }

    g_list_free_full (infos, (GDestroyNotify)repo_info_free);
    return mtime;
}

int
seadrive_fuse_getattr(const char *path, struct stat *stbuf)
{
    FusePathComps comps;
    int ret = 0;
    uid_t uid;
    gid_t gid;
    SeafRepo *repo = NULL;
    RepoTreeStat st;

    seaf_debug ("getattr %s called.\n", path);

    if (!seaf->started)
        return -ENOENT;

    memset (stbuf, 0, sizeof(struct stat));
    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    /* Set file/folder owner/group to the process's euid and egid. */
    uid = geteuid();
    gid = getegid();

    if (comps.root_path) {
        g_free (comps.root_path);
        return -ENOENT;
    }

    if (!comps.repo_type) {
        /* Root or account directory */
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        stbuf->st_size = 4096;
        stbuf->st_uid = uid;
        stbuf->st_gid = gid;
        if (comps.multi_account && comps.account_info)
            // Multiple account directory
            stbuf->st_mtime = get_category_dir_mtime (comps.account_info->server, comps.account_info->username, comps.repo_type);
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Category directory */
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        stbuf->st_size = 4096;
        stbuf->st_uid = uid;
        stbuf->st_gid = gid;
        stbuf->st_mtime = get_category_dir_mtime (comps.account_info->server, comps.account_info->username, comps.repo_type);
    } else if (!comps.repo_path) {
        /* Repo directory */
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        stbuf->st_size = 4096;
        stbuf->st_mtime = comps.repo_info->mtime;
        stbuf->st_uid = uid;
        stbuf->st_gid = gid;
    } else {
        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, comps.repo_info->id);
        if (!repo || !repo->fs_ready) {
            ret = -ENOENT;
            goto out;
        }

        int rc = repo_tree_stat_path (repo->tree, comps.repo_path, &st);
        if (rc < 0) {
            ret = rc;
            goto out;
        }

        gboolean is_writable = (seaf_repo_manager_is_path_writable(seaf->repo_mgr,
                                                                   comps.repo_info->id,
                                                                   comps.repo_path) &&
                                !seaf_filelock_manager_is_file_locked (seaf->filelock_mgr,
                                                                       comps.repo_info->id,
                                                                       comps.repo_path));
        if (S_ISDIR(st.mode)) {
            stbuf->st_size = 4096;
            if (is_writable)
                stbuf->st_mode = S_IFDIR | 0755;
            else
                stbuf->st_mode = S_IFDIR | 0555;
        } else {
            stbuf->st_size = st.size;
            if (is_writable)
                stbuf->st_mode = S_IFREG | 0644;
            else
                stbuf->st_mode = S_IFREG | 0444;
        }
        stbuf->st_mtime = st.mtime;
        stbuf->st_nlink = 1;
        stbuf->st_uid = uid;
        stbuf->st_gid = gid;
    }

out:
    seaf_repo_unref (repo);
    path_comps_free (&comps);
    return ret;
}

static int
readdir_root_accounts (void *buf, fuse_fill_dir_t filler)
{
    GList *accounts = NULL, *ptr;
    SeafAccount *account;
    accounts = seaf_repo_manager_get_account_list (seaf->repo_mgr);
    if (!accounts) {
        return 0;
    }
    for (ptr = accounts; ptr; ptr = ptr->next) {
        account = ptr->data;
        filler (buf, account->name, NULL, 0);
    }
    g_list_free_full (accounts, (GDestroyNotify)seaf_account_free);

    return 0;
}

static int
readdir_root (AccountInfo *account, void *buf, fuse_fill_dir_t filler)
{
    GList *types = repo_type_string_list ();
    GList *ptr;
    char *type_str;
#ifdef __APPLE__
    char *dname_nfd;
#endif

    for (ptr = types; ptr; ptr = ptr->next) {
        type_str = ptr->data;
#ifdef __APPLE__
        dname_nfd = g_utf8_normalize (type_str, -1, G_NORMALIZE_NFD);
        filler (buf, dname_nfd, NULL, 0);
        g_free (dname_nfd);
#else
        filler (buf, type_str, NULL, 0);
#endif
    }

    g_list_free_full (types, g_free);

    return 0;
}

static int
readdir_category (AccountInfo *account, RepoType type, void *buf, fuse_fill_dir_t filler)
{
    GList *repos = seaf_repo_manager_get_account_repos (seaf->repo_mgr, account->server, account->username);
    GList *ptr;
    RepoInfo *info;
    SeafRepo *repo;
    char *dname;
#ifdef __APPLE__
    char *dname_nfd;
#endif

    for (ptr = repos; ptr; ptr = ptr->next) {
        info = ptr->data;

        if (info->type != type)
            continue;

        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, info->id);
        if (repo && repo->encrypted && !repo->is_passwd_set) {
            seaf_repo_unref (repo);
            continue;
        }

        dname = g_path_get_basename (info->display_name);
#ifdef __APPLE__
        dname_nfd = g_utf8_normalize (dname, -1, G_NORMALIZE_NFD);
        filler (buf, dname_nfd, NULL, 0);
        g_free (dname_nfd);
#else
        filler (buf, dname, NULL, 0);
#endif

        g_free (dname);
        seaf_repo_unref (repo);
    }

    g_list_free_full (repos, (GDestroyNotify)repo_info_free);
    return 0;
}

static int
readdir_repo (const char *repo_id, const char *path, void *buf, fuse_fill_dir_t filler)
{
    SeafRepo *repo = NULL;
    GHashTable *dirents;
    GHashTableIter iter;
    gpointer key, value;
    char *dname;
    int ret = 0;
#ifdef __APPLE__
    char *dname_nfd;
#endif

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo || !repo->fs_ready) {
        ret = -ENOENT;
        goto out;
    }

    if (repo->encrypted && !repo->is_passwd_set) {
        ret = -ENOENT;
        goto out;
    }

    dirents = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);

    int rc = repo_tree_readdir (repo->tree, path, dirents);
    if (rc < 0) {
        ret = rc;
        goto out;
    }

    g_hash_table_iter_init (&iter, dirents);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        dname = (char *)key;
        if (seaf_repo_manager_is_path_invisible (seaf->repo_mgr, repo_id, dname)) {
            continue;
        }
        filler (buf, dname, NULL, 0);
    }
    g_hash_table_destroy (dirents);

out:
    seaf_repo_unref (repo);
    return ret;
}

int
seadrive_fuse_readdir(const char *path, void *buf,
                      fuse_fill_dir_t filler, off_t offset,
                      struct fuse_file_info *info)
{
    FusePathComps comps;
    int ret = 0;

    seaf_debug ("readdir %s called.\n", path);

    if (!seaf->started)
        return 0;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (comps.multi_account) {
        if (comps.root_path) {
            ret = -ENOENT;
        } else if (!comps.account_info) {
            /* Root directory */
            ret = readdir_root_accounts (buf, filler);
        } else if (!comps.repo_type) {
            /* Account directory */
            ret = readdir_root (comps.account_info, buf, filler);
        } else if (!comps.repo_info && !comps.repo_path) {
            /* Category directory */
            seaf->last_access_fs_time = (gint64)time(NULL);
            ret = readdir_category (comps.account_info, comps.repo_type, buf, filler);
        } else if (!comps.repo_path) {
            /* Repo directory */
            seaf->last_access_fs_time = (gint64)time(NULL);
            ret = readdir_repo (comps.repo_info->id, "", buf, filler);
        } else {
            seaf->last_access_fs_time = (gint64)time(NULL);
            ret = readdir_repo (comps.repo_info->id, comps.repo_path, buf, filler);
        }
    } else {
        if (comps.root_path) {
            ret = -ENOENT;
        } else if (!comps.repo_type) {
            /* Root directory */
            ret = readdir_root (comps.account_info, buf, filler);
        } else if (!comps.repo_info && !comps.repo_path) {
            /* Category directory */
            seaf->last_access_fs_time = (gint64)time(NULL);
            ret = readdir_category (comps.account_info, comps.repo_type, buf, filler);
        } else if (!comps.repo_path) {
            /* Repo directory */
            seaf->last_access_fs_time = (gint64)time(NULL);
            ret = readdir_repo (comps.repo_info->id, "", buf, filler);
        } else {
            seaf->last_access_fs_time = (gint64)time(NULL);
            ret = readdir_repo (comps.repo_info->id, comps.repo_path, buf, filler);
        }
    }

    path_comps_free (&comps);
    return ret;
}

static int
repo_mknod (const char *repo_id, const char *path, mode_t mode)
{
    SeafRepo *repo = NULL;
    gint64 mtime;
    JournalOp *op;
    int ret = 0;
    char *office_path;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo || !repo->fs_ready) {
        ret = -ENOENT;
        goto out;
    }

    if (!seaf_repo_manager_is_path_writable (seaf->repo_mgr, repo_id, path)) {
        ret = -EACCES;
        goto out;
    }

    mtime = (gint64)time(NULL);

    int rc = repo_tree_create_file (repo->tree, path, EMPTY_SHA1, (guint32)mode, mtime, 0);
    if (rc < 0) {
        ret = rc;
        goto out;
    }

    op = journal_op_new (OP_TYPE_CREATE_FILE, path, NULL, 0, mtime, mode);
    if (journal_append_op (repo->journal, op) < 0) {
        seaf_warning ("Failed to append operation to journal of repo %s.\n",
                      repo_id);
        ret = -ENOMEM;
        journal_op_free (op);
        goto out;
    }

    office_path = NULL;
    if (repo_tree_is_office_lock_file (repo->tree, path, &office_path))
        seaf_sync_manager_lock_file_on_server (seaf->sync_mgr, repo->server, repo->user, repo->id, office_path);
    g_free (office_path);

out:
    seaf_repo_unref (repo);
    return ret;
}

int seadrive_fuse_mknod (const char *path, mode_t mode, dev_t dev)
{
    FusePathComps comps;
    int ret = 0;

    seaf_debug ("mknod %s called. mode = %o.\n", path, mode);

    if (!seaf->started)
        return 0;

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        if (!comps.repo_type)
            return -EACCES;
        else
            return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EACCES;
    } else if (!comps.repo_type) {
        ret = -EACCES;
    } else if (comps.root_path) {
        /* Don't allow creating files in category. */
        ret = -EACCES;
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Category directory */
        ret = -EACCES;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = -EACCES;
    } else {
        if (!S_ISREG(mode)) {
            ret = -EINVAL;
            goto out;
        }
        seaf->last_access_fs_time = (gint64)time(NULL);
        ret = repo_mknod (comps.repo_info->id, comps.repo_path, mode);
    }

out:
    path_comps_free (&comps);
    return ret;
}

static int
repo_mkdir (const char *repo_id, const char *path)
{
    SeafRepo *repo = NULL;
    gint64 mtime;
    JournalOp *op;
    int ret = 0;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo || !repo->fs_ready) {
        ret = -ENOENT;
        goto out;
    }

    if (!seaf_repo_manager_is_path_writable (seaf->repo_mgr, repo_id, path)) {
        ret = -EACCES;
        goto out;
    }

    mtime = (gint64)time(NULL);

    int rc = repo_tree_mkdir (repo->tree, path, mtime);
    if (rc < 0) {
        ret = rc;
        goto out;
    }

    op = journal_op_new (OP_TYPE_MKDIR, path, NULL, 0, mtime, 0);
    if (journal_append_op (repo->journal, op) < 0) {
        seaf_warning ("Failed to append operation to journal of repo %s.\n",
                      repo_id);
        ret = -ENOMEM;
        journal_op_free (op);
    }

    file_cache_mgr_mkdir (seaf->file_cache_mgr, repo_id, path);

out:
    seaf_repo_unref (repo);
    return ret;
}

#define TRASH_PREFIX ".Trash"

int seadrive_fuse_mkdir (const char *path, mode_t mode)
{
    FusePathComps comps;
    int ret = 0;

    seaf_debug ("mkdir %s called.\n", path);

    if (!seaf->started)
        return 0;

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        if (!comps.repo_type)
            return -EACCES;
        else
            return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EACCES;
    }else if (!comps.repo_type) {
        /* Root directory */
        ret = -EACCES;
    } else if (comps.root_path) {
        if (strncmp (comps.root_path, TRASH_PREFIX, strlen(TRASH_PREFIX)) == 0) {
            // Don't create trash dir now, or will lose delete event
            ret = -EACCES;
        } else if (comps.repo_type != REPO_TYPE_MINE) {
            /* Don't allow creating repo under shared or group categories. */
            ret = -EACCES;
        } else {
            // Make root dir to create repo
            seaf->last_access_fs_time = (gint64)time(NULL);
            ret = seaf_sync_manager_create_repo (seaf->sync_mgr,
                                                 comps.account_info->server,
                                                 comps.account_info->username,
                                                 comps.root_path);
            if (ret < 0)
                ret = -EIO;
        }
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Category directory */
        ret = -EEXIST;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = -EEXIST;
    } else {
        seaf->last_access_fs_time = (gint64)time(NULL);
        ret = repo_mkdir (comps.repo_info->id, comps.repo_path);
    }

    path_comps_free (&comps);
    return ret;
}

static int
repo_unlink (const char *repo_id, const char *path)
{
    SeafRepo *repo = NULL;
    JournalOp *op;
    int ret = 0;
    char *office_path;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo || !repo->fs_ready) {
        ret = -ENOENT;
        goto out;
    }

    if (!seaf_repo_manager_is_path_writable (seaf->repo_mgr, repo_id, path)) {
        ret = -EACCES;
        goto out;
    }

    if (seaf_filelock_manager_is_file_locked (seaf->filelock_mgr,
                                              repo_id, path)) {
        ret = -EACCES;
        goto out;
    }

    office_path = NULL;
    if (repo_tree_is_office_lock_file (repo->tree, path, &office_path))
        seaf_sync_manager_unlock_file_on_server (seaf->sync_mgr, repo->server, repo->user, repo->id, office_path);
    g_free (office_path);

    int rc = repo_tree_unlink (repo->tree, path);
    if (rc < 0) {
        ret = rc;
        goto out;
    }

    op = journal_op_new (OP_TYPE_DELETE_FILE, path, NULL, 0, 0, 0);
    if (journal_append_op (repo->journal, op) < 0) {
        seaf_warning ("Failed to append operation to journal of repo %s.\n",
                      repo_id);
        ret = -ENOMEM;
        journal_op_free (op);
        goto out;
    }

    file_cache_mgr_unlink (seaf->file_cache_mgr, repo_id, path);

out:
    seaf_repo_unref (repo);
    return ret;
}

int seadrive_fuse_unlink (const char *path)
{
    FusePathComps comps;
    int ret = 0;

    seaf_debug ("unlink %s called.\n", path);

    if (!seaf->started)
        return 0;

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EACCES;
    } else if (!comps.repo_type) {
        /* Root directory */
        ret = -EACCES;
    } else if (comps.root_path) {
        ret = -EACCES;
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Category directory */
        ret = -EACCES;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = -EACCES;
    } else {
        seaf->last_access_fs_time = (gint64)time(NULL);
        ret = repo_unlink (comps.repo_info->id, comps.repo_path);
    }

    if (ret == 0) {
        seaf_sync_manager_delete_active_path (seaf->sync_mgr,
                                              comps.repo_info->id,
                                              comps.repo_path);
    }

    path_comps_free (&comps);
    return ret;
}

static int
repo_rmdir (const char *repo_id, const char *path)
{
    SeafRepo *repo = NULL;
    JournalOp *op;
    int ret = 0;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo || !repo->fs_ready) {
        ret = -ENOENT;
        goto out;
    }

    if (!seaf_repo_manager_is_path_writable (seaf->repo_mgr, repo_id, path)) {
        ret = -EACCES;
        goto out;
    }

    int rc = repo_tree_rmdir (repo->tree, path);
    if (rc < 0) {
        ret = rc;
        goto out;
    }

    op = journal_op_new (OP_TYPE_RMDIR, path, NULL, 0, 0, 0);
    if (journal_append_op (repo->journal, op) < 0) {
        seaf_warning ("Failed to append operation to journal of repo %s.\n",
                      repo_id);
        ret = -ENOMEM;
        journal_op_free (op);
        goto out;
    }

    file_cache_mgr_rmdir (seaf->file_cache_mgr, repo_id, path);

out:
    seaf_repo_unref (repo);
    return ret;
}

int seadrive_fuse_rmdir (const char *path)
{
    FusePathComps comps;
    int ret = 0;

    seaf_debug ("rmdir %s called.\n", path);

    if (!seaf->started)
        return 0;

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EACCES;
    } else if (!comps.repo_type) {
        ret = -EACCES;
    } else if (comps.root_path) {
        ret = -EACCES;
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Root directory */
        ret = -EACCES;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = seaf_repo_manager_check_delete_repo (comps.repo_info->id, comps.repo_type);
        if (ret < 0)
            goto out;

        ret = seaf_sync_manager_delete_repo (seaf->sync_mgr,
                                            comps.account_info->server,
                                            comps.account_info->username,
                                            comps.repo_info->id);
        if (ret < 0)
            ret = -EIO;
        goto out;
    } else {
        seaf->last_access_fs_time = (gint64)time(NULL);
        ret = repo_rmdir (comps.repo_info->id, comps.repo_path);
    }

    if (ret == 0) {
        seaf_sync_manager_delete_active_path (seaf->sync_mgr,
                                              comps.repo_info->id,
                                              comps.repo_path);
    }

out:
    path_comps_free (&comps);
    return ret;
}

static int
repo_rename (const char *repo_id, const char *oldpath, const char *newpath)
{
    SeafRepo *repo = NULL;
    JournalOp *op;
    int ret = 0;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo || !repo->fs_ready) {
        ret = -ENOENT;
        goto out;
    }

    int rc = repo_tree_rename (repo->tree, oldpath, newpath, TRUE);
    if (rc < 0) {
        ret = rc;
        goto out;
    }

    op = journal_op_new (OP_TYPE_RENAME, oldpath, newpath, 0, 0, 0);
    if (journal_append_op (repo->journal, op) < 0) {
        seaf_warning ("Failed to append operation to journal of repo %s.\n",
                      repo_id);
        ret = -ENOMEM;
        journal_op_free (op);
        goto out;
    }

    file_cache_mgr_rename (seaf->file_cache_mgr, repo_id, oldpath, repo_id, newpath);

out:
    seaf_repo_unref (repo);
    return ret;
}

typedef struct CrossRepoRenameData {
    char *server;
    char *user;
    char *repo_id1;
    char *oldpath;
    char *repo_id2;
    char *newpath;
    gboolean is_file;
} CrossRepoRenameData;

static void
cross_repo_rename_data_free (CrossRepoRenameData *data)
{
    if (!data)
        return;
    g_free (data->server);
    g_free (data->user);
    g_free (data->repo_id1);
    g_free (data->repo_id2);
    g_free (data->oldpath);
    g_free (data->newpath);
    g_free (data);
}

static void
notify_cross_repo_move (const char *src_repo_id, const char *src_path,
                        const char *dst_repo_id, const char *dst_path,
                        gboolean is_start, gboolean failed)
{
    json_t *msg = json_object ();
    char *src_repo_name = NULL, *dst_repo_name = NULL;
    char *srcpath = NULL, *dstpath = NULL;

    if (is_start)
        json_object_set_string_member (msg, "type", "cross-repo-move.start");
    else if (!failed)
        json_object_set_string_member (msg, "type", "cross-repo-move.done");
    else
        json_object_set_string_member (msg, "type", "cross-repo-move.error");

    src_repo_name = seaf_repo_manager_get_repo_display_name (seaf->repo_mgr,
                                                             src_repo_id);
    if (!src_repo_name) {
        goto out;
    }
    srcpath = g_strconcat (src_repo_name, "/", src_path, NULL);

    dst_repo_name = seaf_repo_manager_get_repo_display_name (seaf->repo_mgr,
                                                             dst_repo_id);
    if (!dst_repo_name) {
        goto out;
    }
    dstpath = g_strconcat (dst_repo_name, "/", dst_path, NULL);

    json_object_set_string_member (msg, "srcpath", srcpath);
    json_object_set_string_member (msg, "dstpath", dstpath);

    mq_mgr_push_msg (seaf->mq_mgr, SEADRIVE_NOTIFY_CHAN, msg);

out:
    g_free (src_repo_name);
    g_free (dst_repo_name);
    g_free (srcpath);
    g_free (dstpath);
}

static void *
cross_repo_rename_thread (void *vdata)
{
    CrossRepoRenameData *data = vdata;
    SeafAccount *account = seaf_repo_manager_get_account (seaf->repo_mgr,
                                                          data->server,
                                                          data->user);
    SeafRepo *repo1 = NULL, *repo2 = NULL;

    if (!account) {
        goto out;
    }

    notify_cross_repo_move (data->repo_id1, data->oldpath,
                            data->repo_id2, data->newpath,
                            TRUE, FALSE);

    if (http_tx_manager_api_move_file (seaf->http_tx_mgr,
                                       account->server,
                                       account->token,
                                       data->repo_id1,
                                       data->oldpath,
                                       data->repo_id2,
                                       data->newpath,
                                       data->is_file) < 0) {
        seaf_warning ("Failed to move %s/%s to %s/%s.\n",
                      data->repo_id1, data->oldpath, data->repo_id2, data->newpath);
        notify_cross_repo_move (data->repo_id1, data->oldpath,
                                data->repo_id2, data->newpath,
                                FALSE, TRUE);
        goto out;
    }

    /* Move files/folders in cache. */
    file_cache_mgr_rename (seaf->file_cache_mgr, data->repo_id1, data->oldpath,
                           data->repo_id2, data->newpath);

    /* Trigger repo sync for both repos. */
    repo1 = seaf_repo_manager_get_repo (seaf->repo_mgr, data->repo_id1);
    if (repo1)
        repo1->force_sync_pending = TRUE;
    seaf_repo_unref (repo1);
    repo2 = seaf_repo_manager_get_repo (seaf->repo_mgr, data->repo_id2);
    if (repo2)
        repo2->force_sync_pending = TRUE;
    seaf_repo_unref (repo2);

    notify_cross_repo_move (data->repo_id1, data->oldpath,
                            data->repo_id2, data->newpath,
                            FALSE, FALSE);

out:
    seaf_account_free (account);
    cross_repo_rename_data_free (data);
    return NULL;
}

static int
cross_repo_rename (const char *repo_id1, const char *oldpath,
                   const char *repo_id2, const char *newpath)
{
    SeafRepo *repo1 = NULL, *repo2 = NULL;
    RepoTreeStat st;
    CrossRepoRenameData *data;
    pthread_t tid;
    int rc;
    int ret = 0;

    repo1 = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id1);
    if (!repo1) {
        ret = -ENOENT;
        goto out;
    }

    repo2 = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id2);
    if (!repo2) {
        ret = -ENOENT;
        goto out;
    }

    if (repo_tree_stat_path (repo1->tree, oldpath, &st) < 0) {
        ret = -ENOENT;
        goto out;
    }

    /* TODO: support replacing file/dir by rename. */
    if (repo_tree_stat_path (repo2->tree, newpath, &st) == 0) {
        ret = -EIO;
        goto out;
    }

    data = g_new0 (CrossRepoRenameData, 1);
    data->server = g_strdup (repo1->server);
    data->user = g_strdup (repo1->user);
    data->repo_id1 = g_strdup(repo_id1);
    data->oldpath = g_strdup(oldpath);
    data->repo_id2 = g_strdup(repo_id2);
    data->newpath = g_strdup(newpath);
    data->is_file = S_ISREG(st.mode);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    rc = pthread_create (&tid, &attr, cross_repo_rename_thread, data);
    if (rc != 0) {
        cross_repo_rename_data_free (data);
        ret = -ENOMEM;
    }

out:
    seaf_repo_unref (repo1);
    seaf_repo_unref (repo2);
    return ret;
}

int seadrive_fuse_rename (const char *oldpath, const char *newpath)
{
    FusePathComps comps1, comps2;
    int ret = 0;

    seaf_debug ("rename %s to %s called.\n", oldpath, newpath);

    if (!seaf->started)
        return 0;

    memset (&comps1, 0, sizeof(comps1));
    memset (&comps2, 0, sizeof(comps2));

    if (parse_fuse_path (oldpath, &comps1) < 0) {
        ret = -ENOENT;
        goto out;
    }

    if (parse_fuse_path (newpath, &comps2) < 0) {
        if (!comps2.repo_type)
            ret = -EACCES;
        else
            ret = -ENOENT;
        goto out;
    }

    if (!comps1.account_info || !comps2.account_info) {
        ret = -EACCES;
        goto out;
    }

    if (g_strcmp0 (comps1.account_info->server, comps2.account_info->server) !=0 ||
        g_strcmp0 (comps1.account_info->username, comps2.account_info->username) != 0) {
        ret = -EACCES;
        goto out;
    }

    if (!comps1.repo_type || !comps2.repo_type) {
        ret = -EACCES;
        goto out;
    }

    /* Category level. */

    if (comps1.root_path) {
        ret = -ENOENT;
        goto out;
    }

    if (comps2.root_path) {
        /* Don't allow moving a folder under a repo to category level (become a new repo). */
        if (comps1.repo_path) {
            ret = -EACCES;
            goto out;
        }

        /* Don't allow renaming category dir to a repo dir. */
        if (!comps1.repo_info) {
            ret = -EACCES;
            goto out;
        }

        /* Don't allow moving repos from one category to another;
         * Don't allow renaming repos shared to me or from group.
         */
        if ((comps1.repo_type != comps2.repo_type) || comps1.repo_type != REPO_TYPE_MINE) {
            ret = -EACCES;
            goto out;
        }

        seaf->last_access_fs_time = (gint64)time(NULL);

        // Rename repo
        ret = seaf_sync_manager_rename_repo (seaf->sync_mgr,
                                             comps1.account_info->server,
                                             comps1.account_info->username,
                                             comps1.repo_info->id,
                                             comps2.root_path);
        if (ret < 0)
            ret = -EIO;

        goto out;
    }

    if (!comps1.repo_info && !comps1.repo_path) {
        /* Category directory */
        ret = -EACCES;
        goto out;
    }

    if (!comps2.repo_info && !comps2.repo_path) {
        /* Category directory */
        ret = -EACCES;
        goto out;
    }

    /* Repo level. */

    if (!comps1.repo_path && !comps2.repo_path) {
        /* Renaming one repo to another. */
        ret = -EEXIST;
        goto out;
    }
    if ((comps1.repo_path && !comps2.repo_path) ||
        (!comps1.repo_path && comps2.repo_path)) {
        /* Moving repo to sub-folder, or vice versa. */
        ret = -EACCES;
        goto out;
    }

    if (!seaf_repo_manager_is_path_writable (seaf->repo_mgr,
                                             comps1.repo_info->id,
                                             comps1.repo_path) ||
        !seaf_repo_manager_is_path_writable (seaf->repo_mgr,
                                             comps2.repo_info->id,
                                             comps2.repo_path)) {
        ret = -EACCES;
        goto out;
    }

    if (seaf_filelock_manager_is_file_locked (seaf->filelock_mgr,
                                              comps1.repo_info->id,
                                              comps1.repo_path) ||
        seaf_filelock_manager_is_file_locked (seaf->filelock_mgr,
                                              comps2.repo_info->id,
                                              comps2.repo_path)) {
        ret = -EACCES;
        goto out;
    }

    seaf->last_access_fs_time = (gint64)time(NULL);

    if (strcmp (comps1.repo_info->id, comps2.repo_info->id) != 0) {
        ret = cross_repo_rename (comps1.repo_info->id, comps1.repo_path,
                                 comps2.repo_info->id, comps2.repo_path);
    } else {
        ret = repo_rename (comps1.repo_info->id,
                           comps1.repo_path,
                           comps2.repo_path);

        if (ret == 0) {
            seaf_sync_manager_delete_active_path (seaf->sync_mgr,
                                                  comps1.repo_info->id,
                                                  comps1.repo_path);
        }
    }

out:
    path_comps_free (&comps1);
    path_comps_free (&comps2);
    return ret;
}

int
seadrive_fuse_open(const char *path, struct fuse_file_info *info)
{
    FusePathComps comps;
    CachedFileHandle *handle;
    int ret = 0;

    seaf_debug ("open %s called.\n", path);

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_type) {
        ret = -EINVAL;
        goto out;
    } else if (comps.root_path) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Root directory */
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = -EINVAL;
        goto out;
    }

    if (((info->flags & O_WRONLY) || (info->flags & O_RDWR)) &&
        (!seaf_repo_manager_is_path_writable (seaf->repo_mgr,
                                              comps.repo_info->id,
                                              comps.repo_path) ||
         seaf_filelock_manager_is_file_locked (seaf->filelock_mgr,
                                               comps.repo_info->id,
                                               comps.repo_path))) {
        ret = -EACCES;
        goto out;
    }

    seaf->last_access_fs_time = (gint64)time(NULL);

    handle = file_cache_mgr_open (seaf->file_cache_mgr,
                                  comps.repo_info->id, comps.repo_path,
                                  info->flags);
    if (!handle) {
        ret = -EIO;
        goto out;
    }

    info->fh = (uint64_t)handle;

out:
    path_comps_free (&comps);
    return ret;
}

int
seadrive_fuse_read(const char *path, char *buf, size_t size,
                   off_t offset, struct fuse_file_info *info)
{
    seaf_debug ("read %s called. size = %"G_GINT64_FORMAT", offset = %"G_GINT64_FORMAT"\n",
                path, (gint64)size, (gint64)offset);

    if (!info->fh) {
        return -EIO;
    }

    CachedFileHandle *handle = (CachedFileHandle *)info->fh;

    seaf->last_access_fs_time = (gint64)time(NULL);

    char *repo_id = NULL, *file_path = NULL;
    file_cache_mgr_get_file_info_from_handle (seaf->file_cache_mgr, handle,
                                              &repo_id, &file_path);

    int ret = 0;
    ret = file_cache_mgr_read_by_path (seaf->file_cache_mgr,
                                       repo_id, file_path,
                                       buf, size, offset);
    if (ret < 0) {
        seaf_warning ("Failed to read %s/%s: %s.\n", repo_id, file_path, strerror(-ret));
    }

    g_free (repo_id);
    g_free (file_path);

    return ret;
}

int
seadrive_fuse_write(const char *path, const char *buf, size_t size,
                    off_t offset, struct fuse_file_info *info)
{
    int ret = 0;
    CachedFileHandle *handle;
    FileCacheStat st;
    char *repo_id = NULL, *file_path = NULL;
    SeafRepo *repo = NULL;
    JournalOp *op;
    RepoTreeStat tree_st;

    seaf_debug ("write %s called. size = %"G_GINT64_FORMAT", offset = %"G_GINT64_FORMAT"\n",
                path, size, offset);

    if (!info->fh) {
        return -EIO;
    }

    handle = (CachedFileHandle *)info->fh;

    seaf->last_access_fs_time = (gint64)time(NULL);

    ret = file_cache_mgr_write (seaf->file_cache_mgr,
                                handle,
                                buf, size, offset);
    if (ret < 0)
        return ret;

    if (file_cache_mgr_stat_handle (seaf->file_cache_mgr, handle, &st) < 0) {
        return -EIO;
    }

    file_cache_mgr_get_file_info_from_handle (seaf->file_cache_mgr,
                                              handle,
                                              &repo_id, &file_path);

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to get repo %s.\n", repo_id);
        ret = -EIO;
        goto out;
    }

    repo_tree_set_file_mtime (repo->tree, file_path, st.mtime);
    repo_tree_set_file_size (repo->tree, file_path, st.size);

    if (repo_tree_stat_path (repo->tree, file_path, &tree_st) < 0) {
        ret = -EIO;
        goto out;
    }

    op = journal_op_new (OP_TYPE_UPDATE_FILE, file_path, NULL,
                         st.size, st.mtime, tree_st.mode);

    if (journal_append_op (repo->journal, op) < 0) {
        seaf_warning ("Failed to append op to journal of repo %s.\n", repo->id);
        ret = -ENOMEM;
        journal_op_free (op);
    }

out:
    g_free (repo_id);
    g_free (file_path);
    seaf_repo_unref (repo);
    return ret;
}

int seadrive_fuse_release (const char *path, struct fuse_file_info *info)
{
    CachedFileHandle *handle = (CachedFileHandle *)info->fh;

    seaf_debug ("release %s called.\n", path);

    if (!handle)
        return 0;

    file_cache_mgr_close_file_handle (handle);

    return 0;
}

int
seadrive_fuse_truncate (const char *path, off_t length)
{
    FusePathComps comps;
    char *repo_id, *file_path;
    FileCacheStat st;
    SeafRepo *repo = NULL;
    JournalOp *op;
    RepoTreeStat tree_st;
    int ret = 0;

    seaf_debug ("truncate %s called, len = %"G_GINT64_FORMAT".\n", path, length);

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_type) {
        ret = -EINVAL;
        goto out;
    } else if (comps.root_path) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Root directory */
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = -EINVAL;
        goto out;
    }

    if (!seaf_repo_manager_is_path_writable (seaf->repo_mgr,
                                             comps.repo_info->id,
                                             comps.repo_path) ||
        seaf_filelock_manager_is_file_locked (seaf->filelock_mgr,
                                              comps.repo_info->id,
                                              comps.repo_path)) {
        ret = -EACCES;
        goto out;
    }

    seaf->last_access_fs_time = (gint64)time(NULL);

    repo_id = comps.repo_info->id;
    file_path = comps.repo_path;

    gboolean not_cached = FALSE;
    ret = file_cache_mgr_truncate (seaf->file_cache_mgr,
                                   repo_id, file_path,
                                   length, &not_cached);
    if (ret < 0) {
        goto out;
    }

    if (not_cached) {
        st.mtime = (gint64)time(NULL);
        st.size = 0;
    } else {
        if (file_cache_mgr_stat (seaf->file_cache_mgr, repo_id, file_path, &st) < 0) {
            ret = -EIO;
            goto out;
        }
    }

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to get repo %s.\n", repo_id);
        ret = -EIO;
        goto out;
    }

    repo_tree_set_file_mtime (repo->tree, file_path, st.mtime);
    repo_tree_set_file_size (repo->tree, file_path, st.size);

    if (repo_tree_stat_path (repo->tree, file_path, &tree_st) < 0) {
        ret = -EIO;
        goto out;
    }

    op = journal_op_new (OP_TYPE_UPDATE_FILE, file_path, NULL,
                         st.size, st.mtime, tree_st.mode);

    if (journal_append_op (repo->journal, op) < 0) {
        seaf_warning ("Failed to append op to journal of repo %s.\n", repo->id);
        ret = -ENOMEM;
        journal_op_free (op);
    }

out:
    path_comps_free (&comps);
    seaf_repo_unref (repo);
    return ret;
}

int
seadrive_fuse_statfs (const char *path, struct statvfs *buf)
{
    SeafAccountSpace *space = NULL;
    gint64 total, used;

    GList *accounts = NULL, *ptr;
    SeafAccount *account;
    accounts = seaf_repo_manager_get_account_list (seaf->repo_mgr);
    if (!accounts) {
        total = 0;
        used = 0;
    }

    for (ptr = accounts; ptr; ptr = ptr->next) {
        account = ptr->data;
        space = seaf_repo_manager_get_account_space (seaf->repo_mgr,
                                                     account->server,
                                                     account->username);
        total += space->total;
        used += space->used;
    }
    if (accounts)
        g_list_free_full (accounts, (GDestroyNotify)seaf_account_free);

    buf->f_namemax = 255;
    buf->f_bsize = 4096;
    /*
     * df seems to use f_bsize instead of f_frsize, so make them
     * the same
     */
    buf->f_frsize = buf->f_bsize;
    buf->f_blocks = total / buf->f_frsize;
    buf->f_bfree =  buf->f_bavail = (total - used) / buf->f_frsize;
    buf->f_files = buf->f_ffree = 1000000000;

    g_free (space);
    return 0;
}

int
seadrive_fuse_chmod (const char *path, mode_t mode)
{
    FusePathComps comps;

    seaf_debug ("chmod %s called. mode = %o.\n", path, mode);

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    return 0;
}

int
seadrive_fuse_utimens (const char *path, const struct timespec tv[2])
{
    RepoTreeStat tree_st;
    char *repo_id = NULL, *file_path = NULL;
    SeafRepo *repo = NULL;
    JournalOp *op = NULL;
    int ret = 0;
    FusePathComps comps;
    time_t atime = tv[0].tv_sec;
    time_t mtime = tv[1].tv_sec;

    seaf_debug ("utimens %s called. mtime = %"G_GINT64_FORMAT"\n", path, mtime);

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (!comps.account_info) {
        goto out;
    }

    if (comps.root_path != NULL ||
        !comps.repo_type ||
        !comps.repo_info ||
        !comps.repo_path)
        goto out;

    repo_id = comps.repo_info->id;
    file_path = comps.repo_path;

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        seaf_warning ("Failed to get repo %s.\n", repo_id);
        ret = -ENOENT;
        goto out;
    }

    ret = file_cache_mgr_utimen (seaf->file_cache_mgr, repo_id, file_path, mtime, atime);
    if (ret < 0)
        goto out;

    if (repo_tree_stat_path (repo->tree, file_path, &tree_st) < 0) {
        ret = -ENOENT;
        goto out;
    }

    if (tree_st.mtime == mtime)
        goto out;

    repo_tree_set_file_mtime (repo->tree, file_path, mtime);

    op = journal_op_new (OP_TYPE_UPDATE_ATTR, file_path, NULL,
                         tree_st.size, mtime, tree_st.mode);

    if (journal_append_op (repo->journal, op) < 0) {
        seaf_warning ("Failed to append op to journal of repo %s.\n", repo->id);
        ret = -ENOMEM;
        journal_op_free (op);
    }

out:
    path_comps_free (&comps);
    seaf_repo_unref (repo);
    return ret;
}

int
seadrive_fuse_symlink (const char *from, const char *to)
{
    FusePathComps comps_from, comps_to;

    seaf_debug ("symlink %s %s called.\n", from, to);

    memset (&comps_from, 0, sizeof(comps_from));
    memset (&comps_to, 0, sizeof(comps_to));

    if (parse_fuse_path (from, &comps_from) < 0) {
        return -ENOENT;
    }
    if (parse_fuse_path (to, &comps_to) < 0) {
        return -ENOENT;
    }

    return 0;
}

#ifdef ENABLE_XATTR
int
seadrive_fuse_setxattr (const char *path, const char *name, const char *value,
                        size_t size, int flags)
{
    FusePathComps comps;
    int ret = 0;

    seaf_debug ("setxattr: %s %s\n", path, name);

    // Manual setting of extended properties related to file locking is only supported. 
    if (g_strcmp0("name", "user.seafile-status") != 0){
        return 0;
    }

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_type) {
        ret = -EINVAL;
        goto out;
    } else if (comps.root_path) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Root directory */
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = -EINVAL;
        goto out;
    }

    if (file_cache_mgr_setxattr (seaf->file_cache_mgr, comps.repo_info->id, comps.repo_path, name, value, size) < 0) {
        ret = -ENOENT;
        goto out;
    }

out:
    path_comps_free (&comps);
    return ret;
}

int
seadrive_fuse_getxattr (const char *path, const char *name, char *value, size_t size)
{
    FusePathComps comps;
    int ret = 0;

    seaf_debug ("getxattr: %s %s\n", path, name);

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_type) {
        ret = -EINVAL;
        goto out;
    } else if (comps.root_path) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Root directory */
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = -EINVAL;
        goto out;
    }

    ret = file_cache_mgr_getxattr (seaf->file_cache_mgr, comps.repo_info->id, comps.repo_path, name, value, size);
    if (ret < 0) {
        ret = -ENOENT;
        goto out;
    }

out:
    path_comps_free (&comps);
    return ret;
}

int
seadrive_fuse_listxattr (const char *path, char *list, size_t size)
{
    FusePathComps comps;
    int ret = 0;

    seaf_debug ("listxattr: %s\n", path);

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_type) {
        ret = -EINVAL;
        goto out;
    } else if (comps.root_path) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Root directory */
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = -EINVAL;
        goto out;
    }

    if (!file_cache_mgr_is_file_cached (seaf->file_cache_mgr, comps.repo_info->id, comps.repo_path)) {
        ret = -ENOENT;
        goto out;
    }

    ret = file_cache_mgr_listxattr (seaf->file_cache_mgr, comps.repo_info->id, comps.repo_path, list, size);
    if (ret < 0) {
        ret = -ENOENT;
        goto out;
    }

out:
    path_comps_free (&comps);
    return ret;
}

int
seadrive_fuse_removexattr (const char *path, const char *name)
{
    FusePathComps comps;
    int ret = 0;

    seaf_debug ("removexattr: %s %s\n", path, name);

    memset (&comps, 0, sizeof(comps));

    if (parse_fuse_path (path, &comps) < 0) {
        return -ENOENT;
    }

    if (!comps.account_info) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_type) {
        ret = -EINVAL;
        goto out;
    } else if (comps.root_path) {
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_info && !comps.repo_path) {
        /* Root directory */
        ret = -EINVAL;
        goto out;
    } else if (!comps.repo_path) {
        /* Repo directory */
        ret = -EINVAL;
        goto out;
    }

    if (!file_cache_mgr_is_file_cached (seaf->file_cache_mgr, comps.repo_info->id, comps.repo_path)) {
        ret = -ENOENT;
        goto out;
    }

    if (file_cache_mgr_removexattr (seaf->file_cache_mgr, comps.repo_info->id, comps.repo_path, name) < 0) {
        ret = -ENOENT;
        goto out;
    }

out:
    path_comps_free (&comps);
    return ret;
}
#endif


#endif // __linux__
