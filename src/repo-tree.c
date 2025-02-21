#include "common.h"

#include <sys/stat.h>
#include <pthread.h>

#include "repo-tree.h"
#include "log.h"
#include "utils.h"

#include "seafile-session.h"

struct _RepoTreeDir;
typedef struct _RepoTreeDir RepoTreeDir;

struct _RepoTreeDirent;
typedef struct _RepoTreeDirent RepoTreeDirent;

struct _RepoTree {
    char repo_id[37];
    RepoTreeDir *root;
    pthread_rwlock_t lock;
};

struct _RepoTreeDir {
    char id[41];
    GHashTable *dirents;
};

struct _RepoTreeDirent {
    char id[41];
    char *name;
    guint32 mode;
    gint64 mtime;
    gint64 size;
    RepoTreeDir *subdir;
};

static RepoTreeDir *
repo_tree_dir_new (const char *id);
static void
repo_tree_dir_free (RepoTreeDir *dir);

static RepoTreeDirent *
repo_tree_dirent_new (const char *id,
                      const char *name,
                      guint32 mode,
                      gint64 mtime,
                      gint64 size,
                      RepoTreeDir *subdir)
{
    RepoTreeDirent *dirent = g_new0 (RepoTreeDirent, 1);

    memcpy (dirent->id, id, 40);
    dirent->name = g_strdup(name);
    dirent->mode = mode;
    dirent->mtime = mtime;
    dirent->size = size;

    if (S_ISDIR(mode)) {
        if (!subdir)
            subdir = repo_tree_dir_new (EMPTY_SHA1);
        dirent->subdir = subdir;
    }

    return dirent;
}

static void
repo_tree_dirent_free (RepoTreeDirent *dirent)
{
    if (!dirent)
        return;
    g_free (dirent->name);
    repo_tree_dir_free (dirent->subdir);
    g_free (dirent);
}

static RepoTreeDir *
repo_tree_dir_new (const char *id)
{
    RepoTreeDir *dir = g_new0 (RepoTreeDir, 1);
    memcpy (dir->id, id, 40);
    dir->dirents = g_hash_table_new_full (g_str_hash, g_str_equal,
                                          NULL, (GDestroyNotify)repo_tree_dirent_free);
    return dir;
}

static void
repo_tree_dir_free (RepoTreeDir *dir)
{
    if (!dir)
        return;
    g_hash_table_destroy (dir->dirents);
    g_free (dir);
}

static gboolean
repo_tree_dir_is_empty (RepoTreeDir *dir)
{
    return (g_hash_table_size(dir->dirents) == 0);
}

RepoTree *
repo_tree_new (const char *repo_id)
{
    RepoTree *tree = NULL;

    tree = g_new0 (RepoTree, 1);
    memcpy (tree->repo_id, repo_id, 36);
    pthread_rwlock_init (&tree->lock, NULL);

    return tree;
}

void
repo_tree_free (RepoTree *tree)
{
    if (!tree)
        return;
    repo_tree_dir_free (tree->root);
    pthread_rwlock_destroy (&tree->lock);
    g_free (tree);
}

static RepoTreeDir *
load_dir_recursive (const char *repo_id, const char *dir_id);

int
repo_tree_load_commit (RepoTree *tree, const char *commit_id)
{
    SeafCommit *commit;
    RepoTreeDir *root;
    int ret = 0;

    commit = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                             tree->repo_id, 1,
                                             commit_id);
    if (!commit) {
        seaf_warning ("Failed to get commit %s from repo %s.\n",
                      commit_id, tree->repo_id);
        return -1;
    }

    root = load_dir_recursive (tree->repo_id, commit->root_id);
    if (!root) {
        ret = -1;
        goto out;
    }

    pthread_rwlock_wrlock (&tree->lock);

    repo_tree_dir_free (tree->root);
    tree->root = root;

    pthread_rwlock_unlock (&tree->lock);

out:
    seaf_commit_unref (commit);
    return ret;
}

static RepoTreeDir *
load_dir_recursive (const char *repo_id, const char *dir_id)
{
    SeafDir *seafdir;
    GList *ptr;
    SeafDirent *seafdirent;
    RepoTreeDirent *dirent;
    RepoTreeDir *subdir;
    RepoTreeDir *dir = NULL;

    seafdir = seaf_fs_manager_get_seafdir (seaf->fs_mgr, repo_id, 1, dir_id);
    if (!seafdir) {
        seaf_warning ("Failed to get dir %s from repo %s.\n", dir_id, repo_id);
        return NULL;
    }

    dir = repo_tree_dir_new (dir_id);

    gboolean error = FALSE;
    for (ptr = seafdir->entries; ptr; ptr = ptr->next) {
        seafdirent = (SeafDirent *)ptr->data;

        if (seaf_sync_manager_ignored_on_checkout (seafdirent->name, NULL))
            continue;

        if (S_ISDIR(seafdirent->mode)) {
            subdir = load_dir_recursive (repo_id, seafdirent->id);
            if (!subdir) {
                seaf_warning ("Failed to get dir %s from repo %s.\n",
                              seafdirent->id, repo_id);
                error = TRUE;
                break;
            }
        } else {
            subdir = NULL;
        }

        dirent = repo_tree_dirent_new (seafdirent->id, seafdirent->name,
                                       seafdirent->mode, seafdirent->mtime,
                                       seafdirent->size, subdir);

        g_hash_table_insert (dir->dirents, dirent->name, dirent);
    }

    if (error) {
        repo_tree_dir_free (dir);
        dir = NULL;
    }

    seaf_dir_free (seafdir);
    return dir;
}

void
repo_tree_clear (RepoTree *tree)
{
    pthread_rwlock_wrlock (&tree->lock);

    repo_tree_dir_free (tree->root);
    tree->root = NULL;

    pthread_rwlock_unlock (&tree->lock);
}

gboolean
repo_tree_is_loaded (RepoTree *tree)
{
    gboolean ret = FALSE;
    pthread_rwlock_rdlock (&tree->lock);
    ret = (tree->root != NULL);
    pthread_rwlock_unlock (&tree->lock);
    return ret;
}

static RepoTreeDirent *
resolve_path (RepoTree *tree, const char *path)
{
    char **comps = NULL;
    char *comp;
    guint i, n;
    RepoTreeDir *dir;
    RepoTreeDirent *dirent = NULL;

    comps = g_strsplit (path, "/", -1);
    n = g_strv_length (comps);
    if (!comps || n == 0) {
        g_strfreev (comps);
        return NULL;
    }

    dir = tree->root;
    for (i = 0; i < n; ++i) {
        comp = comps[i];
        dirent = g_hash_table_lookup (dir->dirents, comp);
        if (!dirent)
            break;
        if (S_ISDIR(dirent->mode)) {
            dir = dirent->subdir;
        } else {
            if (i < n - 1) {
                dirent = NULL;
            }
            break;
        }
    }

    g_strfreev (comps);
    return dirent;
}

int
repo_tree_stat_path (RepoTree *tree, const char *path, RepoTreeStat *st)
{
    if (path[0] == '\0')
        return -ENOENT;

    pthread_rwlock_rdlock (&tree->lock);

    if (!tree->root) {
        pthread_rwlock_unlock (&tree->lock);
        return -ENOENT;
    }

    RepoTreeDirent *dirent = resolve_path (tree, path);
    if (!dirent) {
        pthread_rwlock_unlock (&tree->lock);
        return -ENOENT;
    }

    memcpy (st->id, dirent->id, sizeof (st->id));
    st->mode = dirent->mode;
    st->size = dirent->size;
    st->mtime = dirent->mtime;

    pthread_rwlock_unlock (&tree->lock);

    return 0;
}

int
repo_tree_readdir (RepoTree *tree, const char *path, GHashTable *dirents)
{
    RepoTreeDirent *dirent;
    RepoTreeDir *dir;
    GHashTableIter iter;
    gpointer key, value;
    RepoTreeStat *st;
    int ret = 0;

    pthread_rwlock_rdlock (&tree->lock);

    if (!tree->root) {
        ret = -ENOENT;
        goto out;
    }

    if (path[0] == '\0') {
        dir = tree->root;
    } else {
        dirent = resolve_path (tree, path);
        if (!dirent) {
            ret = -ENOENT;
            goto out;
        }
        if (!S_ISDIR(dirent->mode)) {
            ret = -ENOTDIR;
            goto out;
        }

        dir = dirent->subdir;
    }

    char *sub_path = NULL;
    g_hash_table_iter_init (&iter, dir->dirents);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        dirent = (RepoTreeDirent *)value;
        st = g_new0 (RepoTreeStat, 1);
        st->mode = dirent->mode;
        st->size = dirent->size;
        st->mtime = dirent->mtime;
        sub_path = g_build_path ("/", path, dirent->name, NULL);
        if (seaf_repo_manager_is_path_invisible (seaf->repo_mgr, tree->repo_id, sub_path)) {
            g_free (sub_path);
            continue;
        }
        g_free (sub_path);
        g_hash_table_insert (dirents, g_strdup(dirent->name), st);
    }

out:
    pthread_rwlock_unlock (&tree->lock);
    return ret;
}

static int
repo_tree_create_common (RepoTree *tree, const char *path, const char *id,
                         guint32 mode, gint64 mtime, gint64 size)
{
    char *parent = NULL, *dname = NULL;
    RepoTreeDirent *dirent;
    RepoTreeDir *dir, *empty_dir = NULL;
    int ret = 0;

    pthread_rwlock_wrlock (&tree->lock);

    if (!tree->root) {
        ret = -ENOENT;
        goto out;
    }

    if (path[0] == '\0') {
        ret = -EINVAL;
        goto out;
    }

    parent = g_path_get_dirname (path);
    dname = g_path_get_basename (path);
    if (strcmp (parent, ".") == 0) {
        dir = tree->root;
    } else {
        dirent = resolve_path (tree, parent);
        if (!dirent) {
            ret = -ENOENT;
            goto out;
        }

        dir = dirent->subdir;
    }

    if (g_hash_table_lookup (dir->dirents, dname) != NULL) {
        ret = -EEXIST;
        goto out;
    }

    if (S_ISDIR(mode)) {
        empty_dir = repo_tree_dir_new (EMPTY_SHA1);
    }
    dirent = repo_tree_dirent_new (id,
                                   dname,
                                   create_mode(mode),
                                   mtime,
                                   size,
                                   empty_dir);
    g_hash_table_insert (dir->dirents, dirent->name, dirent);

out:
    g_free (parent);
    g_free (dname);
    pthread_rwlock_unlock (&tree->lock);
    return ret;
}

int
repo_tree_create_file (RepoTree *tree, const char *path, const char *id,
                       guint32 mode, gint64 mtime, gint64 size)
{
    return repo_tree_create_common (tree, path, id, mode, mtime, size);
}

int
repo_tree_mkdir (RepoTree *tree, const char *path, gint64 mtime)
{
    return repo_tree_create_common (tree, path, EMPTY_SHA1, S_IFDIR, mtime, 0);
}

static int
repo_tree_remove_common (RepoTree *tree, const char *path, gboolean is_dir)
{
    char *parent = NULL, *dname = NULL;
    RepoTreeDirent *dirent;
    RepoTreeDir *dir;
    int ret = 0;

    pthread_rwlock_wrlock (&tree->lock);

    if (!tree->root) {
        ret = -ENOENT;
        goto out;
    }

    if (path[0] == '\0') {
        ret = -EINVAL;
        goto out;
    }

    parent = g_path_get_dirname (path);
    dname = g_path_get_basename (path);
    if (strcmp (parent, ".") == 0) {
        dir = tree->root;
    } else {
        dirent = resolve_path (tree, parent);
        if (!dirent) {
            ret = -ENOENT;
            goto out;
        }

        dir = dirent->subdir;
    }

    dirent = g_hash_table_lookup (dir->dirents, dname);
    if (!dirent) {
        ret = -ENOENT;
        goto out;
    }

    if (is_dir && !S_ISDIR(dirent->mode)) {
        ret = -ENOTDIR;
        goto out;
    } else if (!is_dir && S_ISDIR(dirent->mode)) {
        ret = -EISDIR;
        goto out;
    }

    if (S_ISDIR(dirent->mode) && !repo_tree_dir_is_empty(dirent->subdir)) {
        ret = -ENOTEMPTY;
        goto out;
    }

    g_hash_table_remove (dir->dirents, dname);

out:
    g_free (parent);
    g_free (dname);
    pthread_rwlock_unlock (&tree->lock);
    return ret;
}

int
repo_tree_unlink (RepoTree *tree, const char *path)
{
    return repo_tree_remove_common (tree, path, FALSE);
}

int
repo_tree_rmdir (RepoTree *tree, const char *path)
{
    return repo_tree_remove_common (tree, path, TRUE);
}

int
repo_tree_rename (RepoTree *tree, const char *oldpath, const char *newpath,
                  gboolean replace_existing)
{
    char *parent1 = NULL, *dname1 = NULL;
    char *parent2 = NULL, *dname2 = NULL;
    RepoTreeDirent *dirent, *dirent1, *dirent2, *new_dirent;
    RepoTreeDir *dir1, *dir2;
    int ret = 0;

    pthread_rwlock_wrlock (&tree->lock);

    if (!tree->root) {
        ret = -ENOENT;
        goto out;
    }

    if (oldpath[0] == '\0' || newpath[0] == '\0') {
        ret = -EINVAL;
        goto out;
    }

    parent1 = g_path_get_dirname (oldpath);
    dname1 = g_path_get_basename (oldpath);
    if (strcmp (parent1, ".") == 0) {
        dir1 = tree->root;
    } else {
        dirent = resolve_path (tree, parent1);
        if (!dirent) {
            ret = -ENOENT;
            goto out;
        }

        dir1 = dirent->subdir;
    }

    dirent1 = g_hash_table_lookup (dir1->dirents, dname1);
    if (!dirent1) {
        ret = -ENOENT;
        goto out;
    }

    parent2 = g_path_get_dirname (newpath);
    dname2 = g_path_get_basename (newpath);
    if (strcmp (parent2, ".") == 0) {
        dir2 = tree->root;
    } else {
        dirent = resolve_path (tree, parent2);
        if (!dirent) {
            ret = -ENOENT;
            goto out;
        }

        dir2 = dirent->subdir;
    }

    dirent2 = g_hash_table_lookup (dir2->dirents, dname2);

    if (dirent2) {
        if (!replace_existing) {
            ret = -EEXIST;
            goto out;
        }

        if (S_ISDIR(dirent2->mode) && !S_ISDIR(dirent1->mode)) {
            ret = -EISDIR;
            goto out;
        }
        if (S_ISDIR(dirent1->mode) && !S_ISDIR(dirent2->mode)) {
            ret = -ENOTDIR;
            goto out;
        }
        if (S_ISDIR(dirent1->mode) && S_ISDIR(dirent2->mode) &&
            !repo_tree_dir_is_empty (dirent2->subdir)) {
            ret = -ENOTEMPTY;
            goto out;
        }
    }

    /* OK, all checks are passed. Replace dirent2 with dirent1. */

    /* Remove dirent1 from dir1 but don't free it. */
    g_hash_table_steal (dir1->dirents, dname1);

    new_dirent = dirent1;
    g_free (new_dirent->name);
    new_dirent->name = g_strdup(dname2);

    g_hash_table_replace (dir2->dirents, new_dirent->name, new_dirent);

out:
    g_free (parent1);
    g_free (dname1);
    g_free (parent2);
    g_free (dname2);

    pthread_rwlock_unlock (&tree->lock);

    return ret;
}

int
repo_tree_add_subtree (RepoTree *tree, const char *path, const char *root_id, gint64 mtime)
{
    char *parent = NULL, *dname = NULL;
    RepoTreeDirent *dirent;
    RepoTreeDir *dir, *subdir = NULL;
    int ret = 0;

    subdir = load_dir_recursive (tree->repo_id, root_id);
    if (!subdir) {
        seaf_warning ("Failed to load repo dir %s of repo %s.\n",
                      root_id, tree->repo_id);
        return -1;
    }

    pthread_rwlock_wrlock (&tree->lock);

    if (!tree->root) {
        ret = -ENOENT;
        goto out;
    }

    if (path[0] == '\0') {
        ret = -EINVAL;
        goto out;
    }

    parent = g_path_get_dirname (path);
    dname = g_path_get_basename (path);
    if (strcmp (parent, ".") == 0) {
        dir = tree->root;
    } else {
        dirent = resolve_path (tree, parent);
        if (!dirent) {
            ret = -ENOENT;
            goto out;
        }

        dir = dirent->subdir;
    }

    if (g_hash_table_lookup (dir->dirents, dname) != NULL) {
        ret = -EEXIST;
        goto out;
    }

    dirent = repo_tree_dirent_new (root_id,
                                   dname,
                                   create_mode(S_IFDIR),
                                   mtime,
                                   0,
                                   subdir);
    g_hash_table_insert (dir->dirents, dirent->name, dirent);

out:
    g_free (parent);
    g_free (dname);
    if (ret < 0)
        repo_tree_dir_free (subdir);
    pthread_rwlock_unlock (&tree->lock);
    return ret;
}

static int
remove_dir_recursive (const char *repo_id, RepoTreeDir *dir, const char *path)
{
    GHashTableIter iter;
    gpointer key, value;
    RepoTreeDirent *dirent;
    char *sub_path;

    g_hash_table_iter_init (&iter, dir->dirents);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        dirent = (RepoTreeDirent *)value;
        sub_path = g_strconcat (path, "/", dirent->name, NULL);
        if (S_ISREG(dirent->mode)) {
            if (file_cache_mgr_is_file_changed (seaf->file_cache_mgr,
                                                repo_id, sub_path,
                                                TRUE)) {
                g_free (sub_path);
                continue;
            }
            g_hash_table_iter_remove (&iter);
            file_cache_mgr_unlink (seaf->file_cache_mgr, repo_id, sub_path);
        } else {
            if (remove_dir_recursive (repo_id, dirent->subdir, sub_path) == 0) {
                g_hash_table_iter_remove (&iter);
                file_cache_mgr_rmdir (seaf->file_cache_mgr, repo_id, sub_path);
            }
        }
        g_free (sub_path);
    }

    int ret = g_hash_table_size(dir->dirents);
    return ret;
}

int
repo_tree_remove_subtree (RepoTree *tree, const char *path)
{
    char *parent = NULL, *dname = NULL;
    RepoTreeDirent *dirent;
    RepoTreeDir *dir;
    int ret = 0;

    pthread_rwlock_wrlock (&tree->lock);

    if (!tree->root) {
        ret = -ENOENT;
        goto out;
    }

    if (path[0] == '\0') {
        ret = -EINVAL;
        goto out;
    }

    parent = g_path_get_dirname (path);
    dname = g_path_get_basename (path);
    if (strcmp (parent, ".") == 0) {
        dir = tree->root;
    } else {
        dirent = resolve_path (tree, parent);
        if (!dirent) {
            ret = -ENOENT;
            goto out;
        }

        dir = dirent->subdir;
    }

    dirent = g_hash_table_lookup (dir->dirents, dname);
    if (!dirent) {
        goto out;
    }

    if (remove_dir_recursive (tree->repo_id, dirent->subdir, path) == 0) {
        g_hash_table_remove (dir->dirents, dname);
        file_cache_mgr_rmdir (seaf->file_cache_mgr, tree->repo_id, path);
    }

out:
    g_free (parent);
    g_free (dname);
    pthread_rwlock_unlock (&tree->lock);
    return ret;
}

int
repo_tree_set_file_size (RepoTree *tree, const char *path, gint64 size)
{
    RepoTreeDirent *dirent;

    pthread_rwlock_wrlock (&tree->lock);

    dirent = resolve_path (tree, path);
    if (!dirent || !S_ISREG(dirent->mode)) {
        pthread_rwlock_unlock (&tree->lock);
        return -1;
    }

    dirent->size = size;

    pthread_rwlock_unlock (&tree->lock);

    return 0;
}

int
repo_tree_set_file_mtime (RepoTree *tree, const char *path, gint64 mtime)
{
    RepoTreeDirent *dirent;

    pthread_rwlock_wrlock (&tree->lock);

    dirent = resolve_path (tree, path);
    if (!dirent || !S_ISREG(dirent->mode)) {
        pthread_rwlock_unlock (&tree->lock);
        return -1;
    }

    dirent->mtime = mtime;

    pthread_rwlock_unlock (&tree->lock);

    return 0;
}

int
repo_tree_set_file_id (RepoTree *tree, const char *path, const char *new_id)
{
    RepoTreeDirent *dirent;

    pthread_rwlock_wrlock (&tree->lock);

    dirent = resolve_path (tree, path);
    if (!dirent || !S_ISREG(dirent->mode)) {
        pthread_rwlock_unlock (&tree->lock);
        return -1;
    }

    memcpy (dirent->id, new_id, 40);

    pthread_rwlock_unlock (&tree->lock);

    return 0;
}

gboolean
repo_tree_is_empty_repo (RepoTree *tree)
{
    GHashTable *dirents = tree->root->dirents;
    return g_hash_table_size(dirents) == 0 ? TRUE:FALSE;
}

gboolean
repo_tree_is_empty_dir (RepoTree *tree, const char *path)
{
    RepoTreeDirent *dirent;
    RepoTreeDir *dir;
    gboolean ret;

    pthread_rwlock_rdlock (&tree->lock);

    if (!tree->root) {
        pthread_rwlock_unlock (&tree->lock);
        return FALSE;
    }

    if (path[0] == '\0') {
        dir = tree->root;
    } else {
        dirent = resolve_path (tree, path);
        if (!dirent) {
            pthread_rwlock_unlock (&tree->lock);
            return FALSE;
        }
        dir = dirent->subdir;
    }

    if (!dir) {
        ret = FALSE;
        goto out;
    }

    ret = repo_tree_dir_is_empty (dir);

out:
    pthread_rwlock_unlock (&tree->lock);

    return ret;
}

static void
traverse_recursive (RepoTree *tree, RepoTreeDir *dir, const char *path,
                    RepoTreeTraverseCallback callback)
{
    GHashTableIter iter;
    gpointer key, value;
    RepoTreeStat st;
    char *dname;
    char *subpath;
    RepoTreeDirent *dirent;

    g_hash_table_iter_init (&iter, dir->dirents);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        dname = (char *)key;
        dirent = (RepoTreeDirent *)value;
        subpath = g_build_path ("/", path, dname, NULL);

        if (S_ISREG(dirent->mode)) {
            memset (&st, 0, sizeof(st));
            memcpy (st.id, dirent->id, 40);
            st.mode = dirent->mode;
            st.size = dirent->size;
            st.mtime = dirent->mtime;
            callback (tree->repo_id, subpath, &st);
        } else {
            memset (&st, 0, sizeof(st));
            memcpy (st.id, dirent->id, 40);
            st.mode = dirent->mode;

            callback (tree->repo_id, subpath, &st);
            traverse_recursive (tree, dirent->subdir, subpath, callback);
        }
        g_free (subpath);
    }
}

int
repo_tree_traverse (RepoTree *tree,
                    const char *root_path,
                    RepoTreeTraverseCallback callback)
{
    RepoTreeDirent *dirent;
    RepoTreeDir *dir;
    int ret = 0;
    RepoTreeStat st;

    pthread_rwlock_rdlock (&tree->lock);

    if (!tree->root) {
        ret = -ENOENT;
        goto out;
    }

    if (root_path[0] == '\0') {
        dir = tree->root;
    } else {
        dirent = resolve_path (tree, root_path);
        if (!dirent) {
            ret = -ENOENT;
            goto out;
        }
        if (!S_ISDIR(dirent->mode)) {
            memset (&st, 0, sizeof(st));
            memcpy (st.id, dirent->id, 40);
            st.mode = dirent->mode;
            st.size = dirent->size;
            st.mtime = dirent->mtime;
            callback (tree->repo_id, root_path, &st);
            goto out;
        } else {
            memset (&st, 0, sizeof(st));
            memcpy (st.id, dirent->id, 40);
            st.mode = dirent->mode;
            callback (tree->repo_id, root_path, &st);
            dir = dirent->subdir;
        }
    }

    traverse_recursive (tree, dir, root_path, callback);

out:
    pthread_rwlock_unlock (&tree->lock);
    return ret;
}

static gboolean
find_office_file_path (RepoTree *tree,
                       const char *parent_dir,
                       const char *lock_file_name,
                       gboolean is_wps,
                       char **office_path)
{
    GHashTable *dirents = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);
    GHashTableIter iter;
    gpointer key, value;
    char *dname, *dname_nohead;
    gboolean ret = FALSE;

    if (repo_tree_readdir (tree, parent_dir, dirents) < 0) {
        g_hash_table_destroy (dirents);
        return FALSE;
    }

    g_hash_table_iter_init (&iter, dirents);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        dname = (char *)key;
        if (strlen(dname) < 2 || strncmp(dname, "~$", 2) == 0 || strncmp(dname, ".~", 2) == 0) {
            continue;
        }
        if (is_wps) {
            dname_nohead = g_utf8_next_char(dname);
            if (strcmp (dname_nohead, lock_file_name) == 0) {
                *office_path = g_build_path("/", parent_dir, dname, NULL);
                ret = TRUE;
                break;
            }
            dname_nohead = g_utf8_next_char(dname_nohead);
        } else {
            dname_nohead = g_utf8_next_char(g_utf8_next_char(dname));
        }
        if (strcmp (dname_nohead, lock_file_name) == 0) {
            *office_path = g_build_path("/", parent_dir, dname, NULL);
            ret = TRUE;
            break;
        }
    }

    g_hash_table_destroy (dirents);
    return ret;
}

gboolean
repo_tree_is_office_lock_file (RepoTree *tree, const char *path, char **office_path)
{
    gboolean ret;
    gboolean is_wps = FALSE;

    if (!seaf->enable_auto_lock || !seaf->office_lock_file_regex ||
        !seaf->libre_office_lock_file_regex || !seaf->wps_lock_file_regex)
        return FALSE;

    if (g_regex_match (seaf->office_lock_file_regex, path, 0, NULL))
        /* Replace ~$abc.docx with abc.docx */
        *office_path = g_regex_replace (seaf->office_lock_file_regex,
                                        path, -1, 0,
                                        "\\1", 0, NULL);
    else if (g_regex_match (seaf->libre_office_lock_file_regex, path, 0, NULL))
        /* Replace .~lock.abc.docx# with abc.docx */
        *office_path = g_regex_replace (seaf->libre_office_lock_file_regex,
                                        path, -1, 0,
                                        "\\1", 0, NULL);
    else if (g_regex_match (seaf->wps_lock_file_regex, path, 0, NULL)) {
        /* Replace .~abc.docx with abc.docx */
        *office_path = g_regex_replace (seaf->wps_lock_file_regex,
                                        path, -1, 0,
                                        "\\1", 0, NULL);
        is_wps = TRUE;
    } else
        return FALSE;

    /* When the filename is long, sometimes the first two characters
       in the filename will be directly replaced with ~$.
       So if the office_path file doesn't exist, we have to match
       against all filenames in this directory, to find the office
       file's name.
    */
    RepoTreeStat st;
    if (repo_tree_stat_path (tree, *office_path, &st) == 0) {
        return TRUE;
    }

    char *lock_file_name = g_path_get_basename(*office_path);
    char *parent_dir = g_path_get_dirname(*office_path);
    if (strcmp(parent_dir, ".") == 0) {
        g_free (parent_dir);
        parent_dir = g_strdup("");
    }
    g_free (*office_path);
    *office_path = NULL;

    ret = find_office_file_path (tree, parent_dir, lock_file_name, is_wps, office_path);

    g_free (lock_file_name);
    g_free (parent_dir);
    return ret;
}
