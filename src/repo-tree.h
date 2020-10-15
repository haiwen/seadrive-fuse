#ifndef SEAF_REPO_TREE_H
#define SEAF_REPO_TREE_H

#include <glib.h>

/*
 * RepoTree is a in-memory representation of the directory hierarchy of a repo.
 */

struct _RepoTree;
typedef struct _RepoTree RepoTree;

RepoTree *
repo_tree_new (const char *repo_id);

int
repo_tree_load_commit (RepoTree *tree, const char *commit_id);

/* Clear the contents of the tree, but not free it. */
void
repo_tree_clear (RepoTree *tree);

void
repo_tree_free (RepoTree *tree);

gboolean
repo_tree_is_loaded (RepoTree *tree);

struct _RepoTreeStat {
    /* Id is only valid for files. The ID of dirs are not always correct. */
    char id[41];
    guint32 mode;
    gint64 size;
    gint64 mtime;
};
typedef struct _RepoTreeStat RepoTreeStat;

int
repo_tree_stat_path (RepoTree *tree, const char *path, RepoTreeStat *st);

int
repo_tree_readdir (RepoTree *tree, const char *path, GHashTable *dirents);

int
repo_tree_create_file (RepoTree *tree, const char *path, const char *id,
                       guint32 mode, gint64 mtime, gint64 size);

int
repo_tree_mkdir (RepoTree *tree, const char *path, gint64 mtime);

int
repo_tree_unlink (RepoTree *tree, const char *path);

int
repo_tree_rmdir (RepoTree *tree, const char *path);

/* Rename @oldpath from @tree to @newpath.
 */
int
repo_tree_rename (RepoTree *tree, const char *oldpath, const char *newpath,
                  gboolean replace_existing);

/* Load the subtree pointed to by @root_id and add this subtree to @path. */
int
repo_tree_add_subtree (RepoTree *tree, const char *path, const char *root_id, gint64 mtime);

/* Recursively remove a subtree on @path */
int
repo_tree_remove_subtree (RepoTree *tree, const char *path);

int
repo_tree_set_file_mtime (RepoTree *tree, const char *path,
                          gint64 mtime);

int
repo_tree_set_file_size (RepoTree *tree, const char *path,
                         gint64 size);

int
repo_tree_set_file_id (RepoTree *tree, const char *path, const char *new_id);

gboolean
repo_tree_is_empty_repo (RepoTree *tree);

gboolean
repo_tree_is_empty_dir (RepoTree *tree, const char *path);

typedef void (*RepoTreeTraverseCallback) (const char * repo_id,
                                          const char *path,
                                          RepoTreeStat *st);

int
repo_tree_traverse (RepoTree *tree,
                    const char *root_path,
                    RepoTreeTraverseCallback callback);

gboolean
repo_tree_is_office_lock_file (RepoTree *tree, const char *path, char **office_path);

#endif
