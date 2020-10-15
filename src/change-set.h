/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef SEAF_CHANGE_SET_H
#define SEAF_CHANGE_SET_H

#include <glib.h>
#include "utils.h"

struct _ChangeSetDir;

struct _ChangeSet {
    char repo_id[37];
    /* A partial tree for all changed directories. */
    struct _ChangeSetDir *tree_root;
};
typedef struct _ChangeSet ChangeSet;

ChangeSet *
changeset_new (const char *repo_id);

void
changeset_free (ChangeSet *changeset);

void
add_to_changeset (ChangeSet *changeset,
                  char status,
                  unsigned char *sha1,
                  SeafStat *st,
                  const char *modifier,
                  const char *path,
                  const char *new_path);

/*
  @remove_parent: remove the parent dir when it becomes empty.
*/
void
remove_from_changeset (ChangeSet *changeset,
                       const char *path,
                       gboolean remove_parent,
                       const char *top_dir);

char *
commit_tree_from_changeset (ChangeSet *changeset);

// Check whether the information in changeset is consistent with the input
gboolean
changeset_check_path (ChangeSet *changeset,
                      const char *path,
                      gint64 size,
                      gint64 mtime);

#endif
