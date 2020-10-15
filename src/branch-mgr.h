#ifndef SEAF_BRANCH_MGR_H
#define SEAF_BRANCH_MGR_H

#include "commit-mgr.h"
#define NO_BRANCH "-"

typedef struct _SeafBranch SeafBranch;

struct _SeafBranch {
    int   ref;
    char *name;
    char  repo_id[37];
    char  commit_id[41];
    gint64 opid;
};

SeafBranch *seaf_branch_new (const char *name,
                             const char *repo_id,
                             const char *commit_id,
                             gint64 opid);
void seaf_branch_free (SeafBranch *branch);
void seaf_branch_set_commit (SeafBranch *branch, const char *commit_id);

void seaf_branch_ref (SeafBranch *branch);
void seaf_branch_unref (SeafBranch *branch);


typedef struct _SeafBranchManager SeafBranchManager;
typedef struct _SeafBranchManagerPriv SeafBranchManagerPriv;

struct _SeafileSession;
struct _SeafBranchManager {
    struct _SeafileSession *seaf;

    SeafBranchManagerPriv *priv;
};

SeafBranchManager *seaf_branch_manager_new (struct _SeafileSession *seaf);
int seaf_branch_manager_init (SeafBranchManager *mgr);

int
seaf_branch_manager_add_branch (SeafBranchManager *mgr, SeafBranch *branch);

int
seaf_branch_manager_del_branch (SeafBranchManager *mgr,
                                const char *repo_id,
                                const char *name);

void
seaf_branch_list_free (GList *blist);

int
seaf_branch_manager_update_branch (SeafBranchManager *mgr,
                                   SeafBranch *branch);

/* Update 'local' and 'master' branch together. */
int
seaf_branch_manager_update_repo_branches (SeafBranchManager *mgr,
                                          SeafBranch *new_branch_info);

SeafBranch *
seaf_branch_manager_get_branch (SeafBranchManager *mgr,
                                const char *repo_id,
                                const char *name);


gboolean
seaf_branch_manager_branch_exists (SeafBranchManager *mgr,
                                   const char *repo_id,
                                   const char *name);

GList *
seaf_branch_manager_get_branch_list (SeafBranchManager *mgr,
                                     const char *repo_id);

int
seaf_branch_manager_update_opid (SeafBranchManager *mgr,
                                 const char *repo_id,
                                 const char *name,
                                 gint64 opid);

#endif /* SEAF_BRANCH_MGR_H */
