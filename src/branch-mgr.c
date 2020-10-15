#include "common.h"

#include "log.h"

#include "db.h"

#include "seafile-session.h"

#include "branch-mgr.h"

#define BRANCH_DB "branch.db"

SeafBranch *
seaf_branch_new (const char *name,
                 const char *repo_id,
                 const char *commit_id,
                 gint64 opid)
{
    SeafBranch *branch;

    branch = g_new0 (SeafBranch, 1);

    branch->name = g_strdup (name);
    memcpy (branch->repo_id, repo_id, 36);
    branch->repo_id[36] = '\0';
    memcpy (branch->commit_id, commit_id, 40);
    branch->commit_id[40] = '\0';
    branch->opid = opid;

    branch->ref = 1;

    return branch;
}

void
seaf_branch_free (SeafBranch *branch)
{
    if (branch == NULL) return;
    g_free (branch->name);
    g_free (branch);
}

void
seaf_branch_list_free (GList *blist)
{
    GList *ptr;

    for (ptr = blist; ptr; ptr = ptr->next) {
        seaf_branch_unref (ptr->data);
    }
    g_list_free (blist);
}


void
seaf_branch_set_commit (SeafBranch *branch, const char *commit_id)
{
    memcpy (branch->commit_id, commit_id, 40);
    branch->commit_id[40] = '\0';
}

void
seaf_branch_ref (SeafBranch *branch)
{
    branch->ref++;
}

void
seaf_branch_unref (SeafBranch *branch)
{
    if (!branch)
        return;

    if (--branch->ref <= 0)
        seaf_branch_free (branch);
}

struct _SeafBranchManagerPriv {
    sqlite3 *db;
    pthread_mutex_t db_lock;
};

static int open_db (SeafBranchManager *mgr);

SeafBranchManager *
seaf_branch_manager_new (struct _SeafileSession *seaf)
{
    SeafBranchManager *mgr;

    mgr = g_new0 (SeafBranchManager, 1);
    mgr->priv = g_new0 (SeafBranchManagerPriv, 1);
    mgr->seaf = seaf;

    pthread_mutex_init (&mgr->priv->db_lock, NULL);

    return mgr;
}

int
seaf_branch_manager_init (SeafBranchManager *mgr)
{
    return open_db (mgr);
}

static int
open_db (SeafBranchManager *mgr)
{
    char *db_path;
    const char *sql;

    db_path = g_build_filename (mgr->seaf->seaf_dir, BRANCH_DB, NULL);
    if (sqlite_open_db (db_path, &mgr->priv->db) < 0) {
        g_critical ("[Branch mgr] Failed to open branch db\n");
        g_free (db_path);
        return -1;
    }
    g_free (db_path);

    sql = "CREATE TABLE IF NOT EXISTS Branch ("
          "name TEXT, repo_id TEXT, commit_id TEXT, opid INTEGER);";
    if (sqlite_query_exec (mgr->priv->db, sql) < 0)
        return -1;

    sql = "CREATE INDEX IF NOT EXISTS branch_index ON Branch(repo_id, name);";
    if (sqlite_query_exec (mgr->priv->db, sql) < 0)
        return -1;

    return 0;
}

int
seaf_branch_manager_add_branch (SeafBranchManager *mgr, SeafBranch *branch)
{
    char sql[256];

    pthread_mutex_lock (&mgr->priv->db_lock);

    sqlite3_snprintf (sizeof(sql), sql,
                      "SELECT 1 FROM Branch WHERE name=%Q and repo_id=%Q",
                      branch->name, branch->repo_id);
    if (sqlite_check_for_existence (mgr->priv->db, sql))
        sqlite3_snprintf (sizeof(sql), sql,
                          "UPDATE Branch SET commit_id=%Q, opid=%lld"
                          " WHERE name=%Q and repo_id=%Q",
                          branch->commit_id, branch->opid, branch->name, branch->repo_id);
    else
        sqlite3_snprintf (sizeof(sql), sql,
                          "INSERT INTO Branch (name, repo_id, commit_id, opid) "
                          "VALUES (%Q, %Q, %Q, %lld)",
                          branch->name, branch->repo_id, branch->commit_id, branch->opid);

    sqlite_query_exec (mgr->priv->db, sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    return 0;
}

int
seaf_branch_manager_del_branch (SeafBranchManager *mgr,
                                const char *repo_id,
                                const char *name)
{
    char *sql;

    pthread_mutex_lock (&mgr->priv->db_lock);

    sql = sqlite3_mprintf ("DELETE FROM Branch WHERE name = %Q AND "
                           "repo_id = %Q", name, repo_id);
    if (sqlite_query_exec (mgr->priv->db, sql) < 0)
        seaf_warning ("Delete branch %s failed\n", name);
    sqlite3_free (sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    return 0;
}

int
seaf_branch_manager_update_branch (SeafBranchManager *mgr, SeafBranch *branch)
{
    sqlite3 *db;
    char *sql;

    pthread_mutex_lock (&mgr->priv->db_lock);

    db = mgr->priv->db;
    sql = sqlite3_mprintf ("UPDATE Branch SET commit_id = %Q, opid = %lld"
                           " WHERE name = %Q AND repo_id = %Q",
                           branch->commit_id, branch->opid, branch->name, branch->repo_id);
    sqlite_query_exec (db, sql);
    sqlite3_free (sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    return 0;
}

int
seaf_branch_manager_update_repo_branches (SeafBranchManager *mgr, SeafBranch *branch_info)
{
    sqlite3 *db;
    char *sql;

    pthread_mutex_lock (&mgr->priv->db_lock);

    db = mgr->priv->db;
    sql = sqlite3_mprintf ("UPDATE Branch SET commit_id = %Q, opid = %lld"
                           " WHERE repo_id = %Q",
                           branch_info->commit_id, branch_info->opid, branch_info->repo_id);
    sqlite_query_exec (db, sql);
    sqlite3_free (sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    return 0;
}

static gboolean
get_branch_cb (sqlite3_stmt *stmt, void *vdata)
{
    SeafBranch **pbranch = vdata;
    const char *name, *repo_id, *commit_id;
    gint64 opid;

    name = (const char *)sqlite3_column_text (stmt, 0);
    repo_id = (const char *)sqlite3_column_text (stmt, 1);
    commit_id = (const char *)sqlite3_column_text (stmt, 2);
    opid = (gint64)sqlite3_column_int64 (stmt, 3);

    *pbranch = seaf_branch_new (name, repo_id, commit_id, opid);

    return FALSE;
}

SeafBranch *
seaf_branch_manager_get_branch (SeafBranchManager *mgr,
                                const char *repo_id,
                                const char *name)
{
    SeafBranch *branch = NULL;
    sqlite3 *db;
    char *sql;

    pthread_mutex_lock (&mgr->priv->db_lock);

    db = mgr->priv->db;
    sql = sqlite3_mprintf ("SELECT name, repo_id, commit_id, opid FROM Branch "
                           "WHERE name = %Q and repo_id=%Q",
                           name, repo_id);

    if (sqlite_foreach_selected_row (db, sql, get_branch_cb, &branch) < 0) {
        seaf_warning ("Failed to get branch %s of repo %s.\n", name, repo_id);
        branch = NULL;
    }

    sqlite3_free (sql);
    pthread_mutex_unlock (&mgr->priv->db_lock);
    return branch;
}

gboolean
seaf_branch_manager_branch_exists (SeafBranchManager *mgr,
                                   const char *repo_id,
                                   const char *name)
{
    char *sql;
    gboolean ret;

    pthread_mutex_lock (&mgr->priv->db_lock);

    sql = sqlite3_mprintf ("SELECT name FROM Branch WHERE name = %Q "
                           "AND repo_id=%Q", name, repo_id);
    ret = sqlite_check_for_existence (mgr->priv->db, sql);
    sqlite3_free (sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);
    return ret;
}

static gboolean
get_branch_list_cb (sqlite3_stmt *stmt, void *vdata)
{
    GList **pret = vdata;
    SeafBranch *branch;
    const char *name, *repo_id, *commit_id;
    gint64 opid;

    name = (const char *)sqlite3_column_text (stmt, 0);
    repo_id = (const char *)sqlite3_column_text (stmt, 1);
    commit_id = (const char *)sqlite3_column_text (stmt, 2);
    opid = (gint64)sqlite3_column_int64 (stmt, 3);

    branch = seaf_branch_new (name, repo_id, commit_id, opid);

    *pret = g_list_prepend (*pret, branch);

    return TRUE;
}

GList *
seaf_branch_manager_get_branch_list (SeafBranchManager *mgr,
                                     const char *repo_id)
{
    sqlite3 *db = mgr->priv->db;    
    char *sql;
    GList *ret = NULL;

    sql = sqlite3_mprintf ("SELECT name, repo_id, commit_id, opid "
                           "FROM branch WHERE repo_id =%Q",
                           repo_id);

    pthread_mutex_lock (&mgr->priv->db_lock);

    if (sqlite_foreach_selected_row (db, sql, get_branch_list_cb, &ret) < 0) {
        seaf_warning ("Failed to get branch list of repo %s.\n", repo_id);
        g_list_free_full (ret, (GDestroyNotify)seaf_branch_free);
    }

    pthread_mutex_unlock (&mgr->priv->db_lock);

    sqlite3_free (sql);

    return g_list_reverse(ret);
}

int
seaf_branch_manager_update_opid (SeafBranchManager *mgr,
                                 const char *repo_id,
                                 const char *name,
                                 gint64 opid)
{
    sqlite3 *db;
    char *sql;

    pthread_mutex_lock (&mgr->priv->db_lock);

    db = mgr->priv->db;
    sql = sqlite3_mprintf ("UPDATE Branch SET opid = %lld"
                           " WHERE name = %Q AND repo_id = %Q",
                           opid, name, repo_id);
    sqlite_query_exec (db, sql);
    sqlite3_free (sql);

    pthread_mutex_unlock (&mgr->priv->db_lock);

    return 0;
}
