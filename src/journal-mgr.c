#include "common.h"

#include <pthread.h>

#include "log.h"
#include "db.h"
#include "utils.h"

#include "seafile-session.h"
#include "journal-mgr.h"

struct _Journal {
    char *repo_id;

    /* The journal associates a worker thread that continuously writes
     * op entries into the database. It reads from @op_queue and commits a
     * transaction to the database in two cases:
     * 1. 100 op entries have been received.
     * 2. no new op entry is received in 1 second.
     */
    GAsyncQueue *op_queue;
    gint pushed_ops;
    gint committed_ops;

    /* The underlying database for the journal.
     * To reduce the write latency, ops are inserted into sqlite in
     * 100-entry transactions.
     */
    char *jdb_path;
    sqlite3 *jdb;
    pthread_mutex_t jdb_lock;

    gboolean write_failed;
    char *error_message;

    /* Statistics used for generating commit. */

    /* Time of last operation written to db */
    gint last_change_time;
    /* Time of last commit generation */
    gint last_commit_time;
    /* Total size of files waiting for commit. */
    gint64 total_size;
};

struct _JournalManager {
    SeafileSession *session;
    char *journal_dir;
};

JournalManager *
journal_manager_new (struct _SeafileSession *session)
{
    JournalManager *manager = g_new0 (JournalManager, 1);
    manager->session = session;

    char *journal_dir = g_build_filename (session->seaf_dir, "journals", NULL);
    if (g_mkdir_with_parents (journal_dir, 0777) < 0) {
        seaf_warning ("Journal dir doesn't exist and failed to create it.\n");
        g_free (manager);
        g_free (journal_dir);
        return NULL;
    }

    manager->journal_dir = journal_dir;

    return manager;
}

int
journal_manager_start (JournalManager *mgr)
{
    return 0;
}

JournalOp *
journal_op_new (JournalOpType type, const char *path, const char *new_path,
                gint64 size, gint64 mtime, guint32 mode)
{
    JournalOp *op = g_new0 (JournalOp, 1);
    op->type = type;
    op->path = g_strdup(path);
    op->new_path = g_strdup(new_path);
    op->size = size;
    op->mtime = mtime;
    op->mode = mode;
    return op;
}

void
journal_op_free (JournalOp *op)
{
    if (!op)
        return;
    g_free (op->path);
    g_free (op->new_path);
    g_free (op);
}

static gboolean
journal_op_same (JournalOp *op1, JournalOp *op2)
{
    return ((op1->type == op2->type) &&
            (g_strcmp0(op1->path, op2->path) == 0) &&
            (g_strcmp0(op1->new_path, op2->new_path) == 0));
}

JournalOp *
journal_op_from_json (const char *json_str)
{
    json_t *obj;
    json_error_t error;
    int type;
    const char *path = NULL, *new_path = NULL;
    gint64 size = 0, mtime = 0;
    guint32 mode = 0;
    JournalOp *op = NULL;

    obj = json_loads (json_str, 0, &error);
    if (!obj) {
        seaf_warning ("Invalid json format for operation: %s.\n"
                      "JSON string: %s\n", error.text, json_str);
        return NULL;
    }

    if (!json_object_has_member (obj, "type")) {
        seaf_warning ("Invalid operation format: no type.\n");
        goto out;
    }
    type = (int)json_object_get_int_member (obj, "type");

    if (!json_object_has_member (obj, "path")) {
        seaf_warning ("Invalid operation format: no path.\n");
        goto out;
    }
    path = json_object_get_string_member (obj, "path");
    if (!path) {
        seaf_warning ("Invalid operation format: null path.\n");
        goto out;
    }

    if (type == OP_TYPE_RENAME) {
        new_path = json_object_get_string_member (obj, "new_path");
        if (!new_path) {
            seaf_warning ("Invalid operation format: no new_path.\n");
            goto out;
        }
    } else if (type == OP_TYPE_CREATE_FILE || type == OP_TYPE_UPDATE_FILE ||
               type == OP_TYPE_UPDATE_ATTR) {
        if (!json_object_has_member (obj, "size")) {
            seaf_warning ("Invalid operation format: no size.\n");
            goto out;
        }
        size = json_object_get_int_member (obj, "size");

        if (!json_object_has_member (obj, "mtime")) {
            seaf_warning ("Invalid operation format: no mtime.\n");
            goto out;
        }
        mtime = json_object_get_int_member (obj, "mtime");

        if (!json_object_has_member (obj, "mode")) {
            seaf_warning ("Invalid operation format: no mode.\n");
            goto out;
        }
        mode = json_object_get_int_member (obj, "mode");
    }

    op = journal_op_new (type, path, new_path, size, mtime, mode);

out:
    json_decref (obj);
    return op;
}

char *
journal_op_to_json (JournalOp *op)
{
    json_t *obj;
    char *json_str = NULL;

    obj = json_object ();

    json_object_set_int_member (obj, "type", (int)op->type);
    json_object_set_string_member (obj, "path", op->path);
    if (op->type == OP_TYPE_RENAME) {
        json_object_set_string_member (obj, "new_path", op->new_path);
    } else if (op->type == OP_TYPE_CREATE_FILE || op->type == OP_TYPE_UPDATE_FILE ||
               op->type == OP_TYPE_UPDATE_ATTR) {
        json_object_set_int_member (obj, "size", op->size);
        json_object_set_int_member (obj, "mtime", op->mtime);
        json_object_set_int_member (obj, "mode", op->mode);
    }

    json_str = json_dumps (obj, JSON_COMPACT);
    if (!json_str) {
        seaf_warning ("Failed to dump json for operation.\n");
    }

    json_decref (obj);
    return json_str;
}

static void *write_journal_worker (void *vdata);

static void
journal_free (Journal *journal)
{
    if (!journal)
        return;
    g_free (journal->repo_id);
    g_async_queue_unref (journal->op_queue);
    g_free (journal->jdb_path);
    if (journal->jdb)
        sqlite_close_db (journal->jdb);
    pthread_mutex_destroy (&journal->jdb_lock);
    g_free (journal);
}

static int
do_open_journal (JournalManager *mgr, Journal *journal)
{
    sqlite3 *db = NULL;
    char *sql;

    if (sqlite_open_db (journal->jdb_path, &db) < 0) {
        return -1;
    }

    sql = "CREATE TABLE IF NOT EXISTS Operations ("
        "opid INTEGER PRIMARY KEY AUTOINCREMENT, operation TEXT)";
    if (sqlite_query_exec (db, sql) < 0) {
        sqlite_close_db (db);
        return -1;
    }

    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (pthread_create (&tid, &attr, write_journal_worker, journal) < 0) {
        seaf_warning ("Failed to start write journal worker.\n");
        sqlite_close_db (db);
        return -1;
    }

    journal->jdb = db;

    return 0;
}

Journal *
journal_manager_open_journal (JournalManager *mgr, const char *repo_id)
{
    char *jdb_path = NULL;
    Journal *jnl = NULL;

    jdb_path = g_strdup_printf ("%s/%s.journal", mgr->journal_dir, repo_id);

    jnl = g_new0 (Journal, 1);
    jnl->repo_id = g_strdup(repo_id);
    jnl->op_queue = g_async_queue_new_full ((GDestroyNotify)journal_op_free);
    jnl->jdb_path = jdb_path;
    pthread_mutex_init (&jnl->jdb_lock, NULL);

    /* Delay creating jdb and worker thread to the time when an op is appended. */
    if (seaf_util_exists (jdb_path)) {
        if (do_open_journal (mgr, jnl) < 0) {
            journal_free (jnl);
            return NULL;
        }
    }

    return jnl;
}

int
journal_manager_delete_journal (JournalManager *mgr, const char *repo_id)
{
    char *jdb_path = NULL;
    int ret;
    int n = 0;

    jdb_path = g_strdup_printf ("%s/%s.journal", mgr->journal_dir, repo_id);

    do {
        if (n > 0)
            g_usleep (1000000);
        ret = seaf_util_unlink (jdb_path);
        n++;
        if (ret < 0 && n == 10)
            seaf_warning ("Failed to delete journal for repo %s after %d retries "
                          "due to permission issues. Continue retrying.\n",
                          repo_id, n);
    } while (ret < 0 && errno == EACCES);

    g_free (jdb_path);
    return ret;
}

void
journal_flush (Journal *journal, int timeout)
{
    gint pushed, committed;
    int elapsed = 0;

    while (1) {
        pushed = g_atomic_int_get (&journal->pushed_ops);
        committed = g_atomic_int_get (&journal->committed_ops);
        if (pushed == committed) {
            break;
        } else if (timeout < 0 || elapsed < timeout) {
            g_usleep (G_USEC_PER_SEC);
            elapsed++;
        } else {
            break;
        }
    }
}

int
journal_close (Journal *journal, gboolean wait)
{
    if (!journal->jdb) {
        journal_free (journal);
        return 0;
    }

    if (wait) {
        journal_flush (journal, -1);
    }

    JournalOp *op = journal_op_new (OP_TYPE_TERMINATE, NULL, NULL, 0, 0, 0);
    g_async_queue_push (journal->op_queue, op);

    /* journal will be free in the worker thread. Otherwise the worker thread
     * may dereference the freed journal structure.
     */
    return 0;
}

static int
insert_op (Journal *journal, JournalOp *op, sqlite3_stmt *stmt)
{
    char *op_json;
    int ret = 0;

    op_json = journal_op_to_json (op);
    if (sqlite3_bind_text (stmt, 1, op_json, -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        journal->write_failed = TRUE;
        journal->error_message = g_strdup(sqlite3_errmsg(journal->jdb));
        seaf_warning ("sqlite3_bind_text failed: %s\n", journal->error_message);
        ret = -1;
        goto out;
    }
    if (sqlite3_step (stmt) != SQLITE_DONE) {
        journal->write_failed = TRUE;
        journal->error_message = g_strdup(sqlite3_errmsg(journal->jdb));
        seaf_warning ("sqlite3_step failed: %s\n", journal->error_message);
        ret = -1;
        goto out;
    }

out:
    sqlite3_clear_bindings (stmt);
    sqlite3_reset (stmt);
    g_free (op_json);
    return ret;
}

static void
flush_write_queue (Journal *journal, GQueue *queue)
{
    JournalOp *op;
    gint committed = 0;
    sqlite3 *jdb = journal->jdb;
    char *sql;
    sqlite3_stmt *stmt = NULL;

    if (g_queue_get_length (queue) == 0)
        return;

    pthread_mutex_lock (&journal->jdb_lock);

    sqlite_begin_transaction (jdb);

    sql = "INSERT INTO Operations (operation) VALUES (?)";
    if (sqlite3_prepare_v2 (jdb, sql, -1, &stmt, NULL) != SQLITE_OK) {
        journal->write_failed = TRUE;
        journal->error_message = g_strdup(sqlite3_errmsg(jdb));
        sqlite_rollback_transaction (jdb);
        goto out;
    }

    while ((op = (JournalOp *)g_queue_pop_head(queue)) != NULL) {
        if (insert_op (journal, op, stmt) < 0) {
            /* Put the failed op back to the queue for retry in the next second.
             * But previously inserted ops should still be committed.
             */
            g_queue_push_head (queue, op);
            break;
        }
        journal_op_free (op);
        ++committed;
    }

    if (sqlite_end_transaction (jdb) < 0) {
        journal->write_failed = TRUE;
        journal->error_message = g_strdup(sqlite3_errmsg(jdb));
        sqlite_rollback_transaction (jdb);
        goto out;
    }

    if (journal->write_failed) {
        journal->write_failed = FALSE;
        g_free (journal->error_message);
        journal->error_message = NULL;
    }

    g_atomic_int_add (&journal->committed_ops, committed);

    g_atomic_int_set (&journal->last_change_time, (gint)time(NULL));

out:
    pthread_mutex_unlock (&journal->jdb_lock);
    if (stmt)
        sqlite3_finalize (stmt);
}

#define FLUSH_QUEUE_TIMEOUT G_USEC_PER_SEC
#define WRITE_BATCH 100

static void *
write_journal_worker (void *vdata)
{
    Journal *journal = (Journal *)vdata;
    JournalOp *op, *prev_op;
    GQueue *write_queue = g_queue_new ();

    while (1) {
        op = (JournalOp *)g_async_queue_timeout_pop (journal->op_queue,
                                                     FLUSH_QUEUE_TIMEOUT);
        if (!op) {
            /* Time out. No op came in in the last second.
             * Flush the write queue to db.
             */
            flush_write_queue (journal, write_queue);
            continue;
        }

        if (op->type == OP_TYPE_TERMINATE) {
            g_queue_free_full (write_queue, (GDestroyNotify)journal_op_free);
            journal_free (journal);
            break;
        }

        prev_op = g_queue_peek_tail (write_queue);
        if (prev_op && journal_op_same (op, prev_op)) {
            if (op->type == OP_TYPE_UPDATE_FILE ||
                op->type == OP_TYPE_UPDATE_ATTR) {
                prev_op->size = op->size;
                prev_op->mtime = op->mtime;
            }
            journal_op_free (op);
            g_atomic_int_inc (&journal->committed_ops);
            continue;
        } else {
            g_queue_push_tail (write_queue, op);
        }

        if (g_queue_get_length (write_queue) >= WRITE_BATCH ||
            ((gint)time(NULL) - journal->last_change_time >= FLUSH_QUEUE_TIMEOUT)) {
            flush_write_queue (journal, write_queue);
        }
    }

    return NULL;
}

int
journal_append_op (Journal *journal, JournalOp *op)
{
    if (!seaf_util_exists (journal->jdb_path)) {
        if (do_open_journal (seaf->journal_mgr, journal) < 0)
            return -1;
    }

    g_async_queue_push (journal->op_queue, op);
    g_atomic_int_inc (&journal->pushed_ops);
    return 0;
}

static gboolean
collect_ops (sqlite3_stmt *stmt, void *vdata)
{
    GList **pret = vdata;
    gint64 opid;
    const char *op_json;
    JournalOp *op;

    opid = (gint64) sqlite3_column_int64 (stmt, 0);
    op_json = (const char *) sqlite3_column_text (stmt, 1);

    op = journal_op_from_json (op_json);
    if (!op)
        return TRUE;
    op->opid = opid;

    *pret = g_list_prepend (*pret, op);

    return TRUE;
}

GList *
journal_read_ops (Journal *journal, gint64 start, gint64 end, gboolean *error)
{
    char *sql;
    GList *ret = NULL;

    if (error)
        *error = FALSE;

    if (!journal->jdb)
        return NULL;

    if (start < 0 || end < 0) {
        seaf_warning ("BUG: start or end out of range.\n");
        return NULL;
    }

    if (end != G_MAXINT64) {
        sql = sqlite3_mprintf ("SELECT opid, operation FROM Operations "
                               "WHERE opid >= %lld AND "
                               "opid <= %lld ORDER BY opid",
                               start, end);
    } else {
        sql = sqlite3_mprintf ("SELECT opid, operation FROM Operations "
                               "WHERE opid >= %lld ORDER BY opid",
                               start, end);
    }

    pthread_mutex_lock (&journal->jdb_lock);

    if (sqlite_foreach_selected_row (journal->jdb, sql, collect_ops, &ret) < 0) {
        seaf_warning ("Failed to read ops from journal for repo %s.\n",
                      journal->repo_id);
        g_list_free_full (ret, (GDestroyNotify)journal_op_free);
        ret = NULL;
        if (error)
            *error = TRUE;
        goto out;
    }
    ret = g_list_reverse (ret);

out:
    pthread_mutex_unlock (&journal->jdb_lock);
    sqlite3_free (sql);

    return ret;
}

int
journal_truncate (Journal *journal, gint64 opid)
{
    char *sql;
    int ret = 0;

    if (!journal->jdb)
        return 0;

    sql = sqlite3_mprintf ("DELETE FROM Operations WHERE opid <= %lld",
                           opid);

    pthread_mutex_lock (&journal->jdb_lock);

    if (sqlite_query_exec (journal->jdb, sql) < 0) {
        seaf_warning ("Failed to truncate journal for repo %s.\n", journal->repo_id);
        ret = -1;
    }

    pthread_mutex_unlock (&journal->jdb_lock);

    sqlite3_free (sql);
    return ret;
}

void
journal_get_stat (Journal *journal, JournalStat *st)
{
    st->last_change_time = g_atomic_int_get (&journal->last_change_time);
    st->last_commit_time = journal->last_commit_time;
    st->total_size = journal->total_size;
}

void
journal_set_last_commit_time (Journal *journal, gint time)
{
    journal->last_commit_time = time;
}
