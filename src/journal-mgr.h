#ifndef JOURNAL_MGR_H
#define JOURNAL_MGR_H

#include <glib.h>

struct _SeafileSession;

struct _JournalManager;
typedef struct _JournalManager JournalManager;

JournalManager *
journal_manager_new (struct _SeafileSession *session);

int
journal_manager_start (JournalManager *mgr);

struct _Journal;
typedef struct _Journal Journal;

Journal *
journal_manager_open_journal (JournalManager *mgr, const char *repo_id);

int
journal_manager_delete_journal (JournalManager *mgr, const char *repo_id);

/* New types should only be added to the end of this declaration.
 * The type numbers are stored directly to db. Inserting new types in the middle
 * will break compatibility.
 */
enum _JournalOpType {
    OP_TYPE_CREATE_FILE = 0,
    OP_TYPE_DELETE_FILE,
    OP_TYPE_UPDATE_FILE,
    OP_TYPE_RENAME,
    OP_TYPE_MKDIR,
    OP_TYPE_RMDIR,
    OP_TYPE_UPDATE_ATTR,
    OP_TYPE_TERMINATE,          /* Special op for journal's internal use. */
};
typedef enum _JournalOpType JournalOpType;

struct _JournalOp {
    gint64 opid;                /* Only set when the op is read from journal */
    JournalOpType type;
    char *path;
    char *new_path;
    gint64 size;
    gint64 mtime;
    guint32 mode;
};
typedef struct _JournalOp JournalOp;

JournalOp *
journal_op_new (JournalOpType type, const char *path, const char *new_path,
                gint64 size, gint64 mtime, guint32 mode);

void
journal_op_free (JournalOp *op);

/* This function takes the ownership of @op. */
int
journal_append_op (Journal *journal, JournalOp *op);

/* Returns the list of operations with opid from @start to @end, inclusively.
 * The returned list is sorted by opid, from small to large.
 * To get operations from the every beginning, set @start to 0;
 * To get operations up to the last one, set @end to G_MAXINT64.
 */
GList *
journal_read_ops (Journal *journal, gint64 start, gint64 end, gboolean *error);

/*
 * Truncate journal entries before @opid, inclusive.
 */
int
journal_truncate (Journal *journal, gint64 opid);

/* Wait until all journal ops are flush to disk, with timeout.
 * Wait indefinitely if timeout < 0.
 */
void
journal_flush (Journal *journal, int timeout);

/* @wait: wait until all ops are committed to disk. */
int
journal_close (Journal *journal, gboolean wait);

struct _JournalStat {
    gint last_change_time;
    gint last_commit_time;
    gint64 total_size;
};
typedef struct _JournalStat JournalStat;

void
journal_get_stat (Journal *journal, JournalStat *st);

void
journal_set_last_commit_time (Journal *journal, gint time);

#endif
