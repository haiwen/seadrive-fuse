/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef SEAFILE_SESSION_H
#define SEAFILE_SESSION_H

#include <glib-object.h>

#include "job-mgr.h"
#include "db.h"

#include "block-mgr.h"
#include "fs-mgr.h"
#include "commit-mgr.h"
#include "branch-mgr.h"
#include "repo-mgr.h"
#include "sync-mgr.h"
#include "http-tx-mgr.h"
#include "filelock-mgr.h"
#include "file-cache-mgr.h"
#include "mq-mgr.h"
#include "notif-mgr.h"

#define SEAFILE_TYPE_SESSION                  (seafile_session_get_type ())
#define SEAFILE_SESSION(obj)                  (G_TYPE_CHECK_INSTANCE_CAST ((obj), SEAFILE_TYPE_SESSION, SeafileSession))
#define SEAFILE_IS_SESSION(obj)               (G_TYPE_CHECK_INSTANCE_TYPE ((obj), SEAFILE_TYPE_SESSION))
#define SEAFILE_SESSION_CLASS(klass)          (G_TYPE_CHECK_CLASS_CAST ((klass), SEAFILE_TYPE_SESSION, SeafileSessionClass))
#define SEAFILE_IS_SESSION_CLASS(klass)       (G_TYPE_CHECK_CLASS_TYPE ((klass), SEAFILE_TYPE_SESSION))
#define SEAFILE_SESSION_GET_CLASS(obj)        (G_TYPE_INSTANCE_GET_CLASS ((obj), SEAFILE_TYPE_SESSION, SeafileSessionClass))


typedef struct _SeafileSession SeafileSession;
typedef struct _SeafileSessionClass SeafileSessionClass;

struct event_base;

typedef enum SeafLang {
    SEAF_LANG_EN_US = 0,
    SEAF_LANG_ZH_CN,
    SEAF_LANG_FR_FR,
    SEAF_LANG_DE_DE,
} SeafLang;

struct _SeafileSession {
    GObject         parent_instance;

    struct event_base   *ev_base;

    char                *client_id;
    char                *client_name;

    char                *seaf_dir;
    char                *tmp_file_dir;
    sqlite3             *config_db;
    char                *deleted_store;
    char                *rpc_socket_path;
    char                *mount_point;

    SeafBlockManager    *block_mgr;
    SeafFSManager       *fs_mgr;
    SeafCommitManager   *commit_mgr;
    SeafBranchManager   *branch_mgr;
    SeafRepoManager     *repo_mgr;
    SeafSyncManager     *sync_mgr;
    SeafNotifManager     *notif_mgr;

    SeafJobManager     *job_mgr;

    HttpTxManager       *http_tx_mgr;
    SeafFilelockManager *filelock_mgr;
    FileCacheMgr        *file_cache_mgr;
    JournalManager *journal_mgr;
    MqMgr               *mq_mgr;

    /* Set after all components are up and running. */
    gboolean             started;

    gboolean             sync_extra_temp_file;
    gboolean             enable_http_sync;
    gboolean             disable_verify_certificate;

    gboolean             use_http_proxy;
    char                *http_proxy_type;
    char                *http_proxy_addr;
    int                  http_proxy_port;
    char                *http_proxy_username;
    char                *http_proxy_password;

    gboolean             enable_auto_lock;
    GRegex              *office_lock_file_regex;

    gint64               last_check_repo_list_time;
    gint64               last_access_fs_time;

    SeafLang             language;
};

struct _SeafileSessionClass
{
    GObjectClass    parent_class;
};


extern SeafileSession *seaf;

SeafileSession *
seafile_session_new(const char *seafile_dir);

void
seafile_session_prepare (SeafileSession *session);

void
seafile_session_start (SeafileSession *session);

int
seafile_session_unmount (SeafileSession *session);

#endif /* SEAFILE_H */
