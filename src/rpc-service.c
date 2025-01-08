#include "common.h"

#include <searpc.h>
#include <searpc-named-pipe-transport.h>
#include "searpc-signature.h"
#include "searpc-marshal.h"

#include <pthread.h>

#include <jansson.h>

#include "repo-mgr.h"
#include "seafile-session.h"
#include "seafile-config.h"
#include "seafile-crypt.h"
#include "seafile-error.h"
#include "utils.h"
#include "log.h"

static int
seafile_add_account (const char *server, const char *username,
                     const char *nickname,
                     const char *token,
                     const char *name,
                     int is_pro, GError **error)
{
    if (!server || !username || !token
        || !name
        ) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Empty Arguments");
        return -1;
    }

    return seaf_repo_manager_add_account (seaf->repo_mgr,
                                          server,
                                          username,
                                          nickname,
                                          token,
                                          name,
                                          (is_pro > 0));
}

/* static int */
/* seafile_update_account (const char *server, const char *username, const char *token, */
/*                         GError **error) */
/* { */
/*     if (!server || !username || !token) { */
/*         g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Empty Arguments"); */
/*         return -1; */
/*     } */

/*     return seaf_repo_manager_update_current_account (seaf->repo_mgr, */
/*                                                      server, */
/*                                                      username, */
/*                                                      token); */
/* } */

static int
seafile_delete_account (const char *server,
                        const char *username,
                        gboolean remove_cache,
                        GError **error)
{
    if (!server || !username
       ) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Empty Arguments");
        return -1;
    }

    return seaf_repo_manager_delete_account (seaf->repo_mgr,
                                             server,
                                             username,
                                             remove_cache,
                                             error);
}

static int
seafile_set_config (const char *key, const char *value, GError **error)
{
    return seafile_session_config_set_string(seaf, key, value);
}

static char *
seafile_get_config (const char *key, GError **error)
{
    return seafile_session_config_get_string(seaf, key);
}

static int
seafile_set_config_int (const char *key, int value, GError **error)
{
    return seafile_session_config_set_int(seaf, key, value);
}

static int
seafile_get_config_int (const char *key, GError **error)
{
    gboolean exists = TRUE;

    int ret = seafile_session_config_get_int(seaf, key, &exists);

    if (!exists) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_GENERAL, "Config not exists");
        return -1;
    }

    return ret;
}

static int
seafile_set_upload_rate_limit (int limit, GError **error)
{
    if (limit < 0)
        limit = 0;

    seaf->sync_mgr->upload_limit = limit;

    return seafile_session_config_set_int (seaf, KEY_UPLOAD_LIMIT, limit);
}

static int
seafile_set_download_rate_limit (int limit, GError **error)
{
    if (limit < 0)
        limit = 0;

    seaf->sync_mgr->download_limit = limit;

    return seafile_session_config_set_int (seaf, KEY_DOWNLOAD_LIMIT, limit);
}

static int
seafile_get_upload_rate(GError **error)
{
    return seaf->sync_mgr->last_sent_bytes;
}

static int
seafile_get_download_rate(GError **error)
{
    return seaf->sync_mgr->last_recv_bytes;
}

static int
seafile_set_clean_cache_interval (int seconds, GError **error)
{
    if (seconds < 0)
        return 0;

    return file_cache_mgr_set_clean_cache_interval (seaf->file_cache_mgr, seconds);
}

static int
seafile_get_clean_cache_interval (GError **error)
{
    return file_cache_mgr_get_clean_cache_interval (seaf->file_cache_mgr);
}

static int
seafile_set_cache_size_limit (gint64 limit, GError **error)
{
    if (limit < 0)
        return 0;

    return file_cache_mgr_set_cache_size_limit (seaf->file_cache_mgr, limit);
}

static gint64
seafile_get_cache_size_limit (GError **error)
{
    return file_cache_mgr_get_cache_size_limit (seaf->file_cache_mgr);
}

static int
seafile_is_file_cached (const char *repo_id, const char *file_path, GError **error)
{
    return file_cache_mgr_is_file_cached (seaf->file_cache_mgr,
                                          repo_id,
                                          file_path);
}


static json_t *
seafile_get_global_sync_status (GError **error)
{
    json_t *object = json_object();
    int is_syncing = seaf_sync_manager_is_syncing (seaf->sync_mgr);
    int auto_sync_enabled = seaf_sync_manager_is_auto_sync_enabled (seaf->sync_mgr);

    json_object_set (object, "auto_sync_enabled", json_integer(auto_sync_enabled));
    json_object_set (object, "is_syncing", json_integer(is_syncing));
    json_object_set (object, "sent_bytes", json_integer(seaf->sync_mgr->last_sent_bytes));
    json_object_set (object, "recv_bytes", json_integer(seaf->sync_mgr->last_recv_bytes));

    return object;
}

int
seafile_disable_auto_sync (GError **error)
{
    return seaf_sync_manager_disable_auto_sync (seaf->sync_mgr);
}

int
seafile_enable_auto_sync (GError **error)
{
    return seaf_sync_manager_enable_auto_sync (seaf->sync_mgr);
}

static char *
seafile_get_path_sync_status (const char *server,
                              const char *username,
                              const char *repo_uname,
                              const char *path,
                              GError **error)
{
    char *repo_id;
    char *canon_path = NULL;
    char *status;

    if (!repo_uname || !path) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return NULL;
    }

    repo_id = seaf_repo_manager_get_repo_id_by_display_name (seaf->repo_mgr, server, username, repo_uname);
    if (!repo_id) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Invalid repo unique name");
        return NULL;
    }

    /* Empty path means to get status of the worktree folder. */
    if (strcmp (path, "") != 0) {
        canon_path = format_path (path);
    } else {
        canon_path = g_strdup(path);
    }

    status = seaf_sync_manager_get_path_sync_status (seaf->sync_mgr,
                                                     repo_id,
                                                     canon_path);
    g_free (repo_id);
    g_free (canon_path);
    return status;
}

static json_t *
seafile_get_sync_notification (GError **error)
{
    return mq_mgr_pop_msg (seaf->mq_mgr, SEADRIVE_NOTIFY_CHAN);
}

static json_t *
seafile_get_events_notification (GError **error)
{
    return mq_mgr_pop_msg (seaf->mq_mgr, SEADRIVE_EVENT_CHAN);
}

static char*
seafile_get_repo_id_by_uname (const char *server, const char *username, const char *repo_uname, GError **error)
{
    char *repo_id;

    if (!repo_uname) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return NULL;
    }

    repo_id = seaf_repo_manager_get_repo_id_by_display_name (seaf->repo_mgr, server, username, repo_uname);
    if (!repo_id) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Invalid repo unique name");
        return NULL;
    }

    return repo_id;
}

static char*
seafile_get_repo_display_name_by_id (const char *repo_id, GError **error)
{
    char *repo_uname;

    if (!repo_id) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return NULL;
    }

    repo_uname = seaf_repo_manager_get_repo_display_name (seaf->repo_mgr, repo_id);
    if (!repo_uname) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Invalid repo id");
        return NULL;
    }

    return repo_uname;
}

static int
seafile_unmount (GError **error)
{
    return seafile_session_unmount (seaf);
}

static json_t *
seafile_get_upload_progress (GError **error)
{
    return http_tx_manager_get_upload_progress (seaf->http_tx_mgr);
}

static json_t *
seafile_get_download_progress (GError **error)
{
    return file_cache_mgr_get_download_progress (seaf->file_cache_mgr);
}

static int
seafile_mark_file_locked (const char *repo_id, const char *path, GError **error)
{
    char *canon_path = NULL;
    int len;
    int ret;

    if (!repo_id || !path) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return -1;
    }

    if (*path == '/')
        ++path;

    if (path[0] == 0) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Invalid path");
        return -1;
    }

    canon_path = g_strdup(path);
    len = strlen(canon_path);
    if (canon_path[len-1] == '/')
        canon_path[len-1] = 0;

    ret = seaf_filelock_manager_mark_file_locked (seaf->filelock_mgr,
                                                  repo_id, canon_path, LOCKED_MANUAL);

    g_free (canon_path);
    return ret;
}

static int
seafile_mark_file_unlocked (const char *repo_id, const char *path, GError **error)
{
    char *canon_path = NULL;
    int len;
    int ret;

    if (!repo_id || !path) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return -1;
    }

    if (*path == '/')
        ++path;

    if (path[0] == 0) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Invalid path");
        return -1;
    }

    canon_path = g_strdup(path);
    len = strlen(canon_path);
    if (canon_path[len-1] == '/')
        canon_path[len-1] = 0;

    ret = seaf_filelock_manager_mark_file_unlocked (seaf->filelock_mgr,
                                                    repo_id, path);

    g_free (canon_path);
    return ret;
}

static int
seafile_cancel_download (const char *server, const char *user, const char *full_file_path, GError **error)
{
    if (!full_file_path) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return -1;
    }

    return file_cache_mgr_cancel_download (seaf->file_cache_mgr, server, user, full_file_path, error);
}

static json_t *
seafile_list_sync_errors (GError **error)
{
    return seaf_sync_manager_list_sync_errors (seaf->sync_mgr);
}

static int
seafile_cache_path (const char *repo_id, const char *path, GError **error)
{
    char *canon_path = NULL;
    int len;

    if (!repo_id || !path) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return -1;
    }

    if (*path == '/')
        ++path;

    canon_path = g_strdup(path);
    len = strlen(canon_path);
    if (len > 0) {
        if (canon_path[len-1] == '/')
            canon_path[len-1] = 0;
    }

    seaf_sync_manager_cache_path (seaf->sync_mgr, repo_id, canon_path);

    g_free (canon_path);
    return 0;
}

int seafile_uncache_path (const char *repo_id, const char *path, GError **error)
{
    return file_cache_mgr_uncache_path(repo_id, path);
}

static json_t *
seafile_get_enc_repo_list (GError **error)
{
    GList *repos = seaf_repo_manager_get_enc_repo_list (seaf->repo_mgr, -1, -1);

    json_t *array, *obj;

    array = json_array ();

    GList *ptr;
    for (ptr = repos; ptr != NULL; ptr = ptr->next) {
        SeafRepo *repo = ptr->data;
        char *display_name = NULL;
        obj = json_object ();

        display_name = seaf_repo_manager_get_repo_display_name (seaf->repo_mgr,repo->id);
        //used to determine whether the repo is in the current account when switch account.
        if (!display_name)
            continue;
        json_object_set_new (obj, "server", json_string(repo->server));
        json_object_set_new (obj, "username", json_string(repo->user));
        json_object_set_new (obj, "repo_id", json_string(repo->id));
        json_object_set_new (obj, "repo_display_name", json_string(display_name));
        if (repo->is_passwd_set)
            json_object_set_new (obj, "is_passwd_set", json_true());
        else
            json_object_set_new (obj, "is_passwd_set", json_false());
        json_array_append_new (array, obj);

        seaf_repo_unref ((SeafRepo *)ptr->data);
    }

    g_list_free (repos);

    return array;
}

static int
seafile_set_enc_repo_passwd (const char *repo_id, const char *passwd, GError **error)
{
    SeafRepo *repo;
    int ret = 0;

    if (!repo_id || !passwd) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return -1;
    }

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "No such library");
        return -1;
    }

    if (repo->pwd_hash_algo) {
        if (seafile_pwd_hash_verify_repo_passwd (repo->enc_version, repo_id, passwd,
                                                 repo->salt, repo->pwd_hash,
                                                 repo->pwd_hash_algo, repo->pwd_hash_params) < 0) {
            g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Wrong password");
            ret = -1;
            goto out;
        }

    } else {
        if (seafile_verify_repo_passwd (repo_id, passwd, repo->magic,
                                        repo->enc_version, repo->salt) < 0) {
            g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Wrong password");
            ret = -1;
            goto out;
        }
    }

    ret = seaf_repo_manager_set_repo_passwd (seaf->repo_mgr, repo, passwd);
    if (ret < 0) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Failed to insert data into db");
        goto out;
    }

out:
    seaf_repo_unref (repo);
    return ret;
}

static int
seafile_clear_enc_repo_passwd (const char *repo_id, GError **error)
{
    int ret = seaf_repo_manager_clear_enc_repo_passwd (repo_id);
    if (ret < 0) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Failed to delete data from db");
    }

    return ret;
}

static int
seafile_add_del_confirmation (const char *confirmation_id, int resync, GError **error)
{
    return seaf_sync_manager_add_del_confirmation (seaf->sync_mgr, confirmation_id, resync);
}

static json_t *
seafile_get_account_by_repo_id (const char *repo_id, GError **error)
{
    return seaf_repo_manager_get_account_by_repo_id (seaf->repo_mgr, repo_id);
}

static char *
seafile_ping (GError **error)
{
    return g_strdup ("pong");
}

#if 0

static GObject *
seafile_generate_magic_and_random_key(int enc_version,
                                      const char* repo_id,
                                      const char *passwd,
                                      GError **error)
{
    if (!repo_id || !passwd) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return NULL;
    }

    gchar magic[65] = {0};
    gchar random_key[97] = {0};

    seafile_generate_magic (CURRENT_ENC_VERSION, repo_id, passwd, magic);
    seafile_generate_random_key (passwd, random_key);

    SeafileEncryptionInfo *sinfo;
    sinfo = g_object_new (SEAFILE_TYPE_ENCRYPTION_INFO,
                          "repo_id", repo_id,
                          "passwd", passwd,
                          "enc_version", CURRENT_ENC_VERSION,
                          "magic", magic,
                          "random_key", random_key,
                          NULL);

    return (GObject *)sinfo;

}

#include "diff-simple.h"

inline static const char*
get_diff_status_str(char status)
{
    if (status == DIFF_STATUS_ADDED)
        return "add";
    if (status == DIFF_STATUS_DELETED)
        return "del";
    if (status == DIFF_STATUS_MODIFIED)
        return "mod";
    if (status == DIFF_STATUS_RENAMED)
        return "mov";
    if (status == DIFF_STATUS_DIR_ADDED)
        return "newdir";
    if (status == DIFF_STATUS_DIR_DELETED)
        return "deldir";
    return NULL;
}

static GList *
seafile_diff (const char *repo_id, const char *arg1, const char *arg2, int fold_dir_diff, GError **error)
{
    SeafRepo *repo;
    char *err_msgs = NULL;
    GList *diff_entries, *p;
    GList *ret = NULL;

    if (!repo_id || !arg1 || !arg2) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Argument should not be null");
        return NULL;
    }

    if (!is_uuid_valid (repo_id)) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Invalid repo id");
        return NULL;
    }

    if ((arg1[0] != 0 && !is_object_id_valid (arg1)) || !is_object_id_valid(arg2)) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Invalid commit id");
        return NULL;
    }

    repo = seaf_repo_manager_get_repo (seaf->repo_mgr, repo_id);
    if (!repo) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "No such repository");
        return NULL;
    }

    diff_entries = seaf_repo_diff (repo, arg1, arg2, fold_dir_diff, &err_msgs);
    if (err_msgs) {
        g_set_error (error, SEAFILE_DOMAIN, -1, "%s", err_msgs);
        g_free (err_msgs);
        seaf_repo_unref (repo);
        return NULL;
    }

    seaf_repo_unref (repo);

    for (p = diff_entries; p != NULL; p = p->next) {
        DiffEntry *de = p->data;
        SeafileDiffEntry *entry = g_object_new (
            SEAFILE_TYPE_DIFF_ENTRY,
            "status", get_diff_status_str(de->status),
            "name", de->name,
            "new_name", de->new_name,
            NULL);
        ret = g_list_prepend (ret, entry);
    }

    for (p = diff_entries; p != NULL; p = p->next) {
        DiffEntry *de = p->data;
        diff_entry_free (de);
    }
    g_list_free (diff_entries);

    return g_list_reverse (ret);
}

#endif


static void
register_rpc_service ()
{
    searpc_server_init (register_marshals);

    searpc_create_service ("seadrive-rpcserver");

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_add_account,
                                     "seafile_add_account",
                                     searpc_signature_int__string_string_string_string_string_int());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_delete_account,
                                     "seafile_delete_account",
                                     searpc_signature_int__string_string_int());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_config,
                                     "seafile_get_config",
                                     searpc_signature_string__string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_set_config,
                                     "seafile_set_config",
                                     searpc_signature_int__string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_config_int,
                                     "seafile_get_config_int",
                                     searpc_signature_int__string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_set_config_int,
                                     "seafile_set_config_int",
                                     searpc_signature_int__string_int());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_set_upload_rate_limit,
                                     "seafile_set_upload_rate_limit",
                                     searpc_signature_int__int());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_set_download_rate_limit,
                                     "seafile_set_download_rate_limit",
                                     searpc_signature_int__int());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_upload_rate,
                                     "seafile_get_upload_rate",
                                     searpc_signature_int__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_download_rate,
                                     "seafile_get_download_rate",
                                     searpc_signature_int__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_clean_cache_interval,
                                     "seafile_get_clean_cache_interval",
                                     searpc_signature_int__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_set_clean_cache_interval,
                                     "seafile_set_clean_cache_interval",
                                     searpc_signature_int__int());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_cache_size_limit,
                                     "seafile_get_cache_size_limit",
                                     searpc_signature_int64__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_is_file_cached,
                                     "seafile_is_file_cached",
                                     searpc_signature_int__string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_set_cache_size_limit,
                                     "seafile_set_cache_size_limit",
                                     searpc_signature_int__int64());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_global_sync_status,
                                     "seafile_get_global_sync_status",
                                     searpc_signature_json__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_disable_auto_sync,
                                     "seafile_disable_auto_sync",
                                     searpc_signature_int__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_enable_auto_sync,
                                     "seafile_enable_auto_sync",
                                     searpc_signature_int__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_path_sync_status,
                                     "seafile_get_path_sync_status",
                                     searpc_signature_string__string_string_string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_sync_notification,
                                     "seafile_get_sync_notification",
                                     searpc_signature_json__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_events_notification,
                                     "seafile_get_events_notification",
                                     searpc_signature_json__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_repo_id_by_uname,
                                     "seafile_get_repo_id_by_uname",
                                     searpc_signature_string__string_string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_repo_display_name_by_id,
                                     "seafile_get_repo_display_name_by_id",
                                     searpc_signature_string__string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_unmount,
                                     "seafile_unmount",
                                     searpc_signature_int__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_upload_progress,
                                     "seafile_get_upload_progress",
                                     searpc_signature_json__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_download_progress,
                                     "seafile_get_download_progress",
                                     searpc_signature_json__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_mark_file_locked,
                                     "seafile_mark_file_locked",
                                     searpc_signature_int__string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_mark_file_unlocked,
                                     "seafile_mark_file_unlocked",
                                     searpc_signature_int__string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_cancel_download,
                                     "seafile_cancel_download",
                                     searpc_signature_int__string_string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_list_sync_errors,
                                     "seafile_list_sync_errors",
                                     searpc_signature_json__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_cache_path,
                                     "seafile_cache_path",
                                     searpc_signature_int__string_string());
    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_uncache_path,
                                     "seafile_uncache_path",
                                     searpc_signature_int__string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_enc_repo_list,
                                     "seafile_get_enc_repo_list",
                                     searpc_signature_json__void());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_set_enc_repo_passwd,
                                     "seafile_set_enc_repo_passwd",
                                     searpc_signature_int__string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_clear_enc_repo_passwd,
                                     "seafile_clear_enc_repo_passwd",
                                     searpc_signature_int__string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_add_del_confirmation,
                                     "seafile_add_del_confirmation",
                                     searpc_signature_int__string_int());
    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_get_account_by_repo_id,
                                     "seafile_get_account_by_repo_id",
                                     searpc_signature_json__string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_ping,
                                     "seafile_ping",
                                     searpc_signature_string__void());

#if 0
    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_generate_magic_and_random_key,
                                     "seafile_generate_magic_and_random_key",
                                     searpc_signature_object__int_string_string());

    searpc_server_register_function ("seadrive-rpcserver",
                                     seafile_diff,
                                     "seafile_diff",
                                     searpc_signature_objlist__string_string_string_int());
#endif
}

#define SEADRIVE_SOCKET_NAME "seadrive.sock"


int
start_searpc_server ()
{
    register_rpc_service ();

    char *path = g_build_filename (seaf->seaf_dir, SEADRIVE_SOCKET_NAME, NULL);

    SearpcNamedPipeServer *server = searpc_create_named_pipe_server (path);
    if (!server) {
        seaf_warning ("Failed to create named pipe server.\n");
        g_free (path);
        return -1;
    }

    seaf->rpc_socket_path = path;

    return searpc_named_pipe_server_start (server);
}
