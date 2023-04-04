/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef SEAFILE_CONFIG_H
#define SEAFILE_CONFIG_H

#include "seafile-session.h"
#include "db.h"

#define KEY_CLIENT_ID "client_id"
#define KEY_CLIENT_NAME "client_name"

#define KEY_UPLOAD_LIMIT "upload_limit"
#define KEY_DOWNLOAD_LIMIT "download_limit"
#define KEY_ALLOW_REPO_NOT_FOUND_ON_SERVER "allow_repo_not_found_on_server"
#define KEY_SYNC_EXTRA_TEMP_FILE "sync_extra_temp_file"

/* Http sync settings. */
#define KEY_ENABLE_HTTP_SYNC "enable_http_sync"
#define KEY_DISABLE_VERIFY_CERTIFICATE "disable_verify_certificate"

/* Http sync proxy settings. */
#define KEY_USE_PROXY "use_proxy"
#define KEY_PROXY_TYPE "proxy_type"
#define KEY_PROXY_ADDR "proxy_addr"
#define KEY_PROXY_PORT "proxy_port"
#define KEY_PROXY_USERNAME "proxy_username"
#define KEY_PROXY_PASSWORD "proxy_password"
#define PROXY_TYPE_HTTP "http"
#define PROXY_TYPE_SOCKS "socks"

/* Cache cleaning settings. */
#define KEY_CACHE_SIZE_LIMIT "cache_size_limit"
#define KEY_CLEAN_CACHE_INTERVAL "clean_cache_interval"

#define KEY_ENABLE_AUTO_LOCK "enable_auto_lock"

#define KEY_CURRENT_SESSION_ACCESS "current_session_access"

#define DELETE_CONFIRM_THRESHOLD "delete_confirm_threshold"

#define KEY_HIDE_WINDOWS_INCOMPATIBLE_PATH_NOTIFICATION "hide_windows_incompatible_path_notification"

gboolean
seafile_session_config_exists (SeafileSession *session, const char *key);

/*
 * Returns: config value in string. The string should be freed by caller. 
 */
char *
seafile_session_config_get_string (SeafileSession *session,
                                   const char *key);

/*
 * Returns:
 * If key exists, @exists will be set to TRUE and returns the value;
 * otherwise, @exists will be set to FALSE and returns -1.
 */
int
seafile_session_config_get_int (SeafileSession *session,
                                const char *key,
                                gboolean *exists);

gint64
seafile_session_config_get_int64 (SeafileSession *session,
                                  const char *key,
                                  gboolean *exists);

/*
 * Returns: config value in boolean. Return FALSE if the value is not configured. 
 */
gboolean
seafile_session_config_get_bool (SeafileSession *session,
                                 const char *key);


int
seafile_session_config_set_string (SeafileSession *session,
                                   const char *key,
                                   const char *value);

int
seafile_session_config_set_int (SeafileSession *session,
                                const char *key,
                                int value);

int
seafile_session_config_set_int64 (SeafileSession *session,
                                  const char *key,
                                  gint64 value);

gboolean
seafile_session_config_get_allow_repo_not_found_on_server(SeafileSession *session);

sqlite3 *
seafile_session_config_open_db (const char *db_path);


#endif
