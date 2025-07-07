/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#include "common.h"

#include <pthread.h>
#include <curl/curl.h>
#include <event2/buffer.h>

#ifndef USE_GPL_CRYPTO
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/bio.h>
#include <openssl/ssl.h>
#endif

#include "seafile-config.h"

#include "seafile-session.h"
#include "http-tx-mgr.h"

#include "seafile-error.h"
#include "utils.h"
#include "diff-simple.h"
#include "job-mgr.h"
#include "timer.h"

#define DEBUG_FLAG SEAFILE_DEBUG_TRANSFER
#include "log.h"

#define HTTP_OK 200
#define HTTP_RES_PARTIAL 206
#define HTTP_MOVED_PERMANENTLY 301
#define HTTP_BAD_REQUEST 400
#define HTTP_UNAUTHORIZED 401
#define HTTP_FORBIDDEN 403
#define HTTP_NOT_FOUND 404
#define HTTP_REQUEST_TIME_OUT 408
#define HTTP_NO_QUOTA 443
#define HTTP_REPO_DELETED 444
#define HTTP_REPO_CORRUPTED 445
#define HTTP_BLOCK_MISSING 446
#define HTTP_REPO_TOO_LARGE 447
#define HTTP_INTERNAL_SERVER_ERROR 500

#define RESET_BYTES_INTERVAL_MSEC 1000

#define CLEAR_POOL_ERR_CNT 3

#ifndef SEAFILE_CLIENT_VERSION
#define SEAFILE_CLIENT_VERSION PACKAGE_VERSION
#endif

#ifdef __APPLE__
#define USER_AGENT_OS "Apple OS X"
#endif

#ifdef __linux__
#define USER_AGENT_OS "Linux"
#endif

struct _Connection {
    CURL *curl;
    gint64 ctime;               /* Used to clean up unused connection. */
    gboolean release;           /* If TRUE, the connection will be released. */
};
typedef struct _Connection Connection;

struct _ConnectionPool {
    char *host;
    GQueue *queue;
    pthread_mutex_t lock;
    int err_cnt;
};
typedef struct _ConnectionPool ConnectionPool;

struct _HttpTxPriv {
    GHashTable *download_tasks;
    GHashTable *upload_tasks;

    GHashTable *connection_pools; /* host -> connection pool */
    pthread_mutex_t pools_lock;

    SeafTimer *reset_bytes_timer;

    char *env_ca_bundle_path;

    // Uploaded file count
    gint uploaded;
    // Upload failed count
    gint upload_err;
    // Total file count need to upload
    gint total_upload;
    // Uploading files: file_path <-> FileUploadProgress
    GHashTable *uploading_files;
    // Uploaded files
    GQueue *uploaded_files;
    pthread_mutex_t progress_lock;
    /* Regex to parse error message returned by update-branch. */
    GRegex *locked_error_regex;
    GRegex *folder_perm_error_regex;
    GRegex *too_many_files_error_regex;
};
typedef struct _HttpTxPriv HttpTxPriv;

/* Http Tx Task */

static HttpTxTask *
http_tx_task_new (HttpTxManager *mgr,
                  const char *repo_id,
                  int repo_version,
                  int type,
                  gboolean is_clone,
                  const char *host,
                  const char *token)
{
    HttpTxTask *task = g_new0 (HttpTxTask, 1);

    task->manager = mgr;
    memcpy (task->repo_id, repo_id, 36);
    task->repo_version = repo_version;
    task->type = type;
    task->is_clone = is_clone;

    task->host = g_strdup(host);
    task->token = g_strdup(token);

    return task;
}

static void
http_tx_task_free (HttpTxTask *task)
{
    if (task->repo_uname)
        g_free (task->repo_uname);
    g_free (task->unsyncable_path);
    g_free (task->host);
    g_free (task->token);
    g_free (task->server);
    g_free (task->user);
    g_free (task);
}

static const char *http_task_state_str[] = {
    "normal",
    "canceled",
    "finished",
    "error",
};

static const char *http_task_rt_state_str[] = {
    "init",
    "check",
    "commit",
    "fs",
    "data",
    "update-branch",
    "finished",
};

static const char *http_task_error_strs[] = {
    "Successful",
    "Permission denied on server",
    "Network error",
    "Cannot resolve proxy address",
    "Cannot resolve server address",
    "Cannot connect to server",
    "Failed to establish secure connection",
    "Data transfer was interrupted",
    "Data transfer timed out",
    "Unhandled http redirect from server",
    "Server error",
    "Bad request",
    "Internal data corrupt on the client",
    "Not enough memory",
    "Failed to write data on the client",
    "Storage quota full",
    "Files are locked by other application",
    "Library deleted on server",
    "Library damaged on server",
    "Unknown error",
};

/* Http connection and connection pool. */

static Connection *
connection_new ()
{
    Connection *conn = g_new0 (Connection, 1);

    conn->curl = curl_easy_init();
    conn->ctime = (gint64)time(NULL);

    return conn;
}

static void
connection_free (Connection *conn)
{
    curl_easy_cleanup (conn->curl);
    g_free (conn);
}

static ConnectionPool *
connection_pool_new (const char *host)
{
    ConnectionPool *pool = g_new0 (ConnectionPool, 1);
    pool->host = g_strdup(host);
    pool->queue = g_queue_new ();
    pthread_mutex_init (&pool->lock, NULL);
    return pool;
}

static ConnectionPool *
find_connection_pool (HttpTxPriv *priv, const char *host)
{
    ConnectionPool *pool;

    pthread_mutex_lock (&priv->pools_lock);
    pool = g_hash_table_lookup (priv->connection_pools, host);
    if (!pool) {
        pool = connection_pool_new (host);
        g_hash_table_insert (priv->connection_pools, g_strdup(host), pool);
    }
    pthread_mutex_unlock (&priv->pools_lock);

    return pool;
}

static Connection *
connection_pool_get_connection (ConnectionPool *pool)
{
    Connection *conn = NULL;

    pthread_mutex_lock (&pool->lock);
    conn = g_queue_pop_head (pool->queue);
    if (!conn) {
        conn = connection_new ();
    }
    pthread_mutex_unlock (&pool->lock);

    return conn;
}

static void
connection_pool_clear (ConnectionPool *pool)
{
    Connection *conn = NULL;

    while (1) {
        conn = g_queue_pop_head (pool->queue);
        if (!conn)
            break;
        connection_free (conn);
    }
}

static void
connection_pool_return_connection (ConnectionPool *pool, Connection *conn)
{
    if (!conn)
        return;

    if (conn->release) {
        connection_free (conn);

        pthread_mutex_lock (&pool->lock);
        if (++pool->err_cnt >= CLEAR_POOL_ERR_CNT) {
            connection_pool_clear (pool);
        }
        pthread_mutex_unlock (&pool->lock);

        return;
    }

    curl_easy_reset (conn->curl);

    /* Reset error count when one connection succeeded. */
    pthread_mutex_lock (&pool->lock);
    pool->err_cnt = 0;
    g_queue_push_tail (pool->queue, conn);
    pthread_mutex_unlock (&pool->lock);
}

#define LOCKED_ERROR_PATTERN "File (.+) is locked"
#define FOLDER_PERM_ERROR_PATTERN "Update to path (.+) is not allowed by folder permission settings"
#define TOO_MANY_FILES_ERROR_PATTERN "Too many files in library"

HttpTxManager *
http_tx_manager_new (struct _SeafileSession *seaf)
{
    HttpTxManager *mgr = g_new0 (HttpTxManager, 1);
    HttpTxPriv *priv = g_new0 (HttpTxPriv, 1);
    const char *env_ca_path = NULL;

    mgr->seaf = seaf;

    priv->download_tasks = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                  g_free,
                                                  (GDestroyNotify)http_tx_task_free);
    priv->upload_tasks = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                g_free,
                                                (GDestroyNotify)http_tx_task_free);

    priv->connection_pools = g_hash_table_new (g_str_hash, g_str_equal);
    pthread_mutex_init (&priv->pools_lock, NULL);

    env_ca_path = g_getenv("SEAFILE_SSL_CA_PATH");
    if (env_ca_path)
        priv->env_ca_bundle_path = g_strdup (env_ca_path);

    priv->uploading_files = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);
    priv->uploaded_files = g_queue_new ();
    pthread_mutex_init (&priv->progress_lock, NULL);

    GError *error = NULL;
    priv->locked_error_regex = g_regex_new (LOCKED_ERROR_PATTERN, 0, 0, &error);
    if (error) {
        seaf_warning ("Failed to create regex '%s': %s\n", LOCKED_ERROR_PATTERN, error->message);
        g_clear_error (&error);
    }

    priv->folder_perm_error_regex = g_regex_new (FOLDER_PERM_ERROR_PATTERN, 0, 0, &error);
    if (error) {
        seaf_warning ("Failed to create regex '%s': %s\n", FOLDER_PERM_ERROR_PATTERN, error->message);
        g_clear_error (&error);
    }

    priv->too_many_files_error_regex = g_regex_new (TOO_MANY_FILES_ERROR_PATTERN, 0, 0, &error);
    if (error) {
        seaf_warning ("Failed to create regex '%s': %s\n", TOO_MANY_FILES_ERROR_PATTERN, error->message);
        g_clear_error (&error);
    }

    mgr->priv = priv;

    return mgr;
}

static int
reset_bytes (void *vdata)
{
    HttpTxManager *mgr = vdata;
    HttpTxPriv *priv = mgr->priv;
    GHashTableIter iter;
    gpointer key, value;
    HttpTxTask *task;

    g_hash_table_iter_init (&iter, priv->download_tasks);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        task = value;
        task->last_tx_bytes = g_atomic_int_get (&task->tx_bytes);
        g_atomic_int_set (&task->tx_bytes, 0);
    }

    g_hash_table_iter_init (&iter, priv->upload_tasks);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        task = value;
        task->last_tx_bytes = g_atomic_int_get (&task->tx_bytes);
        g_atomic_int_set (&task->tx_bytes, 0);
    }

    return 1;
}

/* static void * */
/* save_uploaded_file_list_worker (void *data); */

/* static void */
/* load_uploaded_file_list (HttpTxManager *mgr); */

int
http_tx_manager_start (HttpTxManager *mgr)
{
    /* TODO: add a timer to clean up unused Http connections. */

    mgr->priv->reset_bytes_timer = seaf_timer_new (reset_bytes,
                                                   mgr,
                                                   RESET_BYTES_INTERVAL_MSEC);

    /* load_uploaded_file_list (mgr); */

    /* pthread_t tid; */
    /* int rc; */

    /* rc = pthread_create (&tid, NULL, save_uploaded_file_list_worker, mgr); */
    /* if (rc != 0) { */
    /*     seaf_warning ("Failed to create save uploaded files worker thread.\n"); */
    /* } */

    return 0;
}

/* Common Utility Functions. */

#ifndef USE_GPL_CRYPTO

char *ca_paths[] = {
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/ssl/certs/ca-bundle.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/usr/share/ssl/certs/ca-bundle.crt",
    "/usr/local/share/certs/ca-root-nss.crt",
    "/etc/ssl/cert.pem",
};

static void
load_ca_bundle(CURL *curl)
{
    const char *env_ca_path = seaf->http_tx_mgr->priv->env_ca_bundle_path;
    int i;
    const char *ca_path;
    gboolean found = FALSE;

    for (i = 0; i < sizeof(ca_paths) / sizeof(ca_paths[0]); i++) {
        ca_path = ca_paths[i];
        if (seaf_util_exists (ca_path)) {
            found = TRUE;
            break;
        }
    }

    if (env_ca_path) {
        if (seaf_util_exists (env_ca_path)) {
            curl_easy_setopt (curl, CURLOPT_CAINFO, env_ca_path);
            return;
        }
    }

    if (found)
        curl_easy_setopt (curl, CURLOPT_CAINFO, ca_path);
}

#endif  /* USE_GPL_CRYPTO */

static void
set_proxy (CURL *curl, gboolean is_https)
{
    /* Disable proxy if proxy options are not set properly. */
    if (!seaf->use_http_proxy || !seaf->http_proxy_type || !seaf->http_proxy_addr) {
        curl_easy_setopt (curl, CURLOPT_PROXY, NULL);
        return;
    }

    if (g_strcmp0(seaf->http_proxy_type, PROXY_TYPE_HTTP) == 0) {
        curl_easy_setopt(curl, CURLOPT_PROXYTYPE, CURLPROXY_HTTP);
        /* Use CONNECT method create a SSL tunnel if https is used. */
        if (is_https)
            curl_easy_setopt(curl, CURLOPT_HTTPPROXYTUNNEL, 1L);
        curl_easy_setopt(curl, CURLOPT_PROXY, seaf->http_proxy_addr);
        curl_easy_setopt(curl, CURLOPT_PROXYPORT,
                         seaf->http_proxy_port > 0 ? seaf->http_proxy_port : 80);
        if (seaf->http_proxy_username && seaf->http_proxy_password) {
            curl_easy_setopt(curl, CURLOPT_PROXYAUTH,
                             CURLAUTH_BASIC |
                             CURLAUTH_DIGEST |
                             CURLAUTH_DIGEST_IE |
                             CURLAUTH_GSSNEGOTIATE |
                             CURLAUTH_NTLM);
            curl_easy_setopt(curl, CURLOPT_PROXYUSERNAME, seaf->http_proxy_username);
            curl_easy_setopt(curl, CURLOPT_PROXYPASSWORD, seaf->http_proxy_password);
        }
    } else if (g_strcmp0(seaf->http_proxy_type, PROXY_TYPE_SOCKS) == 0) {
        if (seaf->http_proxy_port < 0)
            return;
        curl_easy_setopt(curl, CURLOPT_PROXYTYPE, CURLPROXY_SOCKS5_HOSTNAME);
        curl_easy_setopt(curl, CURLOPT_PROXY, seaf->http_proxy_addr);
        curl_easy_setopt(curl, CURLOPT_PROXYPORT, seaf->http_proxy_port);
        if (seaf->http_proxy_username && g_strcmp0 (seaf->http_proxy_username, "") != 0)
            curl_easy_setopt(curl, CURLOPT_PROXYUSERNAME, seaf->http_proxy_username);
        if (seaf->http_proxy_password && g_strcmp0 (seaf->http_proxy_password, "") != 0)
            curl_easy_setopt(curl, CURLOPT_PROXYPASSWORD, seaf->http_proxy_password);
    }
}

typedef struct _HttpResponse {
    char *content;
    size_t size;
} HttpResponse;

static size_t
recv_response (void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    HttpResponse *rsp = userp;

    rsp->content = g_realloc (rsp->content, rsp->size + realsize);
    if (!rsp->content) {
        seaf_warning ("Not enough memory.\n");
        /* return a value other than realsize to signify an error. */
        return 0;
    }

    memcpy (rsp->content + rsp->size, contents, realsize);
    rsp->size += realsize;

    return realsize;
}

#define FS_ID_LIST_TIMEOUT_SEC 1800
/* 5 minutes timeout for background requests. */
#define HTTP_TIMEOUT_SEC 300
/* 5 seconds timeout for foreground requests that can impact UI response time. */
#define REPO_OPER_TIMEOUT 5

/*
 * The @timeout parameter is for detecting network connection problems. 
 * The @timeout parameter should be set to TRUE for data-transfer-only operations,
 * such as getting objects, blocks. For operations that requires calculations
 * on the server side, the timeout should be set to FALSE. Otherwise when
 * the server sometimes takes more than 45 seconds to calculate the result,
 * the client will time out.
 */
static int
http_get_common (CURL *curl, const char *url,
                 int *rsp_status, char **rsp_content, gint64 *rsp_size,
                 HttpRecvCallback callback, void *cb_data,
                 gboolean timeout, int timeout_sec,
                 int *pcurl_error)
{
    int ret = 0;

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    if (seafile_debug_flag_is_set (SEAFILE_DEBUG_CURL)) {
        curl_easy_setopt (curl, CURLOPT_VERBOSE, 1);
        curl_easy_setopt (curl, CURLOPT_STDERR, seafile_get_logfp());
    }

    if (timeout) {
        /* Set low speed limit to 1 bytes. This effectively means no data. */
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1);
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, timeout_sec);
    }

#ifndef USE_GPL_CRYPTO
    if (seaf->disable_verify_certificate) {
        curl_easy_setopt (curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt (curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
#endif

    HttpResponse rsp;
    memset (&rsp, 0, sizeof(rsp));
    if (rsp_content) {
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, recv_response);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &rsp);
    } else if (callback) {
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, cb_data);
    }

    gboolean is_https = (strncasecmp(url, "https", strlen("https")) == 0);
    set_proxy (curl, is_https);

    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_UNRESTRICTED_AUTH, 1L);

#ifndef USE_GPL_CRYPTO
    load_ca_bundle (curl);
#endif

    int rc = curl_easy_perform (curl);
    if (rc != 0) {
        if (rc != CURLE_WRITE_ERROR)
            seaf_warning ("libcurl failed to GET %s: %s.\n",
                          url, curl_easy_strerror(rc));
        if (pcurl_error)
            *pcurl_error = rc;
        ret = -1;
        goto out;
    }

    long status;
    rc = curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &status);
    if (rc != CURLE_OK) {
        seaf_warning ("Failed to get status code for GET %s.\n", url);
        ret = -1;
        goto out;
    }

    *rsp_status = status;

    if (rsp_content) {
        *rsp_content = rsp.content;
        *rsp_size = rsp.size;
    }

out:
    if (ret < 0)
        g_free (rsp.content);
    return ret;
}

static int
http_get (CURL *curl, const char *url, const char *token,
          int *rsp_status, char **rsp_content, gint64 *rsp_size,
          HttpRecvCallback callback, void *cb_data,
          gboolean timeout, int timeout_sec,
          int *pcurl_error)
{
    char *token_header;
    struct curl_slist *headers = NULL;
    int ret;

    headers = curl_slist_append (headers, "User-Agent: SeaDrive/"SEAFILE_CLIENT_VERSION" ("USER_AGENT_OS")");

    if (token) {
        token_header = g_strdup_printf ("Seafile-Repo-Token: %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
        token_header = g_strdup_printf ("Authorization: Token %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    ret = http_get_common (curl, url, rsp_status, rsp_content, rsp_size,
                           callback, cb_data, timeout, timeout_sec, pcurl_error);

    curl_slist_free_all (headers);
    return ret;
}

static int
http_get_range (CURL *curl, const char *url, const char *token,
                int *rsp_status, char **rsp_content, gint64 *rsp_size,
                HttpRecvCallback callback, void *cb_data,
                gboolean timeout, int timeout_sec,
                guint64 start, guint64 end)
{
    char *token_header, *range_header;
    struct curl_slist *headers = NULL;
    int ret;

    headers = curl_slist_append (headers, "User-Agent: SeaDrive/"SEAFILE_CLIENT_VERSION" ("USER_AGENT_OS")");

    if (token) {
        token_header = g_strdup_printf ("Seafile-Repo-Token: %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
        token_header = g_strdup_printf ("Authorization: Token %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
    }

    range_header = g_strdup_printf ("Range: bytes=%"G_GUINT64_FORMAT"-%"G_GUINT64_FORMAT,
                                    start, end);
    headers = curl_slist_append (headers, range_header);
    g_free (range_header);

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    ret = http_get_common (curl, url, rsp_status, rsp_content, rsp_size,
                           callback, cb_data, timeout, timeout_sec, NULL);

    curl_slist_free_all (headers);
    return ret;
}

static int
http_api_get (CURL *curl, const char *url, const char *token,
              int *rsp_status, char **rsp_content, gint64 *rsp_size,
              HttpRecvCallback callback, void *cb_data,
              gboolean timeout, int timeout_sec,
              int *pcurl_error)
{
    char *token_header;
    struct curl_slist *headers = NULL;
    int ret;

    headers = curl_slist_append (headers, "User-Agent: SeaDrive/"SEAFILE_CLIENT_VERSION" ("USER_AGENT_OS")");

    if (token) {
        token_header = g_strdup_printf ("Authorization: Token %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    ret = http_get_common (curl, url, rsp_status, rsp_content, rsp_size,
                           callback, cb_data, timeout, timeout_sec, pcurl_error);

    curl_slist_free_all (headers);
    return ret;
}

typedef struct _HttpRequest {
    const char *content;
    const char *origin_content;
    size_t size;
    size_t origin_size;
} HttpRequest;

static size_t
send_request (void *ptr, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size *nmemb;
    size_t copy_size;
    HttpRequest *req = userp;

    if (req->size == 0)
        return 0;

    copy_size = MIN(req->size, realsize);
    memcpy (ptr, req->content, copy_size);
    req->size -= copy_size;
    req->content = req->content + copy_size;

    return copy_size;
}

// Only handle rewinding caused by redirects. In this case, offset and origin should both be 0.
// Do not handle rewinds triggered by other causes.
static size_t
rewind_http_request (void *clientp, curl_off_t offset, int origin)
{
    HttpRequest *req = clientp;
    if (offset == 0 && origin == 0) {
        req->content = req->origin_content;
        req->size = req->origin_size;
        return CURL_SEEKFUNC_OK;
    }

    return CURL_SEEKFUNC_FAIL;
}

typedef size_t (*HttpRewindCallback) (void *, curl_off_t, int);

typedef size_t (*HttpSendCallback) (void *, size_t, size_t, void *);

static int
http_put (CURL *curl, const char *url, const char *token,
          const char *req_content, gint64 req_size,
          HttpSendCallback callback, void *cb_data, HttpRewindCallback rewind_cb,
          int *rsp_status, char **rsp_content, gint64 *rsp_size,
          gboolean timeout,
          int *pcurl_error)
{
    char *token_header;
    struct curl_slist *headers = NULL;
    int ret = 0;

    if (seafile_debug_flag_is_set (SEAFILE_DEBUG_CURL)) {
        curl_easy_setopt (curl, CURLOPT_VERBOSE, 1);
        curl_easy_setopt (curl, CURLOPT_STDERR, seafile_get_logfp());
    }

    headers = curl_slist_append (headers, "User-Agent: SeaDrive/"SEAFILE_CLIENT_VERSION" ("USER_AGENT_OS")");
    /* Disable the default "Expect: 100-continue" header */
    headers = curl_slist_append (headers, "Expect:");

    if (req_content)
        headers = curl_slist_append (headers, "Content-Type: application/octet-stream");

    if (token) {
        token_header = g_strdup_printf ("Seafile-Repo-Token: %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
        token_header = g_strdup_printf ("Authorization: Token %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);

    if (timeout) {
        /* Set low speed limit to 1 bytes. This effectively means no data. */
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1);
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, HTTP_TIMEOUT_SEC);
    }

#ifndef USE_GPL_CRYPTO
    if (seaf->disable_verify_certificate) {
        curl_easy_setopt (curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt (curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
#endif

    HttpRequest req;
    if (req_content) {
        memset (&req, 0, sizeof(req));
        req.content = req_content;
        req.origin_content = req_content;
        req.size = req_size;
        req.origin_size = req_size;
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, send_request);
        curl_easy_setopt(curl, CURLOPT_READDATA, &req);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)req_size);

        curl_easy_setopt(curl, CURLOPT_SEEKFUNCTION, rewind_http_request);
        curl_easy_setopt(curl, CURLOPT_SEEKDATA, &req);
    } else if (callback != NULL) {
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, callback);
        curl_easy_setopt(curl, CURLOPT_READDATA, cb_data);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)req_size);

        if (rewind_cb != NULL) {
            curl_easy_setopt(curl, CURLOPT_SEEKFUNCTION, rewind_cb);
            curl_easy_setopt(curl, CURLOPT_SEEKDATA, cb_data);
        }
    } else {
        curl_easy_setopt (curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)0);
    }

    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    HttpResponse rsp;
    memset (&rsp, 0, sizeof(rsp));
    if (rsp_content) {
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, recv_response);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &rsp);
    }

    gboolean is_https = (strncasecmp(url, "https", strlen("https")) == 0);
    set_proxy (curl, is_https);

    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

#ifndef USE_GPL_CRYPTO
    load_ca_bundle (curl);
#endif

    int rc = curl_easy_perform (curl);
    if (rc != 0) {
        seaf_warning ("libcurl failed to PUT %s: %s.\n",
                      url, curl_easy_strerror(rc));
        if (pcurl_error)
            *pcurl_error = rc;
        ret = -1;
        goto out;
    }

    long status;
    rc = curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &status);
    if (rc != CURLE_OK) {
        seaf_warning ("Failed to get status code for PUT %s.\n", url);
        ret = -1;
        goto out;
    }

    *rsp_status = status;

    if (rsp_content) {
        *rsp_content = rsp.content;
        *rsp_size = rsp.size;
    }

out:
    if (ret < 0)
        g_free (rsp.content);
    curl_slist_free_all (headers);
    return ret;
}

static int
http_post_common (CURL *curl, const char *url,
                  const char *req_content, gint64 req_size,
                  int *rsp_status, char **rsp_content, gint64 *rsp_size,
                  gboolean timeout, int timeout_sec,
                  int *pcurl_error)
{
    int ret = 0;

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    if (seafile_debug_flag_is_set (SEAFILE_DEBUG_CURL)) {
        curl_easy_setopt (curl, CURLOPT_VERBOSE, 1);
        curl_easy_setopt (curl, CURLOPT_STDERR, seafile_get_logfp());
    }

    if (timeout) {
        /* Set low speed limit to 1 bytes. This effectively means no data. */
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1);
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, timeout_sec);
    }

#ifndef USE_GPL_CRYPTO
    if (seaf->disable_verify_certificate) {
        curl_easy_setopt (curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt (curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
#endif

    HttpRequest req;
    memset (&req, 0, sizeof(req));
    req.content = req_content;
    req.origin_content = req_content;
    req.size = req_size;
    req.origin_size = req_size;
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, send_request);
    curl_easy_setopt(curl, CURLOPT_READDATA, &req);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)req_size);

    curl_easy_setopt(curl, CURLOPT_SEEKFUNCTION, rewind_http_request);
    curl_easy_setopt(curl, CURLOPT_SEEKDATA, &req);

    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

    HttpResponse rsp;
    memset (&rsp, 0, sizeof(rsp));
    if (rsp_content) {
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, recv_response);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &rsp);
    }

#ifndef USE_GPL_CRYPTO
    load_ca_bundle (curl);
#endif

    gboolean is_https = (strncasecmp(url, "https", strlen("https")) == 0);
    set_proxy (curl, is_https);

    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTREDIR, CURL_REDIR_POST_ALL);

    int rc = curl_easy_perform (curl);
    if (rc != 0) {
        seaf_warning ("libcurl failed to POST %s: %s.\n",
                      url, curl_easy_strerror(rc));
        if (pcurl_error)
            *pcurl_error = rc;
        ret = -1;
        goto out;
    }

    long status;
    rc = curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &status);
    if (rc != CURLE_OK) {
        seaf_warning ("Failed to get status code for POST %s.\n", url);
        ret = -1;
        goto out;
    }

    *rsp_status = status;

    if (rsp_content) {
        *rsp_content = rsp.content;
        *rsp_size = rsp.size;
    }

out:
    if (ret < 0)
        g_free (rsp.content);
    return ret;
}

static int
http_post (CURL *curl, const char *url, const char *token,
           const char *req_content, gint64 req_size,
           int *rsp_status, char **rsp_content, gint64 *rsp_size,
           gboolean timeout, int timeout_sec,
           int *pcurl_error)
{
    char *token_header;
    struct curl_slist *headers = NULL;
    int ret;

    g_return_val_if_fail (req_content != NULL, -1);

    headers = curl_slist_append (headers, "User-Agent: SeaDrive/"SEAFILE_CLIENT_VERSION" ("USER_AGENT_OS")");
    /* Disable the default "Expect: 100-continue" header */
    headers = curl_slist_append (headers, "Expect:");

    if (req_content) {
        json_t *is_json = NULL;
        is_json = json_loads (req_content, 0, NULL);
        if (is_json) {
            headers = curl_slist_append (headers, "Content-Type: application/json");
            json_decref (is_json);
        } else
            headers = curl_slist_append (headers, "Content-Type: application/octet-stream");
    }

    if (token) {
        token_header = g_strdup_printf ("Seafile-Repo-Token: %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
        token_header = g_strdup_printf ("Authorization: Token %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    ret = http_post_common (curl, url, req_content, req_size,
                            rsp_status, rsp_content, rsp_size,
                            timeout, timeout_sec, pcurl_error);

    curl_slist_free_all (headers);
    return ret;
}

static int
http_api_post_common (CURL *curl, const char *url, const char *token,
                      const char *header,
                      const char *req_content, gint64 req_size,
                      int *rsp_status, char **rsp_content, gint64 *rsp_size,
                      gboolean timeout, int timeout_sec,
                      int *pcurl_error)
{
    char *token_header;
    struct curl_slist *headers = NULL;
    int ret;

    g_return_val_if_fail (req_content != NULL, -1);

    headers = curl_slist_append (headers, "User-Agent: SeaDrive/"SEAFILE_CLIENT_VERSION" ("USER_AGENT_OS")");
    /* Disable the default "Expect: 100-continue" header */
    headers = curl_slist_append (headers, "Expect:");

    if (req_content)
        headers = curl_slist_append (headers, header);

    if (token) {
        token_header = g_strdup_printf ("Authorization: Token %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    ret = http_post_common (curl, url, req_content, req_size,
                            rsp_status, rsp_content, rsp_size,
                            timeout, timeout_sec, pcurl_error);

    curl_slist_free_all (headers);
    return ret;
}

static int
http_api_post (CURL *curl, const char *url, const char *token,
               const char *req_content, gint64 req_size,
               int *rsp_status, char **rsp_content, gint64 *rsp_size,
               gboolean timeout, int timeout_sec,
               int *pcurl_error)
{
    int ret;
    char *header = "Content-Type: application/x-www-form-urlencoded";
    ret = http_api_post_common (curl, url, token, header, req_content, req_size, rsp_status,
                                rsp_content, rsp_size, timeout, timeout_sec, pcurl_error);
    return ret;
}

static int
http_api_json_post (CURL *curl, const char *url, const char *token,
                    const char *req_content, gint64 req_size,
                    int *rsp_status, char **rsp_content, gint64 *rsp_size,
                    gboolean timeout, int timeout_sec,
                    int *pcurl_error)
{
    int ret;
    char *header = "Content-Type: application/json";
    ret = http_api_post_common (curl, url, token, header, req_content, req_size, rsp_status,
                                rsp_content, rsp_size, timeout, timeout_sec, pcurl_error);
    return ret;
}

static int
http_delete_common (CURL *curl, const char *url,
                    int *rsp_status, gboolean timeout,
                    int timeout_sec)
{
    int ret = 0;

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");

    if (seafile_debug_flag_is_set (SEAFILE_DEBUG_CURL)) {
        curl_easy_setopt (curl, CURLOPT_VERBOSE, 1);
        curl_easy_setopt (curl, CURLOPT_STDERR, seafile_get_logfp());
    }

    if (timeout) {
        /* Set low speed limit to 1 bytes. This effectively means no data. */
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1);
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, timeout_sec);
    }

#ifndef USE_GPL_CRYPTO
    if (seaf->disable_verify_certificate) {
        curl_easy_setopt (curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt (curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
#endif

    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);


#ifndef USE_GPL_CRYPTO
    load_ca_bundle (curl);
#endif

    gboolean is_https = (strncasecmp(url, "https", strlen("https")) == 0);
    set_proxy (curl, is_https);

    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_UNRESTRICTED_AUTH, 1L);

    int rc = curl_easy_perform (curl);
    if (rc != 0) {
        seaf_warning ("libcurl failed to DELETE %s: %s.\n",
                      url, curl_easy_strerror(rc));
        ret = -1;
        goto out;
    }

    long status;
    rc = curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &status);
    if (rc != CURLE_OK) {
        seaf_warning ("Failed to get status code for DELETE %s.\n", url);
        ret = -1;
        goto out;
    }

    *rsp_status = status;

out:
    return ret;
}

static int
http_api_delete (CURL *curl, const char *url, const char *token,
                 int *rsp_status, gboolean timeout, int timeout_sec)
{
    char * token_header;
    struct curl_slist *headers = NULL;
    int ret;

    headers = curl_slist_append (headers, "User-Agent: SeaDrive/"SEAFILE_CLIENT_VERSION" ("USER_AGENT_OS")");

    if (token) {
        token_header = g_strdup_printf ("Authorization: Token %s", token);
        headers = curl_slist_append (headers, token_header);
        g_free (token_header);
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    ret = http_delete_common (curl, url, rsp_status, timeout, timeout_sec);

    curl_slist_free_all (headers);

    return ret;
}

static int
http_error_to_http_task_error (int status)
{
    if (status == HTTP_BAD_REQUEST)
        return HTTP_TASK_ERR_BAD_REQUEST;
    else if (status == HTTP_FORBIDDEN)
        return HTTP_TASK_ERR_FORBIDDEN;
    else if (status == HTTP_UNAUTHORIZED)
        return HTTP_TASK_ERR_FORBIDDEN;
    else if (status >= HTTP_INTERNAL_SERVER_ERROR)
        return HTTP_TASK_ERR_SERVER;
    else if (status == HTTP_NOT_FOUND)
        return HTTP_TASK_ERR_SERVER;
    else if (status == HTTP_NO_QUOTA)
        return HTTP_TASK_ERR_NO_QUOTA;
    else if (status == HTTP_REPO_DELETED)
        return HTTP_TASK_ERR_REPO_DELETED;
    else if (status == HTTP_REPO_CORRUPTED)
        return HTTP_TASK_ERR_REPO_CORRUPTED;
    else if (status == HTTP_REQUEST_TIME_OUT || status == HTTP_REPO_TOO_LARGE)
        return HTTP_TASK_ERR_LIBRARY_TOO_LARGE;
    else
        return HTTP_TASK_ERR_UNKNOWN;
}

static void
handle_http_errors (HttpTxTask *task, int status)
{
    task->error = http_error_to_http_task_error (status);
}

static int
curl_error_to_http_task_error (int curl_error)
{
    if (curl_error == CURLE_SSL_CACERT ||
        curl_error == CURLE_PEER_FAILED_VERIFICATION)
        return HTTP_TASK_ERR_SSL;

    switch (curl_error) {
    case CURLE_COULDNT_RESOLVE_PROXY:
        return HTTP_TASK_ERR_RESOLVE_PROXY;
    case CURLE_COULDNT_RESOLVE_HOST:
        return HTTP_TASK_ERR_RESOLVE_HOST;
    case CURLE_COULDNT_CONNECT:
        return HTTP_TASK_ERR_CONNECT;
    case CURLE_OPERATION_TIMEDOUT:
        return HTTP_TASK_ERR_TX_TIMEOUT;
    case CURLE_SSL_CONNECT_ERROR:
    case CURLE_SSL_CERTPROBLEM:
    case CURLE_SSL_CACERT_BADFILE:
    case CURLE_SSL_ISSUER_ERROR:
        return HTTP_TASK_ERR_SSL;
    case CURLE_GOT_NOTHING:
    case CURLE_SEND_ERROR:
    case CURLE_RECV_ERROR:
        return HTTP_TASK_ERR_TX;
    case CURLE_SEND_FAIL_REWIND:
        return HTTP_TASK_ERR_UNHANDLED_REDIRECT;
    default:
        return HTTP_TASK_ERR_NET;
    }
}

static void
handle_curl_errors (HttpTxTask *task, int curl_error)
{
    task->error = curl_error_to_http_task_error (curl_error);
}

static void
emit_transfer_done_signal (HttpTxTask *task)
{
    if (task->type == HTTP_TASK_TYPE_DOWNLOAD)
        g_signal_emit_by_name (seaf, "repo-http-fetched", task);
    else
        g_signal_emit_by_name (seaf, "repo-http-uploaded", task);
}

static void
transition_state (HttpTxTask *task, int state, int rt_state)
{
    seaf_message ("Transfer repo '%.8s': ('%s', '%s') --> ('%s', '%s')\n",
                  task->repo_id,
                  http_task_state_to_str(task->state),
                  http_task_rt_state_to_str(task->runtime_state),
                  http_task_state_to_str(state),
                  http_task_rt_state_to_str(rt_state));

    if (state != task->state)
        task->state = state;
    task->runtime_state = rt_state;

    if (rt_state == HTTP_TASK_RT_STATE_FINISHED) {
        emit_transfer_done_signal (task);
    }
}

typedef struct {
    char *host;
    gboolean use_fileserver_port;
    HttpProtocolVersionCallback callback;
    void *user_data;

    gboolean success;
    gboolean not_supported;
    int version;
    int error_code;
} CheckProtocolData;

static int
parse_protocol_version (const char *rsp_content, int rsp_size, CheckProtocolData *data)
{
    json_t *object = NULL;
    json_error_t jerror;
    int version;

    object = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!object) {
        seaf_warning ("Parse response failed: %s.\n", jerror.text);
        return -1;
    }

    if (json_object_has_member (object, "version")) {
        version = json_object_get_int_member (object, "version");
        data->version = version;
    } else {
        seaf_warning ("Response doesn't contain protocol version.\n");
        json_decref (object);
        return -1;
    }

    json_decref (object);
    return 0;
}

static void *
check_protocol_version_thread (void *vdata)
{
    CheckProtocolData *data = vdata;
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;

    pool = find_connection_pool (priv, data->host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", data->host);
        return vdata;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", data->host);
        return vdata;
    }

    curl = conn->curl;

    if (!data->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/protocol-version", data->host);
    else
        url = g_strdup_printf ("%s/protocol-version", data->host);

    int curl_error;
    if (http_get (curl, url, NULL, &status, &rsp_content, &rsp_size, NULL, NULL,
                  TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        data->error_code = curl_error_to_http_task_error (curl_error);
        goto out;
    }

    data->success = TRUE;

    if (status == HTTP_OK) {
        if (rsp_size == 0)
            data->not_supported = TRUE;
        else if (parse_protocol_version (rsp_content, rsp_size, data) < 0)
            data->not_supported = TRUE;
    } else {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
        data->not_supported = TRUE;
        data->error_code = http_error_to_http_task_error (status);
    }

out:
    g_free (url);
    g_free (rsp_content);
    connection_pool_return_connection (pool, conn);

    return vdata;
}

static void
check_protocol_version_done (void *vdata)
{
    CheckProtocolData *data = vdata;
    HttpProtocolVersion result;

    memset (&result, 0, sizeof(result));
    result.check_success = data->success;
    result.not_supported = data->not_supported;
    result.version = data->version;
    result.error_code = data->error_code;

    data->callback (&result, data->user_data);

    g_free (data->host);
    g_free (data);
}

int
http_tx_manager_check_protocol_version (HttpTxManager *manager,
                                        const char *host,
                                        gboolean use_fileserver_port,
                                        HttpProtocolVersionCallback callback,
                                        void *user_data)
{
    CheckProtocolData *data = g_new0 (CheckProtocolData, 1);

    data->host = g_strdup(host);
    data->use_fileserver_port = use_fileserver_port;
    data->callback = callback;
    data->user_data = user_data;

    int ret = seaf_job_manager_schedule_job (seaf->job_mgr,
                                             check_protocol_version_thread,
                                             check_protocol_version_done,
                                             data);
    if (ret < 0) {
        g_free (data->host);
        g_free (data);
    }

    return ret;
}

typedef struct {
    char *host;
    gboolean use_notif_server_port;
    HttpNotifServerCallback callback;
    void *user_data;

    gboolean success;
    gboolean not_supported;
} CheckNotifServerData;

static void *
check_notif_server_thread (void *vdata)
{
    CheckNotifServerData *data = vdata;
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;

    pool = find_connection_pool (priv, data->host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", data->host);
        return vdata;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", data->host);
        return vdata;
    }

    curl = conn->curl;

    if (!data->use_notif_server_port)
        url = g_strdup_printf ("%s/notification/ping", data->host);
    else
        url = g_strdup_printf ("%s/ping", data->host);

    int curl_error;
    if (http_get (curl, url, NULL, &status, &rsp_content, &rsp_size, NULL, NULL, TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        goto out;
    }

    data->success = TRUE;

    if (status != HTTP_OK) {
        data->not_supported = TRUE;
    }

out:
    g_free (url);
    g_free (rsp_content);
    connection_pool_return_connection (pool, conn);

    return vdata;
}

static void
check_notif_server_done (void *vdata)
{
    CheckNotifServerData *data = vdata;

    data->callback ((data->success && !data->not_supported), data->user_data);

    g_free (data->host);
    g_free (data);
}

int
http_tx_manager_check_notif_server (HttpTxManager *manager,
                                    const char *host,
                                    gboolean use_notif_server_port,
                                    HttpNotifServerCallback callback,
                                    void *user_data)
{
    CheckNotifServerData *data = g_new0 (CheckNotifServerData, 1);

    data->host = g_strdup(host);
    data->use_notif_server_port = use_notif_server_port;
    data->callback = callback;
    data->user_data = user_data;

    int ret = seaf_job_manager_schedule_job (seaf->job_mgr,
                                             check_notif_server_thread,
                                             check_notif_server_done,
                                             data);
    if (ret < 0) {
        g_free (data->host);
        g_free (data);
    }

    return ret;
}

/* Check Head Commit. */

typedef struct {
    char repo_id[41];
    int repo_version;
    char *host;
    char *token;
    gboolean use_fileserver_port;
    HttpHeadCommitCallback callback;
    void *user_data;

    gboolean success;
    gboolean is_corrupt;
    gboolean is_deleted;
    gboolean perm_denied;
    char head_commit[41];
    int error_code;
} CheckHeadData;

static int
parse_head_commit_info (const char *rsp_content, int rsp_size, CheckHeadData *data)
{
    json_t *object = NULL;
    json_error_t jerror;
    const char *head_commit;

    object = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!object) {
        seaf_warning ("Parse response failed: %s.\n", jerror.text);
        return -1;
    }

    if (json_object_has_member (object, "is_corrupted") &&
        json_object_get_int_member (object, "is_corrupted"))
        data->is_corrupt = TRUE;

    if (!data->is_corrupt) {
        head_commit = json_object_get_string_member (object, "head_commit_id");
        if (!head_commit) {
            seaf_warning ("Check head commit for repo %s failed. "
                          "Response doesn't contain head commit id.\n",
                          data->repo_id);
            json_decref (object);
            return -1;
        }
        memcpy (data->head_commit, head_commit, 40);
    }

    json_decref (object);
    return 0;
}

static void *
check_head_commit_thread (void *vdata)
{
    CheckHeadData *data = vdata;
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;

    pool = find_connection_pool (priv, data->host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", data->host);
        return vdata;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", data->host);
        return vdata;
    }

    curl = conn->curl;

    if (!data->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/commit/HEAD",
                               data->host, data->repo_id);
    else
        url = g_strdup_printf ("%s/repo/%s/commit/HEAD",
                               data->host, data->repo_id);

    int curl_error;
    if (http_get (curl, url, data->token, &status, &rsp_content, &rsp_size,
                  NULL, NULL, TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        data->error_code = curl_error_to_http_task_error (curl_error);
        goto out;
    }

    if (status == HTTP_OK) {
        if (parse_head_commit_info (rsp_content, rsp_size, data) < 0)
            goto out;
        data->success = TRUE;
    } else if (status == HTTP_FORBIDDEN) {
        data->perm_denied = TRUE;
    } else if (status == HTTP_REPO_DELETED) {
        data->is_deleted = TRUE;
        data->success = TRUE;
    } else {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
        data->error_code = http_error_to_http_task_error (status);
    }

out:
    g_free (url);
    g_free (rsp_content);
    connection_pool_return_connection (pool, conn);
    return vdata;
}

static void
check_head_commit_done (void *vdata)
{
    CheckHeadData *data = vdata;
    HttpHeadCommit result;

    memset (&result, 0, sizeof(result));
    result.check_success = data->success;
    result.is_corrupt = data->is_corrupt;
    result.is_deleted = data->is_deleted;
    result.perm_denied = data->perm_denied;
    memcpy (result.head_commit, data->head_commit, 40);
    result.error_code = data->error_code;

    data->callback (&result, data->user_data);

    g_free (data->host);
    g_free (data->token);
    g_free (data);
}

int
http_tx_manager_check_head_commit (HttpTxManager *manager,
                                   const char *repo_id,
                                   int repo_version,
                                   const char *host,
                                   const char *token,
                                   gboolean use_fileserver_port,
                                   HttpHeadCommitCallback callback,
                                   void *user_data)
{
    CheckHeadData *data = g_new0 (CheckHeadData, 1);

    memcpy (data->repo_id, repo_id, 36);
    data->repo_version = repo_version;
    data->host = g_strdup(host);
    data->token = g_strdup(token);
    data->callback = callback;
    data->user_data = user_data;
    data->use_fileserver_port = use_fileserver_port;

    if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                       check_head_commit_thread,
                                       check_head_commit_done,
                                       data) < 0) {
        g_free (data->host);
        g_free (data->token);
        g_free (data);
        return -1;
    }

    return 0;
}

/* Get folder permissions. */

void
http_folder_perm_req_free (HttpFolderPermReq *req)
{
    if (!req)
        return;
    g_free (req->token);
    g_free (req);
}

void
http_folder_perm_res_free (HttpFolderPermRes *res)
{
    GList *ptr;

    if (!res)
        return;
    for (ptr = res->user_perms; ptr; ptr = ptr->next)
        folder_perm_free ((FolderPerm *)ptr->data);
    for (ptr = res->group_perms; ptr; ptr = ptr->next)
        folder_perm_free ((FolderPerm *)ptr->data);
    g_free (res);
}

typedef struct {
    char *host;
    gboolean use_fileserver_port;
    GList *requests;
    HttpGetFolderPermsCallback callback;
    void *user_data;

    gboolean success;
    GList *results;
} GetFolderPermsData;

/* Make sure the path starts with '/' but doesn't end with '/'. */
static char *
canonical_perm_path (const char *path)
{
    int len = strlen(path);
    char *copy, *ret;

    if (strcmp (path, "/") == 0)
        return g_strdup(path);

    if (path[0] == '/' && path[len-1] != '/')
        return g_strdup(path);

    copy = g_strdup(path);

    if (copy[len-1] == '/')
        copy[len-1] = 0;

    if (copy[0] != '/')
        ret = g_strconcat ("/", copy, NULL);
    else
        ret = copy;

    return ret;
}

static GList *
parse_permission_list (json_t *array, gboolean *error)
{
    GList *ret = NULL, *ptr;
    json_t *object, *member;
    size_t n;
    int i;
    FolderPerm *perm;
    const char *str;

    *error = FALSE;

    n = json_array_size (array);
    for (i = 0; i < n; ++i) {
        object = json_array_get (array, i);

        perm = g_new0 (FolderPerm, 1);

        member = json_object_get (object, "path");
        if (!member) {
            seaf_warning ("Invalid folder perm response format: no path.\n");
            *error = TRUE;
            goto out;
        }
        str = json_string_value(member);
        if (!str) {
            seaf_warning ("Invalid folder perm response format: invalid path.\n");
            *error = TRUE;
            goto out;
        }
        perm->path = canonical_perm_path (str);

        member = json_object_get (object, "permission");
        if (!member) {
            seaf_warning ("Invalid folder perm response format: no permission.\n");
            *error = TRUE;
            goto out;
        }
        str = json_string_value(member);
        if (!str) {
            seaf_warning ("Invalid folder perm response format: invalid permission.\n");
            *error = TRUE;
            goto out;
        }
        perm->permission = g_strdup(str);

        ret = g_list_append (ret, perm);
    }

out:
    if (*error) {
        for (ptr = ret; ptr; ptr = ptr->next)
            folder_perm_free ((FolderPerm *)ptr->data);
        g_list_free (ret);
        ret = NULL;
    }

    return ret;
}

static int
parse_folder_perms (const char *rsp_content, int rsp_size, GetFolderPermsData *data)
{
    json_t *array = NULL, *object, *member;
    json_error_t jerror;
    size_t n;
    int i;
    GList *results = NULL, *ptr;
    HttpFolderPermRes *res;
    const char *repo_id;
    int ret = 0;
    gboolean error;

    array = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!array) {
        seaf_warning ("Parse response failed: %s.\n", jerror.text);
        return -1;
    }

    n = json_array_size (array);
    for (i = 0; i < n; ++i) {
        object = json_array_get (array, i);

        res = g_new0 (HttpFolderPermRes, 1);

        member = json_object_get (object, "repo_id");
        if (!member) {
            seaf_warning ("Invalid folder perm response format: no repo_id.\n");
            ret = -1;
            goto out;
        }
        repo_id = json_string_value(member);
        if (strlen(repo_id) != 36) {
            seaf_warning ("Invalid folder perm response format: invalid repo_id.\n");
            ret = -1;
            goto out;
        }
        memcpy (res->repo_id, repo_id, 36);
 
        member = json_object_get (object, "ts");
        if (!member) {
            seaf_warning ("Invalid folder perm response format: no timestamp.\n");
            ret = -1;
            goto out;
        }
        res->timestamp = json_integer_value (member);

        member = json_object_get (object, "user_perms");
        if (!member) {
            seaf_warning ("Invalid folder perm response format: no user_perms.\n");
            ret = -1;
            goto out;
        }
        res->user_perms = parse_permission_list (member, &error);
        if (error) {
            ret = -1;
            goto out;
        }

        member = json_object_get (object, "group_perms");
        if (!member) {
            seaf_warning ("Invalid folder perm response format: no group_perms.\n");
            ret = -1;
            goto out;
        }
        res->group_perms = parse_permission_list (member, &error);
        if (error) {
            ret = -1;
            goto out;
        }

        results = g_list_append (results, res);
    }

out:
    json_decref (array);

    if (ret < 0) {
        for (ptr = results; ptr; ptr = ptr->next)
            http_folder_perm_res_free ((HttpFolderPermRes *)ptr->data);
        g_list_free (results);
    } else {
        data->results = results;
    }

    return ret;
}

static char *
compose_get_folder_perms_request (GList *requests)
{
    GList *ptr;
    HttpFolderPermReq *req;
    json_t *object, *array;
    char *req_str = NULL;

    array = json_array ();

    for (ptr = requests; ptr; ptr = ptr->next) {
        req = ptr->data;

        object = json_object ();
        json_object_set_new (object, "repo_id", json_string(req->repo_id));
        json_object_set_new (object, "token", json_string(req->token));
        json_object_set_new (object, "ts", json_integer(req->timestamp));

        json_array_append_new (array, object);
    }

    req_str = json_dumps (array, 0);
    if (!req_str) {
        seaf_warning ("Faile to json_dumps.\n");
    }

    json_decref (array);
    return req_str;
}

static void *
get_folder_perms_thread (void *vdata)
{
    GetFolderPermsData *data = vdata;
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    char *req_content = NULL;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    GList *ptr;

    pool = find_connection_pool (priv, data->host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", data->host);
        return vdata;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", data->host);
        return vdata;
    }

    curl = conn->curl;

    if (!data->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/folder-perm", data->host);
    else
        url = g_strdup_printf ("%s/repo/folder-perm", data->host);

    req_content = compose_get_folder_perms_request (data->requests);
    if (!req_content)
        goto out;

    if (http_post (curl, url, NULL, req_content, strlen(req_content),
                   &status, &rsp_content, &rsp_size, TRUE, HTTP_TIMEOUT_SEC, NULL) < 0) {
        conn->release = TRUE;
        goto out;
    }

    if (status == HTTP_OK) {
        if (parse_folder_perms (rsp_content, rsp_size, data) < 0)
            goto out;
        data->success = TRUE;
    } else {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
    }

out:
    for (ptr = data->requests; ptr; ptr = ptr->next)
        http_folder_perm_req_free ((HttpFolderPermReq *)ptr->data);
    g_list_free (data->requests);

    g_free (url);
    g_free (req_content);
    g_free (rsp_content);
    connection_pool_return_connection (pool, conn);
    return vdata;
}

static void
get_folder_perms_done (void *vdata)
{
    GetFolderPermsData *data = vdata;
    HttpFolderPerms cb_data;

    memset (&cb_data, 0, sizeof(cb_data));
    cb_data.success = data->success;
    cb_data.results = data->results;

    data->callback (&cb_data, data->user_data);

    GList *ptr;
    for (ptr = data->results; ptr; ptr = ptr->next)
        http_folder_perm_res_free ((HttpFolderPermRes *)ptr->data);
    g_list_free (data->results);

    g_free (data->host);
    g_free (data);
}

int
http_tx_manager_get_folder_perms (HttpTxManager *manager,
                                  const char *host,
                                  gboolean use_fileserver_port,
                                  GList *folder_perm_requests,
                                  HttpGetFolderPermsCallback callback,
                                  void *user_data)
{
    GetFolderPermsData *data = g_new0 (GetFolderPermsData, 1);

    data->host = g_strdup(host);
    data->requests = folder_perm_requests;
    data->callback = callback;
    data->user_data = user_data;
    data->use_fileserver_port = use_fileserver_port;

    if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                        get_folder_perms_thread,
                                        get_folder_perms_done,
                                        data) < 0) {
        g_free (data->host);
        g_free (data);
        return -1;
    }

    return 0;
}

/* Get Locked Files. */

void
http_locked_files_req_free (HttpLockedFilesReq *req)
{
    if (!req)
        return;
    g_free (req->token);
    g_free (req);
}

void
http_locked_files_res_free (HttpLockedFilesRes *res)
{
    if (!res)
        return;

    g_hash_table_destroy (res->locked_files);
    g_free (res);
}

typedef struct {
    char *host;
    gboolean use_fileserver_port;
    GList *requests;
    HttpGetLockedFilesCallback callback;
    void *user_data;

    gboolean success;
    GList *results;
} GetLockedFilesData;

static GHashTable *
parse_locked_file_list (json_t *array)
{
    GHashTable *ret = NULL;
    size_t n, i;
    json_t *obj, *string, *integer;

    ret = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);
    if (!ret) {
        return NULL;
    }

    n = json_array_size (array);
    for (i = 0; i < n; ++i) {
        obj = json_array_get (array, i);
        string = json_object_get (obj, "path");
        if (!string) {
            g_hash_table_destroy (ret);
            return NULL;
        }
        integer = json_object_get (obj, "by_me");
        if (!integer) {
            g_hash_table_destroy (ret);
            return NULL;
        }
        g_hash_table_insert (ret,
                             g_strdup(json_string_value(string)),
                             (void*)(long)json_integer_value(integer));
    }

    return ret;
}

static int
parse_locked_files (const char *rsp_content, int rsp_size, GetLockedFilesData *data)
{
    json_t *array = NULL, *object, *member;
    json_error_t jerror;
    size_t n;
    int i;
    GList *results = NULL;
    HttpLockedFilesRes *res;
    const char *repo_id;
    int ret = 0;

    array = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!array) {
        seaf_warning ("Parse response failed: %s.\n", jerror.text);
        return -1;
    }

    n = json_array_size (array);
    for (i = 0; i < n; ++i) {
        object = json_array_get (array, i);

        res = g_new0 (HttpLockedFilesRes, 1);

        member = json_object_get (object, "repo_id");
        if (!member) {
            seaf_warning ("Invalid locked files response format: no repo_id.\n");
            ret = -1;
            goto out;
        }
        repo_id = json_string_value(member);
        if (strlen(repo_id) != 36) {
            seaf_warning ("Invalid locked files response format: invalid repo_id.\n");
            ret = -1;
            goto out;
        }
        memcpy (res->repo_id, repo_id, 36);
 
        member = json_object_get (object, "ts");
        if (!member) {
            seaf_warning ("Invalid locked files response format: no timestamp.\n");
            ret = -1;
            goto out;
        }
        res->timestamp = json_integer_value (member);

        member = json_object_get (object, "locked_files");
        if (!member) {
            seaf_warning ("Invalid locked files response format: no locked_files.\n");
            ret = -1;
            goto out;
        }

        res->locked_files = parse_locked_file_list (member);
        if (res->locked_files == NULL) {
            ret = -1;
            goto out;
        }

        results = g_list_append (results, res);
    }

out:
    json_decref (array);

    if (ret < 0) {
        g_list_free_full (results, (GDestroyNotify)http_locked_files_res_free);
    } else {
        data->results = results;
    }

    return ret;
}

static char *
compose_get_locked_files_request (GList *requests)
{
    GList *ptr;
    HttpLockedFilesReq *req;
    json_t *object, *array;
    char *req_str = NULL;

    array = json_array ();

    for (ptr = requests; ptr; ptr = ptr->next) {
        req = ptr->data;

        object = json_object ();
        json_object_set_new (object, "repo_id", json_string(req->repo_id));
        json_object_set_new (object, "token", json_string(req->token));
        json_object_set_new (object, "ts", json_integer(req->timestamp));

        json_array_append_new (array, object);
    }

    req_str = json_dumps (array, 0);
    if (!req_str) {
        seaf_warning ("Faile to json_dumps.\n");
    }

    json_decref (array);
    return req_str;
}

static void *
get_locked_files_thread (void *vdata)
{
    GetLockedFilesData *data = vdata;
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    char *req_content = NULL;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;

    pool = find_connection_pool (priv, data->host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", data->host);
        return vdata;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", data->host);
        return vdata;
    }

    curl = conn->curl;

    if (!data->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/locked-files", data->host);
    else
        url = g_strdup_printf ("%s/repo/locked-files", data->host);

    req_content = compose_get_locked_files_request (data->requests);
    if (!req_content)
        goto out;

    if (http_post (curl, url, NULL, req_content, strlen(req_content),
                   &status, &rsp_content, &rsp_size, TRUE, HTTP_TIMEOUT_SEC, NULL) < 0) {
        conn->release = TRUE;
        goto out;
    }

    if (status == HTTP_OK) {
        if (parse_locked_files (rsp_content, rsp_size, data) < 0)
            goto out;
        data->success = TRUE;
    } else {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
    }

out:
    g_list_free_full (data->requests, (GDestroyNotify)http_locked_files_req_free);

    g_free (url);
    g_free (req_content);
    g_free (rsp_content);
    connection_pool_return_connection (pool, conn);
    return vdata;
}

static void
get_locked_files_done (void *vdata)
{
    GetLockedFilesData *data = vdata;
    HttpLockedFiles cb_data;

    memset (&cb_data, 0, sizeof(cb_data));
    cb_data.success = data->success;
    cb_data.results = data->results;

    data->callback (&cb_data, data->user_data);

    g_list_free_full (data->results, (GDestroyNotify)http_locked_files_res_free);

    g_free (data->host);
    g_free (data);
}

int
http_tx_manager_get_locked_files (HttpTxManager *manager,
                                  const char *host,
                                  gboolean use_fileserver_port,
                                  GList *locked_files_requests,
                                  HttpGetLockedFilesCallback callback,
                                  void *user_data)
{
    GetLockedFilesData *data = g_new0 (GetLockedFilesData, 1);

    data->host = g_strdup(host);
    data->requests = locked_files_requests;
    data->callback = callback;
    data->user_data = user_data;
    data->use_fileserver_port = use_fileserver_port;

    if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                        get_locked_files_thread,
                                        get_locked_files_done,
                                        data) < 0) {
        g_free (data->host);
        g_free (data);
        return -1;
    }

    return 0;
}

/* Synchronous interfaces for locking/unlocking a file on the server. */

int
http_tx_manager_lock_file (HttpTxManager *manager,
                           const char *host,
                           gboolean use_fileserver_port,
                           const char *token,
                           const char *repo_id,
                           const char *path)
{
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    int status;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    char *esc_path = g_uri_escape_string(path, NULL, FALSE);
    if (!use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/lock-file?p=%s", host, repo_id, esc_path);
    else
        url = g_strdup_printf ("%s/repo/%s/lock-file?p=%s", host, repo_id, esc_path);
    g_free (esc_path);

    if (http_put (curl, url, token, NULL, 0, NULL, NULL, NULL,
                  &status, NULL, NULL, TRUE, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for PUT %s: %d.\n", url, status);
        ret = -1;
    }

out:
    g_free (url);
    connection_pool_return_connection (pool, conn);
    return ret;
}

int
http_tx_manager_unlock_file (HttpTxManager *manager,
                             const char *host,
                             gboolean use_fileserver_port,
                             const char *token,
                             const char *repo_id,
                             const char *path)
{
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    int status;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    char *esc_path = g_uri_escape_string(path, NULL, FALSE);
    if (!use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/unlock-file?p=%s", host, repo_id, esc_path);
    else
        url = g_strdup_printf ("%s/repo/%s/unlock-file?p=%s", host, repo_id, esc_path);
    g_free (esc_path);

    if (http_put (curl, url, token, NULL, 0, NULL, NULL, NULL,
                  &status, NULL, NULL, TRUE, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for PUT %s: %d.\n", url, status);
        ret = -1;
    }

out:
    g_free (url);
    connection_pool_return_connection (pool, conn);
    return ret;
}

/* Asynchronous interface for sending a API GET request to seahub. */

typedef struct {
    char *host;
    char *url;
    char *api_token;
    HttpAPIGetCallback callback;
    void *user_data;

    gboolean success;
    char *rsp_content;
    int rsp_size;
    int error_code;
    int http_status;
} APIGetData;

static void *
api_get_request_thread (void *vdata)
{
    APIGetData *data = vdata;
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;

    pool = find_connection_pool (priv, data->host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", data->host);
        return vdata;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", data->host);
        return vdata;
    }

    curl = conn->curl;

    int curl_error;
    if (http_api_get (curl, data->url, data->api_token,
                      &status, &rsp_content, &rsp_size,
                      NULL, NULL, TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        data->error_code = curl_error_to_http_task_error (curl_error);
        goto out;
    }

    // Free rsp_content even if status is not HTTP_OK, prevent memory leak
    data->rsp_content = rsp_content;
    data->rsp_size = rsp_size;

    if (status == HTTP_OK) {
        data->success = TRUE;
    } else {
        seaf_warning ("Bad response code for GET %s: %d.\n", data->url, status);
        data->error_code = http_error_to_http_task_error (status);
    }

out:
    connection_pool_return_connection (pool, conn);
    return vdata;
}

static void
api_get_request_done (void *vdata)
{
    APIGetData *data = vdata;
    HttpAPIGetResult cb_data;

    memset (&cb_data, 0, sizeof(cb_data));
    cb_data.success = data->success;
    cb_data.rsp_content = data->rsp_content;
    cb_data.rsp_size = data->rsp_size;
    cb_data.error_code = data->error_code;

    data->callback (&cb_data, data->user_data);

    g_free (data->rsp_content);

    g_free (data->host);
    g_free (data->url);
    g_free (data->api_token);
    g_free (data);
}

int
http_tx_manager_api_get (HttpTxManager *manager,
                         const char *host,
                         const char *url,
                         const char *api_token,
                         HttpAPIGetCallback callback,
                         void *user_data)
{
    APIGetData *data = g_new0 (APIGetData, 1);

    data->host = g_strdup(host);
    data->url = g_strdup(url);
    data->api_token = g_strdup(api_token);
    data->callback = callback;
    data->user_data = user_data;

    if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                       api_get_request_thread,
                                       api_get_request_done,
                                       data) < 0) {
        g_free (data->host);
        g_free (data->url);
        g_free (data->api_token);
        g_free (data);
        return -1;
    }

    return 0;
}

static void *
fileserver_api_get_request (void *vdata)
{
    APIGetData *data = vdata;
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;


    pool = find_connection_pool (priv, data->host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", data->host);
        return vdata;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", data->host);
        return vdata;
    }

    curl = conn->curl;

    int curl_error;
    if (http_get (curl, data->url, data->api_token, &status, &rsp_content, &rsp_size, NULL, NULL,
                  TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        data->error_code = curl_error_to_http_task_error (curl_error);
        goto out;
    }

    data->rsp_content = rsp_content;
    data->rsp_size = rsp_size;

    if (status == HTTP_OK) {
        data->success = TRUE;
    } else {
        seaf_warning ("Bad response code for GET %s: %d.\n", data->url, status);
        data->error_code = http_error_to_http_task_error (status);
        data->http_status = status;
    }

out:
    connection_pool_return_connection (pool, conn);
    return vdata;

}

static void
fileserver_api_get_request_done (void *vdata)
{
    APIGetData *data = vdata;
    HttpAPIGetResult cb_data;

    memset (&cb_data, 0, sizeof(cb_data));
    cb_data.success = data->success;
    cb_data.rsp_content = data->rsp_content;
    cb_data.rsp_size = data->rsp_size;
    cb_data.error_code = data->error_code;
    cb_data.http_status = data->http_status;

    data->callback (&cb_data, data->user_data);

    g_free (data->rsp_content);
    g_free (data->host);
    g_free (data->url);
    g_free (data->api_token);
    g_free (data);
}

int
http_tx_manager_fileserver_api_get  (HttpTxManager *manager,
                                     const char *host,
                                     const char *url,
                                     const char *api_token,
                                     HttpAPIGetCallback callback,
                                     void *user_data)
{
    APIGetData *data = g_new0 (APIGetData, 1);

    data->host = g_strdup(host);
    data->url = g_strdup(url);
    data->api_token = g_strdup(api_token);
    data->callback = callback;
    data->user_data = user_data;

    if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                       fileserver_api_get_request,
                                       fileserver_api_get_request_done,
                                       data) < 0) {
        g_free (data->rsp_content);
        g_free (data->host);
        g_free (data->url);
        g_free (data->api_token);
        g_free (data);
        return -1;
    }

    return 0;
}

static gboolean
remove_task_help (gpointer key, gpointer value, gpointer user_data)
{
    HttpTxTask *task = value;
    const char *repo_id = user_data;

    if (strcmp(task->repo_id, repo_id) != 0)
        return FALSE;

    return TRUE;
}

static void
clean_tasks_for_repo (HttpTxManager *manager, const char *repo_id)
{
    g_hash_table_foreach_remove (manager->priv->download_tasks,
                                 remove_task_help, (gpointer)repo_id);

    g_hash_table_foreach_remove (manager->priv->upload_tasks,
                                 remove_task_help, (gpointer)repo_id);
}

static int
check_permission (HttpTxTask *task, Connection *conn)
{
    CURL *curl;
    char *url;
    int status;
    int ret = 0;
    char *rsp_content = NULL;
    gint64 rsp_size;
    json_t *rsp_obj = NULL, *reason = NULL, *unsyncable_path = NULL;
    const char *reason_str = NULL, *unsyncable_path_str = NULL;
    json_error_t jerror;

    curl = conn->curl;

    const char *type = (task->type == HTTP_TASK_TYPE_DOWNLOAD) ? "download" : "upload";
    const char *url_prefix = (task->use_fileserver_port) ? "" : "seafhttp/";

    char *client_name = g_uri_escape_string (seaf->client_name, NULL, FALSE);
    url = g_strdup_printf ("%s/%srepo/%s/permission-check/?op=%s"
                           "&client_id=%s&client_name=%s",
                           task->host, url_prefix, task->repo_id, type,
                           seaf->client_id, client_name);
    g_free (client_name);

    int curl_error;
    if (http_get (curl, url, task->token, &status, &rsp_content, &rsp_size, NULL, NULL,
                  TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        handle_curl_errors (task, curl_error);
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
        if (status != HTTP_FORBIDDEN || !rsp_content) {
	    handle_http_errors (task, status);
	    ret = -1;
	    goto out;
        }

        rsp_obj = json_loadb (rsp_content, rsp_size, 0 ,&jerror);
        if (!rsp_obj) {
            seaf_warning ("Parse check permission response failed: %s.\n", jerror.text);
            handle_http_errors (task, status);
            json_decref (rsp_obj);
            ret = -1;
            goto out;
        }

        reason = json_object_get (rsp_obj, "reason");
        if (!reason) {
            handle_http_errors (task, status);
            json_decref (rsp_obj);
            ret = -1;
            goto out;
        }

        reason_str = json_string_value (reason);
        if (g_strcmp0 (reason_str, "no write permission") == 0) {
            task->error = HTTP_TASK_ERR_NO_WRITE_PERMISSION;
        } else if (g_strcmp0 (reason_str, "unsyncable share permission") == 0) {
            task->error = HTTP_TASK_ERR_NO_PERMISSION_TO_SYNC;
            unsyncable_path = json_object_get (rsp_obj, "unsyncable_path");
            if (!unsyncable_path) {
                json_decref (rsp_obj);
                ret = -1;
                goto out;
            }
            unsyncable_path_str = json_string_value (unsyncable_path);
            if (unsyncable_path_str)
                task->unsyncable_path = g_strdup (unsyncable_path_str);
        } else {
            task->error = HTTP_TASK_ERR_FORBIDDEN;
        }
        ret = -1;
    }

out:
    g_free (url);
    curl_easy_reset (curl);

    return ret;
}

/* Upload. */

static void *http_upload_thread (void *vdata);
static void http_upload_done (void *vdata);

int
http_tx_manager_add_upload (HttpTxManager *manager,
                            const char *repo_id,
                            int repo_version,
                            const char *server,
                            const char *user,
                            const char *repo_uname,
                            const char *host,
                            const char *token,
                            int protocol_version,
                            gboolean use_fileserver_port,
                            GError **error)
{
    HttpTxTask *task;

    if (!repo_id || !repo_uname || !token || !host) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Empty argument(s)");
        return -1;
    }

    clean_tasks_for_repo (manager, repo_id);

    task = http_tx_task_new (manager, repo_id, repo_version,
                             HTTP_TASK_TYPE_UPLOAD, FALSE,
                             host, token);
    task->repo_uname = g_strdup (repo_uname);

    task->protocol_version = protocol_version;

    task->state = HTTP_TASK_STATE_NORMAL;

    task->use_fileserver_port = use_fileserver_port;

    task->server = g_strdup (server);
    task->user = g_strdup (user);

    g_hash_table_insert (manager->priv->upload_tasks,
                         g_strdup(repo_id),
                         task);

    if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                       http_upload_thread,
                                       http_upload_done,
                                       task) < 0) {
        g_hash_table_remove (manager->priv->upload_tasks, repo_id);
        return -1;
    }

    return 0;
}

static gboolean
dirent_same (SeafDirent *dent1, SeafDirent *dent2)
{
    return (strcmp(dent1->id, dent2->id) == 0 &&
            dent1->mode == dent2->mode &&
            dent1->mtime == dent2->mtime);
}

typedef struct {
    HttpTxTask *task;
    gint64 delta;
    GHashTable *active_paths;
} CalcQuotaDeltaData;

static int
check_quota_and_active_paths_diff_files (int n, const char *basedir,
                                         SeafDirent *files[], void *vdata)
{
    CalcQuotaDeltaData *data = vdata;
    SeafDirent *file1 = files[0];
    SeafDirent *file2 = files[1];
    gint64 size1, size2;
    char *path;

    if (file1 && file2) {
        size1 = file1->size;
        size2 = file2->size;
        data->delta += (size1 - size2);

        if (!dirent_same (file1, file2)) {
            path = g_strconcat(basedir, file1->name, NULL);
            g_hash_table_replace (data->active_paths, path, (void*)(long)S_IFREG);
        }
    } else if (file1 && !file2) {
        data->delta += file1->size;

        path = g_strconcat (basedir, file1->name, NULL);
        g_hash_table_replace (data->active_paths, path, (void*)(long)S_IFREG);
    } else if (!file1 && file2) {
        data->delta -= file2->size;
    }

    return 0;
}

static int
check_quota_and_active_paths_diff_dirs (int n, const char *basedir,
                                        SeafDirent *dirs[], void *vdata,
                                        gboolean *recurse)
{
    CalcQuotaDeltaData *data = vdata;
    SeafDirent *dir1 = dirs[0];
    SeafDirent *dir2 = dirs[1];
    char *path;

    /* When a new empty dir is created, or a dir became empty. */
    if ((!dir2 && dir1 && strcmp(dir1->id, EMPTY_SHA1) == 0) ||
	(dir2 && dir1 && strcmp(dir1->id, EMPTY_SHA1) == 0 && strcmp(dir2->id, EMPTY_SHA1) != 0)) {
        path = g_strconcat (basedir, dir1->name, NULL);
        g_hash_table_replace (data->active_paths, path, (void*)(long)S_IFDIR);
    }

    return 0;
}

static int
calculate_upload_size_delta_and_active_paths (HttpTxTask *task,
                                              gint64 *delta,
                                              GHashTable *active_paths)
{
    int ret = 0;
    SeafBranch *local = NULL, *master = NULL;
    SeafCommit *local_head = NULL, *master_head = NULL;

    local = seaf_branch_manager_get_branch (seaf->branch_mgr, task->repo_id, "local");
    if (!local) {
        seaf_warning ("Branch local not found for repo %.8s.\n", task->repo_id);
        ret = -1;
        goto out;
    }
    master = seaf_branch_manager_get_branch (seaf->branch_mgr, task->repo_id, "master");
    if (!master) {
        seaf_warning ("Branch master not found for repo %.8s.\n", task->repo_id);
        ret = -1;
        goto out;
    }

    local_head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                                 task->repo_id, task->repo_version,
                                                 local->commit_id);
    if (!local_head) {
        seaf_warning ("Local head commit not found for repo %.8s.\n",
                      task->repo_id);
        ret = -1;
        goto out;
    }
    master_head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                                 task->repo_id, task->repo_version,
                                                 master->commit_id);
    if (!master_head) {
        seaf_warning ("Master head commit not found for repo %.8s.\n",
                      task->repo_id);
        ret = -1;
        goto out;
    }

    CalcQuotaDeltaData data;
    memset (&data, 0, sizeof(data));
    data.task = task;
    data.active_paths = active_paths;

    DiffOptions opts;
    memset (&opts, 0, sizeof(opts));
    memcpy (opts.store_id, task->repo_id, 36);
    opts.version = task->repo_version;
    opts.file_cb = check_quota_and_active_paths_diff_files;
    opts.dir_cb = check_quota_and_active_paths_diff_dirs;
    opts.data = &data;

    const char *trees[2];
    trees[0] = local_head->root_id;
    trees[1] = master_head->root_id;
    if (diff_trees (2, trees, &opts) < 0) {
        seaf_warning ("Failed to diff local and master head for repo %.8s.\n",
                      task->repo_id);
        ret = -1;
        goto out;
    }

    *delta = data.delta;

out:
    seaf_branch_unref (local);
    seaf_branch_unref (master);
    seaf_commit_unref (local_head);
    seaf_commit_unref (master_head);

    return ret;
}

static int
check_quota (HttpTxTask *task, Connection *conn, gint64 delta)
{
    CURL *curl;
    char *url;
    int status;
    int ret = 0;

    curl = conn->curl;

    if (!task->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/quota-check/?delta=%"G_GINT64_FORMAT"",
                               task->host, task->repo_id, delta);
    else
        url = g_strdup_printf ("%s/repo/%s/quota-check/?delta=%"G_GINT64_FORMAT"",
                               task->host, task->repo_id, delta);

    int curl_error;
    if (http_get (curl, url, task->token, &status, NULL, NULL, NULL, NULL,
                  TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        handle_curl_errors (task, curl_error);
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
        handle_http_errors (task, status);
        ret = -1;
    }

out:
    g_free (url);
    curl_easy_reset (curl);

    return ret;
}

static int
send_commit_object (HttpTxTask *task, Connection *conn)
{
    CURL *curl;
    char *url;
    int status;
    char *data;
    int len;
    int ret = 0;

    if (seaf_obj_store_read_obj (seaf->commit_mgr->obj_store,
                                 task->repo_id, task->repo_version,
                                 task->head, (void**)&data, &len) < 0) {
        seaf_warning ("Failed to read commit %s.\n", task->head);
        task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
        return -1;
    }

    curl = conn->curl;

    if (!task->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/commit/%s",
                               task->host, task->repo_id, task->head);
    else
        url = g_strdup_printf ("%s/repo/%s/commit/%s",
                               task->host, task->repo_id, task->head);

    int curl_error;
    if (http_put (curl, url, task->token,
                  data, len,
                  NULL, NULL, NULL,
                  &status, NULL, NULL, TRUE, &curl_error) < 0) {
        conn->release = TRUE;
        handle_curl_errors (task, curl_error);
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for PUT %s: %d.\n", url, status);
        handle_http_errors (task, status);
        ret = -1;
    }

out:
    g_free (url);
    curl_easy_reset (curl);
    g_free (data);

    return ret;
}

typedef struct {
    GList **pret;
    GHashTable *checked_objs;
    GHashTable *needed_file_pair;
    GHashTable *deleted_file_pair;
} CalcFsListData;

static int
collect_file_ids (int n, const char *basedir, SeafDirent *files[], void *vdata)
{
    SeafDirent *file1 = files[0];
    SeafDirent *file2 = files[1];
    CalcFsListData *data = vdata;
    GList **pret = data->pret;
    int dummy;

    if (!file1 && file2) {
        g_hash_table_replace (data->deleted_file_pair, g_strdup(file2->id),
                              g_strconcat(basedir, file2->name, NULL));
        return 0;
    }

    if (strcmp (file1->id, EMPTY_SHA1) == 0)
        return 0;

    if (g_hash_table_lookup (data->checked_objs, file1->id))
        return 0;

    if (!file2 || strcmp (file1->id, file2->id) != 0) {
        *pret = g_list_prepend (*pret, g_strdup(file1->id));
        g_hash_table_insert (data->checked_objs, g_strdup(file1->id), &dummy);
        g_hash_table_replace (data->needed_file_pair, g_strdup(file1->id),
                              g_strconcat (basedir, file1->name, NULL));
    }

    return 0;
}

static int
collect_dir_ids (int n, const char *basedir, SeafDirent *dirs[], void *vdata,
                 gboolean *recurse)
{
    SeafDirent *dir1 = dirs[0];
    SeafDirent *dir2 = dirs[1];
    CalcFsListData *data = vdata;
    GList **pret = data->pret;
    int dummy;

    if (!dir1 || strcmp (dir1->id, EMPTY_SHA1) == 0)
        return 0;

    if (g_hash_table_lookup (data->checked_objs, dir1->id))
        return 0;

    if (!dir2 || strcmp (dir1->id, dir2->id) != 0) {
        *pret = g_list_prepend (*pret, g_strdup(dir1->id));
        g_hash_table_insert (data->checked_objs, g_strdup(dir1->id), &dummy);
    }

    return 0;
}

static gboolean
remove_renamed_ids (gpointer key, gpointer value, gpointer user_data)
{
    char *file_id = key;
    GHashTable *deleted_ids = user_data;

    if (g_hash_table_lookup (deleted_ids, file_id))
        return TRUE;
    else
        return FALSE;
}

static GList *
calculate_send_fs_object_list (HttpTxTask *task, GHashTable *needed_file_pair)
{
    GList *ret = NULL;
    SeafBranch *local = NULL, *master = NULL;
    SeafCommit *local_head = NULL, *master_head = NULL;
    GList *ptr;

    local = seaf_branch_manager_get_branch (seaf->branch_mgr, task->repo_id, "local");
    if (!local) {
        seaf_warning ("Branch local not found for repo %.8s.\n", task->repo_id);
        goto out;
    }
    master = seaf_branch_manager_get_branch (seaf->branch_mgr, task->repo_id, "master");
    if (!master) {
        seaf_warning ("Branch master not found for repo %.8s.\n", task->repo_id);
        goto out;
    }

    local_head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                                 task->repo_id, task->repo_version,
                                                 local->commit_id);
    if (!local_head) {
        seaf_warning ("Local head commit not found for repo %.8s.\n",
                      task->repo_id);
        goto out;
    }
    master_head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                                  task->repo_id, task->repo_version,
                                                  master->commit_id);
    if (!master_head) {
        seaf_warning ("Master head commit not found for repo %.8s.\n",
                      task->repo_id);
        goto out;
    }

    /* Diff won't traverse the root object itself. */
    if (strcmp (local_head->root_id, master_head->root_id) != 0)
        ret = g_list_prepend (ret, g_strdup(local_head->root_id));

    CalcFsListData *data = g_new0(CalcFsListData, 1);
    data->pret = &ret;
    data->checked_objs = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                g_free, NULL);
    data->needed_file_pair = needed_file_pair;
    data->deleted_file_pair = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                     g_free, g_free);

    DiffOptions opts;
    memset (&opts, 0, sizeof(opts));
    memcpy (opts.store_id, task->repo_id, 36);
    opts.version = task->repo_version;
    opts.file_cb = collect_file_ids;
    opts.dir_cb = collect_dir_ids;
    opts.data = data;

    const char *trees[2];
    trees[0] = local_head->root_id;
    trees[1] = master_head->root_id;
    if (diff_trees (2, trees, &opts) < 0) {
        seaf_warning ("Failed to diff local and master head for repo %.8s.\n",
                      task->repo_id);
        for (ptr = ret; ptr; ptr = ptr->next)
            g_free (ptr->data);
        ret = NULL;
    }

    g_hash_table_foreach_remove (needed_file_pair, remove_renamed_ids,
                                 data->deleted_file_pair);

    g_hash_table_destroy (data->checked_objs);
    g_hash_table_destroy (data->deleted_file_pair);
    g_free (data);

out:
    seaf_branch_unref (local);
    seaf_branch_unref (master);
    seaf_commit_unref (local_head);
    seaf_commit_unref (master_head);
    return ret;
}

#define ID_LIST_SEGMENT_N 1000

static int
upload_check_id_list_segment (HttpTxTask *task, Connection *conn, const char *url,
                              GList **send_id_list, GList **recv_id_list)
{
    json_t *array;
    json_error_t jerror;
    char *obj_id;
    int n_sent = 0;
    char *data = NULL;
    int len;
    CURL *curl;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    int ret = 0;

    /* Convert object id list to JSON format. */

    array = json_array ();

    while (*send_id_list != NULL) {
        obj_id = (*send_id_list)->data;
        json_array_append_new (array, json_string(obj_id));

        *send_id_list = g_list_delete_link (*send_id_list, *send_id_list);
        g_free (obj_id);

        if (++n_sent >= ID_LIST_SEGMENT_N)
            break;
    }

    seaf_debug ("Check %d ids for %s:%s.\n",
                n_sent, task->host, task->repo_id);

    data = json_dumps (array, 0);
    len = strlen(data);
    json_decref (array);

    /* Send fs object id list. */

    curl = conn->curl;

    int curl_error;
    if (http_post (curl, url, task->token,
                   data, len,
                   &status, &rsp_content, &rsp_size, TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        handle_curl_errors (task, curl_error);
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for POST %s: %d.\n", url, status);
        handle_http_errors (task, status);
        ret = -1;
        goto out;
    }

    /* Process needed object id list. */

    array = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!array) {
        seaf_warning ("Invalid JSON response from the server: %s.\n", jerror.text);
        task->error = HTTP_TASK_ERR_SERVER;
        ret = -1;
        goto out;
    }

    int i;
    size_t n = json_array_size (array);
    json_t *str;

    seaf_debug ("%lu objects or blocks are needed for %s:%s.\n",
                n, task->host, task->repo_id);

    for (i = 0; i < n; ++i) {
        str = json_array_get (array, i);
        if (!str) {
            seaf_warning ("Invalid JSON response from the server.\n");
            json_decref (array);
            ret = -1;
            goto out;
        }

        *recv_id_list = g_list_prepend (*recv_id_list, g_strdup(json_string_value(str)));
    }

    json_decref (array);

out:
    curl_easy_reset (curl);
    g_free (data);
    g_free (rsp_content);

    return ret;
}

#define MAX_OBJECT_PACK_SIZE (1 << 20) /* 1MB */

struct ObjectHeader {
    char obj_id[40];
    guint32 obj_size;
    guint8 object[0];
} __attribute__((__packed__));

typedef struct ObjectHeader ObjectHeader;

static int
send_fs_objects (HttpTxTask *task, Connection *conn, GList **send_fs_list)
{
    struct evbuffer *buf;
    ObjectHeader hdr;
    char *obj_id;
    char *data;
    int len;
    int total_size;
    unsigned char *package;
    CURL *curl;
    char *url = NULL;
    int status;
    int ret = 0;
    int n_sent = 0;

    buf = evbuffer_new ();
    curl = conn->curl;

    while (*send_fs_list != NULL) {
        obj_id = (*send_fs_list)->data;

        if (seaf_obj_store_read_obj (seaf->fs_mgr->obj_store,
                                     task->repo_id, task->repo_version,
                                     obj_id, (void **)&data, &len) < 0) {
            seaf_warning ("Failed to read fs object %s in repo %s.\n",
                          obj_id, task->repo_id);
            task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
            ret = -1;
            goto out;
        }

        ++n_sent;

        memcpy (hdr.obj_id, obj_id, 40);
        hdr.obj_size = htonl (len);

        evbuffer_add (buf, &hdr, sizeof(hdr));
        evbuffer_add (buf, data, len);

        g_free (data);
        *send_fs_list = g_list_delete_link (*send_fs_list, *send_fs_list);
        g_free (obj_id);

        total_size = evbuffer_get_length (buf);
        if (total_size >= MAX_OBJECT_PACK_SIZE)
            break;
    }

    seaf_debug ("Sending %d fs objects for %s:%s.\n",
                n_sent, task->host, task->repo_id);

    package = evbuffer_pullup (buf, -1);

    if (!task->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/recv-fs/",
                               task->host, task->repo_id);
    else
        url = g_strdup_printf ("%s/repo/%s/recv-fs/",
                               task->host, task->repo_id);

    int curl_error;
    if (http_post (curl, url, task->token,
                   (char *)package, evbuffer_get_length(buf),
                   &status, NULL, NULL, TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        handle_curl_errors (task, curl_error);
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for POST %s: %d.\n", url, status);
        handle_http_errors (task, status);
        ret = -1;
    }

out:
    g_free (url);
    evbuffer_free (buf);
    curl_easy_reset (curl);

    return ret;
}

#if 0

static void
add_to_block_list (GList **block_list, GHashTable *added_blocks, const char *block_id)
{
    int dummy;

    if (g_hash_table_lookup (added_blocks, block_id))
        return;

    *block_list = g_list_prepend (*block_list, g_strdup(block_id));
    g_hash_table_insert (added_blocks, g_strdup(block_id), &dummy);
}

static int
add_one_file_to_block_list (const char *repo_id, int repo_version,
                            const char *path, unsigned char *sha1,
                            GList **block_list, GHashTable *added_blocks,
                            GHashTable *needed_fs)
{
    char file_id1[41];
    Seafile *f1 = NULL;
    int i;

    rawdata_to_hex (sha1, file_id1, 20);

    if (!g_hash_table_lookup (needed_fs, file_id1))
        return 0;

    f1 = seaf_fs_manager_get_seafile (seaf->fs_mgr,
                                      repo_id, repo_version,
                                      file_id1);
    if (!f1) {
        seaf_warning ("Failed to get seafile object %s:%s. Path is %s.\n",
                      repo_id, file_id1, path);
        return -1;
    }
    for (i = 0; i < f1->n_blocks; ++i)
        add_to_block_list (block_list, added_blocks, f1->blk_sha1s[i]);
    seafile_unref (f1);

    return 0;
}

static int
diff_two_files_to_block_list (const char *repo_id, int repo_version,
                              const char *path,
                              unsigned char *sha1, unsigned char *old_sha1,
                              GList **block_list, GHashTable *added_blocks)
{
    Seafile *f1 = NULL, *f2 = NULL;
    char file_id1[41], file_id2[41];
    int i;

    rawdata_to_hex (sha1, file_id1, 20);
    rawdata_to_hex (old_sha1, file_id2, 20);
    f1 = seaf_fs_manager_get_seafile (seaf->fs_mgr,
                                      repo_id, repo_version,
                                      file_id1);
    if (!f1) {
        seaf_warning ("Failed to get seafile object %s:%s. Path is %s.\n",
                      repo_id, file_id1, path);
        return -1;
    }
    f2 = seaf_fs_manager_get_seafile (seaf->fs_mgr,
                                      repo_id, repo_version,
                                      file_id2);
    if (!f2) {
        seafile_unref (f1);
        seaf_warning ("Failed to get seafile object %s:%s. Path is %s.\n",
                      repo_id, file_id2, path);
        return -1;
    }

    GHashTable *h = g_hash_table_new (g_str_hash, g_str_equal);
    int dummy;
    for (i = 0; i < f2->n_blocks; ++i)
        g_hash_table_insert (h, f2->blk_sha1s[i], &dummy);

    for (i = 0; i < f1->n_blocks; ++i)
        if (!g_hash_table_lookup (h, f1->blk_sha1s[i]))
            add_to_block_list (block_list, added_blocks, f1->blk_sha1s[i]);

    seafile_unref (f1);
    seafile_unref (f2);
    g_hash_table_destroy (h);

    return 0;
}

typedef struct ExpandAddedDirData {
    char repo_id[41];
    int repo_version;
    GList **block_list;
    GHashTable *added_blocks;
    GHashTable *needed_fs;
} ExpandAddedDirData;

static gboolean
expand_dir_added_cb (SeafFSManager *mgr,
                     const char *path,
                     SeafDirent *dent,
                     void *user_data,
                     gboolean *stop)
{
    ExpandAddedDirData *data = user_data;
    unsigned char sha1[20];

    if (S_ISREG(dent->mode)) {
        hex_to_rawdata (dent->id, sha1, 20);

        if (add_one_file_to_block_list (data->repo_id, data->repo_version,
                                        dent->name, sha1,
                                        data->block_list, data->added_blocks,
                                        data->needed_fs) < 0)
            return FALSE;
    }

    return TRUE;
}

static int
add_new_dir_to_block_list (const char *repo_id, int version,
                           const char *root, DiffEntry *de,
                           GList **block_list, GHashTable *added_blocks,
                           GHashTable *needed_fs)
{
    ExpandAddedDirData data;
    char obj_id[41];

    memcpy (data.repo_id, repo_id, 40);
    data.repo_version = version;
    data.block_list = block_list;
    data.added_blocks = added_blocks;
    data.needed_fs = needed_fs;

    rawdata_to_hex (de->sha1, obj_id, 20);
    if (seaf_fs_manager_traverse_path (seaf->fs_mgr,
                                       repo_id, version,
                                       root,
                                       de->name,
                                       expand_dir_added_cb,
                                       &data) < 0) {
        return -1;
    }

    return 0;
}

static int
calculate_block_list_from_diff_results (HttpTxTask *task, const char *local_root,
                                        GHashTable *needed_fs,
                                        GList *results, GList **block_list)
{
    GHashTable *added_blocks;
    GList *ptr;
    DiffEntry *de;
    int ret = 0;

    added_blocks = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);

    for (ptr = results; ptr; ptr = ptr->next) {
        de = ptr->data;

        if (de->status == DIFF_STATUS_ADDED) {
            ret = add_one_file_to_block_list (task->repo_id, task->repo_version,
                                              de->name, de->sha1,
                                              block_list, added_blocks,
                                              needed_fs);
            if (ret < 0)
                break;
        } else if (de->status == DIFF_STATUS_MODIFIED) {
            ret = diff_two_files_to_block_list (task->repo_id, task->repo_version,
                                                de->name, de->sha1, de->old_sha1,
                                                block_list, added_blocks);
            if (ret < 0)
                break;
        } else if (de->status == DIFF_STATUS_DIR_ADDED) {
            ret = add_new_dir_to_block_list (task->repo_id, task->repo_version,
                                             local_root, de,
                                             block_list, added_blocks,
                                             needed_fs);
            if (ret < 0)
                break;
        }
    }

    g_hash_table_destroy (added_blocks);
    return ret;
}

static int
calculate_block_list (HttpTxTask *task, GList **plist, GHashTable *needed_fs)
{
    int ret = 0;
    SeafBranch *local = NULL, *master = NULL;
    SeafCommit *local_head = NULL, *master_head = NULL;
    GList *results = NULL;
    GList *block_list = NULL;

    local = seaf_branch_manager_get_branch (seaf->branch_mgr, task->repo_id, "local");
    if (!local) {
        seaf_warning ("Branch local not found for repo %.8s.\n", task->repo_id);
        ret = -1;
        goto out;
    }
    master = seaf_branch_manager_get_branch (seaf->branch_mgr, task->repo_id, "master");
    if (!master) {
        seaf_warning ("Branch master not found for repo %.8s.\n", task->repo_id);
        ret = -1;
        goto out;
    }

    local_head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                                 task->repo_id, task->repo_version,
                                                 local->commit_id);
    if (!local_head) {
        seaf_warning ("Local head commit not found for repo %.8s.\n",
                      task->repo_id);
        ret = -1;
        goto out;
    }
    master_head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                                 task->repo_id, task->repo_version,
                                                 master->commit_id);
    if (!master_head) {
        seaf_warning ("Master head commit not found for repo %.8s.\n",
                      task->repo_id);
        ret = -1;
        goto out;
    }

    if (diff_commit_roots (task->repo_id, task->repo_version,
                           master_head->root_id, local_head->root_id,
                           &results, TRUE) < 0) {
        seaf_warning ("Failed to diff local and master heads for repo %.8s.\n",
                      task->repo_id);
        ret = -1;
        goto out;
    }

    if (calculate_block_list_from_diff_results (task, local_head->root_id,
                                                needed_fs,
                                                results, &block_list) < 0) {
        ret = -1;
        goto out;
    }

    *plist = block_list;

out:
    seaf_branch_unref (local);
    seaf_branch_unref (master);
    seaf_commit_unref (local_head);
    seaf_commit_unref (master_head);
    g_list_free_full (results, (GDestroyNotify)diff_entry_free);
    if (ret < 0)
        g_list_free_full (block_list, g_free);
    return ret;
}

#endif  /* 0 */

typedef struct FileUploadProgress {
    char *server;
    char *user;
    guint64 uploaded;
    guint64 total_upload;
} FileUploadProgress;

typedef struct FileUploadedInfo {
    char *server;
    char *user;
    char *file_path;
} FileUploadedInfo;

typedef struct {
    char block_id[41];
    BlockHandle *block;
    HttpTxTask *task;
    FileUploadProgress *progress;
} SendBlockData;

static FileUploadedInfo *
file_uploaded_info_new (const char *server, const char *user,
                       const char *file_path)
{
    FileUploadedInfo *info = g_new0 (FileUploadedInfo, 1);
    info->server = g_strdup (server);
    info->user = g_strdup (user);
    info->file_path = g_strdup (file_path);

    return info;
}

static void
file_uploaded_info_free (FileUploadedInfo *info)
{
    if (!info)
        return;
    g_free (info->server);
    g_free (info->user);
    g_free (info->file_path);
    g_free (info);

    return;
}

static size_t
send_block_callback (void *ptr, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size *nmemb;
    SendBlockData *data = userp;
    HttpTxTask *task = data->task;
    int n;

    if (task->state == HTTP_TASK_STATE_CANCELED || task->all_stop)
        return CURL_READFUNC_ABORT;

    n = seaf_block_manager_read_block (seaf->block_mgr,
                                       data->block,
                                       ptr, realsize);
    if (n < 0) {
        seaf_warning ("Failed to read block %s in repo %.8s.\n",
                      data->block_id, task->repo_id);
        task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
        return CURL_READFUNC_ABORT;
    }

    /* Update global transferred bytes. */
    g_atomic_int_add (&(seaf->sync_mgr->sent_bytes), n);

    /* Update transferred bytes for this task */
    g_atomic_int_add (&task->tx_bytes, n);

    data->progress->uploaded += n;

    /* If uploaded bytes exceeds the limit, wait until the counter
     * is reset. We check the counter every 100 milliseconds, so we
     * can waste up to 100 milliseconds without sending data after
     * the counter is reset.
     */
    while (1) {
        gint sent = g_atomic_int_get(&(seaf->sync_mgr->sent_bytes));
        if (seaf->sync_mgr->upload_limit > 0 &&
            sent > seaf->sync_mgr->upload_limit)
            /* 100 milliseconds */
            g_usleep (100000);
        else
            break;
    }

    return n;
}

static size_t
rewind_block_callback (void *clientp, curl_off_t offset, int origin)
{
    if (offset != 0 || origin != 0) {
        return CURL_SEEKFUNC_FAIL;
    }

    SendBlockData *data = clientp;
    HttpTxTask *task = data->task;

    int rc = seaf_block_manager_rewind_block (seaf->block_mgr,
                                              data->block);
    if (rc < 0) {
        seaf_warning ("Failed to rewind block %s in repo %s.\n",
                      data->block_id, task->repo_id);
        return CURL_SEEKFUNC_FAIL;
    }

    return CURL_SEEKFUNC_OK;
}

static int
send_block (HttpTxTask *task, Connection *conn,
            const char *block_id, uint32_t size,
            FileUploadProgress *progress)
{
    CURL *curl;
    char *url;
    int status;
    BlockHandle *block;
    int ret = 0;

    block = seaf_block_manager_open_block (seaf->block_mgr,
                                           task->repo_id, task->repo_version,
                                           block_id, BLOCK_READ);
    if (!block) {
        seaf_warning ("Failed to open block %s in repo %s.\n",
                      block_id, task->repo_id);
        return -1;
    }

    SendBlockData data;
    memset (&data, 0, sizeof(data));
    memcpy (data.block_id, block_id, 40);
    data.block = block;
    data.task = task;
    data.progress = progress;

    curl = conn->curl;

    if (!task->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/block/%s",
                               task->host, task->repo_id, block_id);
    else
        url = g_strdup_printf ("%s/repo/%s/block/%s",
                               task->host, task->repo_id, block_id);

    int curl_error;
    if (http_put (curl, url, task->token,
                  NULL, size,
                  send_block_callback, &data, rewind_block_callback,
                  &status, NULL, NULL, TRUE, &curl_error) < 0) {
        if (task->state == HTTP_TASK_STATE_CANCELED)
            goto out;

        if (task->error == HTTP_TASK_OK) {
            /* Only release connection when it's a network error */
            conn->release = TRUE;
            handle_curl_errors (task, curl_error);
        }
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for PUT %s: %d.\n", url, status);
        handle_http_errors (task, status);
        ret = -1;
    }

out:
    g_free (url);
    curl_easy_reset (curl);
    seaf_block_manager_close_block (seaf->block_mgr, block);
    seaf_block_manager_block_handle_free (seaf->block_mgr, block);

    return ret;
}

gboolean
get_needed_block_list (Connection *conn, HttpTxTask *task,
                       char *url, Seafile *file, GList **block_list)
{
    int i, j;
    int check_num = 0;
    json_t *check_list = json_array ();
    char *check_list_str;
    json_t *ret_list;
    json_error_t jerror;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    const char *block_id;
    gboolean ret = TRUE;
    int curl_error;
    CURL *curl = conn->curl;

    for (i = 0; i < file->n_blocks; i++) {
        json_array_append_new (check_list, json_string (file->blk_sha1s[i]));
        check_num++;

        if (check_num == ID_LIST_SEGMENT_N || i == file->n_blocks - 1) {
            check_list_str = json_dumps (check_list, JSON_COMPACT);
            curl_error = 0;
            if (http_post (curl, url, task->token,
                           check_list_str, strlen(check_list_str),
                           &status, &rsp_content, &rsp_size, TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
                conn->release = TRUE;
                handle_curl_errors (task, curl_error);
                g_free (check_list_str);
                ret = FALSE;
                goto out;
            }
            g_free (check_list_str);

            if (status != HTTP_OK) {
                seaf_warning ("Bad response code for POST %s: %d.\n", url, status);
                handle_http_errors (task, status);
                g_free (rsp_content);
                ret = FALSE;
                goto out;
            }

            ret_list = json_loadb (rsp_content, rsp_size, 0, &jerror);
            if (!ret_list) {
                seaf_warning ("Invalid JSON response from the server: %s.\n", jerror.text);
                task->error = HTTP_TASK_ERR_SERVER;
                g_free (rsp_content);
                ret = FALSE;
                goto out;
            }
            g_free (rsp_content);

            for (j = 0; j < json_array_size (ret_list); j++) {
                block_id = json_string_value (json_array_get (ret_list, j));
                if (!block_id || strlen (block_id) != 40) {
                    seaf_warning ("Invalid block_id list returned from server. "
                                  "Bad block id %s\n", block_id);
                    task->error = HTTP_TASK_ERR_SERVER;
                    json_decref (ret_list);
                    ret = FALSE;
                    goto out;
                }

                *block_list = g_list_prepend (*block_list, g_strdup (block_id));
            }
            json_decref (ret_list);

            rsp_content = NULL;
            check_num = 0;
            json_array_clear (check_list);
        }
    }

out:
    curl_easy_reset (curl);
    json_decref (check_list);
    return ret;
}

static int
upload_file (HttpTxTask *http_task, Connection *conn,
             const char *file_path, guint64 file_size, GList *block_list)
{
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    FileUploadProgress progress;
    GHashTable *block_size_pair;
    char *abs_path;
    GList *iter;
    char *block_id;
    BlockMetadata *meta;
    guint64 upload_size = 0;
    int ret = 0;

    abs_path = g_build_filename (http_task->repo_uname, file_path, NULL);

    if (!block_list && file_size == 0) {
        FileUploadedInfo *file_info = file_uploaded_info_new (http_task->server, http_task->user, abs_path);
        pthread_mutex_lock (&priv->progress_lock);
        g_queue_push_head (priv->uploaded_files, file_info);
        if (priv->uploaded_files->length > MAX_GET_FINISHED_FILES)
            file_uploaded_info_free (g_queue_pop_tail (priv->uploaded_files));
        pthread_mutex_unlock (&priv->progress_lock);

        g_free (abs_path);
        return 0;
    } else if (!block_list) {
        g_free (abs_path);
        return 0;
    }

    progress.server = http_task->server;
    progress.user = http_task->user;
    progress.uploaded = 0;
    progress.total_upload = file_size;
    block_size_pair = g_hash_table_new (g_str_hash, g_str_equal);

    // Collect file total upload size
    for (iter = block_list; iter; iter = iter->next) {
        block_id = iter->data;
        meta = seaf_block_manager_stat_block (seaf->block_mgr,
                                              http_task->repo_id,
                                              http_task->repo_version,
                                              block_id);
        if (!meta) {
            seaf_warning ("Failed to stat block %s for file %s in repo %s.\n",
                          block_id, file_path, http_task->repo_id);
            http_task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
            ret = -1;
            goto out;
        }

        upload_size += meta->size;
        g_hash_table_replace (block_size_pair, block_id, (void *)meta->size);
        g_free (meta);
    }
    progress.uploaded = file_size - upload_size;

    // Update upload progress info
    pthread_mutex_lock (&priv->progress_lock);
    g_hash_table_replace (priv->uploading_files, g_strdup (abs_path), &progress);
    pthread_mutex_unlock (&priv->progress_lock);

    int n = 0;
    for (iter = block_list; iter; iter = iter->next) {
        block_id = iter->data;
        if (send_block (http_task, conn, block_id,
                        (uint32_t)g_hash_table_lookup (block_size_pair, block_id),
                        &progress) < 0) {
            ret = -1;
            break;
        }
        ++n;
    }

    seaf_debug ("Uploaded file %s, %d blocks.\n", file_path, n);

    // Update upload progress info
    pthread_mutex_lock (&priv->progress_lock);
    g_hash_table_remove (priv->uploading_files, abs_path);
    if (ret == 0) {
        FileUploadedInfo *file_info = file_uploaded_info_new (http_task->server, http_task->user, abs_path);
        g_queue_push_head (priv->uploaded_files, file_info);
        if (priv->uploaded_files->length > MAX_GET_FINISHED_FILES)
            file_uploaded_info_free (g_queue_pop_tail (priv->uploaded_files));
    }
    pthread_mutex_unlock (&priv->progress_lock);

out:
    g_hash_table_destroy (block_size_pair);
    g_free (abs_path);
    return ret;
}

typedef struct FileUploadData {
    HttpTxTask *http_task;
    char *check_block_url;
    ConnectionPool *cpool;
    GAsyncQueue *finished_tasks;
} FileUploadData;

typedef struct FileUploadTask {
    char *file_id;
    char *file_path;
    int result;
} FileUploadTask;

static void
upload_file_thread_func (gpointer data, gpointer user_data)
{
    FileUploadTask *task = data;
    FileUploadData *tx_data = user_data;
    HttpTxTask *http_task = tx_data->http_task;
    Connection *conn;
    Seafile *file = NULL;
    GList *block_list = NULL;
    int ret = 0;

    conn = connection_pool_get_connection (tx_data->cpool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", http_task->host);
        ret = -1;
        http_task->error = HTTP_TASK_ERR_NOT_ENOUGH_MEMORY;
        goto out;
    }

    file = seaf_fs_manager_get_seafile (seaf->fs_mgr,
                                        http_task->repo_id,
                                        http_task->repo_version,
                                        task->file_id);
    if (!file) {
        seaf_warning ("Failed to get file %s in repo %.8s.\n",
                      task->file_id, http_task->repo_id);
        http_task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
        ret = -1;
        goto out;
    }

    if (!get_needed_block_list (conn, http_task,
                                tx_data->check_block_url, file,
                                &block_list)) {
        seaf_warning ("Failed to get needed block list for file %s\n", task->file_path);
        ret = -1;
        goto out;
    }

    ret = upload_file (http_task, conn, task->file_path, file->file_size, block_list);

    g_list_free_full (block_list, (GDestroyNotify)g_free);

out:
    task->result = ret;
    g_async_queue_push (tx_data->finished_tasks, task);

    if (file)
        seafile_unref (file);
    if (conn)
        connection_pool_return_connection (tx_data->cpool, conn);
}

#define DEFAULT_UPLOAD_BLOCK_THREADS 3

static int
multi_threaded_send_files (HttpTxTask *http_task, GHashTable *needed_file_pair)
{
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    GThreadPool *tpool;
    GAsyncQueue *finished_tasks;
    GHashTable *pending_tasks;
    char *check_block_url;
    ConnectionPool *cpool;
    GHashTableIter iter;
    gpointer key;
    gpointer value;
    FileUploadTask *task;
    int ret = 0;

    // No file need to be uploaded, return directly
    if (g_hash_table_size (needed_file_pair) == 0) {
        return 0;
    }

    cpool = find_connection_pool (priv, http_task->host);
    if (!cpool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", http_task->host);
        http_task->error = HTTP_TASK_ERR_NOT_ENOUGH_MEMORY;
        return -1;
    }

    if (!http_task->use_fileserver_port)
        check_block_url = g_strdup_printf ("%s/seafhttp/repo/%s/check-blocks/",
                                           http_task->host, http_task->repo_id);
    else
        check_block_url = g_strdup_printf ("%s/repo/%s/check-blocks/",
                                           http_task->host, http_task->repo_id);

    finished_tasks = g_async_queue_new ();

    FileUploadData data;
    data.http_task = http_task;
    data.finished_tasks = finished_tasks;
    data.check_block_url = check_block_url;
    data.cpool = cpool;

    tpool = g_thread_pool_new (upload_file_thread_func, &data,
                               DEFAULT_UPLOAD_BLOCK_THREADS, FALSE, NULL);

    pending_tasks = g_hash_table_new_full (g_str_hash, g_str_equal,
                                           NULL, (GDestroyNotify)g_free);

    g_hash_table_iter_init (&iter, needed_file_pair);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        task = g_new0 (FileUploadTask, 1);
        task->file_id = (char *)key;
        task->file_path = (char *)value;

        g_hash_table_insert (pending_tasks, task->file_id, task);

        g_thread_pool_push (tpool, task, NULL);
    }

    while ((task = g_async_queue_pop (finished_tasks)) != NULL) {
        if (task->result < 0 || http_task->state == HTTP_TASK_STATE_CANCELED) {
            ret = task->result;
            http_task->all_stop = TRUE;
            break;
        }

        g_hash_table_remove (pending_tasks, task->file_id);
        if (g_hash_table_size(pending_tasks) == 0)
            break;
    }

    g_thread_pool_free (tpool, TRUE, TRUE);
    g_hash_table_destroy (pending_tasks);
    g_async_queue_unref (finished_tasks);
    g_free (check_block_url);

    return ret;
}

static void
notify_permission_error (HttpTxTask *task, const char *error_str)
{
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    GMatchInfo *match_info;
    char *path;

    if (g_regex_match (priv->locked_error_regex, error_str, 0, &match_info)) {
        path = g_match_info_fetch (match_info, 1);
        task->unsyncable_path = path;
        /* Set more accurate error. */
        task->error = HTTP_TASK_ERR_FILE_LOCKED;
    } else if (g_regex_match (priv->folder_perm_error_regex, error_str, 0, &match_info)) {
        path = g_match_info_fetch (match_info, 1);
        task->unsyncable_path = path;
        task->error = HTTP_TASK_ERR_FOLDER_PERM_DENIED;
    } else if (g_regex_match (priv->too_many_files_error_regex, error_str, 0, &match_info)) {
        task->error = HTTP_TASK_ERR_TOO_MANY_FILES;
    }

    g_match_info_free (match_info);
}

static int
update_branch (HttpTxTask *task, Connection *conn)
{
    CURL *curl;
    char *url;
    int status;
    int ret = 0;
    char *rsp_content;
    char *rsp_content_str = NULL;
    gint64 rsp_size;

    curl = conn->curl;

    if (!task->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/commit/HEAD/?head=%s",
                               task->host, task->repo_id, task->head);
    else
        url = g_strdup_printf ("%s/repo/%s/commit/HEAD/?head=%s",
                               task->host, task->repo_id, task->head);

    int curl_error;
    if (http_put (curl, url, task->token,
                  NULL, 0,
                  NULL, NULL, NULL,
                  &status, &rsp_content, &rsp_size, TRUE, &curl_error) < 0) {
        conn->release = TRUE;
        handle_curl_errors (task, curl_error);
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for PUT %s: %d.\n", url, status);
        handle_http_errors (task, status);

        if (status == HTTP_FORBIDDEN || status == HTTP_BLOCK_MISSING) {
            rsp_content_str = g_new0 (gchar, rsp_size + 1);
            memcpy (rsp_content_str, rsp_content, rsp_size);
            if (status == HTTP_FORBIDDEN) {
                seaf_warning ("%s\n", rsp_content_str);
                notify_permission_error (task, rsp_content_str);
            } else if (status == HTTP_BLOCK_MISSING) {
                seaf_warning ("Failed to upload files: %s\n", rsp_content_str);
                task->error = HTTP_TASK_ERR_BLOCK_MISSING;
            }
            g_free (rsp_content_str);
        }

        ret = -1;
    }

out:
    g_free (url);
    curl_easy_reset (curl);

    return ret;
}

static int
update_master_branch (HttpTxTask *task)
{
    SeafBranch *master = NULL, *local = NULL;
    int ret = 0;

    local = seaf_branch_manager_get_branch (seaf->branch_mgr,
                                            task->repo_id,
                                            "local");
    if (!local) {
        seaf_warning ("Failed to get local branch for repo %s.\n", task->repo_id);
        return -1;
    }

    master = seaf_branch_manager_get_branch (seaf->branch_mgr,
                                             task->repo_id,
                                             "master");
    if (!master) {
        master = seaf_branch_new ("master", task->repo_id, task->head, 0);
        if (seaf_branch_manager_add_branch (seaf->branch_mgr, master) < 0) {
            ret = -1;
            goto out;
        }
    } else {
        seaf_branch_set_commit (master, task->head);
        master->opid = local->opid;
        if (seaf_branch_manager_update_branch (seaf->branch_mgr, master) < 0) {
            ret = -1;
            goto out;
        }
    }

    /* The locally cache repo status from server need to be updated too.
     * Otherwise sync-mgr will find the master head and server head are different,
     * then start a download.
     */
    seaf_repo_manager_set_repo_info_head_commit (seaf->repo_mgr,
                                                 task->repo_id,
                                                 task->head);

out:
    seaf_branch_unref (master);
    seaf_branch_unref (local);
    return ret;
}

static void
set_path_status_syncing (gpointer key, gpointer value, gpointer user_data)
{
    HttpTxTask *task = user_data;
    char *path = key;
    int mode = (int)(long)value;
    seaf_sync_manager_update_active_path (seaf->sync_mgr,
                                          task->repo_id,
                                          path,
                                          mode,
                                          SYNC_STATUS_SYNCING);
}

static void
set_path_status_synced (gpointer key, gpointer value, gpointer user_data)
{
    HttpTxTask *task = user_data;
    char *path = key;
    int mode = (int)(long)value;

    seaf_sync_manager_update_active_path (seaf->sync_mgr,
                                          task->repo_id,
                                          path,
                                          mode,
                                          SYNC_STATUS_SYNCED);

    if (S_ISREG(mode)) {
        file_cache_mgr_set_file_uploaded (seaf->file_cache_mgr,
                                          task->repo_id, path, TRUE);
    }
}

static void *
http_upload_thread (void *vdata)
{
    HttpTxTask *task = vdata;
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn = NULL;
    char *url = NULL;
    GList *send_fs_list = NULL, *needed_fs_list = NULL;
    GHashTable *active_paths = NULL;
    // file_id <-> file_path
    GHashTable *need_upload_file_pair = NULL;

    SeafBranch *local = seaf_branch_manager_get_branch (seaf->branch_mgr,
                                                        task->repo_id, "local");
    if (!local) {
        seaf_warning ("Failed to get branch local of repo %.8s.\n", task->repo_id);
        task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
        return vdata;
    }
    memcpy (task->head, local->commit_id, 40);
    seaf_branch_unref (local);

    pool = find_connection_pool (priv, task->host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", task->host);
        task->error = HTTP_TASK_ERR_NOT_ENOUGH_MEMORY;
        goto out;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", task->host);
        task->error = HTTP_TASK_ERR_NOT_ENOUGH_MEMORY;
        goto out;
    }

    seaf_message ("Upload with HTTP sync protocol version %d.\n",
                  task->protocol_version);

    transition_state (task, task->state, HTTP_TASK_RT_STATE_CHECK);

    gint64 delta = 0;
    active_paths = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);

    if (calculate_upload_size_delta_and_active_paths (task, &delta, active_paths) < 0) {
        seaf_warning ("Failed to calculate upload size delta for repo %s.\n",
                      task->repo_id);
        task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
        goto out;
    }

    g_hash_table_foreach (active_paths, set_path_status_syncing, task);

    if (check_permission (task, conn) < 0) {
        seaf_warning ("Upload permission denied for repo %.8s on server %s.\n",
                      task->repo_id, task->host);
        goto out;
    }

    if (check_quota (task, conn, delta) < 0) {
        seaf_warning ("Not enough quota for repo %.8s on server %s.\n",
                      task->repo_id, task->host);
        goto out;
    }

    if (task->state == HTTP_TASK_STATE_CANCELED)
        goto out;

    transition_state (task, task->state, HTTP_TASK_RT_STATE_COMMIT);

    if (send_commit_object (task, conn) < 0) {
        seaf_warning ("Failed to send head commit for repo %.8s.\n", task->repo_id);
        goto out;
    }

    if (task->state == HTTP_TASK_STATE_CANCELED)
        goto out;

    transition_state (task, task->state, HTTP_TASK_RT_STATE_FS);

    need_upload_file_pair = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);

    send_fs_list = calculate_send_fs_object_list (task, need_upload_file_pair);
    if (!send_fs_list) {
        seaf_warning ("Failed to calculate fs object list for repo %.8s.\n",
                      task->repo_id);
        task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
        goto out;
    }

    if (!task->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/check-fs/",
                               task->host, task->repo_id);
    else
        url = g_strdup_printf ("%s/repo/%s/check-fs/",
                               task->host, task->repo_id);

    while (send_fs_list != NULL) {
        if (upload_check_id_list_segment (task, conn, url,
                                          &send_fs_list, &needed_fs_list) < 0) {
            seaf_warning ("Failed to check fs list for repo %.8s.\n", task->repo_id);
            goto out;
        }

        if (task->state == HTTP_TASK_STATE_CANCELED)
            goto out;
    }
    g_free (url);
    url = NULL;

    while (needed_fs_list != NULL) {
        if (send_fs_objects (task, conn, &needed_fs_list) < 0) {
            seaf_warning ("Failed to send fs objects for repo %.8s.\n", task->repo_id);
            goto out;
        }

        if (task->state == HTTP_TASK_STATE_CANCELED)
            goto out;
    }

    transition_state (task, task->state, HTTP_TASK_RT_STATE_BLOCK);

    if (multi_threaded_send_files(task, need_upload_file_pair) < 0 ||
        task->state == HTTP_TASK_STATE_CANCELED)
        goto out;

    transition_state (task, task->state, HTTP_TASK_RT_STATE_UPDATE_BRANCH);

    if (update_branch (task, conn) < 0) {
        seaf_warning ("Failed to update branch of repo %.8s.\n", task->repo_id);
        goto out;
    }

    /* After successful upload, the cached 'master' branch should be updated to
     * the head commit of 'local' branch.
     */
    if (update_master_branch (task) < 0) {
        task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
        goto out;
    }

    if (active_paths != NULL)
        g_hash_table_foreach (active_paths, set_path_status_synced, task);

out:
    string_list_free (send_fs_list);
    string_list_free (needed_fs_list);

    if (active_paths)
        g_hash_table_destroy (active_paths);
    if (need_upload_file_pair)
        g_hash_table_destroy (need_upload_file_pair);

    connection_pool_return_connection (pool, conn);

    return vdata;
}

static void
http_upload_done (void *vdata)
{
    HttpTxTask *task = vdata;

    if (task->error != HTTP_TASK_OK)
        transition_state (task, HTTP_TASK_STATE_ERROR, HTTP_TASK_RT_STATE_FINISHED);
    else if (task->state == HTTP_TASK_STATE_CANCELED)
        transition_state (task, task->state, HTTP_TASK_RT_STATE_FINISHED);
    else
        transition_state (task, HTTP_TASK_STATE_FINISHED, HTTP_TASK_RT_STATE_FINISHED);
}

/* Download */

static void *http_download_thread (void *vdata);
static void http_download_done (void *vdata);
/* static void notify_conflict (CEvent *event, void *data); */

int
http_tx_manager_add_download (HttpTxManager *manager,
                              const char *repo_id,
                              int repo_version,
                              const char *server,
                              const char *user,
                              const char *host,
                              const char *token,
                              const char *server_head_id,
                              gboolean is_clone,
                              int protocol_version,
                              gboolean use_fileserver_port,
                              GError **error)
{
    HttpTxTask *task;

    if (!repo_id || !token || !host || !server_head_id) {
        g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Empty argument(s)");
        return -1;
    }

    if (!is_clone) {
        if (!seaf_repo_manager_repo_exists (seaf->repo_mgr, repo_id)) {
            g_set_error (error, SEAFILE_DOMAIN, SEAF_ERR_BAD_ARGS, "Repo not found");
            return -1;
        }
    }

    clean_tasks_for_repo (manager, repo_id);

    task = http_tx_task_new (manager, repo_id, repo_version,
                             HTTP_TASK_TYPE_DOWNLOAD, is_clone,
                             host, token);

    memcpy (task->head, server_head_id, 40);
    task->protocol_version = protocol_version;

    task->state = HTTP_TASK_STATE_NORMAL;

    task->use_fileserver_port = use_fileserver_port;
    task->server = g_strdup (server);
    task->user = g_strdup (user);

    g_hash_table_insert (manager->priv->download_tasks,
                         g_strdup(repo_id),
                         task);

    if (seaf_job_manager_schedule_job (seaf->job_mgr,
                                       http_download_thread,
                                       http_download_done,
                                       task) < 0) {
        g_hash_table_remove (manager->priv->download_tasks, repo_id);
        return -1;
    }

    return 0;
}

static int
get_commit_object (HttpTxTask *task, Connection *conn)
{
    CURL *curl;
    char *url;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    int ret = 0;

    curl = conn->curl;

    if (!task->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/commit/%s",
                               task->host, task->repo_id, task->head);
    else
        url = g_strdup_printf ("%s/repo/%s/commit/%s",
                               task->host, task->repo_id, task->head);

    int curl_error;
    if (http_get (curl, url, task->token, &status,
                  &rsp_content, &rsp_size,
                  NULL, NULL, TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        handle_curl_errors (task, curl_error);
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
        handle_http_errors (task, status);
        ret = -1;
        goto out;
    }

    int rc = seaf_obj_store_write_obj (seaf->commit_mgr->obj_store,
                                       task->repo_id, task->repo_version,
                                       task->head,
                                       rsp_content,
                                       rsp_size,
                                       FALSE);
    if (rc < 0) {
        seaf_warning ("Failed to save commit %s in repo %.8s.\n",
                      task->head, task->repo_id);
        task->error = HTTP_TASK_ERR_WRITE_LOCAL_DATA;
        ret = -1;
    }

out:
    g_free (url);
    g_free (rsp_content);
    curl_easy_reset (curl);

    return ret;
}

static int
get_needed_fs_id_list (HttpTxTask *task, Connection *conn, GList **fs_id_list)
{
    SeafBranch *master;
    CURL *curl;
    char *url = NULL;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    int ret = 0;
    json_t *array;
    json_error_t jerror;
    const char *obj_id;

    const char *url_prefix = (task->use_fileserver_port) ? "" : "seafhttp/";

    if (!task->is_clone) {
        master = seaf_branch_manager_get_branch (seaf->branch_mgr,
                                                 task->repo_id,
                                                 "master");
        if (!master) {
            seaf_warning ("Failed to get branch master for repo %.8s.\n",
                          task->repo_id);
            return -1;
        }

        url = g_strdup_printf ("%s/%srepo/%s/fs-id-list/"
                               "?server-head=%s&client-head=%s&dir-only=1",
                               task->host, url_prefix, task->repo_id,
                               task->head, master->commit_id);

        seaf_branch_unref (master);
    } else {
        url = g_strdup_printf ("%s/%srepo/%s/fs-id-list/?server-head=%s&dir-only=1",
                               task->host, url_prefix, task->repo_id, task->head);
    }

    curl = conn->curl;

    int curl_error;
    if (http_get (curl, url, task->token, &status,
                  &rsp_content, &rsp_size,
                  NULL, NULL, (!task->is_clone), FS_ID_LIST_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        handle_curl_errors (task, curl_error);
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
        handle_http_errors (task, status);
        ret = -1;
        goto out;
    }

    array = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!array) {
        seaf_warning ("Invalid JSON response from the server: %s.\n", jerror.text);
        task->error = HTTP_TASK_ERR_SERVER;
        ret = -1;
        goto out;
    }

    int i;
    size_t n = json_array_size (array);
    json_t *str;

    seaf_debug ("Received fs object list size %lu from %s:%s.\n",
                n, task->host, task->repo_id);

    task->n_fs_objs = (int)n;

    GHashTable *checked_objs = g_hash_table_new_full (g_str_hash, g_str_equal,
                                                      g_free, NULL);

    for (i = 0; i < n; ++i) {
        str = json_array_get (array, i);
        if (!str) {
            seaf_warning ("Invalid JSON response from the server.\n");
            json_decref (array);
            string_list_free (*fs_id_list);
            ret = -1;
            goto out;
        }

        obj_id = json_string_value(str);

        if (g_hash_table_lookup (checked_objs, obj_id)) {
            ++(task->done_fs_objs);
            continue;
        }
        char *key = g_strdup(obj_id);
        g_hash_table_replace (checked_objs, key, key);

        if (!seaf_obj_store_obj_exists (seaf->fs_mgr->obj_store,
                                        task->repo_id, task->repo_version,
                                        obj_id)) {
            *fs_id_list = g_list_prepend (*fs_id_list, g_strdup(obj_id));
        } else if (task->is_clone) {
            gboolean io_error = FALSE;
            gboolean sound;
            sound = seaf_fs_manager_verify_object (seaf->fs_mgr,
                                                   task->repo_id, task->repo_version,
                                                   obj_id, FALSE, &io_error);
            if (!sound && !io_error) {
                *fs_id_list = g_list_prepend (*fs_id_list, g_strdup(obj_id));
            } else {
                ++(task->done_fs_objs);
            }
        } else {
            ++(task->done_fs_objs);
        }
    }

    json_decref (array);
    g_hash_table_destroy (checked_objs);

out:
    g_free (url);
    g_free (rsp_content);
    curl_easy_reset (curl);

    return ret;
}

#define GET_FS_OBJECT_N 100

static int
get_fs_objects (HttpTxTask *task, Connection *conn, GList **fs_list)
{
    json_t *array;
    char *obj_id;
    int n_sent = 0;
    char *data = NULL;
    int len;
    CURL *curl;
    char *url = NULL;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    int ret = 0;
    GHashTable *requested;

    /* Convert object id list to JSON format. */

    requested = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);

    array = json_array ();

    while (*fs_list != NULL) {
        obj_id = (*fs_list)->data;
        json_array_append_new (array, json_string(obj_id));

        *fs_list = g_list_delete_link (*fs_list, *fs_list);

        g_hash_table_replace (requested, obj_id, obj_id);

        if (++n_sent >= GET_FS_OBJECT_N)
            break;
    }

    seaf_debug ("Requesting %d fs objects from %s:%s.\n",
                n_sent, task->host, task->repo_id);

    data = json_dumps (array, 0);
    len = strlen(data);
    json_decref (array);

    /* Send fs object id list. */

    curl = conn->curl;

    if (!task->use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/pack-fs/", task->host, task->repo_id);
    else
        url = g_strdup_printf ("%s/repo/%s/pack-fs/", task->host, task->repo_id);

    int curl_error;
    if (http_post (curl, url, task->token,
                   data, len,
                   &status, &rsp_content, &rsp_size, TRUE, HTTP_TIMEOUT_SEC, &curl_error) < 0) {
        conn->release = TRUE;
        handle_curl_errors (task, curl_error);
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for POST %s: %d.\n", url, status);
        handle_http_errors (task, status);
        ret = -1;
        goto out;
    }

    /* Save received fs objects. */

    int n_recv = 0;
    char *p = rsp_content;
    ObjectHeader *hdr = (ObjectHeader *)p;
    char recv_obj_id[41];
    int n = 0;
    int size;
    int rc;
    while (n < rsp_size) {
        memcpy (recv_obj_id, hdr->obj_id, 40);
        recv_obj_id[40] = 0;
        size = ntohl (hdr->obj_size);
        if (n + sizeof(ObjectHeader) + size > rsp_size) {
            seaf_warning ("Incomplete object package received for repo %.8s.\n",
                          task->repo_id);
            task->error = HTTP_TASK_ERR_SERVER;
            ret = -1;
            goto out;
        }

        ++n_recv;

        rc = seaf_obj_store_write_obj (seaf->fs_mgr->obj_store,
                                       task->repo_id, task->repo_version,
                                       recv_obj_id,
                                       hdr->object,
                                       size, FALSE);
        if (rc < 0) {
            seaf_warning ("Failed to write fs object %s in repo %.8s.\n",
                          recv_obj_id, task->repo_id);
            task->error = HTTP_TASK_ERR_WRITE_LOCAL_DATA;
            ret = -1;
            goto out;
        }

        g_hash_table_remove (requested, recv_obj_id);

        ++(task->done_fs_objs);

        p += (sizeof(ObjectHeader) + size);
        n += (sizeof(ObjectHeader) + size);
        hdr = (ObjectHeader *)p;
    }

    seaf_debug ("Received %d fs objects from %s:%s.\n",
                n_recv, task->host, task->repo_id);

    /* The server may not return all the objects we requested.
     * So we need to add back the remaining object ids into fs_list.
     */
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init (&iter, requested);
    while (g_hash_table_iter_next (&iter, &key, &value)) {
        obj_id = key;
        *fs_list = g_list_prepend (*fs_list, g_strdup(obj_id));
    }
    g_hash_table_destroy (requested);

out:
    g_free (url);
    g_free (data);
    g_free (rsp_content);
    curl_easy_reset (curl);

    return ret;
}

int
http_tx_manager_get_block (HttpTxManager *mgr, const char *host,
                           gboolean use_fileserver_port, const char *token,
                           const char *repo_id, const char *block_id,
                           HttpRecvCallback get_blk_cb, void *user_data,
                           int *http_status)
{
    HttpTxPriv *priv = mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    if (!use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/block/%s",
                               host, repo_id, block_id);
    else
        url = g_strdup_printf ("%s/repo/%s/block/%s",
                               host, repo_id, block_id);

    if (http_get (curl, url, token, http_status, NULL, NULL,
                  get_blk_cb, user_data, TRUE, HTTP_TIMEOUT_SEC, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (*http_status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, *http_status);
        ret = -1;
    }

out:
    g_free (url);
    connection_pool_return_connection (pool, conn);
    return ret;
}

int
http_tx_manager_get_fs_object (HttpTxManager *mgr, const char *host,
                               gboolean use_fileserver_port, const char *token,
                               const char *repo_id, const char *obj_id,
                               const char *file_path)
{
    HttpTxPriv *priv = mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    json_t *array;
    char *data = NULL;
    int len;
    CURL *curl;
    char *url = NULL;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    /* Convert object id list to JSON format. */

    array = json_array ();
    json_array_append_new (array, json_string(obj_id));
    data = json_dumps (array, 0);
    len = strlen(data);
    json_decref (array);

    /* Send fs object id list. */

    curl = conn->curl;

    char *esc_path = g_uri_escape_string (file_path, NULL, FALSE);
    if (!use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/pack-fs/?path=%s", host, repo_id, esc_path);
    else
        url = g_strdup_printf ("%s/repo/%s/pack-fs/?path=%s", host, repo_id, esc_path);
    g_free (esc_path);

    if (http_post (curl, url, token,
                   data, len,
                   &status, &rsp_content, &rsp_size, TRUE, HTTP_TIMEOUT_SEC, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for POST %s: %d.\n", url, status);
        ret = -1;
        goto out;
    }

    /* Save received fs objects. */

    char *p = rsp_content;
    ObjectHeader *hdr = (ObjectHeader *)p;
    char recv_obj_id[41];
    int n = 0;
    int size;
    int rc;
    while (n < rsp_size) {
        memcpy (recv_obj_id, hdr->obj_id, 40);
        recv_obj_id[40] = 0;
        size = ntohl (hdr->obj_size);
        if (n + sizeof(ObjectHeader) + size > rsp_size) {
            seaf_warning ("Incomplete object package received for repo %.8s.\n",
                          repo_id);
            ret = -1;
            goto out;
        }

        rc = seaf_obj_store_write_obj (seaf->fs_mgr->obj_store,
                                       repo_id, 1,
                                       recv_obj_id,
                                       hdr->object,
                                       size, FALSE);
        if (rc < 0) {
            seaf_warning ("Failed to write fs object %s in repo %.8s.\n",
                          recv_obj_id, repo_id);
            ret = -1;
            goto out;
        }

        p += (sizeof(ObjectHeader) + size);
        n += (sizeof(ObjectHeader) + size);
        hdr = (ObjectHeader *)p;
    }

out:
    g_free (url);
    g_free (data);
    g_free (rsp_content);
    connection_pool_return_connection (pool, conn);

    return ret;
}

int
http_tx_manager_get_file (HttpTxManager *mgr, const char *host,
                          gboolean use_fileserver_port, const char *token,
                          const char *repo_id,
                          const char *path,
                          gint64 block_offset,
                          HttpRecvCallback get_blk_cb, void *user_data,
                          int *http_status)
{
    HttpTxPriv *priv = mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    char *esc_path = g_uri_escape_string(path, NULL, FALSE);
    if (!use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/file/?path=%s&offset=%"G_GINT64_FORMAT"",
                               host, repo_id, esc_path, block_offset);
    else
        url = g_strdup_printf ("%s/repo/%s/file/?path=%s&offset=%"G_GINT64_FORMAT"",
                               host, repo_id, esc_path, block_offset);
    g_free (esc_path);

    if (http_get (curl, url, token, http_status, NULL, NULL,
                  get_blk_cb, user_data, TRUE, HTTP_TIMEOUT_SEC, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (*http_status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, *http_status);
        ret = -1;
    }

out:
    g_free (url);
    connection_pool_return_connection (pool, conn);
    return ret;
}

/* typedef struct { */
/*     char block_id[41]; */
/*     BlockHandle *block; */
/*     HttpTxTask *task; */
/* } GetBlockData; */

/* static size_t */
/* get_block_callback (void *ptr, size_t size, size_t nmemb, void *userp) */
/* { */
/*     size_t realsize = size *nmemb; */
/*     SendBlockData *data = userp; */
/*     HttpTxTask *task = data->task; */
/*     size_t n; */

/*     if (task->state == HTTP_TASK_STATE_CANCELED || task->all_stop) */
/*         return 0; */

/*     n = seaf_block_manager_write_block (seaf->block_mgr, */
/*                                         data->block, */
/*                                         ptr, realsize); */
/*     if (n < realsize) { */
/*         seaf_warning ("Failed to write block %s in repo %.8s.\n", */
/*                       data->block_id, task->repo_id); */
/*         task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA; */
/*         return n; */
/*     } */

/*     /\* Update global transferred bytes. *\/ */
/*     g_atomic_int_add (&(seaf->sync_mgr->recv_bytes), n); */

/*     /\* Update transferred bytes for this task *\/ */
/*     g_atomic_int_add (&task->tx_bytes, n); */

/*     /\* If uploaded bytes exceeds the limit, wait until the counter */
/*      * is reset. We check the counter every 100 milliseconds, so we */
/*      * can waste up to 100 milliseconds without sending data after */
/*      * the counter is reset. */
/*      *\/ */
/*     while (1) { */
/*         gint sent = g_atomic_int_get(&(seaf->sync_mgr->recv_bytes)); */
/*         if (seaf->sync_mgr->download_limit > 0 && */
/*             sent > seaf->sync_mgr->download_limit) */
/*             /\* 100 milliseconds *\/ */
/*             g_usleep (100000); */
/*         else */
/*             break; */
/*     } */

/*     return n; */
/* } */

/* int */
/* get_block (HttpTxTask *task, Connection *conn, const char *block_id) */
/* { */
/*     CURL *curl; */
/*     char *url; */
/*     int status; */
/*     BlockHandle *block; */
/*     int ret = 0; */
/*     int *pcnt; */

/*     block = seaf_block_manager_open_block (seaf->block_mgr, */
/*                                            task->repo_id, task->repo_version, */
/*                                            block_id, BLOCK_WRITE); */
/*     if (!block) { */
/*         seaf_warning ("Failed to open block %s in repo %.8s.\n", */
/*                       block_id, task->repo_id); */
/*         return -1; */
/*     } */

/*     GetBlockData data; */
/*     memcpy (data.block_id, block_id, 40); */
/*     data.block = block; */
/*     data.task = task; */

/*     curl = conn->curl; */

/*     if (!task->use_fileserver_port) */
/*         url = g_strdup_printf ("%s/seafhttp/repo/%s/block/%s", */
/*                                task->host, task->repo_id, block_id); */
/*     else */
/*         url = g_strdup_printf ("%s/repo/%s/block/%s", */
/*                                task->host, task->repo_id, block_id); */

/*     if (http_get (curl, url, task->token, &status, NULL, NULL, */
/*                   get_block_callback, &data, TRUE) < 0) { */
/*         if (task->state == HTTP_TASK_STATE_CANCELED) */
/*             goto error; */

/*         if (task->error == HTTP_TASK_OK) { */
/*             /\* Only release the connection when it's a network error. *\/ */
/*             conn->release = TRUE; */
/*             task->error = HTTP_TASK_ERR_NET; */
/*         } */
/*         ret = -1; */
/*         goto error; */
/*     } */

/*     if (status != HTTP_OK) { */
/*         seaf_warning ("Bad response code for GET %s: %d.\n", url, status); */
/*         handle_http_errors (task, status); */
/*         ret = -1; */
/*         goto error; */
/*     } */

/*     seaf_block_manager_close_block (seaf->block_mgr, block); */

/*     pthread_mutex_lock (&task->ref_cnt_lock); */

/*     /\* Don't overwrite the block if other thread already downloaded it. */
/*      * Since we've locked ref_cnt_lock, we can be sure the block won't be removed. */
/*      *\/ */
/*     if (!seaf_block_manager_block_exists (seaf->block_mgr, */
/*                                           task->repo_id, task->repo_version, */
/*                                           block_id) && */
/*         seaf_block_manager_commit_block (seaf->block_mgr, block) < 0) */
/*     { */
/*         seaf_warning ("Failed to commit block %s in repo %.8s.\n", */
/*                       block_id, task->repo_id); */
/*         task->error = HTTP_TASK_ERR_WRITE_LOCAL_DATA; */
/*         ret = -1; */
/*     } */

/*     if (ret == 0) { */
/*         pcnt = g_hash_table_lookup (task->blk_ref_cnts, block_id); */
/*         if (!pcnt) { */
/*             pcnt = g_new0(int, 1); */
/*             g_hash_table_insert (task->blk_ref_cnts, g_strdup(block_id), pcnt); */
/*         } */
/*         *pcnt += 1; */
/*     } */

/*     pthread_mutex_unlock (&task->ref_cnt_lock); */

/*     seaf_block_manager_block_handle_free (seaf->block_mgr, block); */

/*     g_free (url); */

/*     return ret; */

/* error: */
/*     g_free (url); */

/*     seaf_block_manager_close_block (seaf->block_mgr, block); */
/*     seaf_block_manager_block_handle_free (seaf->block_mgr, block); */

/*     return ret; */
/* } */

/* int */
/* http_tx_task_download_file_blocks (HttpTxTask *task, const char *file_id) */
/* { */
/*     Seafile *file; */
/*     HttpTxPriv *priv = seaf->http_tx_mgr->priv; */
/*     ConnectionPool *pool; */
/*     Connection *conn; */
/*     int ret = 0; */

/*     file = seaf_fs_manager_get_seafile (seaf->fs_mgr, */
/*                                         task->repo_id, */
/*                                         task->repo_version, */
/*                                         file_id); */
/*     if (!file) { */
/*         seaf_warning ("Failed to find seafile object %s in repo %.8s.\n", */
/*                       file_id, task->repo_id); */
/*         return -1; */
/*     } */

/*     pool = find_connection_pool (priv, task->host); */
/*     if (!pool) { */
/*         seaf_warning ("Failed to create connection pool for host %s.\n", task->host); */
/*         task->error = HTTP_TASK_ERR_NOT_ENOUGH_MEMORY; */
/*         seafile_unref (file); */
/*         return -1; */
/*     } */

/*     conn = connection_pool_get_connection (pool); */
/*     if (!conn) { */
/*         seaf_warning ("Failed to get connection to host %s.\n", task->host); */
/*         task->error = HTTP_TASK_ERR_NOT_ENOUGH_MEMORY; */
/*         seafile_unref (file); */
/*         return -1; */
/*     } */

/*     int i; */
/*     char *block_id; */
/*     for (i = 0; i < file->n_blocks; ++i) { */
/*         block_id = file->blk_sha1s[i]; */
/*         ret = get_block (task, conn, block_id); */
/*         if (ret < 0 || task->state == HTTP_TASK_STATE_CANCELED) */
/*             break; */
/*     } */

/*     connection_pool_return_connection (pool, conn); */

/*     seafile_unref (file); */

/*     return ret; */
/* } */

static int
update_local_repo (HttpTxTask *task)
{
    SeafRepo *repo = NULL;
    SeafCommit *new_head;
    SeafBranch *branch;
    int ret = 0;

    new_head = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                               task->repo_id,
                                               task->repo_version,
                                               task->head);
    if (!new_head) {
        seaf_warning ("Failed to get commit %s:%s.\n", task->repo_id, task->head);
        task->error = HTTP_TASK_ERR_BAD_LOCAL_DATA;
        return -1;
    }

    if (task->is_clone) {
        /* If repo doesn't exist, create it.
         * Note that branch doesn't exist either in this case.
         */
        repo = seaf_repo_manager_get_repo (seaf->repo_mgr, new_head->repo_id);
        if (repo != NULL) {
            seaf_repo_unref (repo);
            goto out;
        }

        repo = seaf_repo_new (new_head->repo_id, NULL, NULL);
        if (repo == NULL) {
            /* create repo failed */
            task->error = HTTP_TASK_ERR_NOT_ENOUGH_MEMORY;
            ret = -1;
            goto out;
        }

        repo->server = g_strdup (task->server);
        repo->user = g_strdup (task->user);

        seaf_repo_from_commit (repo, new_head);

        /* If it's a new repo, create 'local' and 'master' branch */
        branch = seaf_branch_new ("local", task->repo_id, task->head, 0);
        seaf_branch_manager_add_branch (seaf->branch_mgr, branch);
        /* Set repo head branch to local */
        seaf_repo_set_head (repo, branch);
        seaf_branch_unref (branch);

        branch = seaf_branch_new ("master", task->repo_id, task->head, 0);
        seaf_branch_manager_add_branch (seaf->branch_mgr, branch);
        seaf_branch_unref (branch);

        seaf_repo_manager_add_repo (seaf->repo_mgr, repo);
    }
    /* If repo already exists, update branches in sync-mgr. */

out:
    seaf_commit_unref (new_head);
    return ret;
}

static void *
http_download_thread (void *vdata)
{
    HttpTxTask *task = vdata;
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn = NULL;
    GList *fs_id_list = NULL;

    pool = find_connection_pool (priv, task->host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", task->host);
        task->error = HTTP_TASK_ERR_NOT_ENOUGH_MEMORY;
        goto out;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("failed to get connection to host %s.\n", task->host);
        task->error = HTTP_TASK_ERR_NOT_ENOUGH_MEMORY;
        goto out;
    }

    seaf_message ("Download with HTTP sync protocol version %d.\n",
                  task->protocol_version);

    transition_state (task, task->state, HTTP_TASK_RT_STATE_CHECK);

    if (check_permission (task, conn) < 0) {
        seaf_warning ("Download permission denied for repo %.8s on server %s.\n",
                      task->repo_id, task->host);
        goto out;
    }

    if (task->state == HTTP_TASK_STATE_CANCELED)
        goto out;

    transition_state (task, task->state, HTTP_TASK_RT_STATE_COMMIT);

    if (get_commit_object (task, conn) < 0) {
        seaf_warning ("Failed to get server head commit for repo %.8s on server %s.\n",
                      task->repo_id, task->host);
        goto out;
    }

    if (task->state == HTTP_TASK_STATE_CANCELED)
        goto out;

    transition_state (task, task->state, HTTP_TASK_RT_STATE_FS);

    if (get_needed_fs_id_list (task, conn, &fs_id_list) < 0) {
        seaf_warning ("Failed to get fs id list for repo %.8s on server %s.\n",
                      task->repo_id, task->host);
        goto out;
    }

    if (task->state == HTTP_TASK_STATE_CANCELED)
        goto out;

    while (fs_id_list != NULL) {
        if (get_fs_objects (task, conn, &fs_id_list) < 0) {
            seaf_warning ("Failed to get fs objects for repo %.8s on server %s.\n",
                          task->repo_id, task->host);
            goto out;
        }

        if (task->state == HTTP_TASK_STATE_CANCELED)
            goto out;
    }

    update_local_repo (task);

out:
    connection_pool_return_connection (pool, conn);
    string_list_free (fs_id_list);
    return vdata;
}

static void
http_download_done (void *vdata)
{
    HttpTxTask *task = vdata;

    if (task->error != HTTP_TASK_OK)
        transition_state (task, HTTP_TASK_STATE_ERROR, HTTP_TASK_RT_STATE_FINISHED);
    else if (task->state == HTTP_TASK_STATE_CANCELED)
        transition_state (task, task->state, HTTP_TASK_RT_STATE_FINISHED);
    else
        transition_state (task, HTTP_TASK_STATE_FINISHED, HTTP_TASK_RT_STATE_FINISHED);
}

#define SYNC_MOVE_TIMEOUT 600

int
synchronous_move (Connection *conn, const char *host, const char *api_token,
                  const char *repo_id1, const char *oldpath,
                  const char *repo_id2, const char *newpath)
{
    CURL *curl = conn->curl;
    char *dst_dir = NULL, *esc_oldpath = NULL;
    char *url = NULL;
    char *req_content = NULL;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    int ret = 0;

    dst_dir = g_path_get_dirname (newpath);
    if (strcmp(dst_dir, ".") == 0) {
        g_free (dst_dir);
        dst_dir = g_strdup("");
    }
    esc_oldpath = g_uri_escape_string (oldpath, NULL, FALSE);
    req_content = g_strdup_printf ("operation=move&dst_repo=%s&dst_dir=/%s",
                                   repo_id2, dst_dir);

    url = g_strdup_printf ("%s/api2/repos/%s/file/?p=/%s", host, repo_id1, esc_oldpath);

    if (http_api_post (curl, url, api_token, req_content, strlen(req_content),
                       &status, &rsp_content, &rsp_size,
                       TRUE, SYNC_MOVE_TIMEOUT, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_MOVED_PERMANENTLY) {
        seaf_warning ("Bad response code for POST %s: %d, response error: %s.\n", url, status, (rsp_content ? rsp_content : "no response body"));
        ret = -1;
    }

out:
    g_free (dst_dir);
    g_free (esc_oldpath);
    g_free (req_content);
    g_free (url);
    return ret;
}

int
asynchronous_move (Connection *conn, const char *host, const char *api_token,
                   const char *repo_id1, const char *oldpath,
                   const char *repo_id2, const char *newpath,
                   gboolean is_file, gboolean *no_api)
{
    CURL *curl = conn->curl;
    char *src_dir = NULL, *src_filename = NULL, *dst_dir = NULL;
    char *url = NULL;
    char *req_content = NULL;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    int ret = 0;
    json_t *obj = NULL;
    json_t *req_obj = NULL;
    json_t *array = NULL;
    json_error_t jerror;

    src_dir = g_path_get_dirname (oldpath);
    if (strcmp(src_dir, ".") == 0) {
        g_free (src_dir);
        src_dir = g_strdup("/");
    }
    src_filename = g_path_get_basename (oldpath);
    
    dst_dir = g_path_get_dirname (newpath);
    if (strcmp(dst_dir, ".") == 0) {
        g_free (dst_dir);
        dst_dir = g_strdup("/");
    }

    req_obj = json_object ();
    json_object_set_new (req_obj, "src_repo_id", json_string(repo_id1));
    json_object_set_new (req_obj, "src_parent_dir", json_string(src_dir));

    array = json_array ();
    json_array_append_new (array, json_string(src_filename));
    json_object_set_new (req_obj, "src_dirents", array);

    json_object_set_new (req_obj, "dst_repo_id", json_string(repo_id2));
    json_object_set_new (req_obj, "dst_parent_dir", json_string(dst_dir));

    req_content = json_dumps (req_obj, 0);

    url = g_strdup_printf ("%s/api/v2.1/repos/async-batch-move-item/", host);

    if (http_api_json_post (curl, url, api_token, req_content, strlen(req_content),
                            &status, &rsp_content, &rsp_size,
                            TRUE, REPO_OPER_TIMEOUT, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    curl_easy_reset (curl);

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for POST %s: %d, response error: %s.\n", url, status, (rsp_content ? rsp_content : "no response body"));
        if (status == HTTP_NOT_FOUND && no_api)
            *no_api = TRUE;
        ret = -1;
        goto out;
    }

    obj = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!obj) {
        seaf_warning ("Failed to load json response: %s\n", jerror.text);
        ret = -1;
        goto out;
    }
    if (json_typeof(obj) != JSON_OBJECT) {
        seaf_warning ("Response is not a json object.\n");
        ret = -1;
        goto out;
    }

    const char *task_id = json_string_value(json_object_get(obj, "task_id"));
    if (!task_id || strlen(task_id) == 0) {
        if (g_strcmp0 (repo_id1, repo_id2) == 0)
            goto out;
        seaf_warning ("No copy move task id returned from server.\n");
        ret = -1;
        goto out;
    }

    g_free (url);
    url = g_strdup_printf ("%s/api/v2.1/query-copy-move-progress/?task_id=%s",
                           host, task_id);

    while (1) {
        g_free (rsp_content);
        rsp_content = NULL;
        if (http_api_get (curl, url, api_token, &status, &rsp_content, &rsp_size,
                          NULL, NULL, TRUE, REPO_OPER_TIMEOUT, NULL) < 0) {
            conn->release = TRUE;
            ret = -1;
            goto out;
        }

        curl_easy_reset (curl);

        if (status != HTTP_OK) {
            seaf_warning ("Bad response code GET %s: %d\n", url, status);
            ret = -1;
            goto out;
        }

        json_decref (obj);
        obj = json_loadb (rsp_content, rsp_size, 0, &jerror);
        if (!obj) {
            seaf_warning ("Failed to load json response: %s\n", jerror.text);
            ret = -1;
            goto out;
        }
        if (json_typeof(obj) != JSON_OBJECT) {
            seaf_warning ("Response is not a json object.\n");
            ret = -1;
            goto out;
        }

        json_t *succ = json_object_get (obj, "successful");
        if (!succ) {
            seaf_warning ("Invalid json object format.\n");
            ret = -1;
            goto out;
        }
        json_t *failed = json_object_get (obj, "failed");
        if (!failed) {
            seaf_warning ("Invalid json object format.\n");
            ret = -1;
            goto out;
        }
        if (succ == json_true()) {
            break;
        } else if (failed == json_true()) {
            seaf_warning ("Move %s/%s to %s/%s failed on server.\n",
                          repo_id1, oldpath, repo_id2, newpath);
            ret = -1;
            break;
        }

        seaf_sleep (1);
    }

out:
    g_free (src_dir);
    g_free (src_filename);
    g_free (dst_dir);
    g_free (url);
    g_free (req_content);
    g_free (rsp_content);
    json_decref (obj);
    json_decref (req_obj);
    return ret;
}

int
http_tx_manager_api_move_file (HttpTxManager *mgr,
                               const char *host,
                               const char *api_token,
                               const char *repo_id1,
                               const char *oldpath,
                               const char *repo_id2,
                               const char *newpath,
                               gboolean is_file)
{
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    gboolean no_api = FALSE;
    ret = asynchronous_move (conn, host, api_token,
                             repo_id1, oldpath, repo_id2, newpath,
                             is_file, &no_api);
    if (ret < 0 && no_api) {
        /* Async move api not found, try old api. */
        ret = synchronous_move (conn, host, api_token,
                                repo_id1, oldpath, repo_id2, newpath);
    }

    connection_pool_return_connection (pool, conn);
    return ret;
}

static int
parse_space_usage (const char *rsp_content, int rsp_size, gint64 *total, gint64 *used)
{
    json_t *object, *member;
    json_error_t jerror;
    int ret = 0;

    object = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!object) {
        seaf_warning ("Failed to load json object: %s\n", jerror.text);
        return -1;
    }

    if (json_typeof(object) != JSON_OBJECT) {
        seaf_warning ("Json response is not object\n");
        ret = -1;
        goto out;
    }

    member = json_object_get (object, "total");
    if (json_typeof(member) != JSON_INTEGER) {
        seaf_warning ("Space usage not an integer\n");
        ret = -1;
        goto out;
    }
    *total = json_integer_value (member);

    member = json_object_get (object, "usage");
    if (json_typeof(member) != JSON_INTEGER) {
        seaf_warning ("Space usage not an integer\n");
        ret = -1;
        goto out;
    }
    *used = json_integer_value (member);

out:
    json_decref (object);
    return ret;
}

int
http_tx_manager_api_get_space_usage (HttpTxManager *mgr,
                                     const char *host,
                                     const char *api_token,
                                     gint64 *total,
                                     gint64 *used)
{
    HttpTxPriv *priv = seaf->http_tx_mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url = NULL;
    int status;
    char *rsp_content = NULL;
    gint64 rsp_size;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    url = g_strdup_printf ("%s/api2/account/info/", host);

    if (http_api_get (curl, url, api_token,
                      &status, &rsp_content, &rsp_size,
                      NULL, NULL, TRUE, HTTP_TIMEOUT_SEC, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
        ret = -1;
        goto out;
    }

    if (parse_space_usage (rsp_content, rsp_size, total, used) < 0) {
        ret = -1;
    }

out:
    connection_pool_return_connection (pool, conn);
    g_free (url);
    g_free (rsp_content);
    return ret;
}

static int
parse_block_map (const char *rsp_content, gint64 rsp_size,
                 gint64 **pblock_map, int *n_blocks)
{
    json_t *array, *element;
    json_error_t jerror;
    size_t n, i;
    gint64 *block_map = NULL;
    int ret = 0;

    array = json_loadb (rsp_content, rsp_size, 0, &jerror);
    if (!array) {
        seaf_warning ("Failed to load json: %s\n", jerror.text);
        return -1;
    }

    if (json_typeof (array) != JSON_ARRAY) {
        seaf_warning ("Response is not a json array.\n");
        ret = -1;
        goto out;
    }

    n = json_array_size (array);
    block_map = g_new0 (gint64, n);

    for (i = 0; i < n; ++i) {
        element = json_array_get (array, i);
        if (json_typeof (element) != JSON_INTEGER) {
            seaf_warning ("Block map element not an integer.\n");
            ret = -1;
            goto out;
        }
        block_map[i] = (gint64)json_integer_value (element);
    }

out:
    json_decref (array);
    if (ret < 0) {
        g_free (block_map);
        *pblock_map = NULL;
    } else {
        *pblock_map = block_map;
        *n_blocks = (int)n;
    }
    return ret;
}

int
http_tx_manager_get_file_block_map (HttpTxManager *mgr,
                                    const char *host,
                                    gboolean use_fileserver_port,
                                    const char *token,
                                    const char *repo_id,
                                    const char *file_id,
                                    gint64 **pblock_map,
                                    int *n_blocks)
{
    HttpTxPriv *priv = mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url;
    int ret = 0;
    char *rsp_content = NULL;
    gint64 rsp_size;
    int status;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    if (!use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/block-map/%s",
                               host, repo_id, file_id);
    else
        url = g_strdup_printf ("%s/repo/%s/block-map/%s",
                               host, repo_id, file_id);

    if (http_get (curl, url, token, &status, &rsp_content, &rsp_size,
                  NULL, NULL, TRUE, HTTP_TIMEOUT_SEC, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
        ret = -1;
        goto out;
    }

    if (parse_block_map (rsp_content, rsp_size, pblock_map, n_blocks) < 0) {
        ret = -1;
        goto out;
    }

out:
    g_free (url);
    g_free (rsp_content);
    connection_pool_return_connection (pool, conn);
    return ret;
}

int
http_tx_manager_get_commit (HttpTxManager *mgr,
                            const char *host,
                            gboolean use_fileserver_port,
                            const char *sync_token,
                            const char *repo_id,
                            const char *commit_id,
                            char **resp,
                            gint64 *resp_size)
{
    HttpTxPriv *priv = mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url = NULL;
    int status;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    if (!use_fileserver_port)
        url = g_strdup_printf ("%s/seafhttp/repo/%s/commit/%s",
                               host, repo_id, commit_id);
    else
        url = g_strdup_printf ("%s/repo/%s/commit/%s",
                               host, repo_id, commit_id);

    if (http_get (curl, url, sync_token, &status,
                  resp, resp_size,
                  NULL, NULL, TRUE, REPO_OPER_TIMEOUT, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d.\n", url, status);
        ret = -1;
    }

out:
    g_free (url);
    connection_pool_return_connection (pool, conn);
    return ret;
}

int
http_tx_manager_api_create_repo (HttpTxManager *mgr,
                                 const char *host,
                                 const char *api_token,
                                 const char *repo_name,
                                 char **resp,
                                 gint64 *resp_size)
{
    HttpTxPriv *priv = mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url = NULL;
    char *req_content = NULL;
    int status;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    url = g_strdup_printf ("%s/api2/repos/", host);

    req_content = g_strdup_printf ("name=%s&desc=%s", repo_name, repo_name);

    if (http_api_post (curl, url, api_token, req_content, strlen(req_content),
                       &status, resp, resp_size,
                       TRUE, REPO_OPER_TIMEOUT, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for POST %s: %d.\n", url, status);
        ret = -1;
    }

out:
    connection_pool_return_connection (pool, conn);
    g_free (req_content);
    g_free (url);
    return ret;
}

int
http_tx_manager_api_rename_repo (HttpTxManager *mgr,
                                 const char *host,
                                 const char *api_token,
                                 const char *repo_id,
                                 const char *new_name)
{
    HttpTxPriv *priv = mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url = NULL;
    char *req_content = NULL;
    char *resp = NULL;
    gint64 resp_size;
    int status;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    url = g_strdup_printf ("%s/api2/repos/%s/?op=rename", host, repo_id);

    req_content = g_strdup_printf ("repo_name=%s", new_name);

    if (http_api_post (curl, url, api_token, req_content, strlen(req_content),
                       &status, &resp, &resp_size,
                       TRUE, REPO_OPER_TIMEOUT, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for POST %s: %d(%s).\n", url, status,
                      resp ? resp : "");
        ret = -1;
    }

out:
    connection_pool_return_connection (pool, conn);
    g_free (req_content);
    g_free (resp);
    g_free (url);
    return ret;
}

int
http_tx_manager_api_delete_repo (HttpTxManager *mgr,
                                 const char *host,
                                 const char *api_token,
                                 const char *repo_id)
{
    HttpTxPriv *priv = mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url = NULL;
    int status;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    url = g_strdup_printf ("%s/api/v2.1/repos/%s/", host, repo_id);

    if (http_api_delete (curl, url, api_token, &status,
                         TRUE, REPO_OPER_TIMEOUT) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for DELETE %s:%d.\n", url, status);
        ret = -1;
    }

out:
   connection_pool_return_connection (pool, conn);
   g_free (url);
   return ret;
}

typedef struct FileRangeData {
    char *buf;
    size_t size;
} FileRangeData;

static size_t
get_file_range_cb (void *contents, size_t size, size_t nmemb, void *userp)
{
    FileRangeData *data = userp;
    size_t realsize = size * nmemb;

    if (data->size < realsize) {
        seaf_warning ("Get file range failed. Returned data size larger than buffer.\n");
        return data->size;
    }

    memcpy (data->buf, contents, realsize);
    data->buf += realsize;
    data->size -= realsize;

    return realsize;
}

gssize
http_tx_manager_get_file_range (HttpTxManager *mgr,
                                const char *host,
                                const char *api_token,
                                const char *repo_id,
                                const char *path,
                                char *buf,
                                guint64 offset,
                                size_t size)
{
    HttpTxPriv *priv = mgr->priv;
    ConnectionPool *pool;
    Connection *conn;
    CURL *curl;
    char *url = NULL, *file_url;
    char *resp = NULL;
    gint64 resp_size;
    int status;
    int ret = 0;

    pool = find_connection_pool (priv, host);
    if (!pool) {
        seaf_warning ("Failed to create connection pool for host %s.\n", host);
        return -1;
    }

    conn = connection_pool_get_connection (pool);
    if (!conn) {
        seaf_warning ("Failed to get connection to host %s.\n", host);
        return -1;
    }

    curl = conn->curl;

    char *esc_path = g_uri_escape_string(path, NULL, FALSE);
    url = g_strdup_printf ("%s/api2/repos/%s/file/?p=%s", host, repo_id, esc_path);
    g_free (esc_path);

    if (http_api_get (curl, url, api_token,
                      &status, &resp, &resp_size, NULL, NULL,
                      TRUE, REPO_OPER_TIMEOUT, NULL) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_OK) {
        seaf_warning ("Bad response code for GET %s: %d(%s).\n", url, status,
                      resp ? resp : "");
        ret = -1;
        goto out;
    }

    /* Returned file url is in "url" format. */
    resp[resp_size-1] = '\0';
    file_url = resp + 1;

    FileRangeData data;
    data.buf = buf;
    data.size = size;

    curl_easy_reset (curl);

    if (http_get_range (curl, file_url, NULL,
                        &status, NULL, NULL, get_file_range_cb, &data,
                        TRUE, REPO_OPER_TIMEOUT,
                        offset, offset+size-1) < 0) {
        conn->release = TRUE;
        ret = -1;
        goto out;
    }

    if (status != HTTP_RES_PARTIAL) {
        seaf_warning ("Failed to get file range. Bad response code for GET %s: %d.\n",
                      url, status);
        ret = -1;
        goto out;
    }

    ret = (int)(size - data.size);

out:
    connection_pool_return_connection (pool, conn);
    g_free (resp);
    g_free (url);
    return ret;
}

/* typedef struct FileConflictData { */
/*     char *repo_id; */
/*     char *repo_name; */
/*     char *path; */
/* } FileConflictData; */

/* static void */
/* notify_conflict (CEvent *event, void *handler_data) */
/* { */
/*     FileConflictData *data = event->data; */
/*     json_t *object; */
/*     char *str; */

/*     object = json_object (); */
/*     json_object_set_new (object, "repo_id", json_string(data->repo_id)); */
/*     json_object_set_new (object, "repo_name", json_string(data->repo_name)); */
/*     json_object_set_new (object, "path", json_string(data->path)); */

/*     str = json_dumps (object, 0); */

/*     seaf_mq_manager_publish_notification (seaf->mq_mgr, */
/*                                           "sync.conflict", */
/*                                           str); */

/*     free (str); */
/*     json_decref (object); */
/*     g_free (data->repo_id); */
/*     g_free (data->repo_name); */
/*     g_free (data->path); */
/*     g_free (data); */
/* } */

/* void */
/* http_tx_manager_notify_conflict (HttpTxTask *task, const char *path) */
/* { */
/*     FileConflictData *data = g_new0 (FileConflictData, 1); */
/*     data->repo_id = g_strdup(task->repo_id); */
/*     data->repo_name = g_strdup(task->repo_name); */
/*     data->path = g_strdup(path); */

/*     cevent_manager_add_event (seaf->ev_mgr, task->cevent_id, data); */
/* } */

GList*
http_tx_manager_get_upload_tasks (HttpTxManager *manager)
{
    return g_hash_table_get_values (manager->priv->upload_tasks);
}

GList*
http_tx_manager_get_download_tasks (HttpTxManager *manager)
{
    return g_hash_table_get_values (manager->priv->download_tasks);
}

HttpTxTask *
http_tx_manager_find_task (HttpTxManager *manager, const char *repo_id)
{
    HttpTxTask *task = NULL;

    task = g_hash_table_lookup (manager->priv->upload_tasks, repo_id);
    if (task)
        return task;

    task = g_hash_table_lookup (manager->priv->download_tasks, repo_id);
    return task;
}

void
http_tx_manager_cancel_task (HttpTxManager *manager,
                             const char *repo_id,
                             int task_type)
{
    HttpTxTask *task = NULL;

    if (task_type == HTTP_TASK_TYPE_DOWNLOAD)
        task = g_hash_table_lookup (manager->priv->download_tasks, repo_id);
    else
        task = g_hash_table_lookup (manager->priv->upload_tasks, repo_id);

    if (!task)
        return;

    if (task->state != HTTP_TASK_STATE_NORMAL) {
        return;
    }

    if (task->runtime_state == HTTP_TASK_RT_STATE_INIT) {
        transition_state (task, HTTP_TASK_STATE_CANCELED, HTTP_TASK_RT_STATE_FINISHED);
        return;
    }

    /* Only change state. runtime_state will be changed in worker thread. */
    transition_state (task, HTTP_TASK_STATE_CANCELED, task->runtime_state);
}

int
http_tx_task_get_rate (HttpTxTask *task)
{
    return task->last_tx_bytes;
}

const char *
http_task_state_to_str (int state)
{
    if (state < 0 || state >= N_HTTP_TASK_STATE)
        return "unknown";

    return http_task_state_str[state];
}

const char *
http_task_rt_state_to_str (int rt_state)
{
    if (rt_state < 0 || rt_state >= N_HTTP_TASK_RT_STATE)
        return "unknown";

    return http_task_rt_state_str[rt_state];
}

const char *
http_task_error_str (int task_errno)
{
    if (task_errno < 0 || task_errno >= N_HTTP_TASK_ERROR)
        return "unknown error";

    return http_task_error_strs[task_errno];
}

static void
collect_uploading_files (gpointer key, gpointer value,
                         gpointer user_data)
{
    char *abs_path = key;
    FileUploadProgress *progress = value;
    json_t *uploading = user_data;
    json_t *upload_info = json_object ();

    json_object_set_string_member (upload_info, "server", progress->server);
    json_object_set_string_member (upload_info, "username", progress->user);
    json_object_set_string_member (upload_info, "file_path", abs_path);
    json_object_set_int_member (upload_info, "uploaded", progress->uploaded);
    json_object_set_int_member (upload_info, "total_upload", progress->total_upload);

    json_array_append_new (uploading, upload_info);
}

json_t *
http_tx_manager_get_upload_progress (HttpTxManager *mgr)
{
    HttpTxPriv *priv = mgr->priv;
    int i = 0;
    FileUploadedInfo *uploaded_info;
    json_t *uploaded = json_array ();
    json_t *uploading = json_array ();
    json_t *progress = json_object ();
    json_t *uploaded_obj;

    pthread_mutex_lock (&priv->progress_lock);

    g_hash_table_foreach (priv->uploading_files, collect_uploading_files,
                          uploading);

    while (i < MAX_GET_FINISHED_FILES) {
        uploaded_info = g_queue_peek_nth (priv->uploaded_files, i);
        if (uploaded_info) {
            uploaded_obj = json_object ();
            json_object_set_string_member (uploaded_obj, "server", uploaded_info->server);
            json_object_set_string_member (uploaded_obj, "username", uploaded_info->user);
            json_object_set_string_member (uploaded_obj, "file_path", uploaded_info->file_path);
            json_array_append_new (uploaded, uploaded_obj);
        } else {
            break;
        }
        i++;
    }

    pthread_mutex_unlock (&priv->progress_lock);

    json_object_set_new (progress, "uploaded_files", uploaded);
    json_object_set_new (progress, "uploading_files", uploading);

    return progress;
}

#if 0

#define UPLOADED_FILE_LIST_NAME "uploaded.json"
#define SAVE_UPLOADED_FILE_LIST_INTERVAL 5
#define UPLOADED_FILE_LIST_VERSION 1

static void *
save_uploaded_file_list_worker (void *data)
{
    HttpTxManager *mgr = data;
    char *path = g_build_filename (seaf->seaf_dir, UPLOADED_FILE_LIST_NAME, NULL);
    json_t *progress = NULL, *uploaded, *list;
    char *txt = NULL;
    GError *error = NULL;

    while (1) {
        seaf_sleep (SAVE_UPLOADED_FILE_LIST_INTERVAL);

        progress = http_tx_manager_get_upload_progress (mgr);
        uploaded = json_object_get (progress, "uploaded_files");
        if (json_array_size(uploaded) > 0) {
            list = json_object();
            json_object_set_new (list, "version",
                                 json_integer(UPLOADED_FILE_LIST_VERSION));
            json_object_set (list, "uploaded_files", uploaded);
            txt = json_dumps (list, 0);
            if (!g_file_set_contents (path, txt, -1, &error)) {
                seaf_warning ("Failed to save uploaded file list: %s\n",
                              error->message);
                g_clear_error (&error);
            }
            json_decref (list);
            free (txt);
        }
        json_decref (progress);
    }

    g_free (path);
    return NULL;
}

static void
load_uploaded_file_list (HttpTxManager *mgr)
{
    char *path = g_build_filename (seaf->seaf_dir, UPLOADED_FILE_LIST_NAME, NULL);
    char *txt = NULL;
    gsize len;
    GError *error = NULL;
    json_t *list = NULL, *uploaded = NULL;
    json_error_t jerror;

    if (!g_file_test (path, G_FILE_TEST_IS_REGULAR)) {
        g_free (path);
        return;
    }

    if (!g_file_get_contents (path, &txt, &len, &error)) {
        seaf_warning ("Failed to read uploaded file list: %s\n", error->message);
        g_clear_error (&error);
        g_free (path);
        return;
    }

    list = json_loadb (txt, len, 0, &jerror);
    if (!list) {
        seaf_warning ("Failed to load uploaded file list: %s\n", jerror.text);
        goto out;
    }
    if (json_typeof(list) != JSON_OBJECT) {
        seaf_warning ("Bad uploaded file list format.\n");
        goto out;
    }

    uploaded = json_object_get (list, "uploaded_files");
    if (!uploaded) {
        seaf_warning ("No uploaded_files in json object.\n");
        goto out;
    }
    if (json_typeof(uploaded) != JSON_ARRAY) {
        seaf_warning ("Bad uploaded file list format.\n");
        goto out;
    }

    json_t *iter;
    size_t n = json_array_size (uploaded), i;
    for (i = 0; i < n; ++i) {
        iter = json_array_get (uploaded, i);
        if (json_typeof(iter) != JSON_STRING) {
            seaf_warning ("Bad uploaded file list format.\n");
            goto out;
        }
        g_queue_push_tail (mgr->priv->uploaded_files,
                           g_strdup(json_string_value(iter)));
    }

out:
    if (list)
        json_decref (list);
    g_free (path);
    g_free (txt);
}

#endif
