#ifndef HTTP_TX_MGR_H
#define HTTP_TX_MGR_H

#include <pthread.h>
#include <jansson.h>

enum {
    HTTP_TASK_TYPE_DOWNLOAD = 0,
    HTTP_TASK_TYPE_UPLOAD,
};


/**
 * The state that can be set by user.
 *
 * A task in NORMAL state can be canceled;
 * A task in RT_STATE_FINISHED can be removed.
 */
enum HttpTaskState {
    HTTP_TASK_STATE_NORMAL = 0,
    HTTP_TASK_STATE_CANCELED,
    HTTP_TASK_STATE_FINISHED,
    HTTP_TASK_STATE_ERROR,
    N_HTTP_TASK_STATE,
};

enum HttpTaskRuntimeState {
    HTTP_TASK_RT_STATE_INIT = 0,
    HTTP_TASK_RT_STATE_CHECK,
    HTTP_TASK_RT_STATE_COMMIT,
    HTTP_TASK_RT_STATE_FS,
    HTTP_TASK_RT_STATE_BLOCK,         /* Only used in upload. */
    HTTP_TASK_RT_STATE_UPDATE_BRANCH, /* Only used in upload. */
    HTTP_TASK_RT_STATE_FINISHED,
    N_HTTP_TASK_RT_STATE,
};

enum HttpTaskError {
    HTTP_TASK_OK = 0,
    HTTP_TASK_ERR_FORBIDDEN,
    HTTP_TASK_ERR_NO_WRITE_PERMISSION,
    HTTP_TASK_ERR_NO_PERMISSION_TO_SYNC,
    HTTP_TASK_ERR_NET,
    HTTP_TASK_ERR_RESOLVE_PROXY,
    HTTP_TASK_ERR_RESOLVE_HOST,
    HTTP_TASK_ERR_CONNECT,
    HTTP_TASK_ERR_SSL,
    HTTP_TASK_ERR_TX,
    HTTP_TASK_ERR_TX_TIMEOUT,
    HTTP_TASK_ERR_UNHANDLED_REDIRECT,
    HTTP_TASK_ERR_SERVER,
    HTTP_TASK_ERR_BAD_REQUEST,
    HTTP_TASK_ERR_BAD_LOCAL_DATA,
    HTTP_TASK_ERR_NOT_ENOUGH_MEMORY,
    HTTP_TASK_ERR_WRITE_LOCAL_DATA,
    HTTP_TASK_ERR_NO_QUOTA,
    HTTP_TASK_ERR_FILES_LOCKED,
    HTTP_TASK_ERR_REPO_DELETED,
    HTTP_TASK_ERR_REPO_CORRUPTED,
    HTTP_TASK_ERR_FILE_LOCKED,
    HTTP_TASK_ERR_FOLDER_PERM_DENIED,
    HTTP_TASK_ERR_LIBRARY_TOO_LARGE,
    HTTP_TASK_ERR_TOO_MANY_FILES,
    HTTP_TASK_ERR_BLOCK_MISSING,
    HTTP_TASK_ERR_UNKNOWN,
    N_HTTP_TASK_ERROR,
};

struct _SeafileSession;
struct _HttpTxPriv;

struct _HttpTxManager {
    struct _SeafileSession   *seaf;

    struct _HttpTxPriv *priv;
};

typedef struct _HttpTxManager HttpTxManager;

struct _HttpTxTask {
    HttpTxManager *manager;

    char repo_id[37];
    int repo_version;
    // repo_uname is the same as display_name.
    char *repo_uname;
    char *token;
    int protocol_version;
    int type;
    char *host;
    gboolean is_clone;
    char *email;
    gboolean use_fileserver_port;
    char *server;
    char *user;

    char head[41];

    int state;
    int runtime_state;
    int error;
    /* Used to signify stop transfer for all threads. */
    gboolean all_stop;

    /* For clone fs object progress */
    int n_fs_objs;
    int done_fs_objs;

    /* For upload progress */
    int n_blocks;
    int done_blocks;
    /* For download progress */
    int n_files;
    int done_files;

    gint tx_bytes;              /* bytes transferred in this second. */
    gint last_tx_bytes;         /* bytes transferred in the last second. */

    char *unsyncable_path;
};
typedef struct _HttpTxTask HttpTxTask;

HttpTxManager *
http_tx_manager_new (struct _SeafileSession *seaf);

int
http_tx_manager_start (HttpTxManager *mgr);

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
                              GError **error);

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
                            GError **error);

struct _HttpProtocolVersion {
    gboolean check_success;     /* TRUE if we get response from the server. */
    gboolean not_supported;
    int version;
    int error_code;
};
typedef struct _HttpProtocolVersion HttpProtocolVersion;

typedef void (*HttpProtocolVersionCallback) (HttpProtocolVersion *result,
                                             void *user_data);

/* Asynchronous interface for getting protocol version from a server.
 * Also used to determine if the server support http sync.
 */
int
http_tx_manager_check_protocol_version (HttpTxManager *manager,
                                        const char *host,
                                        gboolean use_fileserver_port,
                                        HttpProtocolVersionCallback callback,
                                        void *user_data);

typedef void (*HttpNotifServerCallback) (gboolean is_alive,
                                         void *user_data);

int
http_tx_manager_check_notif_server (HttpTxManager *manager,
                                    const char *host,
                                    gboolean use_notif_server_port,
                                    HttpNotifServerCallback callback,
                                    void *user_data);

struct _HttpHeadCommit {
    gboolean check_success;
    gboolean perm_denied;
    gboolean is_corrupt;
    gboolean is_deleted;
    char head_commit[41];
    int error_code;
};
typedef struct _HttpHeadCommit HttpHeadCommit;

typedef void (*HttpHeadCommitCallback) (HttpHeadCommit *result,
                                        void *user_data);

/* Asynchronous interface for getting head commit info from a server. */
int
http_tx_manager_check_head_commit (HttpTxManager *manager,
                                   const char *repo_id,
                                   int repo_version,
                                   const char *host,
                                   const char *token,
                                   gboolean use_fileserver_port,
                                   HttpHeadCommitCallback callback,
                                   void *user_data);

typedef struct _HttpFolderPermReq {
    char repo_id[37];
    char *token;
    gint64 timestamp;
} HttpFolderPermReq;

typedef struct _HttpFolderPermRes {
    char repo_id[37];
    gint64 timestamp;
    GList *user_perms;
    GList *group_perms;
} HttpFolderPermRes;

void
http_folder_perm_req_free (HttpFolderPermReq *req);

void
http_folder_perm_res_free (HttpFolderPermRes *res);

struct _HttpFolderPerms {
    gboolean success;
    GList *results;             /* List of HttpFolderPermRes */
};
typedef struct _HttpFolderPerms HttpFolderPerms;

typedef void (*HttpGetFolderPermsCallback) (HttpFolderPerms *result,
                                            void *user_data);

/* Asynchronous interface for getting folder permissions for a repo. */
int
http_tx_manager_get_folder_perms (HttpTxManager *manager,
                                  const char *host,
                                  gboolean use_fileserver_port,
                                  GList *folder_perm_requests, /* HttpFolderPermReq */
                                  HttpGetFolderPermsCallback callback,
                                  void *user_data);

typedef struct _HttpLockedFilesReq {
    char repo_id[37];
    char *token;
    gint64 timestamp;
} HttpLockedFilesReq;

typedef struct _HttpLockedFilesRes {
    char repo_id[37];
    gint64 timestamp;
    GHashTable *locked_files;   /* path -> by_me */
} HttpLockedFilesRes;

void
http_locked_files_req_free (HttpLockedFilesReq *req);

void
http_locked_files_res_free (HttpLockedFilesRes *res);

struct _HttpLockedFiles {
    gboolean success;
    GList *results;             /* List of HttpLockedFilesRes */
};
typedef struct _HttpLockedFiles HttpLockedFiles;

typedef void (*HttpGetLockedFilesCallback) (HttpLockedFiles *result,
                                            void *user_data);

/* Asynchronous interface for getting locked files for a repo. */
int
http_tx_manager_get_locked_files (HttpTxManager *manager,
                                  const char *host,
                                  gboolean use_fileserver_port,
                                  GList *locked_files_requests,
                                  HttpGetLockedFilesCallback callback,
                                  void *user_data);

/* Synchronous interface for locking/unlocking a file on the server. */
int
http_tx_manager_lock_file (HttpTxManager *manager,
                           const char *host,
                           gboolean use_fileserver_port,
                           const char *token,
                           const char *repo_id,
                           const char *path);

int
http_tx_manager_unlock_file (HttpTxManager *manager,
                             const char *host,
                             gboolean use_fileserver_port,
                             const char *token,
                             const char *repo_id,
                             const char *path);

/* Asynchronous interface for sending a API GET request to seahub. */

struct _HttpAPIGetResult {
    gboolean success;
    char *rsp_content;
    int rsp_size;
    int error_code;
    int http_status;
};
typedef struct _HttpAPIGetResult HttpAPIGetResult;

typedef void (*HttpAPIGetCallback) (HttpAPIGetResult *result,
                                    void *user_data);

int
http_tx_manager_api_get (HttpTxManager *manager,
                         const char *host,
                         const char *url,
                         const char *token,
                         HttpAPIGetCallback callback,
                         void *user_data);

int
http_tx_manager_fileserver_api_get  (HttpTxManager *manager,
                                     const char *host,
                                     const char *url,
                                     const char *token,
                                     HttpAPIGetCallback callback,
                                     void *user_data);

int
http_tx_task_download_file_blocks (HttpTxTask *task, const char *file_id);

GList*
http_tx_manager_get_upload_tasks (HttpTxManager *manager);

GList*
http_tx_manager_get_download_tasks (HttpTxManager *manager);

HttpTxTask *
http_tx_manager_find_task (HttpTxManager *manager, const char *repo_id);

void
http_tx_manager_cancel_task (HttpTxManager *manager,
                             const char *repo_id,
                             int task_type);

/* Only useful for download task. */
void
http_tx_manager_notify_conflict (HttpTxTask *task, const char *path);

int
http_tx_task_get_rate (HttpTxTask *task);

const char *
http_task_state_to_str (int state);

const char *
http_task_rt_state_to_str (int rt_state);

const char *
http_task_error_str (int task_errno);

typedef size_t (*HttpRecvCallback) (void *, size_t, size_t, void *);

int
http_tx_manager_get_block (HttpTxManager *mgr, const char *host,
                           gboolean use_fileserver_port, const char *token,
                           const char *repo_id, const char *block_id,
                           HttpRecvCallback blk_cb, void *user_data,
                           int *http_status);

int
http_tx_manager_get_fs_object (HttpTxManager *mgr, const char *host,
                               gboolean use_fileserver_port, const char *token,
                               const char *repo_id, const char *obj_id, const char *file_path);

int
http_tx_manager_get_file (HttpTxManager *mgr, const char *host,
                          gboolean use_fileserver_port, const char *token,
                          const char *repo_id,
                          const char *path,
                          gint64 block_offset,
                          HttpRecvCallback get_blk_cb, void *user_data,
                          int *http_status);

int
http_tx_manager_api_move_file (HttpTxManager *mgr,
                               const char *host,
                               const char *api_token,
                               const char *repo_id1,
                               const char *oldpath,
                               const char *repo_id2,
                               const char *newpath,
                               gboolean is_file);

int
http_tx_manager_get_file_block_map (HttpTxManager *mgr,
                                    const char *host,
                                    gboolean use_fileserver_port,
                                    const char *token,
                                    const char *repo_id,
                                    const char *file_id,
                                    gint64 **block_map,
                                    int *n_blocks);

int
http_tx_manager_api_get_space_usage (HttpTxManager *mgr,
                                     const char *host,
                                     const char *api_token,
                                     gint64 *total,
                                     gint64 *used);

int
http_tx_manager_get_commit (HttpTxManager *mgr,
                            const char *host,
                            gboolean use_fileserver_port,
                            const char *sync_token,
                            const char *repo_id,
                            const char *commit_id,
                            char **resp,
                            gint64 *resp_size);

int
http_tx_manager_api_create_repo (HttpTxManager *mgr,
                                 const char *host,
                                 const char *api_token,
                                 const char *repo_name,
                                 char **resp,
                                 gint64 *resp_size);

int
http_tx_manager_api_rename_repo (HttpTxManager *mgr,
                                 const char *host,
                                 const char *api_token,
                                 const char *repo_id,
                                 const char *new_name);

int
http_tx_manager_api_delete_repo (HttpTxManager *mgr,
                                 const char *host,
                                 const char *api_token,
                                 const char *repo_id);

gssize
http_tx_manager_get_file_range (HttpTxManager *mgr,
                                const char *host,
                                const char *api_token,
                                const char *repo_id,
                                const char *path,
                                char *buf,
                                guint64 offset,
                                size_t size);

// "uploading_files": [{"file_path":, "uploaded":, "total_upload":}, ], "uploaded_files": [ten latest uploaded files]}
json_t *
http_tx_manager_get_upload_progress (HttpTxManager *mgr);

#endif
