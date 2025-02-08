/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <signal.h>

#include <glib.h>
#include <glib-object.h>

#ifdef HAVE_BREAKPAD_SUPPORT
#include <c_bpwrapper.h>
#endif // HAVE_BREAKPAD_SUPPORT

#include "seafile-session.h"
#include "log.h"
#include "utils.h"
#include "seafile-config.h"
#ifndef USE_GPL_CRYPTO
#include "curl-init.h"
#endif

#include "cdc.h"

#include "rpc-service.h"

#ifndef SEAFILE_CLIENT_VERSION
#define SEAFILE_CLIENT_VERSION PACKAGE_VERSION
#endif

SeafileSession *seaf;

static void print_version ()
{
    fprintf (stdout, "SeaDrive "SEAFILE_CLIENT_VERSION"\n");
}

#include <fcntl.h>
#include <sys/file.h>

#define FUSE_USE_VERSION  26
#include <fuse.h>
#include <fuse_opt.h>

#include "fuse-ops.h"

struct options {
    char *seafile_dir;
    char *log_file;
    char *debug_str;
    char *config_file;
    char *language;
} options;

#define SEAF_FUSE_OPT_KEY(t, p, v) { t, offsetof(struct options, p), v }

enum {
    KEY_VERSION,
    KEY_HELP,
};

static struct fuse_opt seadrive_fuse_opts[] = {
    SEAF_FUSE_OPT_KEY("-d %s", seafile_dir, 0),
    SEAF_FUSE_OPT_KEY("--seafdir %s", seafile_dir, 0),
    SEAF_FUSE_OPT_KEY("-l %s", log_file, 0),
    SEAF_FUSE_OPT_KEY("--logfile %s", log_file, 0),
    SEAF_FUSE_OPT_KEY("-D %s", debug_str, 0),
    SEAF_FUSE_OPT_KEY("--debug %s", debug_str, 0),
    SEAF_FUSE_OPT_KEY("-c %s", config_file, 0),
    SEAF_FUSE_OPT_KEY("--config %s", config_file, 0),
    SEAF_FUSE_OPT_KEY("-L %s", language, 0),
    SEAF_FUSE_OPT_KEY("--language %s", language, 0),

    FUSE_OPT_KEY("-V", KEY_VERSION),
    FUSE_OPT_KEY("--version", KEY_VERSION),
    FUSE_OPT_KEY("-h", KEY_HELP),
    FUSE_OPT_KEY("--help", KEY_HELP),
    FUSE_OPT_END
};

static struct fuse_operations seadrive_fuse_ops = {
    .getattr = seadrive_fuse_getattr,
    .readdir = seadrive_fuse_readdir,
    .mknod = seadrive_fuse_mknod,
    .mkdir = seadrive_fuse_mkdir,
    .unlink = seadrive_fuse_unlink,
    .rmdir = seadrive_fuse_rmdir,
    .rename = seadrive_fuse_rename,
    .open    = seadrive_fuse_open,
    .read    = seadrive_fuse_read,
    .write   = seadrive_fuse_write,
    .release = seadrive_fuse_release,
    .truncate = seadrive_fuse_truncate,
    .statfs = seadrive_fuse_statfs,
    .chmod = seadrive_fuse_chmod,
    .utimens = seadrive_fuse_utimens,
    .symlink = seadrive_fuse_symlink,
    /* .setxattr = seadrive_fuse_setxattr, */
    /* .getxattr = seadrive_fuse_getxattr, */
    /* .listxattr = seadrive_fuse_listxattr, */
    /* .removexattr = seadrive_fuse_removexattr */
};

static void usage ()
{
    fprintf (stderr, "usage: seadrive -d <seafile_dir> <mount_dir>\n"
             "Options:\n"
             "-f: run in foreground\n"
             "-o <mount_options>: please refer to fuse manpage\n"
             "-l <logfile>\n"
             "-c <config-file>\n"
             "-L <language>\n");
}

static int
seadrive_fuse_opt_proc_func(void *data, const char *arg, int key,
                                struct fuse_args *outargs)
{
    if (key == KEY_VERSION) {
        print_version ();
        exit (0);
    } else if (key == KEY_HELP) {
        usage ();
        exit (0);
    }

    return 1;
}

static void
set_signal_handlers (SeafileSession *session)
{
    signal (SIGPIPE, SIG_IGN);
}


static void *
seafile_session_thread (void *vdata)
{
    seafile_session_start (seaf);

    return NULL;
}

#define ACCOUNT_GROUP "account"
#define ACCOUNT_SERVER "server"
#define ACCOUNT_USERNAME "username"
#define ACCOUNT_TOKEN "token"
#define ACCOUNT_IS_PRO "is_pro"

static void
load_account_from_file (const char *account_file)
{
    GKeyFile *key_file = g_key_file_new ();
    char *full_account_file = NULL;
    GError *error = NULL;
    char *server = NULL, *username = NULL, *token = NULL;
    gboolean is_pro;

    full_account_file = ccnet_expand_path (account_file);

    if (!g_key_file_load_from_file (key_file, full_account_file, 0, &error)) {
        seaf_warning ("Failed to load account file %s: %s.\n",
                      full_account_file, error->message);
        g_clear_error (&error);
        goto out;
    }

    server = g_key_file_get_string (key_file, ACCOUNT_GROUP, ACCOUNT_SERVER, &error);
    if (!server) {
        g_clear_error (&error);
        goto out;
    }

    username = g_key_file_get_string (key_file, ACCOUNT_GROUP, ACCOUNT_USERNAME, &error);
    if (!username) {
        g_clear_error (&error);
        goto out;
    }

    token = g_key_file_get_string (key_file, ACCOUNT_GROUP, ACCOUNT_TOKEN, &error);
    if (!token) {
        g_clear_error (&error);
        goto out;
    }

    /* Default false. */
    is_pro = g_key_file_get_boolean (key_file, ACCOUNT_GROUP, ACCOUNT_IS_PRO, NULL);

    seaf_repo_manager_add_account (seaf->repo_mgr,
                                   server, username,
                                   username,
                                   token,
                                   username,
                                   is_pro);

out:
    g_free (server);
    g_free (username);
    g_free (token);
    g_free (full_account_file);
    g_key_file_free (key_file);
}

#define GENERAL_GROUP "general"
#define CLIENT_NAME "client_name"
#define DELETE_THRESHOLD "delete_confirm_threshold"

#define NETWORK_GROUP "network"
#define DISABLE_VERIFY_CERTIFICATE "disable_verify_certificate"

#define PROXY_GROUP "proxy"
#define PROXY_TYPE "type"
#define PROXY_ADDR "addr"
#define PROXY_PORT "port"
#define PROXY_USERNAME "username"
#define PROXY_PASSWORD "password"

#define CACHE_GROUP "cache"
#define CACHE_SIZE_LIMIT "size_limit"
#define CLEAN_CACHE_INTERVAL "clean_cache_interval"

static gint64
convert_size_str (const char *size_str)
{
#define KB 1000L
#define MB 1000000L
#define GB 1000000000L
#define TB 1000000000000L

    char *end;
    gint64 size_int;
    gint64 multiplier = GB;
    gint64 size;

    size_int = strtoll (size_str, &end, 10);
    if (size_int == LLONG_MIN || size_int == LLONG_MAX) {
        return -1;
    }

    if (*end != '\0') {
        if (strcasecmp(end, "kb") == 0 || strcasecmp(end, "k") == 0)
            multiplier = KB;
        else if (strcasecmp(end, "mb") == 0 || strcasecmp(end, "m") == 0)
            multiplier = MB;
        else if (strcasecmp(end, "gb") == 0 || strcasecmp(end, "g") == 0)
            multiplier = GB;
        else if (strcasecmp(end, "tb") == 0 || strcasecmp(end, "t") == 0)
            multiplier = TB;
        else {
            seaf_warning ("Unrecognized %s\n", size_str);
            return -1;
        }
    }

    size = size_int * multiplier;

    return size;
}

static void
load_config_from_file (const char *config_file)
{
    GKeyFile *key_file = g_key_file_new ();
    char *full_config_file = NULL;
    GError *error = NULL;
    char *client_name = NULL;
    gboolean disable_verify_certificate;
    char *proxy_type = NULL, *proxy_addr = NULL, *proxy_username = NULL, *proxy_password = NULL;
    int proxy_port;
    char *cache_size_limit_str = NULL;
    gint64 cache_size_limit;
    int clean_cache_interval;
    int delete_confirm_threshold = 1000000;

    full_config_file = ccnet_expand_path (config_file);

    if (!g_key_file_load_from_file (key_file, full_config_file, 0, &error)) {
        seaf_warning ("Failed to load config file %s: %s.\n",
                      full_config_file, error->message);
        g_clear_error (&error);
        g_free (full_config_file);
        return;
    }

    client_name = g_key_file_get_string (key_file, GENERAL_GROUP, CLIENT_NAME, NULL);
    if (client_name) {
        g_free (seaf->client_name);
        seaf->client_name = g_strdup(client_name);
    }

    delete_confirm_threshold = g_key_file_get_integer (key_file, GENERAL_GROUP, DELETE_THRESHOLD,
                                                      &error);
    if (!error) {
        if (delete_confirm_threshold > 0)
            seaf->delete_confirm_threshold = delete_confirm_threshold;
    }
    g_clear_error (&error);

    disable_verify_certificate = g_key_file_get_boolean (key_file, NETWORK_GROUP,
                                                         DISABLE_VERIFY_CERTIFICATE,
                                                         &error);
    if (!error) {
        seaf->disable_verify_certificate = disable_verify_certificate;
    }
    g_clear_error (&error);

    proxy_type = g_key_file_get_string (key_file, PROXY_GROUP, PROXY_TYPE, &error);
    if (!error &&
        (g_strcmp0 (proxy_type, "http") == 0 ||
         g_strcmp0 (proxy_type, "socks") == 0)) {
        seaf->use_http_proxy = TRUE;
        g_free (seaf->http_proxy_type);
        seaf->http_proxy_type = g_strdup(proxy_type);

        proxy_addr = g_key_file_get_string (key_file, PROXY_GROUP, PROXY_ADDR, &error);
        if (!error) {
            g_free (seaf->http_proxy_addr);
            seaf->http_proxy_addr = g_strdup(proxy_addr);
        }
        g_clear_error (&error);

        proxy_port = g_key_file_get_integer (key_file, PROXY_GROUP, PROXY_PORT, &error);
        if (!error) {
            seaf->http_proxy_port = proxy_port;
        }
        g_clear_error (&error);

        proxy_username = g_key_file_get_string (key_file, PROXY_GROUP, PROXY_USERNAME, &error);
        if (!error) {
            g_free (seaf->http_proxy_username);
            seaf->http_proxy_username = g_strdup(proxy_username);
        }
        g_clear_error (&error);

        proxy_password = g_key_file_get_string (key_file, PROXY_GROUP, PROXY_PASSWORD, &error);
        if (!error) {
            g_free (seaf->http_proxy_password);
            seaf->http_proxy_password = g_strdup(proxy_password);
        }
        g_clear_error (&error);
    }
    g_clear_error (&error);

    cache_size_limit_str = g_key_file_get_string (key_file, CACHE_GROUP, CACHE_SIZE_LIMIT,
                                                  &error);
    if (!error) {
        cache_size_limit = convert_size_str (cache_size_limit_str);
        if (cache_size_limit >= 0)
            file_cache_mgr_set_cache_size_limit (seaf->file_cache_mgr, cache_size_limit);
    }
    g_clear_error (&error);

    clean_cache_interval = g_key_file_get_integer (key_file, CACHE_GROUP, CLEAN_CACHE_INTERVAL,
                                                   &error);
    if (!error) {
        file_cache_mgr_set_clean_cache_interval (seaf->file_cache_mgr, clean_cache_interval * 60);
    }
    g_clear_error (&error);

    g_free (client_name);
    g_free (proxy_type);
    g_free (proxy_addr);
    g_free (proxy_username);
    g_free (proxy_password);
    g_free (cache_size_limit_str);
    g_free (full_config_file);
    g_key_file_free (key_file);
}

static int
write_pidfile (char *pidfile)
{
    int fd = open (pidfile, O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        seaf_warning ("Failed to open pidfile %s: %s\n",
                      pidfile, strerror(errno));
        return -1;
    }

    if (flock (fd, LOCK_EX | LOCK_NB) < 0) {
        seaf_warning ("Failed to lock pidfile %s: %s\n",
                      pidfile, strerror(errno));
        close (fd);
        return -1;
    }

    return 0;
}

int
main (int argc, char **argv)
{

    char *seafile_dir = NULL;
    char *full_seafdir = NULL, *log_dir = NULL;
    char *logfile = NULL;
    const char *debug_str = NULL;
    const char *mount_point = NULL;
    char *config_file = NULL;
    char *language = NULL;
    char *pidfile = NULL;

    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    memset(&options, 0, sizeof(struct options));

    if (fuse_opt_parse(&args, &options, seadrive_fuse_opts,
                       seadrive_fuse_opt_proc_func) == -1) {
        usage();
        exit(1);
    }

    seafile_dir = options.seafile_dir;
    logfile = options.log_file;
    debug_str = options.debug_str;
    config_file = options.config_file;
    language = options.language;
    mount_point = args.argc[args.argv - 1];

    if (!seafile_dir) {
        usage ();
        exit(1);
    }

    if (!mount_point) {
        usage ();
        exit (1);
    }

    if (language != NULL &&
        strcmp (language, "zh_cn") != 0 &&
        strcmp (language, "en_us") != 0 &&
        strcmp (language, "fr_fr") != 0 &&
        strcmp (language, "de_de") != 0) {
        fprintf (stderr, "Unknown language %s\n", language);
        exit (1);
    }

    cdc_init ();

#if !GLIB_CHECK_VERSION(2, 35, 0)
    g_type_init();
#endif
#if !GLIB_CHECK_VERSION(2, 31, 0)
    g_thread_init(NULL);
#endif

    full_seafdir = ccnet_expand_path (seafile_dir);

    fprintf (stderr, "seafile dir: %s\n", full_seafdir);

    log_dir = g_build_filename (full_seafdir, "logs", NULL);
    if (checkdir_with_mkdir (log_dir) < 0) {
        fprintf (stderr, "Failed to create logs directory.\n");
        exit (1);
    }

    if (!debug_str)
        debug_str = g_getenv("SEADRIVE_DEBUG");
    seafile_debug_set_flags_string (debug_str);

    if (logfile == NULL)
        logfile = g_build_filename (log_dir, "seadrive.log", NULL);
    if (seafile_log_init (logfile) < 0) {
        seaf_warning ("Failed to init log.\n");
        exit (1);
    }

    pidfile = g_build_filename (full_seafdir, "seadrive.pid", NULL);
    if (write_pidfile (pidfile) < 0) {
        seaf_warning ("Seadrive is already running.\n");
        exit (1);
    }

#if defined(HAVE_BREAKPAD_SUPPORT)
    char *real_logdir = g_path_get_dirname (logfile);
    char *dump_dir = g_build_filename (real_logdir, "dumps", NULL);
    checkdir_with_mkdir(dump_dir);
    CBPWrapperExceptionHandler bp_exception_handler = newCBPWrapperExceptionHandler(dump_dir);
    g_free (real_logdir);
    g_free (dump_dir);
#endif

    g_free (full_seafdir);
    g_free (log_dir);
    g_free (logfile);

    set_signal_handlers (seaf);

#ifndef USE_GPL_CRYPTO
    seafile_curl_init();
#endif

    seaf = seafile_session_new (seafile_dir);
    if (!seaf) {
        seaf_warning ("Failed to create seafile session.\n");
        exit (1);
    }
    if (mount_point) {
        seaf->mount_point = ccnet_expand_path (mount_point);
    }

    if (g_strcmp0 (language, "zh_cn") == 0)
        seaf->language = SEAF_LANG_ZH_CN;
    else if (g_strcmp0 (language, "fr_fr") == 0)
        seaf->language = SEAF_LANG_FR_FR;
    else if (g_strcmp0 (language, "de_de") == 0)
        seaf->language = SEAF_LANG_DE_DE;
    else
        seaf->language = SEAF_LANG_EN_US;

    seaf_message ("Starting SeaDrive client "SEAFILE_CLIENT_VERSION"\n");
#if defined(SEADRIVE_SOURCE_COMMIT_ID)
    seaf_message ("SeaDrive client source code version "SEADRIVE_SOURCE_COMMIT_ID"\n");
#endif

    seafile_session_prepare (seaf);

    if (config_file) {
        load_account_from_file (config_file);
        load_config_from_file (config_file);
    }

    /* Start seafile session thread. */

    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (pthread_create (&tid, &attr, seafile_session_thread, NULL) < 0) {
        seaf_warning ("Failed to create seafile session thread.\n");
        exit (1);
    }

    if (start_searpc_server () < 0) {
        seaf_warning ("Failed to start searpc server.\n");
        exit (1);
    }

    seaf_message ("rpc server started.\n");

    fuse_main(args.argc, args.argv, &seadrive_fuse_ops, NULL);
    fuse_opt_free_args (&args);

#ifndef USE_GPL_CRYPTO
    seafile_curl_deinit();
#endif

    return 0;
}
