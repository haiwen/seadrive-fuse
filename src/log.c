/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#include "common.h"

#include <stdarg.h>
#include <stdio.h>
#include <glib/gstdio.h>

#include "log.h"
#include "utils.h"

#define MAX_LOG_SIZE 300 * 1024 * 1024 

/* message with greater log levels will be ignored */
static int seafile_log_level;
static char *logfile;
static FILE *logfp;

static void 
seafile_log (const gchar *log_domain, GLogLevelFlags log_level,
             const gchar *message,    gpointer user_data)
{
    time_t t;
    struct tm *tm;
    char buf[1024];
    int len;

    if (log_level > seafile_log_level)
        return;

    t = time(NULL);
    tm = localtime(&t);
    len = strftime (buf, 1024, "[%x %X] ", tm);
    g_return_if_fail (len < 1024);
    if (logfp != NULL) {    
        fputs (buf, logfp);
        fputs (message, logfp);
        fflush (logfp);
    } else { // log file not available
        printf("%s %s", buf, message);
    }
}

int
seafile_log_init (const char *_logfile)
{
    g_log_set_handler (NULL, G_LOG_LEVEL_MASK | G_LOG_FLAG_FATAL
                       | G_LOG_FLAG_RECURSION, seafile_log, NULL);

    /* record all log message */
    seafile_log_level = G_LOG_LEVEL_DEBUG;

    if (strcmp(_logfile, "-") == 0) {
        logfp = stdout;
        logfile = g_strdup (_logfile);
    }
    else {
        logfile = ccnet_expand_path(_logfile);
        if ((logfp = g_fopen (logfile, "a+")) == NULL) {
            seaf_message ("Failed to open file %s\n", logfile);
            return -1;
        }
    }

    return 0;
}

const int
seafile_log_reopen (const char *logfile_err, const char *logfile_old)
{
    FILE *fp, *errfp;

    if ((errfp = g_fopen (logfile_err, "a+")) == NULL) {
        seaf_warning ("Failed to open file %s\n", logfile_err);
        return -1;
    }

    if (fclose(logfp) < 0) {
        seaf_warning ("Failed to close file %s\n", logfile);
        fclose (errfp);
        return -1;
    }
    logfp = errfp;

    if (seaf_util_rename (logfile, logfile_old) < 0) {
        seaf_warning ("Failed to rename %s to %s, error: %s\n", logfile, logfile_old, strerror(errno));
        return -1;
    }

    if ((fp = g_fopen (logfile, "a+")) == NULL) {
        seaf_warning ("Failed to open file %s\n", logfile);
        return -1;
    }
    logfp = fp;

    if (fclose (errfp) < 0) {
        seaf_warning ("Failed to close file %s\n", logfile_err);
    }

    return 0;
}

static SeafileDebugFlags debug_flags = 0;

static GDebugKey debug_keys[] = {
  { "Transfer", SEAFILE_DEBUG_TRANSFER },
  { "Sync", SEAFILE_DEBUG_SYNC },
  { "Watch", SEAFILE_DEBUG_WATCH },
  { "Http", SEAFILE_DEBUG_HTTP },
  { "Merge", SEAFILE_DEBUG_MERGE },
  { "Fs", SEAFILE_DEBUG_FS },
  { "Curl", SEAFILE_DEBUG_CURL },
  { "Notification", SEAFILE_DEBUG_NOTIFICATION },
  { "Other", SEAFILE_DEBUG_OTHER },
};

gboolean
seafile_debug_flag_is_set (SeafileDebugFlags flag)
{
    return (debug_flags & flag) != 0;
}

void
seafile_debug_set_flags (SeafileDebugFlags flags)
{
    g_message ("Set debug flags %#x\n", flags);
    debug_flags |= flags;
}

void
seafile_debug_set_flags_string (const gchar *flags_string)
{
    guint nkeys = G_N_ELEMENTS (debug_keys);

    if (flags_string)
        seafile_debug_set_flags (
            g_parse_debug_string (flags_string, debug_keys, nkeys));
}

void
seafile_debug_impl (SeafileDebugFlags flag, const gchar *format, ...)
{
    if (flag & debug_flags) {
        va_list args;
        va_start (args, format);
        g_logv (G_LOG_DOMAIN, G_LOG_LEVEL_DEBUG, format, args);
        va_end (args);
    }
}

FILE *
seafile_get_logfp ()
{
    return logfp;
}

static void
check_and_reopen_log ()
{
    SeafStat st;

    if (g_strcmp0(logfile, "-") != 0 && seaf_stat (logfile, &st) >= 0) {
        if (st.st_size >= MAX_LOG_SIZE) {
            char *dirname = g_path_get_dirname (logfile);
            char *logfile_old  = g_build_filename (dirname, "seadrive-old.log", NULL);
            // seadrive-error.log is used to record log, when failed to open new log file.
            char *logfile_err = g_build_filename (dirname, "seadrive-error.log", NULL);

            if (seafile_log_reopen (logfile_err, logfile_old) >= 0) {
                seaf_util_unlink (logfile_err);
             }

            g_free (dirname);
            g_free (logfile_old);
            g_free (logfile_err);
        }
    }
}

static void*
log_rotate (void *vdata)
{
    while (1) {
        check_and_reopen_log ();
        g_usleep (3600LL * G_USEC_PER_SEC);
    }
    return NULL;
}

int
seafile_log_rotate_start ()
{
    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    int rc = pthread_create (&tid, &attr, log_rotate, NULL);
    if (rc != 0) {
        seaf_warning ("Failed to start log rotate thread: %s\n", strerror(rc));
        return -1;
    }

    return 0;
}
