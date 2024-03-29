#ifndef LOG_H
#define LOG_H

#include <stdio.h>

#define SEAFILE_DOMAIN g_quark_from_string("seafile")

#ifndef seaf_warning
#define seaf_warning(fmt, ...) g_warning("%s(%d): " fmt, __FILE__, __LINE__, ##__VA_ARGS__)
#endif

#ifndef seaf_message
#define seaf_message(fmt, ...) g_message("%s(%d): " fmt, __FILE__, __LINE__, ##__VA_ARGS__)
#endif

int seafile_log_init (const char *logfile);
int seafile_log_reopen ();

void
seafile_debug_set_flags_string (const gchar *flags_string);

typedef enum
{
    SEAFILE_DEBUG_TRANSFER = 1 << 1,
    SEAFILE_DEBUG_SYNC = 1 << 2,
    SEAFILE_DEBUG_WATCH = 1 << 3, /* wt-monitor */
    SEAFILE_DEBUG_HTTP = 1 << 4,  /* http server */
    SEAFILE_DEBUG_MERGE = 1 << 5,
    SEAFILE_DEBUG_FS = 1 << 6,  /* Virtual FS */
    SEAFILE_DEBUG_CURL = 1 << 7, /* libcurl verbose output */
    SEAFILE_DEBUG_NOTIFICATION = 1 << 8,
    SEAFILE_DEBUG_OTHER = 1 << 9,
} SeafileDebugFlags;

gboolean
seafile_debug_flag_is_set (SeafileDebugFlags flag);

void seafile_debug_impl (SeafileDebugFlags flag, const gchar *format, ...);

#ifdef DEBUG_FLAG

#undef seaf_debug

#define seaf_debug(fmt, ...)  \
    seafile_debug_impl (DEBUG_FLAG, "%.10s(%d): " fmt, __FILE__, __LINE__, ##__VA_ARGS__)

#endif  /* DEBUG_FLAG */

#endif

FILE *seafile_get_logfp ();
