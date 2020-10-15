/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#include "common.h"

#include "utils.h"
#define DEBUG_FLAG SEAFILE_DEBUG_SYNC
#include "log.h"

#include "set-perm.h"

#include <sys/stat.h>

int
seaf_set_path_permission (const char *path, SeafPathPerm perm, gboolean recursive)
{
    struct stat st;
    mode_t new_mode;

    if (stat (path, &st) < 0) {
        seaf_warning ("Failed to stat %s: %s\n", path, strerror(errno));
        return -1;
    }

    new_mode = st.st_mode;
    if (perm == SEAF_PATH_PERM_RO)
        new_mode &= ~(S_IWUSR);
    else if (perm == SEAF_PATH_PERM_RW)
        new_mode |= S_IWUSR;

    if (chmod (path, new_mode) < 0) {
        seaf_warning ("Failed to chmod %s to %d: %s\n", path, new_mode, strerror(errno));
        return -1;
    }

    return 0;
}

int
seaf_unset_path_permission (const char *path, gboolean recursive)
{
    return 0;
}

SeafPathPerm
seaf_get_path_permission (const char *path)
{
    return SEAF_PATH_PERM_UNKNOWN;
}
