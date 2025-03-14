/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#include "common.h"

#include "log.h"

#include <jansson.h>

#include "utils.h"
#include "db.h"

#include "seafile-session.h"
#include "commit-mgr.h"

#define MAX_TIME_SKEW 259200    /* 3 days */

struct _SeafCommitManagerPriv {
    int dummy;
};

static SeafCommit *
load_commit (SeafCommitManager *mgr,
             const char *repo_id, int version,
             const char *commit_id);
static int
save_commit (SeafCommitManager *manager,
             const char *repo_id, int version,
             SeafCommit *commit);
static void
delete_commit (SeafCommitManager *mgr,
               const char *repo_id, int version,
               const char *id);
static json_t *
commit_to_json_object (SeafCommit *commit);
static SeafCommit *
commit_from_json_object (const char *id, json_t *object);

static void compute_commit_id (SeafCommit* commit)
{
    GChecksum *ctx = g_checksum_new(G_CHECKSUM_SHA1);
    uint8_t sha1[20];    
    gint64 ctime_n;

    g_checksum_update (ctx, (guchar *)commit->root_id, 41);
    g_checksum_update (ctx, (guchar *)commit->creator_id, 41);
    if (commit->creator_name)
        g_checksum_update (ctx, (guchar *)commit->creator_name, strlen(commit->creator_name)+1);
    g_checksum_update (ctx, (guchar *)commit->desc, strlen(commit->desc)+1);

    /* convert to network byte order */
    ctime_n = hton64 (commit->ctime);
    g_checksum_update (ctx, (guchar *)&ctime_n, sizeof(ctime_n));

    gsize len = 20;
    g_checksum_get_digest (ctx, sha1, &len);
    
    rawdata_to_hex (sha1, commit->commit_id, 20);
    g_checksum_free (ctx);
}

SeafCommit*
seaf_commit_new (const char *commit_id,
                 const char *repo_id,
                 const char *root_id,
                 const char *creator_name,
                 const char *creator_id,
                 const char *desc,
                 guint64 ctime)
{
    SeafCommit *commit;

    g_return_val_if_fail (repo_id != NULL, NULL);
    g_return_val_if_fail (root_id != NULL && creator_id != NULL, NULL);

    commit = g_new0 (SeafCommit, 1);

    memcpy (commit->repo_id, repo_id, 36);
    commit->repo_id[36] = '\0';
    
    memcpy (commit->root_id, root_id, 40);
    commit->root_id[40] = '\0';

    commit->creator_name = g_strdup (creator_name);

    memcpy (commit->creator_id, creator_id, 40);
    commit->creator_id[40] = '\0';

    commit->desc = g_strdup (desc);
    
    if (ctime == 0) {
        /* TODO: use more precise timer */
        commit->ctime = (gint64)time(NULL);
    } else
        commit->ctime = ctime;

    if (commit_id == NULL)
        compute_commit_id (commit);
    else {
        memcpy (commit->commit_id, commit_id, 40);
        commit->commit_id[40] = '\0';        
    }

    commit->ref = 1;
    return commit;
}

char *
seaf_commit_to_data (SeafCommit *commit, gsize *len)
{
    json_t *object;
    char *json_data;
    char *ret;

    object = commit_to_json_object (commit);

    json_data = json_dumps (object, 0);
    *len = strlen (json_data);
    json_decref (object);

    ret = g_strdup (json_data);
    free (json_data);
    return ret;
}

SeafCommit *
seaf_commit_from_data (const char *id, char *data, gsize len)
{
    json_t *object;
    SeafCommit *commit;
    json_error_t jerror;

    object = json_loadb (data, len, 0, &jerror);
    if (!object) {
        /* Perhaps the commit object contains invalid UTF-8 character. */
        if (data[len-1] == 0)
            clean_utf8_data (data, len - 1);
        else
            clean_utf8_data (data, len);

        object = json_loadb (data, len, 0, &jerror);
        if (!object) {
            seaf_warning ("Failed to load commit json: %s.\n", jerror.text);
            return NULL;
        }
    }

    commit = commit_from_json_object (id, object);

    json_decref (object);

    return commit;
}

static void
seaf_commit_free (SeafCommit *commit)
{
    g_free (commit->desc);
    g_free (commit->creator_name);
    if (commit->parent_id) g_free (commit->parent_id);
    if (commit->second_parent_id) g_free (commit->second_parent_id);
    if (commit->repo_name) g_free (commit->repo_name);
    if (commit->repo_desc) g_free (commit->repo_desc);
    if (commit->repo_category) g_free (commit->repo_category);
    if (commit->device_name) g_free (commit->device_name);
    g_free (commit->client_version);
    g_free (commit->magic);
    g_free (commit->random_key);
    g_free (commit->pwd_hash);
    g_free (commit->pwd_hash_algo);
    g_free (commit->pwd_hash_params);
    g_free (commit);
}

void
seaf_commit_ref (SeafCommit *commit)
{
    commit->ref++;
}

void
seaf_commit_unref (SeafCommit *commit)
{
    if (!commit)
        return;

    if (--commit->ref <= 0)
        seaf_commit_free (commit);
}

SeafCommitManager*
seaf_commit_manager_new (SeafileSession *seaf)
{
    SeafCommitManager *mgr = g_new0 (SeafCommitManager, 1);

    mgr->priv = g_new0 (SeafCommitManagerPriv, 1);
    mgr->seaf = seaf;
    mgr->obj_store = seaf_obj_store_new (mgr->seaf, "commits");

    return mgr;
}

int
seaf_commit_manager_init (SeafCommitManager *mgr)
{
    if (seaf_obj_store_init (mgr->obj_store) < 0) {
        seaf_warning ("[commit mgr] Failed to init commit object store.\n");
        return -1;
    }

    return 0;
}

int
seaf_commit_manager_add_commit (SeafCommitManager *mgr,
                                SeafCommit *commit)
{
    int ret;

    /* add_commit_to_cache (mgr, commit); */
    if ((ret = save_commit (mgr, commit->repo_id, commit->version, commit)) < 0)
        return -1;
    
    return 0;
}

void
seaf_commit_manager_del_commit (SeafCommitManager *mgr,
                                const char *repo_id,
                                int version,
                                const char *id)
{
    g_return_if_fail (id != NULL);

    delete_commit (mgr, repo_id, version, id);
}

SeafCommit* 
seaf_commit_manager_get_commit (SeafCommitManager *mgr,
                                const char *repo_id,
                                int version,
                                const char *id)
{
    SeafCommit *commit;

    commit = load_commit (mgr, repo_id, version, id);
    if (!commit)
        return NULL;

    /* add_commit_to_cache (mgr, commit); */

    return commit;
}

SeafCommit *
seaf_commit_manager_get_commit_compatible (SeafCommitManager *mgr,
                                           const char *repo_id,
                                           const char *id)
{
    SeafCommit *commit = NULL;

    /* First try version 1 layout. */
    commit = seaf_commit_manager_get_commit (mgr, repo_id, 1, id);
    if (commit)
        return commit;

    /* For compatibility with version 0. */
    commit = seaf_commit_manager_get_commit (mgr, repo_id, 0, id);

    return commit;
}

static gint
compare_commit_by_time (gconstpointer a, gconstpointer b, gpointer unused)
{
    const SeafCommit *commit_a = a;
    const SeafCommit *commit_b = b;

    /* Latest commit comes first in the list. */
    return (commit_b->ctime - commit_a->ctime);
}

inline static int
insert_parent_commit (GList **list, GHashTable *hash,
                      const char *repo_id, int version,
                      const char *parent_id, gboolean allow_truncate)
{
    SeafCommit *p;
    char *key;

    if (g_hash_table_lookup (hash, parent_id) != NULL)
        return 0;

    p = seaf_commit_manager_get_commit (seaf->commit_mgr,
                                        repo_id, version,
                                        parent_id);
    if (!p) {
        if (allow_truncate)
            return 0;
        seaf_warning ("Failed to find commit %s\n", parent_id);
        return -1;
    }

    *list = g_list_insert_sorted_with_data (*list, p,
                                           compare_commit_by_time,
                                           NULL);

    key = g_strdup (parent_id);
    g_hash_table_replace (hash, key, key);

    return 0;
}

gboolean
seaf_commit_manager_traverse_commit_tree_with_limit (SeafCommitManager *mgr,
                                                     const char *repo_id,
                                                     int version,
                                                     const char *head,
                                                     CommitTraverseFunc func,
                                                     int limit,
                                                     void *data,
                                                     gboolean skip_errors)
{
    SeafCommit *commit;
    GList *list = NULL;
    GHashTable *commit_hash;
    gboolean ret = TRUE;

    /* A hash table for recording id of traversed commits. */
    commit_hash = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);

    commit = seaf_commit_manager_get_commit (mgr, repo_id, version, head);
    if (!commit) {
        seaf_warning ("Failed to find commit %s.\n", head);
        return FALSE;
    }

    list = g_list_insert_sorted_with_data (list, commit,
                                           compare_commit_by_time,
                                           NULL);

    char *key = g_strdup (commit->commit_id);
    g_hash_table_replace (commit_hash, key, key);

    int count = 0;
    while (list) {
        gboolean stop = FALSE;
        commit = list->data;
        list = g_list_delete_link (list, list);

        if (!func (commit, data, &stop)) {
            if (!skip_errors) {
                seaf_commit_unref (commit);
                ret = FALSE;
                goto out;
            }
        }

        /* Stop when limit is reached. If limit < 0, there is no limit; */
        if (limit > 0 && ++count == limit) {
            seaf_commit_unref (commit);
            break;
        }
        
        if (stop) {
            seaf_commit_unref (commit);
            /* stop traverse down from this commit,
             * but not stop traversing the tree 
             */
            continue;
        }

        if (commit->parent_id) {
            if (insert_parent_commit (&list, commit_hash, repo_id, version,
                                      commit->parent_id, FALSE) < 0) {
                if (!skip_errors) {
                    seaf_commit_unref (commit);
                    ret = FALSE;
                    goto out;
                }
            }
        }
        if (commit->second_parent_id) {
            if (insert_parent_commit (&list, commit_hash, repo_id, version,
                                      commit->second_parent_id, FALSE) < 0) {
                if (!skip_errors) {
                    seaf_commit_unref (commit);
                    ret = FALSE;
                    goto out;
                }
            }
        }
        seaf_commit_unref (commit);
    }

out:
    g_hash_table_destroy (commit_hash);
    while (list) {
        commit = list->data;
        seaf_commit_unref (commit);
        list = g_list_delete_link (list, list);
    }
    return ret;
}

static gboolean
traverse_commit_tree_common (SeafCommitManager *mgr,
                             const char *repo_id,
                             int version,
                             const char *head,
                             CommitTraverseFunc func,
                             void *data,
                             gboolean skip_errors,
                             gboolean allow_truncate)
{
    SeafCommit *commit;
    GList *list = NULL;
    GHashTable *commit_hash;
    gboolean ret = TRUE;

    commit = seaf_commit_manager_get_commit (mgr, repo_id, version, head);
    if (!commit) {
        seaf_warning ("Failed to find commit %s.\n", head);
        // For head commit corrupted, directly return FALSE
        // user can repair head by fsck then retraverse the tree
        return FALSE;
    }

    /* A hash table for recording id of traversed commits. */
    commit_hash = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);

    list = g_list_insert_sorted_with_data (list, commit,
                                           compare_commit_by_time,
                                           NULL);

    char *key = g_strdup (commit->commit_id);
    g_hash_table_replace (commit_hash, key, key);

    while (list) {
        gboolean stop = FALSE;
        commit = list->data;
        list = g_list_delete_link (list, list);

        if (!func (commit, data, &stop)) {
            seaf_warning("[comit-mgr] CommitTraverseFunc failed\n");

            /* If skip errors, continue to traverse parents. */
            if (!skip_errors) {
                seaf_commit_unref (commit);
                ret = FALSE;
                goto out;
            }
        }
        if (stop) {
            seaf_commit_unref (commit);
            /* stop traverse down from this commit,
             * but not stop traversing the tree 
             */
            continue;
        }

        if (commit->parent_id) {
            if (insert_parent_commit (&list, commit_hash, repo_id, version,
                                      commit->parent_id, allow_truncate) < 0) {
                seaf_warning("[comit-mgr] insert parent commit failed\n");

                /* If skip errors, try insert second parent. */
                if (!skip_errors) {
                    seaf_commit_unref (commit);
                    ret = FALSE;
                    goto out;
                }
            }
        }
        if (commit->second_parent_id) {
            if (insert_parent_commit (&list, commit_hash, repo_id, version,
                                      commit->second_parent_id, allow_truncate) < 0) {
                seaf_warning("[comit-mgr]insert second parent commit failed\n");

                if (!skip_errors) {
                    seaf_commit_unref (commit);
                    ret = FALSE;
                    goto out;
                }
            }
        }
        seaf_commit_unref (commit);
    }

out:
    g_hash_table_destroy (commit_hash);
    while (list) {
        commit = list->data;
        seaf_commit_unref (commit);
        list = g_list_delete_link (list, list);
    }
    return ret;
}

gboolean
seaf_commit_manager_traverse_commit_tree (SeafCommitManager *mgr,
                                          const char *repo_id,
                                          int version,
                                          const char *head,
                                          CommitTraverseFunc func,
                                          void *data,
                                          gboolean skip_errors)
{
    return traverse_commit_tree_common (mgr, repo_id, version, head,
                                        func, data, skip_errors, FALSE);
}

gboolean
seaf_commit_manager_traverse_commit_tree_truncated (SeafCommitManager *mgr,
                                                    const char *repo_id,
                                                    int version,
                                                    const char *head,
                                                    CommitTraverseFunc func,
                                                    void *data,
                                                    gboolean skip_errors)
{
    return traverse_commit_tree_common (mgr, repo_id, version, head,
                                        func, data, skip_errors, TRUE);
}

gboolean
seaf_commit_manager_commit_exists (SeafCommitManager *mgr,
                                   const char *repo_id,
                                   int version,
                                   const char *id)
{
#if 0
    commit = g_hash_table_lookup (mgr->priv->commit_cache, id);
    if (commit != NULL)
        return TRUE;
#endif

    return seaf_obj_store_obj_exists (mgr->obj_store, repo_id, version, id);
}

inline static const char *json_object_get_string_or_null_member (json_t *object,const char *member_name)
{
    json_t *ret = json_object_get (object, member_name);
    if (ret)
      return json_string_value(ret);
    else
      return NULL;
}

inline static void json_object_set_string_or_null_member (json_t *object,const char *member_name,const char *value)
{
    if (value)
      json_object_set_new(object,member_name, json_string(value));
    else
      json_object_set_new(object,member_name, json_null());
}

static json_t *
commit_to_json_object (SeafCommit *commit)
{
    json_t *object;
    
    object = json_object ();
 
    json_object_set_string_member (object, "commit_id", commit->commit_id);
    json_object_set_string_member (object, "root_id", commit->root_id);
    json_object_set_string_member (object, "repo_id", commit->repo_id);
    if (commit->creator_name)
        json_object_set_string_member (object, "creator_name", commit->creator_name);
    json_object_set_string_member (object, "creator", commit->creator_id);
    json_object_set_string_member (object, "description", commit->desc);
    json_object_set_int_member (object, "ctime", (gint64)commit->ctime);
    json_object_set_string_or_null_member (object, "parent_id", commit->parent_id);
    json_object_set_string_or_null_member (object, "second_parent_id",
                                           commit->second_parent_id);
    /*
     * also save repo's properties to commit file, for easy sharing of
     * repo info 
     */
    json_object_set_string_member (object, "repo_name", commit->repo_name);
    json_object_set_string_member (object, "repo_desc",
                                   commit->repo_desc);
    json_object_set_string_or_null_member (object, "repo_category",
                                           commit->repo_category);
    if (commit->device_name)
        json_object_set_string_member (object, "device_name", commit->device_name);

    if (commit->client_version)
        json_object_set_string_member (object, "client_version", commit->client_version);

    if (commit->encrypted)
        json_object_set_string_member (object, "encrypted", "true");

    if (commit->encrypted) {
        json_object_set_int_member (object, "enc_version", commit->enc_version);
        if (commit->enc_version >= 1 && !commit->pwd_hash)
            json_object_set_string_member (object, "magic", commit->magic);
        if (commit->enc_version >= 2)
            json_object_set_string_member (object, "key", commit->random_key);
        if (commit->enc_version >= 3)
            json_object_set_string_member (object, "salt", commit->salt);
        if (commit->pwd_hash) {
            json_object_set_string_member (object, "pwd_hash", commit->pwd_hash);
            json_object_set_string_member (object, "pwd_hash_algo", commit->pwd_hash_algo);
            json_object_set_string_member (object, "pwd_hash_params", commit->pwd_hash_params);
        }
    }
    if (commit->version != 0)
        json_object_set_int_member (object, "version", commit->version);
    if (commit->conflict)
        json_object_set_int_member (object, "conflict", 1);
    if (commit->new_merge)
        json_object_set_int_member (object, "new_merge", 1);
    if (commit->repaired)
        json_object_set_int_member (object, "repaired", 1);

    return object;
}

static SeafCommit *
commit_from_json_object (const char *commit_id, json_t *object)
{
    SeafCommit *commit = NULL;
    const char *root_id;
    const char *repo_id;
    const char *creator_name = NULL;
    const char *creator;
    const char *desc;
    gint64 ctime;
    const char *parent_id, *second_parent_id;
    const char *repo_name;
    const char *repo_desc;
    const char *repo_category;
    const char *device_name;
    const char *client_version;
    const char *encrypted = NULL;
    int enc_version = 0;
    const char *magic = NULL;
    const char *random_key = NULL;
    const char *salt = NULL;
    const char *pwd_hash = NULL;
    const char *pwd_hash_algo = NULL;
    const char *pwd_hash_params = NULL;
    int version = 0;
    int conflict = 0, new_merge = 0;
    int repaired = 0;

    root_id = json_object_get_string_member (object, "root_id");
    repo_id = json_object_get_string_member (object, "repo_id");
    if (json_object_has_member (object, "creator_name"))
        creator_name = json_object_get_string_or_null_member (object, "creator_name");
    creator = json_object_get_string_member (object, "creator");
    desc = json_object_get_string_member (object, "description");
    if (!desc)
        desc = "";
    ctime = (guint64) json_object_get_int_member (object, "ctime");
    parent_id = json_object_get_string_or_null_member (object, "parent_id");
    second_parent_id = json_object_get_string_or_null_member (object, "second_parent_id");

    repo_name = json_object_get_string_member (object, "repo_name");
    if (!repo_name)
        repo_name = "";
    repo_desc = json_object_get_string_member (object, "repo_desc");
    if (!repo_desc)
        repo_desc = "";
    repo_category = json_object_get_string_or_null_member (object, "repo_category");
    device_name = json_object_get_string_or_null_member (object, "device_name");
    client_version = json_object_get_string_or_null_member (object, "client_version");

    if (json_object_has_member (object, "encrypted"))
        encrypted = json_object_get_string_or_null_member (object, "encrypted");

    if (encrypted && strcmp(encrypted, "true") == 0
        && json_object_has_member (object, "enc_version")) {
        enc_version = json_object_get_int_member (object, "enc_version");
        magic = json_object_get_string_member (object, "magic");
        pwd_hash = json_object_get_string_member (object, "pwd_hash");
        pwd_hash_algo = json_object_get_string_member (object, "pwd_hash_algo");
        pwd_hash_params = json_object_get_string_member (object, "pwd_hash_params");
    }

    if (enc_version >= 2)
        random_key = json_object_get_string_member (object, "key");
    if (enc_version >= 3)
        salt = json_object_get_string_member (object, "salt");

    if (json_object_has_member (object, "version"))
        version = json_object_get_int_member (object, "version");
    if (json_object_has_member (object, "new_merge"))
        new_merge = json_object_get_int_member (object, "new_merge");

    if (json_object_has_member (object, "conflict"))
        conflict = json_object_get_int_member (object, "conflict");

    if (json_object_has_member (object, "repaired"))
        repaired = json_object_get_int_member (object, "repaired");


    /* sanity check for incoming values. */
    if (!repo_id || !is_uuid_valid(repo_id)  ||
        !root_id || !is_object_id_valid(root_id) ||
        !creator || strlen(creator) != 40 ||
        (parent_id && !is_object_id_valid(parent_id)) ||
        (second_parent_id && !is_object_id_valid(second_parent_id)))
        return commit;

    // If pwd_hash is set, the magic field is no longer included in the commit of the newly created repo.
    if (!magic)
        magic = pwd_hash;

    switch (enc_version) {
    case 0:
        break;
    case 1:
        if (!magic || strlen(magic) != 32)
            return NULL;
        break;
    case 2:
        if (!magic || strlen(magic) != 64)
            return NULL;
        if (!random_key || strlen(random_key) != 96)
            return NULL;
        break;
    case 3:
        if (!magic || strlen(magic) != 64)
            return NULL;
        if (!random_key || strlen(random_key) != 96)
            return NULL;
        if (!salt || strlen(salt) != 64)
            return NULL;
        break;
    case 4:
        if (!magic || strlen(magic) != 64)
            return NULL;
        if (!random_key || strlen(random_key) != 96)
            return NULL;
        if (!salt || strlen(salt) != 64)
            return NULL;
        break;
    default:
        seaf_warning ("Unknown encryption version %d.\n", enc_version);
        return NULL;
    }

    char *creator_name_l = creator_name ? g_ascii_strdown (creator_name, -1) : NULL;
    commit = seaf_commit_new (commit_id, repo_id, root_id,
                              creator_name_l, creator, desc, ctime);
    g_free (creator_name_l);

    commit->parent_id = parent_id ? g_strdup(parent_id) : NULL;
    commit->second_parent_id = second_parent_id ? g_strdup(second_parent_id) : NULL;

    commit->repo_name = g_strdup(repo_name);
    commit->repo_desc = g_strdup(repo_desc);
    if (encrypted && strcmp(encrypted, "true") == 0)
        commit->encrypted = TRUE;
    else
        commit->encrypted = FALSE;
    if (repo_category)
        commit->repo_category = g_strdup(repo_category);
    commit->device_name = g_strdup(device_name);
    commit->client_version = g_strdup(client_version);

    if (commit->encrypted) {
        commit->enc_version = enc_version;
        if (enc_version >= 1 && !pwd_hash)
            commit->magic = g_strdup(magic);
        if (enc_version >= 2)
            commit->random_key = g_strdup (random_key);
        if (enc_version >= 3)
            commit->salt = g_strdup (salt);
        if (pwd_hash) {
            commit->pwd_hash = g_strdup (pwd_hash);
            commit->pwd_hash_algo = g_strdup (pwd_hash_algo);
            commit->pwd_hash_params = g_strdup (pwd_hash_params);
        }
    }
    commit->version = version;
    if (new_merge)
        commit->new_merge = TRUE;
    if (conflict)
        commit->conflict = TRUE;
    if (repaired)
        commit->repaired = TRUE;

    return commit;
}

static SeafCommit *
load_commit (SeafCommitManager *mgr,
             const char *repo_id,
             int version,
             const char *commit_id)
{
    char *data = NULL;
    int len;
    SeafCommit *commit = NULL;
    json_t *object = NULL;
    json_error_t jerror;

    if (!commit_id || strlen(commit_id) != 40)
        return NULL;

    if (seaf_obj_store_read_obj (mgr->obj_store, repo_id, version,
                                 commit_id, (void **)&data, &len) < 0)
        return NULL;

    object = json_loadb (data, len, 0, &jerror);
    if (!object) {
        /* Perhaps the commit object contains invalid UTF-8 character. */
        if (data[len-1] == 0)
            clean_utf8_data (data, len - 1);
        else
            clean_utf8_data (data, len);

        object = json_loadb (data, len, 0, &jerror);
        if (!object) {
            seaf_warning ("Failed to load commit json object: %s.\n", jerror.text);
            goto out;
        }
    }

    commit = commit_from_json_object (commit_id, object);
    if (commit)
        commit->manager = mgr;

out:
    if (object) json_decref (object);
    g_free (data);

    return commit;
}

static int
save_commit (SeafCommitManager *manager,
             const char *repo_id,
             int version,
             SeafCommit *commit)
{
    json_t *object = NULL;
    char *data;
    gsize len;

    object = commit_to_json_object (commit);

    data = json_dumps (object, 0);
    len = strlen (data);

    json_decref (object);

#ifdef SEAFILE_SERVER
    if (seaf_obj_store_write_obj (manager->obj_store,
                                  repo_id, version,
                                  commit->commit_id,
                                  data, (int)len, TRUE) < 0) {
        g_free (data);
        return -1;
    }
#else
    if (seaf_obj_store_write_obj (manager->obj_store,
                                  repo_id, version,
                                  commit->commit_id,
                                  data, (int)len, FALSE) < 0) {
        g_free (data);
        return -1;
    }
#endif
    free (data);

    return 0;
}

static void
delete_commit (SeafCommitManager *mgr,
               const char *repo_id,
               int version,
               const char *id)
{
    seaf_obj_store_delete_obj (mgr->obj_store, repo_id, version, id);
}

int
seaf_commit_manager_remove_store (SeafCommitManager *mgr,
                                  const char *store_id)
{
    return seaf_obj_store_remove_store (mgr->obj_store, store_id);
}
