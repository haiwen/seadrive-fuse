#include "common.h"
#include "log.h"
#include "utils.h"
#include "mq-mgr.h"

typedef struct MqMgrPriv {
    // chan <-> async_queue
    GHashTable *chans;
} MqMgrPriv;

MqMgr *
mq_mgr_new ()
{
    MqMgr *mgr = g_new0 (MqMgr, 1);
    mgr->priv = g_new0 (MqMgrPriv, 1);
    mgr->priv->chans = g_hash_table_new_full (g_str_hash, g_str_equal,
                                              (GDestroyNotify)g_free,
                                              (GDestroyNotify)g_async_queue_unref);

    return mgr;
}

void
mq_mgr_init (MqMgr *mgr)
{
    g_hash_table_replace (mgr->priv->chans, g_strdup (SEADRIVE_NOTIFY_CHAN),
                          g_async_queue_new_full ((GDestroyNotify)json_decref));
    g_hash_table_replace (mgr->priv->chans, g_strdup (SEADRIVE_EVENT_CHAN),
                          g_async_queue_new_full ((GDestroyNotify)json_decref));
}

void
mq_mgr_push_msg (MqMgr *mgr, const char *chan, json_t *msg)
{
    GAsyncQueue *async_queue = g_hash_table_lookup (mgr->priv->chans, chan);
    if (!async_queue) {
        seaf_warning ("Unkonwn message channel %s.\n", chan);
        return;
    }

    if (!msg) {
        seaf_warning ("Msg should not be NULL.\n");
        return;
    }

    g_async_queue_push (async_queue, msg);
}

json_t *
mq_mgr_pop_msg (MqMgr *mgr, const char *chan)
{
    GAsyncQueue *async_queue = g_hash_table_lookup (mgr->priv->chans, chan);
    if (!async_queue) {
        seaf_warning ("Unkonwn message channel %s.\n", chan);
        return NULL;
    }

    return g_async_queue_try_pop (async_queue);
}
