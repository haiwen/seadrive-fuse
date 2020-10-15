#ifndef MQ_MGR_H
#define MQ_MGR_H

#define SEADRIVE_NOTIFY_CHAN "seadrive.notification"
#define SEADRIVE_EVENT_CHAN "seadrive.event"

struct MqMgrPriv;

typedef struct MqMgr {
    struct MqMgrPriv *priv;
} MqMgr;

MqMgr *
mq_mgr_new ();

void
mq_mgr_init (MqMgr *mgr);

void
mq_mgr_push_msg (MqMgr *mgr, const char *chan, json_t *msg);

json_t *
mq_mgr_pop_msg (MqMgr *mgr, const char *chan);

#endif
