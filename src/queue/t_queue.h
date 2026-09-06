#ifndef T_QUEUE_H
#define T_QUEUE_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

typedef enum t_qtype {
    T_QUEUE_FIFO = 0,
    T_QUEUE_PRIORITY,
    T_QUEUE_BROADCAST
} t_qtype;

typedef enum t_queue_flags {
    T_QUEUE_FLAG_NONE      = 0,
    T_QUEUE_FLAG_DURABLE   = 1 << 0,
    T_QUEUE_FLAG_EXCLUSIVE = 1 << 1,
    T_QUEUE_FLAG_AUTODELETE = 1 << 2
} t_queue_flags;

/* Align with T_PROTO_MAX_PAYLOAD / storage value cap. */
#define T_QUEUE_MAX_PAYLOAD (16 * 1024 * 1024)

typedef struct t_msg {
    uint64_t        msg_id;
    const char     *queue_name;
    const uint8_t  *data;
    size_t          data_len;
    int             priority;
    uint64_t        timestamp_ns;
} t_msg;

typedef struct t_queue t_queue;

/* Push callback: msg/data are valid only for the duration of the call; do not retain. */
typedef void (*t_queue_msg_cb)(const t_msg *msg, void *ud);

t_queue      *t_queue_create(const char *name, t_qtype type, int flags);
void          t_queue_destroy(t_queue *q);
const char   *t_queue_name(const t_queue *q);
t_qtype       t_queue_get_type(const t_queue *q);
int           t_queue_get_flags(const t_queue *q);
/* Attach an append-only WAL and replay live records into pending. */
int           t_queue_open_wal(t_queue *q, const char *path, int sync_every);
int           t_queue_flush(t_queue *q);
const char   *t_queue_wal_path(const t_queue *q);
/* Inject a recovered message; does not append to the WAL. */
int           t_queue_restore(t_queue *q, uint64_t msg_id, const uint8_t *data,
                              size_t len, int priority);

int           t_queue_post(t_queue *q, const uint8_t *data, size_t len, int priority);
/* out_msg borrows inflight payload; valid until ack/nack/requeue/destroy. */
int           t_queue_consume(t_queue *q, t_msg *out_msg);
size_t        t_queue_pending_count(const t_queue *q);

uint64_t      t_queue_add_consumer(t_queue *q, t_queue_msg_cb cb, void *ud);
int           t_queue_remove_consumer(t_queue *q, uint64_t consumer_id);
int           t_queue_remove_consumer_ud(t_queue *q, void *ud);
int           t_queue_has_consumer_ud(const t_queue *q, void *ud);
size_t        t_queue_consumer_count(const t_queue *q);

int           t_queue_ack(t_queue *q, uint64_t msg_id);
/* Remove msg_id from inflight or pending. Used by Raft ACK apply. */
int           t_queue_drop(t_queue *q, uint64_t msg_id);
int           t_queue_nack(t_queue *q, uint64_t msg_id);
int           t_queue_requeue(t_queue *q, uint64_t msg_id);

void          t_queue_close(t_queue *q);
/* After close, post/subscribe are rejected; consume may still drain pending. */
int           t_queue_is_closed(const t_queue *q);
int           t_queue_is_delivering(const t_queue *q);
/* 1 if pull consume has unacked/nacked messages (payloads still borrowed). */
int           t_queue_has_inflight(const t_queue *q);
/* 1 if destroy was requested while delivering (owner must reap). */
int           t_queue_is_free_pending(const t_queue *q);
/* When set, deferred destroy is left for the map owner (e.g. domain). */
void          t_queue_set_owner_held(t_queue *q, int held);

size_t        t_queue_total_published(const t_queue *q);
size_t        t_queue_total_consumed(const t_queue *q);

#endif /* T_QUEUE_H */
