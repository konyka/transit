#ifndef T_BROKER_H
#define T_BROKER_H

#include "t_compiler.h"
#include "t_cluster.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_broker t_broker;
typedef struct t_domain t_domain;

typedef void (*t_broker_msg_cb)(const char *queue_name, const uint8_t *data, size_t len, void *ud);

t_broker *t_broker_create(const char *broker_id);
void      t_broker_destroy(t_broker *broker);
/* Dispatch (and similar) retain the broker so destroy waits for release. */
void      t_broker_retain(t_broker *broker);
void      t_broker_release(t_broker *broker);

const char *t_broker_id(const t_broker *broker);
int         t_broker_start(t_broker *broker);
int         t_broker_stop(t_broker *broker);
int         t_broker_is_running(const t_broker *broker);

t_domain   *t_broker_create_domain(t_broker *broker, const char *domain_name);
t_domain   *t_broker_get_domain(t_broker *broker, const char *domain_name);
int         t_broker_remove_domain(t_broker *broker, const char *domain_name);
size_t      t_broker_domain_count(const t_broker *broker);

int         t_broker_create_queue(t_broker *broker, const char *domain_name,
                                  const char *queue_name, int type, int flags);
int         t_broker_delete_queue(t_broker *broker, const char *domain_name,
                                  const char *queue_name);
/* Directory for durable WALs (mkdir 0700). Required for T_QUEUE_FLAG_DURABLE. */
int         t_broker_set_datadir(t_broker *broker, const char *path);
const char *t_broker_datadir(const t_broker *broker);
int         t_broker_set_wal_sync_every(t_broker *broker, int n);
/* Borrowed cluster pointer. When set, publish is leader-only. */
int         t_broker_set_cluster(t_broker *broker, t_cluster *cluster);
int         t_broker_is_leader(const t_broker *broker);
t_cluster  *t_broker_cluster(t_broker *broker);

int         t_broker_publish(t_broker *broker, const char *queue_name,
                              const uint8_t *data, size_t len, int priority);
int         t_broker_subscribe(t_broker *broker, const char *queue_name,
                                t_broker_msg_cb cb, void *ud);
int         t_broker_unsubscribe(t_broker *broker, const char *queue_name,
                                  t_broker_msg_cb cb, void *ud);
int         t_broker_has_subscription(t_broker *broker, const char *queue_name,
                                      t_broker_msg_cb cb, void *ud);
/* 1 if queue_name is inside a push fanout on any domain. */
int         t_broker_is_queue_delivering(const t_broker *broker, const char *queue_name);

size_t      t_broker_total_queues(const t_broker *broker);
size_t      t_broker_total_messages(const t_broker *broker);
size_t      t_broker_total_delivered(const t_broker *broker);

#endif /* T_BROKER_H */
