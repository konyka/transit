#ifndef T_DISPATCH_H
#define T_DISPATCH_H

#include "t_compiler.h"
#include "t_broker.h"
#include "t_session.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_dispatch t_dispatch;

t_dispatch *t_dispatch_create(t_broker *broker);
void        t_dispatch_destroy(t_dispatch *disp);

int         t_dispatch_register(t_dispatch *disp, uint64_t session_id, t_session *sess);
int         t_dispatch_unregister(t_dispatch *disp, uint64_t session_id);

int         t_dispatch_publish(t_dispatch *disp, uint64_t session_id,
                                const char *queue_name,
                                const uint8_t *data, size_t len, int priority);
int         t_dispatch_subscribe(t_dispatch *disp, uint64_t session_id,
                                  const char *queue_name);
int         t_dispatch_unsubscribe(t_dispatch *disp, uint64_t session_id,
                                    const char *queue_name);

size_t      t_dispatch_session_count(const t_dispatch *disp);
size_t      t_dispatch_total_published(const t_dispatch *disp);
size_t      t_dispatch_total_delivered(const t_dispatch *disp);

/* Complete destroy deferred while a queue fanout still held cbud snaps.
 * Safe to call anytime; broker/domain invoke after publish paths. */
void        t_dispatch_reap_deferred(void);

#endif /* T_DISPATCH_H */
