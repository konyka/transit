#ifndef T_CLIENT_H
#define T_CLIENT_H

#include "t_compiler.h"
#include "t_evloop.h"
#include <stdint.h>
#include <stddef.h>

#define T_CLIENT_OPEN_PRODUCER 0x01
#define T_CLIENT_OPEN_CONSUMER 0x02
/* Queue flags live in the high byte so they do not collide with mode. */
#define T_CLIENT_QFLAG_DURABLE     0x0100
#define T_CLIENT_QFLAG_EXCLUSIVE   0x0200
#define T_CLIENT_QFLAG_AUTODELETE  0x0400

typedef struct t_client t_client;

typedef void (*t_client_msg_cb)(const char *queue_name, const uint8_t *data, size_t len, void *ud);

t_client  *t_client_create(const char *client_id);
void       t_client_destroy(t_client *client);
const char *t_client_id(const t_client *client);
int        t_client_is_connected(const t_client *client);
int        t_client_connect(t_client *client, const char *host, uint16_t port);
/* Real TCP dial. `t_client_connect` remains the in-process stub. */
int        t_client_dial(t_client *client, t_evloop *loop, const char *host, uint16_t port);
int        t_client_set_psk(t_client *client, const uint8_t *psk, size_t len);
int        t_client_disconnect(t_client *client);
int        t_client_last_status(const t_client *client);
/* Monotonic count of decoded ACK frames. 0 until the first ACK arrives.
 * Wait for this to change; do not treat last_status==0 as "ACK received". */
unsigned   t_client_ack_seq(const t_client *client);
const char *t_client_last_ack_name(const t_client *client);
/* Parse ACK name `host_port` (last `_` splits IPv4/hostname from port). */
int        t_client_parse_leader_hint(const char *name, char *host, size_t host_cap,
                                      uint16_t *port);
int        t_client_leader_hint(const t_client *client, char *host, size_t host_cap,
                                uint16_t *port);
/* Drop the current session and dial the last leader hint. Re-OPEN after. */
int        t_client_redial_leader(t_client *client);
int        t_client_open_queue(t_client *client, const char *queue_name, int flags);
int        t_client_close_queue(t_client *client, const char *queue_name);
int        t_client_post(t_client *client, const char *queue_name,
                         const uint8_t *data, size_t len, int priority);
int        t_client_join(t_client *client, const char *group,
                         const char *consumer_id, const char *queue_name);
int        t_client_subscribe(t_client *client, const char *queue_name,
                              t_client_msg_cb cb, void *ud);
int        t_client_unsubscribe(t_client *client, const char *queue_name);
size_t     t_client_queue_count(const t_client *client);
size_t     t_client_total_published(const t_client *client);
size_t     t_client_total_consumed(const t_client *client);

#endif /* T_CLIENT_H */
