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
/* Queue type lives in bits 16–23 (T_QUEUE_FIFO / PRIORITY / BROADCAST). */
#define T_CLIENT_QTYPE_FIFO        0x00000
#define T_CLIENT_QTYPE_PRIORITY    0x10000
#define T_CLIENT_QTYPE_BROADCAST   0x20000
/* Default idle keepalive so the server 30s idle timeout does not drop waiters. */
#define T_CLIENT_HEARTBEAT_DEFAULT_MS 10000
/* How long t_client_dial waits for the AUTH ACK when a PSK is set. */
#define T_CLIENT_AUTH_WAIT_DEFAULT_MS 1000

typedef struct t_client t_client;

typedef void (*t_client_msg_cb)(const char *queue_name, const uint8_t *data, size_t len, void *ud);

t_client  *t_client_create(const char *client_id);
void       t_client_destroy(t_client *client);
const char *t_client_id(const t_client *client);
int        t_client_is_connected(const t_client *client);
int        t_client_connect(t_client *client, const char *host, uint16_t port);
/* Real TCP dial. `t_client_connect` remains the in-process stub.
 * With a PSK, returns 0 only after an AUTH ACK T_OK. Timeout or a
 * non-OK ACK drops the socket (fail closed). Does not pump the evloop.
 * After an unexpected drop, local OPENs are unacked — re-OPEN
 * (open_follow / subscribe_follow) before POST or expecting PUSH. */
int        t_client_dial(t_client *client, t_evloop *loop, const char *host, uint16_t port);
int        t_client_set_psk(t_client *client, const uint8_t *psk, size_t len);
/* Send T_MSG_HEARTBEAT. TCP only. ACK does not advance ack_seq. */
int        t_client_heartbeat(t_client *client);
/* Repeat interval in ms. 0 disables. Default T_CLIENT_HEARTBEAT_DEFAULT_MS.
 * Negative is invalid. Re-arms when already dialed. */
int        t_client_set_heartbeat(t_client *client, int interval_ms);
/* TCP drop (idle, peer close, or this call) un-ACKs local OPENs.
 * Callbacks and JOINs stay until disconnect/destroy clears them. */
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
/* Drop the current session and dial the last leader hint. Re-OPEN after.
 * Subscriber callbacks, remembered JOINs, and local OPEN flags stay
 * (opens are marked unacked). Fail closed if the hint is missing or
 * names the peer already dialed. */
int        t_client_redial_leader(t_client *client);
/* Block until ack_seq moves past prev, or timeout_ms elapses.
 * Does not pump the evloop. timeout_ms < 0 is invalid. */
int        t_client_wait_ack(t_client *client, unsigned prev_seq, int timeout_ms);
/* OPEN, wait for ACK. On T_ERR_AGAIN with a different client-port hint,
 * redial once and OPEN again. Returns 0 only after a T_OK ACK.
 * Already-acked with the requested mode bits is a no-op; extra bits
 * (producer after subscribe, consumer after produce) send a merged OPEN.
 * A T_OK OPEN also re-OPENs other unacked queues on this session. */
int        t_client_open_follow(t_client *client, const char *queue_name, int flags,
                                int timeout_ms);
/* JOIN after a consumer OPEN. Follows a different client-port hint once.
 * The triple is remembered and replayed after a later consumer OPEN
 * (leader redial or join-before-open). */
int        t_client_join_follow(t_client *client, const char *group,
                                const char *consumer_id, const char *queue_name,
                                int timeout_ms);
/* OPEN producer if needed, POST, follow a different client-port hint once. */
int        t_client_post_follow(t_client *client, const char *queue_name,
                                const uint8_t *data, size_t len, int priority,
                                int timeout_ms);
/* CLOSE, wait for ACK. An unacked name (drop) re-OPENs first. On
 * T_ERR_AGAIN with a different client-port hint, redial once, OPEN
 * with the saved flags, and CLOSE again. */
int        t_client_close_follow(t_client *client, const char *queue_name,
                                 int timeout_ms);
/* Default 1: send CONFIRM after each PUSH callback. 0 = caller must
 * confirm or reject the last PUSH (fail closed: no silent ack). */
int        t_client_set_auto_confirm(t_client *client, int on);
uint64_t   t_client_last_push_id(const t_client *client);
/* Priority of the last PUSH (or stub post). Valid during the callback. */
int        t_client_last_push_priority(const t_client *client);
/* CONFIRM / REJECT the last PUSH on `queue_name`. TCP only. A second
 * settle of the same PUSH, a stub client, or a queue mismatch is -1. */
int        t_client_confirm(t_client *client, const char *queue_name);
int        t_client_reject(t_client *client, const char *queue_name);
/* CONFIRM/REJECT then wait. On T_ERR_AGAIN with a different client-port
 * hint, redial once and return -1 (a new session must wait for redelivery;
 * REJECT of the old id would ACK T_OK for an unknown id). */
int        t_client_confirm_follow(t_client *client, const char *queue_name,
                                   int timeout_ms);
int        t_client_reject_follow(t_client *client, const char *queue_name,
                                  int timeout_ms);
int        t_client_open_queue(t_client *client, const char *queue_name, int flags);
/* TCP: requires an acked OPEN. After a drop the name is unacked —
 * this is -1 and keeps the local flags. Use close_follow to re-OPEN. */
int        t_client_close_queue(t_client *client, const char *queue_name);
/* TCP: requires an acked producer OPEN. A stale local name after a
 * drop, or consumer-only OPEN, is -1 (does not bump published). */
int        t_client_post(t_client *client, const char *queue_name,
                         const uint8_t *data, size_t len, int priority);
int        t_client_join(t_client *client, const char *group,
                         const char *consumer_id, const char *queue_name);
int        t_client_subscribe(t_client *client, const char *queue_name,
                              t_client_msg_cb cb, void *ud);
/* Register the callback first, then consumer OPEN (plus qflags) and wait.
 * On T_ERR_AGAIN with a different client-port hint, redial once
 * (callback stays). A failed wait drops the callback just added. */
int        t_client_subscribe_follow(t_client *client, const char *queue_name,
                                     t_client_msg_cb cb, void *ud, int flags,
                                     int timeout_ms);
/* Drop callbacks. Consumer-only OPEN is CLOSEd. A producer+consumer
 * open CLOSEs then re-OPENs producer (OPEN cannot drop bits) so the
 * session stops taking PUSH. After a drop the session OPEN is gone:
 * callbacks are dropped and flags stay (0, not -1). */
int        t_client_unsubscribe(t_client *client, const char *queue_name);
size_t     t_client_queue_count(const t_client *client);
size_t     t_client_total_published(const t_client *client);
size_t     t_client_total_consumed(const t_client *client);

#endif /* T_CLIENT_H */
