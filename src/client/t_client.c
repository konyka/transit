#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "t_client.h"
#include "t_queue.h"
#include "t_conn.h"
#include "t_socket.h"
#include "t_wire.h"
#include "t_proto.h"
#include "t_error.h"
#include "t_hmac.h"
#include "t_atomic.h"
#include "t_time.h"

/* Minimal in-process client implementation with queue registry and subscriptions. */
typedef struct t_client_queue_entry {
    char *name;
    int   flags;
    int   acked;
} t_client_queue_entry;

typedef struct t_client_subscription {
    char *queue;
    t_client_msg_cb cb;
    void *ud;
} t_client_subscription;

struct t_client {
    char *id;
    int connected;

    t_client_queue_entry *queues;
    size_t queues_cap;
    size_t queues_size;

    t_client_subscription *subs;
    size_t subs_cap;
    size_t subs_count;

    size_t published;
    size_t consumed;
    int posting;      /* set while fanout callbacks run */
    int free_pending; /* destroy deferred until post returns */

    t_evloop *loop;
    t_conn   *conn;
    int       net_mode;
    t_atomic_int last_status;
    t_atomic_int ack_seq;
    char      last_ack_name[T_WIRE_MAX_NAME + 1];
    uint8_t  *psk;
    size_t    psk_len;
    char     *dial_host;
    uint16_t  dial_port;
    int       heartbeat_ms;
    int64_t   heartbeat_timer_id;
};

typedef struct t_cb_snap {
    t_client_msg_cb cb;
    void *ud;
} t_cb_snap;

/* Helpers */
static int client_ensure_queues_cap(t_client *c, size_t need) {
    if (c->queues_size >= need) return 0;
    size_t new_cap = (c->queues_cap == 0) ? 4 : c->queues_cap;
    while (need > new_cap) {
        if (new_cap > SIZE_MAX / 2) return -1;
        new_cap *= 2;
    }
    if (new_cap > SIZE_MAX / sizeof(t_client_queue_entry)) return -1;
    t_client_queue_entry *newq = (t_client_queue_entry *)realloc(c->queues, new_cap * sizeof(t_client_queue_entry));
    if (!newq) return -1;
    c->queues = newq;
    c->queues_cap = new_cap;
    return 0;
}

static int client_ensure_subs_cap(t_client *c, size_t need) {
    if (c->subs_count >= need) return 0;
    size_t new_cap = (c->subs_cap == 0) ? 4 : c->subs_cap;
    while (need > new_cap) {
        if (new_cap > SIZE_MAX / 2) return -1;
        new_cap *= 2;
    }
    if (new_cap > SIZE_MAX / sizeof(t_client_subscription)) return -1;
    t_client_subscription *news = (t_client_subscription *)realloc(c->subs, new_cap * sizeof(t_client_subscription));
    if (!news) return -1;
    c->subs = news;
    c->subs_cap = new_cap;
    return 0;
}

static void client_on_close(t_conn *conn, void *ud);
static void client_on_msg(t_conn *conn, const t_proto_msg *msg, void *ud);
static int client_send_payload(t_client *c, t_msg_type type, const uint8_t *payload, size_t plen);

static int client_send_payload(t_client *c, t_msg_type type, const uint8_t *payload, size_t plen) {
    if (!c || !c->conn) return -1;
    t_proto_msg msg;
    t_proto_header_init(&msg.header, type, (uint32_t)plen);
    msg.payload = (uint8_t *)payload;
    msg.payload_len = plen;
    return t_conn_send(c->conn, &msg);
}

static void client_heartbeat_tick(void *ud);

static void client_hb_disarm(t_client *c) {
    if (!c || c->heartbeat_timer_id < 0) return;
    if (c->loop) t_evloop_timer_del(c->loop, c->heartbeat_timer_id);
    c->heartbeat_timer_id = -1;
}

static void client_hb_arm(t_client *c) {
    if (!c || !c->loop || !c->net_mode || c->heartbeat_ms <= 0) return;
    client_hb_disarm(c);
    c->heartbeat_timer_id = t_evloop_timer_add(c->loop, (int64_t)c->heartbeat_ms, 1,
                                               client_heartbeat_tick, c);
}

static void client_heartbeat_tick(void *ud) {
    t_client *c = (t_client *)ud;
    if (!c || c->free_pending || !c->net_mode || !c->connected || !c->conn)
        return;
    (void)client_send_payload(c, T_MSG_HEARTBEAT, NULL, 0);
}

static int client_queue_ready(const t_client *c, const char *name) {
    if (!c || !name) return 0;
    for (size_t i = 0; i < c->queues_size; ++i) {
        if (c->queues[i].name && strcmp(c->queues[i].name, name) == 0)
            return !c->net_mode || c->queues[i].acked != 0;
    }
    return 0;
}

static void client_mark_queue_acked(t_client *c, const char *name) {
    if (!c || !name || !name[0]) return;
    for (size_t i = 0; i < c->queues_size; ++i) {
        if (c->queues[i].name && strcmp(c->queues[i].name, name) == 0) {
            c->queues[i].acked = 1;
            return;
        }
    }
}

static int client_send_open(t_client *c, const char *queue_name, int flags) {
    if (!c->net_mode) return 0;
    uint8_t mode = (uint8_t)(flags & 0xFF);
    uint8_t qflags = (uint8_t)((flags >> 8) & 0xFF);
    if (mode == 0) mode = T_CLIENT_OPEN_PRODUCER;
    uint8_t buf[3 + 2 + T_WIRE_MAX_NAME];
    int n = t_wire_encode_open(buf, sizeof(buf), T_QUEUE_FIFO, qflags,
                               mode, queue_name);
    if (n < 0) return -1;
    return client_send_payload(c, T_MSG_OPEN_QUEUE, buf, (size_t)n);
}

/* 0 = T_OK, 1 = T_ERR_AGAIN (caller may follow once), -1 = fail. */
static int client_wait_status(t_client *c, unsigned seq, int timeout_ms) {
    if (t_client_wait_ack(c, seq, timeout_ms) != 0) return -1;
    int st = t_client_last_status(c);
    if (st == 0) return 0;
    if (st != (int)T_ERR_AGAIN) return -1;
    return 1;
}

static void client_drop_conn(t_client *c) {
    if (!c || !c->conn) return;
    t_conn *conn = c->conn;
    c->conn = NULL;
    c->connected = 0;
    t_conn_destroy(conn);
}

static void client_on_close(t_conn *conn, void *ud) {
    t_client *c = (t_client *)ud;
    if (!c) return;
    c->connected = 0;
    if (c->conn == conn) {
        c->conn = NULL;
        t_conn_destroy(conn);
    }
}

static void client_on_msg(t_conn *conn, const t_proto_msg *msg, void *ud) {
    t_client *c = (t_client *)ud;
    (void)conn;
    if (!c || c->free_pending || !msg) return;
    if (msg->header.type == T_MSG_ACK) {
        t_wire_ack a;
        if (t_wire_decode_ack(msg->payload, msg->payload_len, &a) == 0) {
            if (a.req_type == T_MSG_HEARTBEAT || a.req_type == T_MSG_NOP)
                return;
            t_atomic_store_int(&c->last_status, a.status);
            c->last_ack_name[0] = '\0';
            if (a.name && a.name_len)
                (void)t_wire_name_copy(c->last_ack_name, sizeof(c->last_ack_name),
                                       a.name, a.name_len);
            if (a.status == 0 && c->last_ack_name[0])
                client_mark_queue_acked(c, c->last_ack_name);
            (void)t_atomic_add_fetch_int(&c->ack_seq, 1);
        }
        return;
    }
    if (msg->header.type != T_MSG_PUSH) return;
    t_wire_push p;
    if (t_wire_decode_push(msg->payload, msg->payload_len, &p) != 0) return;
    char name[T_WIRE_MAX_NAME + 1];
    if (t_wire_name_copy(name, sizeof(name), p.name, p.name_len) != 0) return;
    size_t n = c->subs_count;
    t_cb_snap *snaps = NULL;
    size_t snap_n = 0;
    if (n > 0) {
        snaps = (t_cb_snap *)calloc(n, sizeof(*snaps));
        if (!snaps) return;
        for (size_t i = 0; i < n; ++i) {
            if (c->subs[i].queue && strcmp(c->subs[i].queue, name) == 0 && c->subs[i].cb) {
                snaps[snap_n].cb = c->subs[i].cb;
                snaps[snap_n].ud = c->subs[i].ud;
                snap_n++;
            }
        }
    }
    c->posting++;
    for (size_t i = 0; i < snap_n; ++i) {
        snaps[i].cb(name, p.data, p.data_len, snaps[i].ud);
        c->consumed++;
        if (c->free_pending) break;
    }
    c->posting--;
    free(snaps);
    if (c->net_mode && c->conn && !c->free_pending) {
        uint8_t cbuf[8 + 2 + T_WIRE_MAX_NAME];
        int cn = t_wire_encode_confirm(cbuf, sizeof(cbuf), p.msg_id, name);
        if (cn > 0)
            (void)client_send_payload(c, T_MSG_CONFIRM, cbuf, (size_t)cn);
    }
    if (c->posting == 0 && c->free_pending) t_client_destroy(c);
}

/* API */
t_client *t_client_create(const char *client_id) {
    t_client *c = (t_client *)calloc(1, sizeof(t_client));
    if (!c) return NULL;
    c->id = strdup(client_id ? client_id : "");
    if (!c->id) {
        free(c);
        return NULL;
    }
    c->connected = 0;
    c->queues = NULL; c->queues_cap = 0; c->queues_size = 0;
    c->subs = NULL; c->subs_cap = 0; c->subs_count = 0;
    c->published = 0; c->consumed = 0;
    c->loop = NULL;
    c->conn = NULL;
    c->net_mode = 0;
    t_atomic_store_int(&c->last_status, 0);
    t_atomic_store_int(&c->ack_seq, 0);
    c->last_ack_name[0] = '\0';
    c->heartbeat_ms = T_CLIENT_HEARTBEAT_DEFAULT_MS;
    c->heartbeat_timer_id = -1;
    return c;
}

void t_client_destroy(t_client *client) {
    if (!client) return;
    if (client->posting > 0) {
        client->free_pending = 1;
        return;
    }
    client_hb_disarm(client);
    client_drop_conn(client);
    if (client->id) free(client->id);
    for (size_t i = 0; i < client->queues_size; ++i) {
        free(client->queues[i].name);
    }
    free(client->queues);
    for (size_t i = 0; i < client->subs_count; ++i) {
        free(client->subs[i].queue);
    }
    free(client->subs);
    if (client->psk) {
        t_hmac_wipe(client->psk, client->psk_len);
        free(client->psk);
    }
    free(client->dial_host);
    free(client);
}

const char *t_client_id(const t_client *client) {
    return client ? client->id : NULL;
}

int t_client_is_connected(const t_client *client) {
    return client ? client->connected != 0 : 0;
}

int t_client_connect(t_client *client, const char *host, uint16_t port) {
    (void)host; (void)port; /* in-process stub; use t_client_dial for TCP */
    if (!client || client->free_pending || client->net_mode) return -1;
    client->connected = 1;
    t_atomic_store_int(&client->last_status, 0);
    client->last_ack_name[0] = '\0';
    return 0;
}

int t_client_dial(t_client *client, t_evloop *loop, const char *host, uint16_t port) {
    if (!client || client->free_pending || !loop || !host || port == 0) return -1;
    if (client->connected || client->conn) return -1;
    int fd = t_socket_dial_ipv4(host, port);
    if (fd < 0) return -1;
    t_conn *conn = t_conn_create(fd, loop);
    if (!conn) return -1;
    client->loop = loop;
    client->conn = conn;
    client->net_mode = 1;
    client->connected = 0;
    t_atomic_store_int(&client->last_status, 0);
    client->last_ack_name[0] = '\0';
    t_conn_set_on_msg(conn, client_on_msg, client);
    t_conn_set_on_close(conn, client_on_close, client);
    if (client->psk_len > 0) {
        uint8_t mac[T_HMAC_SHA256_LEN];
        uint8_t buf[T_WIRE_AUTH_MAC_LEN];
        if (t_auth_mac(mac, client->psk, client->psk_len) != 0) {
            client_drop_conn(client);
            return -1;
        }
        int n = t_wire_encode_auth(buf, sizeof(buf), mac);
        t_hmac_wipe(mac, sizeof(mac));
        unsigned prev = t_client_ack_seq(client);
        if (n < 0 || client_send_payload(client, T_MSG_AUTH, buf, (size_t)n) != 0) {
            client_drop_conn(client);
            return -1;
        }
        if (t_client_wait_ack(client, prev, T_CLIENT_AUTH_WAIT_DEFAULT_MS) != 0) {
            t_atomic_store_int(&client->last_status, (int)T_ERR_TIMEOUT);
            client_drop_conn(client);
            return -1;
        }
        if (t_client_last_status(client) != 0) {
            client_drop_conn(client);
            return -1;
        }
    }
    char *hcopy = strdup(host);
    if (!hcopy) {
        client_drop_conn(client);
        return -1;
    }
    free(client->dial_host);
    client->dial_host = hcopy;
    client->dial_port = port;
    client->connected = 1;
    client_hb_arm(client);
    return 0;
}

int t_client_set_psk(t_client *client, const uint8_t *psk, size_t len) {
    if (!client || client->free_pending) return -1;
    if (len == 0 || len > T_AUTH_PSK_MAX || !psk) return -1;
    uint8_t *copy = (uint8_t *)malloc(len);
    if (!copy) return -1;
    memcpy(copy, psk, len);
    if (client->psk) {
        t_hmac_wipe(client->psk, client->psk_len);
        free(client->psk);
    }
    client->psk = copy;
    client->psk_len = len;
    return 0;
}

int t_client_heartbeat(t_client *client) {
    if (!client || client->free_pending || !client->net_mode || !client->connected)
        return -1;
    return client_send_payload(client, T_MSG_HEARTBEAT, NULL, 0);
}

int t_client_set_heartbeat(t_client *client, int interval_ms) {
    if (!client || client->free_pending || interval_ms < 0) return -1;
    client->heartbeat_ms = interval_ms;
    if (interval_ms == 0) {
        client_hb_disarm(client);
        return 0;
    }
    if (client->net_mode && client->connected && client->loop)
        client_hb_arm(client);
    return 0;
}

int t_client_last_status(const t_client *client) {
    return client ? t_atomic_load_int((t_atomic_int *)&client->last_status)
                  : (int)T_ERR_INVALID;
}

unsigned t_client_ack_seq(const t_client *client) {
    if (!client) return 0u;
    int n = t_atomic_load_int((t_atomic_int *)&client->ack_seq);
    return n > 0 ? (unsigned)n : 0u;
}

const char *t_client_last_ack_name(const t_client *client) {
    return client ? client->last_ack_name : NULL;
}

int t_client_parse_leader_hint(const char *name, char *host, size_t host_cap,
                               uint16_t *port) {
    if (!name || !host || host_cap == 0 || !port) return -1;
    const char *us = strrchr(name, '_');
    if (!us || us == name || us[1] == '\0') return -1;
    size_t hlen = (size_t)(us - name);
    if (hlen >= host_cap) return -1;
    if (us[1] < '1' || us[1] > '9') return -1;
    unsigned long p = 0;
    const char *d = us + 1;
    while (*d >= '0' && *d <= '9') {
        p = p * 10 + (unsigned long)(*d - '0');
        if (p > 65535ul) return -1;
        d++;
    }
    if (*d != '\0' || p < 1 || p > 65535ul) return -1;
    memcpy(host, name, hlen);
    host[hlen] = '\0';
    *port = (uint16_t)p;
    return 0;
}

int t_client_leader_hint(const t_client *client, char *host, size_t host_cap,
                         uint16_t *port) {
    if (!client) return -1;
    return t_client_parse_leader_hint(client->last_ack_name, host, host_cap, port);
}

static void client_clear_session(t_client *client) {
    for (size_t i = 0; i < client->subs_count; ++i)
        free(client->subs[i].queue);
    free(client->subs);
    client->subs = NULL;
    client->subs_count = 0;
    client->subs_cap = 0;
    for (size_t i = 0; i < client->queues_size; ++i)
        free(client->queues[i].name);
    free(client->queues);
    client->queues = NULL;
    client->queues_size = 0;
    client->queues_cap = 0;
}

int t_client_redial_leader(t_client *client) {
    if (!client || client->free_pending || !client->net_mode || !client->loop)
        return -1;
    char host[T_WIRE_MAX_NAME + 1];
    uint16_t port = 0;
    if (t_client_leader_hint(client, host, sizeof(host), &port) != 0)
        return -1;
    if (client->dial_host && client->dial_port == port &&
        strcmp(client->dial_host, host) == 0)
        return -1;
    client_drop_conn(client);
    client_clear_session(client);
    return t_client_dial(client, client->loop, host, port);
}

int t_client_wait_ack(t_client *client, unsigned prev_seq, int timeout_ms) {
    if (!client || timeout_ms < 0) return -1;
    int64_t start = t_time_now_ms();
    while (t_client_ack_seq(client) == prev_seq) {
        if (t_time_now_ms() - start >= (int64_t)timeout_ms) return -1;
        t_time_sleep_ms(5);
    }
    return 0;
}

int t_client_open_follow(t_client *client, const char *queue_name, int flags,
                         int timeout_ms) {
    if (!client || !queue_name || timeout_ms < 0) return -1;
    if (!client->connected) return -1;
    if (client_queue_ready(client, queue_name)) return 0;
    if (!client->net_mode)
        return t_client_open_queue(client, queue_name, flags);

    unsigned seq = t_client_ack_seq(client);
    if (t_client_open_queue(client, queue_name, flags) != 0) return -1;
    int wr = client_wait_status(client, seq, timeout_ms);
    if (wr <= 0) return wr;
    if (t_client_redial_leader(client) != 0) return -1;
    seq = t_client_ack_seq(client);
    if (t_client_open_queue(client, queue_name, flags) != 0) return -1;
    return client_wait_status(client, seq, timeout_ms) == 0 ? 0 : -1;
}

int t_client_join_follow(t_client *client, const char *group,
                         const char *consumer_id, const char *queue_name,
                         int timeout_ms) {
    if (!client || !group || !consumer_id || !queue_name || timeout_ms < 0)
        return -1;
    if (!client->net_mode) return -1;
    if (t_client_open_follow(client, queue_name, T_CLIENT_OPEN_CONSUMER,
                             timeout_ms) != 0)
        return -1;
    unsigned seq = t_client_ack_seq(client);
    if (t_client_join(client, group, consumer_id, queue_name) != 0) return -1;
    int wr = client_wait_status(client, seq, timeout_ms);
    if (wr <= 0) return wr;
    if (t_client_redial_leader(client) != 0) return -1;
    if (t_client_open_follow(client, queue_name, T_CLIENT_OPEN_CONSUMER,
                             timeout_ms) != 0)
        return -1;
    seq = t_client_ack_seq(client);
    if (t_client_join(client, group, consumer_id, queue_name) != 0) return -1;
    return client_wait_status(client, seq, timeout_ms) == 0 ? 0 : -1;
}

int t_client_post_follow(t_client *client, const char *queue_name,
                         const uint8_t *data, size_t len, int priority,
                         int timeout_ms) {
    if (!client || !queue_name || timeout_ms < 0) return -1;
    if (len > 0 && !data) return -1;
    if (!client->net_mode) {
        if (t_client_open_queue(client, queue_name, T_CLIENT_OPEN_PRODUCER) != 0)
            return -1;
        return t_client_post(client, queue_name, data, len, priority);
    }
    if (t_client_open_follow(client, queue_name, T_CLIENT_OPEN_PRODUCER,
                             timeout_ms) != 0)
        return -1;
    unsigned seq = t_client_ack_seq(client);
    if (t_client_post(client, queue_name, data, len, priority) != 0) return -1;
    int wr = client_wait_status(client, seq, timeout_ms);
    if (wr <= 0) return wr;
    if (t_client_redial_leader(client) != 0) return -1;
    if (t_client_open_follow(client, queue_name, T_CLIENT_OPEN_PRODUCER,
                             timeout_ms) != 0)
        return -1;
    seq = t_client_ack_seq(client);
    if (t_client_post(client, queue_name, data, len, priority) != 0) return -1;
    return client_wait_status(client, seq, timeout_ms) == 0 ? 0 : -1;
}

int t_client_disconnect(t_client *client) {
    if (!client || client->free_pending) return -1;
    client_hb_disarm(client);
    client_drop_conn(client);
    client->net_mode = 0;
    client->connected = 0;
    client_clear_session(client);
    free(client->dial_host);
    client->dial_host = NULL;
    client->dial_port = 0;
    return 0;
}

int t_client_open_queue(t_client *client, const char *queue_name, int flags) {
    if (!client || client->free_pending || !queue_name || !client->connected) return -1;
    for (size_t i = 0; i < client->queues_size; ++i) {
        if (strcmp(client->queues[i].name, queue_name) == 0) {
            if (client->queues[i].acked) return 0;
            client->queues[i].flags = flags;
            return client_send_open(client, queue_name, flags);
        }
    }
    if (client_ensure_queues_cap(client, client->queues_size + 1) != 0) return -1;
    char *qn = strdup(queue_name);
    if (!qn) return -1;
    client->queues[client->queues_size].name = qn;
    client->queues[client->queues_size].flags = flags;
    client->queues[client->queues_size].acked = 0;
    client->queues_size++;
    if (client_send_open(client, queue_name, flags) != 0) {
        free(qn);
        client->queues_size--;
        return -1;
    }
    return 0;
}

int t_client_close_queue(t_client *client, const char *queue_name) {
    if (!client || client->free_pending || !queue_name) return -1;
    for (size_t i = 0; i < client->queues_size; ++i) {
        if (strcmp(client->queues[i].name, queue_name) == 0) {
            free(client->queues[i].name);
            /* shift */
            for (size_t j = i; j + 1 < client->queues_size; ++j) {
                client->queues[j] = client->queues[j+1];
            }
            client->queues_size--;
            /* Drop all subscriptions for this queue. */
            (void)t_client_unsubscribe(client, queue_name);
            if (client->net_mode && client->conn) {
                uint8_t buf[2 + T_WIRE_MAX_NAME];
                int n = t_wire_encode_close(buf, sizeof(buf), queue_name);
                if (n > 0)
                    (void)client_send_payload(client, T_MSG_CLOSE_QUEUE, buf, (size_t)n);
            }
            return 0;
        }
    }
    return -1;
}

int t_client_post(t_client *client, const char *queue_name,
                  const uint8_t *data, size_t len, int priority) {
    (void)priority; /* priority currently unused in this stub */
    if (!client || client->free_pending || !queue_name || !client->connected) return -1;
    if (len > 0 && !data) return -1;
    if (len > T_QUEUE_MAX_PAYLOAD) return -1;
    int open = 0;
    for (size_t i = 0; i < client->queues_size; ++i) {
        if (strcmp(client->queues[i].name, queue_name) == 0) {
            open = 1;
            break;
        }
    }
    if (!open) return -1;
    if (client->net_mode) {
        size_t nlen = strlen(queue_name);
        if (nlen > T_WIRE_MAX_NAME) return -1;
        if (len > T_QUEUE_MAX_PAYLOAD) return -1;
        size_t need = 1u + 2u + nlen + 4u + len;
        uint8_t stack[512];
        uint8_t *buf = stack;
        int heap = 0;
        if (need > sizeof(stack)) {
            buf = (uint8_t *)malloc(need);
            if (!buf) return -1;
            heap = 1;
        }
        uint8_t pri = (priority < 0) ? 0 : (priority > 255 ? 255 : (uint8_t)priority);
        int n = t_wire_encode_post(buf, need, pri, queue_name, data, (uint32_t)len);
        int rc = -1;
        if (n > 0) rc = client_send_payload(client, T_MSG_POST, buf, (size_t)n);
        if (heap) free(buf);
        if (rc == 0) client->published++;
        return rc;
    }
    /* Snapshot (cb,ud) so unsubscribe/disconnect in a callback cannot skip peers. */
    size_t n = client->subs_count;
    t_cb_snap *snaps = NULL;
    size_t snap_n = 0;
    if (n > 0) {
        snaps = (t_cb_snap *)calloc(n, sizeof(*snaps));
        if (!snaps) return -1;
        for (size_t i = 0; i < n; ++i) {
            if (client->subs[i].queue && strcmp(client->subs[i].queue, queue_name) == 0 &&
                client->subs[i].cb) {
                snaps[snap_n].cb = client->subs[i].cb;
                snaps[snap_n].ud = client->subs[i].ud;
                snap_n++;
            }
        }
    }
    if (snap_n == 0) {
        free(snaps);
        return -1; /* no subscribers — do not silently drop */
    }
    client->posting++;
    client->published++;
    size_t delivered = 0;
    for (size_t i = 0; i < snap_n; ++i) {
        snaps[i].cb(queue_name, data, len, snaps[i].ud);
        delivered++;
    }
    client->consumed += delivered;
    client->posting--;
    free(snaps);
    if (client->posting == 0 && client->free_pending) {
        t_client_destroy(client);
        return -1; /* client freed; caller must not touch it */
    }
    return 0;
}

int t_client_join(t_client *client, const char *group,
                  const char *consumer_id, const char *queue_name) {
    if (!client || client->free_pending || !group || !consumer_id || !queue_name)
        return -1;
    if (!client->connected || !client->net_mode || !client->conn) return -1;
    uint8_t buf[6 + 3 * T_WIRE_MAX_NAME];
    int n = t_wire_encode_join(buf, sizeof(buf), group, consumer_id, queue_name);
    if (n < 0) return -1;
    return client_send_payload(client, T_MSG_JOIN, buf, (size_t)n);
}

int t_client_subscribe(t_client *client, const char *queue_name,
                       t_client_msg_cb cb, void *ud) {
    if (!client || client->free_pending || !queue_name || !cb || !client->connected) return -1;
    for (size_t i = 0; i < client->subs_count; ++i) {
        if (client->subs[i].queue && strcmp(client->subs[i].queue, queue_name) == 0 &&
            client->subs[i].cb == cb && client->subs[i].ud == ud) {
            return -1;
        }
    }
    if (client_ensure_subs_cap(client, client->subs_count + 1) != 0) return -1;
    char *qn = strdup(queue_name);
    if (!qn) return -1;
    client->subs[client->subs_count].queue = qn;
    client->subs[client->subs_count].cb = cb;
    client->subs[client->subs_count].ud = ud;
    client->subs_count++;
    if (client->net_mode) {
        uint8_t buf[3 + 2 + T_WIRE_MAX_NAME];
        int n = t_wire_encode_open(buf, sizeof(buf), T_QUEUE_FIFO, T_QUEUE_FLAG_NONE,
                                   T_CLIENT_OPEN_CONSUMER, queue_name);
        if (n < 0) {
            free(qn);
            client->subs_count--;
            return -1;
        }
        if (client_send_payload(client, T_MSG_OPEN_QUEUE, buf, (size_t)n) != 0) {
            free(qn);
            client->subs_count--;
            return -1;
        }
    }
    return 0;
}

int t_client_unsubscribe(t_client *client, const char *queue_name) {
    if (!client || client->free_pending || !queue_name) return -1;
    int removed = 0;
    for (size_t i = 0; i < client->subs_count; ) {
        if (client->subs[i].queue && strcmp(client->subs[i].queue, queue_name) == 0) {
            free(client->subs[i].queue);
            for (size_t j = i; j + 1 < client->subs_count; ++j) {
                client->subs[j] = client->subs[j+1];
            }
            client->subs_count--;
            removed++;
        } else {
            ++i;
        }
    }
    return removed ? 0 : -1;
}

size_t t_client_queue_count(const t_client *client) {
    return client ? client->queues_size : 0;
}

size_t t_client_total_published(const t_client *client) {
    return client ? client->published : 0;
}

size_t t_client_total_consumed(const t_client *client) {
    return client ? client->consumed : 0;
}
