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

/* Minimal in-process client implementation with queue registry and subscriptions. */
typedef struct t_client_queue_entry {
    char *name;
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
    int       last_status;
};

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

static int client_send_payload(t_client *c, t_msg_type type, const uint8_t *payload, size_t plen) {
    if (!c || !c->conn) return -1;
    t_proto_msg msg;
    t_proto_header_init(&msg.header, type, (uint32_t)plen);
    msg.payload = (uint8_t *)payload;
    msg.payload_len = plen;
    return t_conn_send(c->conn, &msg);
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
        if (t_wire_decode_ack(msg->payload, msg->payload_len, &a) == 0)
            c->last_status = a.status;
        return;
    }
    if (msg->header.type != T_MSG_PUSH) return;
    t_wire_push p;
    if (t_wire_decode_push(msg->payload, msg->payload_len, &p) != 0) return;
    char name[T_WIRE_MAX_NAME + 1];
    if (t_wire_name_copy(name, sizeof(name), p.name, p.name_len) != 0) return;
    size_t n = c->subs_count;
    typedef struct { t_client_msg_cb cb; void *ud; } t_push_snap;
    t_push_snap *snaps = NULL;
    size_t snap_n = 0;
    if (n > 0) {
        snaps = (t_push_snap *)calloc(n, sizeof(*snaps));
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
    c->last_status = 0;
    return c;
}

void t_client_destroy(t_client *client) {
    if (!client) return;
    if (client->posting > 0) {
        client->free_pending = 1;
        return;
    }
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
    client->last_status = 0;
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
    client->connected = 1;
    client->last_status = 0;
    t_conn_set_on_msg(conn, client_on_msg, client);
    t_conn_set_on_close(conn, client_on_close, client);
    return 0;
}

int t_client_last_status(const t_client *client) {
    return client ? client->last_status : (int)T_ERR_INVALID;
}

int t_client_disconnect(t_client *client) {
    if (!client || client->free_pending) return -1;
    client_drop_conn(client);
    client->net_mode = 0;
    client->connected = 0;
    /* Drop all subscriptions so reconnect cannot revive stale callbacks. */
    for (size_t i = 0; i < client->subs_count; ++i) {
        free(client->subs[i].queue);
    }
    free(client->subs);
    client->subs = NULL;
    client->subs_count = 0;
    client->subs_cap = 0;
    /* Drop opened queues; reconnect must open_queue again. */
    for (size_t i = 0; i < client->queues_size; ++i) {
        free(client->queues[i].name);
    }
    free(client->queues);
    client->queues = NULL;
    client->queues_size = 0;
    client->queues_cap = 0;
    return 0;
}

int t_client_open_queue(t_client *client, const char *queue_name, int flags) {
    if (!client || client->free_pending || !queue_name || !client->connected) return -1;
    /* ensure exists */
    for (size_t i = 0; i < client->queues_size; ++i) {
        if (strcmp(client->queues[i].name, queue_name) == 0) return 0;
    }
    if (client_ensure_queues_cap(client, client->queues_size + 1) != 0) return -1;
    char *qn = strdup(queue_name);
    if (!qn) return -1;
    client->queues[client->queues_size].name = qn;
    client->queues_size++;
    if (client->net_mode) {
        uint8_t mode = (uint8_t)(flags & 0xFF);
        uint8_t qflags = (uint8_t)((flags >> 8) & 0xFF);
        if (mode == 0) mode = T_CLIENT_OPEN_PRODUCER;
        uint8_t buf[3 + 2 + T_WIRE_MAX_NAME];
        int n = t_wire_encode_open(buf, sizeof(buf), T_QUEUE_FIFO, qflags,
                                   mode, queue_name);
        if (n < 0) {
            free(qn);
            client->queues_size--;
            return -1;
        }
        if (client_send_payload(client, T_MSG_OPEN_QUEUE, buf, (size_t)n) != 0) {
            free(qn);
            client->queues_size--;
            return -1;
        }
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
    typedef struct { t_client_msg_cb cb; void *ud; } t_post_snap;
    t_post_snap *snaps = NULL;
    size_t snap_n = 0;
    if (n > 0) {
        snaps = (t_post_snap *)calloc(n, sizeof(*snaps));
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

int t_client_subscribe(t_client *client, const char *queue_name,
                       t_client_msg_cb cb, void *ud) {
    if (!client || client->free_pending || !queue_name || !cb || !client->connected) return -1;
    int open = 0;
    for (size_t i = 0; i < client->queues_size; ++i) {
        if (client->queues[i].name && strcmp(client->queues[i].name, queue_name) == 0) {
            open = 1;
            break;
        }
    }
    if (!open) return -1;
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
