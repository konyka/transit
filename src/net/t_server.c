#include "t_server.h"
#include "t_tcp.h"
#include "t_conn.h"
#include "t_socket.h"
#include "t_session.h"
#include "t_ratelimit.h"
#include "t_wire.h"
#include "t_proto.h"
#include "t_error.h"
#include "t_map.h"
#include "t_vec.h"
#include "t_domain.h"
#include "t_queue.h"
#include "t_time.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <limits.h>

typedef struct t_server_conn {
    t_server     *srv;
    t_conn       *conn;
    t_session    *sess;
    t_ratelimit  *rl;
    t_map         opened; /* name -> (void*)(uintptr_t)mode */
    uint64_t      next_push_id;
    int           in_cb;
    int           freed;
    int           free_pending;
} t_server_conn;

struct t_server {
    t_evloop        *loop;
    t_broker        *broker;
    t_tcp_server    *tcp;
    char            *host;
    uint16_t         port;
    uint16_t         bound_port;
    size_t           max_conns;
    size_t           rate_tokens;
    double           rate_refill;
    int64_t          idle_timeout_ms;
    int64_t          idle_timer_id;
    t_vec            conns;
    t_map            open_refs; /* queue name -> open count */
    size_t           dropped_conns;
    size_t           msgs_in;
    size_t           msgs_dropped;
    int              running;
    int              stopping;
    int              in_cb;
    int              free_pending;
};

static void server_on_accept(t_tcp_server *tcp, int client_fd, t_sockaddr *peer, void *ud);
static void server_on_msg(t_conn *conn, const t_proto_msg *msg, void *ud);
static void server_on_close(t_conn *conn, void *ud);
static void server_conn_free(t_server_conn *sc);
static void server_idle_tick(void *ud);

void t_server_config_init(t_server_config *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->host = "127.0.0.1";
    cfg->port = 4222;
    cfg->max_conns = 1024;
    cfg->rate_tokens = 128;
    cfg->rate_refill = 0.05; /* 50 tokens/sec */
    cfg->idle_timeout_ms = 30000;
}

static int send_frame(t_conn *conn, t_msg_type type, const uint8_t *payload, size_t plen) {
    if (!conn) return -1;
    t_proto_msg msg;
    t_proto_header_init(&msg.header, type, (uint32_t)plen);
    msg.payload = (uint8_t *)payload;
    msg.payload_len = plen;
    return t_conn_send(conn, &msg);
}

static int send_ack(t_server_conn *sc, uint16_t req_type, int32_t status, const char *name) {
    uint8_t buf[2 + 4 + 2 + T_WIRE_MAX_NAME];
    int n = t_wire_encode_ack(buf, sizeof(buf), req_type, status, name);
    if (n < 0) return -1;
    t_session_record_send(sc->sess);
    return send_frame(sc->conn, T_MSG_ACK, buf, (size_t)n);
}

static void server_push_cb(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    t_server_conn *sc = (t_server_conn *)ud;
    if (!sc || sc->freed || !sc->conn || !queue_name) return;
    if (len > T_PROTO_MAX_PAYLOAD) return;
    size_t nlen = strlen(queue_name);
    if (nlen > T_WIRE_MAX_NAME) return;
    if (len > SIZE_MAX - (8u + 1u + 2u + nlen + 4u)) return;
    size_t need = 8u + 1u + 2u + nlen + 4u + len;
    uint8_t stack[512];
    uint8_t *buf = stack;
    int heap = 0;
    if (need > sizeof(stack)) {
        buf = (uint8_t *)malloc(need);
        if (!buf) return;
        heap = 1;
    }
    if (sc->next_push_id == UINT64_MAX) sc->next_push_id = 0;
    uint64_t id = ++sc->next_push_id;
    int n = t_wire_encode_push(buf, need, id, 0, queue_name, data, (uint32_t)len);
    if (n > 0) {
        (void)send_frame(sc->conn, T_MSG_PUSH, buf, (size_t)n);
        t_session_record_send(sc->sess);
    }
    if (heap) free(buf);
}

static t_queue *server_lookup_queue(t_broker *b, const char *name) {
    t_domain *d = t_broker_get_domain(b, "default");
    if (!d) return NULL;
    return (t_queue *)t_domain_get_queue(d, name);
}

static void server_unref_open(t_server *srv, const char *name) {
    if (!srv || !name) return;
    uintptr_t n = (uintptr_t)t_map_get(&srv->open_refs, name);
    if (n == 0) return;
    n--;
    if (n > 0) {
        (void)t_map_insert(&srv->open_refs, name, (void *)n);
        return;
    }
    (void)t_map_remove(&srv->open_refs, name);
    t_queue *q = server_lookup_queue(srv->broker, name);
    if (q && (t_queue_get_flags(q) & T_QUEUE_FLAG_AUTODELETE))
        (void)t_broker_delete_queue(srv->broker, "default", name);
}

static void server_unsub_all(t_server_conn *sc) {
    if (!sc || !sc->srv || !sc->srv->broker) return;
    t_map_iter it = t_map_iter_begin(&sc->opened);
    const char *k;
    void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        uintptr_t mode = (uintptr_t)v;
        if (mode & T_WIRE_MODE_CONSUMER)
            (void)t_broker_unsubscribe(sc->srv->broker, k, server_push_cb, sc);
        server_unref_open(sc->srv, k);
    }
}

static void server_conn_detach(t_server *srv, t_server_conn *sc) {
    if (!srv || !sc) return;
    for (size_t i = 0; i < srv->conns.len; i++) {
        if (srv->conns.items[i] == sc) {
            (void)t_vec_remove(&srv->conns, i);
            return;
        }
    }
}

static void server_conn_free(t_server_conn *sc) {
    if (!sc || sc->freed) return;
    if (sc->in_cb > 0) {
        sc->free_pending = 1;
        return;
    }
    sc->freed = 1;
    server_unsub_all(sc);
    if (sc->srv) server_conn_detach(sc->srv, sc);
    t_map_destroy(&sc->opened);
    if (sc->sess) {
        (void)t_session_disconnect(sc->sess);
        (void)t_session_destroy(sc->sess);
        sc->sess = NULL;
    }
    t_ratelimit_destroy(sc->rl);
    sc->rl = NULL;
    sc->conn = NULL;
    free(sc);
}

static void server_on_close(t_conn *conn, void *ud) {
    t_server_conn *sc = (t_server_conn *)ud;
    if (!sc) return;
    int owned = (sc->conn == conn);
    sc->conn = NULL;
    server_conn_free(sc);
    if (owned) t_conn_destroy(conn);
}

static int queue_exists(t_broker *broker, const char *name) {
    t_domain *d = t_broker_get_domain(broker, "default");
    if (d && t_domain_get_queue(d, name)) return 1;
    return 0;
}

static int32_t handle_open(t_server_conn *sc, const t_proto_msg *msg) {
    t_wire_open o;
    if (t_wire_decode_open(msg->payload, msg->payload_len, &o) != 0)
        return T_ERR_PROTO;
    char name[T_WIRE_MAX_NAME + 1];
    if (t_wire_name_copy(name, sizeof(name), o.name, o.name_len) != 0)
        return T_ERR_INVALID;
    if ((o.mode & (T_WIRE_MODE_PRODUCER | T_WIRE_MODE_CONSUMER)) == 0)
        return T_ERR_INVALID;
    if (o.qtype > (uint8_t)T_QUEUE_BROADCAST)
        return T_ERR_INVALID;
    t_broker *b = sc->srv->broker;
    if ((o.mode & T_WIRE_MODE_CONSUMER) && !t_broker_is_running(b))
        return T_ERR_CLOSED;
    if ((o.qflags & T_QUEUE_FLAG_DURABLE) && o.qtype == (uint8_t)T_QUEUE_BROADCAST)
        return T_ERR_INVALID;
    if (!queue_exists(b, name)) {
        if (t_broker_create_queue(b, "default", name, (int)o.qtype, (int)o.qflags) != 0) {
            if (o.qflags & T_QUEUE_FLAG_DURABLE) return T_ERR_IO;
            return T_ERR_GENERIC;
        }
    }
    uintptr_t prev = (uintptr_t)t_map_get(&sc->opened, name);
    uintptr_t mode = prev | (uintptr_t)o.mode;
    if ((mode & T_WIRE_MODE_CONSUMER) && !(prev & T_WIRE_MODE_CONSUMER)) {
        t_queue *q = server_lookup_queue(b, name);
        if (q && (t_queue_get_flags(q) & T_QUEUE_FLAG_EXCLUSIVE) &&
            t_queue_consumer_count(q) > 0)
            return T_ERR_BUSY;
    }
    if (t_map_insert(&sc->opened, name, (void *)mode) != 0)
        return T_ERR_NOMEM;
    if (prev == 0) {
        uintptr_t refs = (uintptr_t)t_map_get(&sc->srv->open_refs, name);
        if (t_map_insert(&sc->srv->open_refs, name, (void *)(refs + 1)) != 0) {
            (void)t_map_remove(&sc->opened, name);
            return T_ERR_NOMEM;
        }
    }
    if ((mode & T_WIRE_MODE_CONSUMER) && !(prev & T_WIRE_MODE_CONSUMER)) {
        if (t_broker_subscribe(b, name, server_push_cb, sc) != 0) {
            if (prev == 0) {
                (void)t_map_remove(&sc->opened, name);
                server_unref_open(sc->srv, name);
            } else {
                (void)t_map_insert(&sc->opened, name, (void *)prev);
            }
            t_queue *q = server_lookup_queue(b, name);
            if (q && (t_queue_get_flags(q) & T_QUEUE_FLAG_EXCLUSIVE))
                return T_ERR_BUSY;
            return T_ERR_GENERIC;
        }
    }
    if (send_ack(sc, T_MSG_OPEN_QUEUE, T_OK_CODE, name) != 0)
        return T_ERR_IO;
    return T_OK_CODE;
}

static int32_t handle_close(t_server_conn *sc, const t_proto_msg *msg) {
    t_wire_close c;
    if (t_wire_decode_close(msg->payload, msg->payload_len, &c) != 0)
        return T_ERR_PROTO;
    char name[T_WIRE_MAX_NAME + 1];
    if (t_wire_name_copy(name, sizeof(name), c.name, c.name_len) != 0)
        return T_ERR_INVALID;
    void *v = t_map_remove(&sc->opened, name);
    if (!v) return T_ERR_NOTFOUND;
    uintptr_t mode = (uintptr_t)v;
    if (mode & T_WIRE_MODE_CONSUMER)
        (void)t_broker_unsubscribe(sc->srv->broker, name, server_push_cb, sc);
    server_unref_open(sc->srv, name);
    if (send_ack(sc, T_MSG_CLOSE_QUEUE, T_OK_CODE, name) != 0)
        return T_ERR_IO;
    return T_OK_CODE;
}

static int32_t handle_post(t_server_conn *sc, const t_proto_msg *msg) {
    t_wire_post p;
    if (t_wire_decode_post(msg->payload, msg->payload_len, &p) != 0)
        return T_ERR_PROTO;
    char name[T_WIRE_MAX_NAME + 1];
    if (t_wire_name_copy(name, sizeof(name), p.name, p.name_len) != 0)
        return T_ERR_INVALID;
    uintptr_t mode = (uintptr_t)t_map_get(&sc->opened, name);
    if ((mode & T_WIRE_MODE_PRODUCER) == 0)
        return T_ERR_PERMISSION;
    if (!t_broker_is_running(sc->srv->broker))
        return T_ERR_CLOSED;
    if (!t_broker_is_leader(sc->srv->broker))
        return T_ERR_AGAIN;
    if (t_broker_publish(sc->srv->broker, name, p.data, p.data_len, (int)p.priority) != 0)
        return T_ERR_GENERIC;
    if (send_ack(sc, T_MSG_POST, T_OK_CODE, name) != 0)
        return T_ERR_IO;
    return T_OK_CODE;
}

static int32_t handle_confirm(t_server_conn *sc, const t_proto_msg *msg) {
    t_wire_confirm c;
    if (t_wire_decode_confirm(msg->payload, msg->payload_len, &c) != 0)
        return T_ERR_PROTO;
    char name[T_WIRE_MAX_NAME + 1];
    if (t_wire_name_copy(name, sizeof(name), c.name, c.name_len) != 0)
        return T_ERR_INVALID;
    if (!t_map_contains(&sc->opened, name))
        return T_ERR_PERMISSION;
    /* Push delivery is fire-and-forget; accept confirm as client liveness. */
    if (send_ack(sc, (uint16_t)msg->header.type, T_OK_CODE, name) != 0)
        return T_ERR_IO;
    return T_OK_CODE;
}

static void server_on_msg(t_conn *conn, const t_proto_msg *msg, void *ud) {
    t_server_conn *sc = (t_server_conn *)ud;
    if (!sc || sc->freed || !msg) return;
    sc->in_cb++;
    sc->srv->msgs_in++;
    t_session_update_activity(sc->sess);
    t_session_record_recv(sc->sess);

    int close_conn = 0;
    int32_t status = T_OK_CODE;
    uint64_t now_ms = (uint64_t)t_time_now_ms();
    if (!t_ratelimit_allow(sc->rl, now_ms)) {
        sc->srv->msgs_dropped++;
        (void)send_ack(sc, msg->header.type, (int32_t)T_ERR_BUSY, NULL);
        sc->in_cb--;
        if (sc->free_pending) server_conn_free(sc);
        return;
    }

    switch (msg->header.type) {
    case T_MSG_NOP:
    case T_MSG_HEARTBEAT:
        (void)send_ack(sc, msg->header.type, T_OK_CODE, NULL);
        break;
    case T_MSG_OPEN_QUEUE:
        status = handle_open(sc, msg);
        break;
    case T_MSG_CLOSE_QUEUE:
        status = handle_close(sc, msg);
        break;
    case T_MSG_POST:
        status = handle_post(sc, msg);
        break;
    case T_MSG_CONFIRM:
    case T_MSG_REJECT:
        status = handle_confirm(sc, msg);
        break;
    default:
        close_conn = 1;
        break;
    }

    if (!close_conn && status != T_OK_CODE) {
        const char *nm = NULL;
        char namebuf[T_WIRE_MAX_NAME + 1];
        if (msg->header.type == T_MSG_OPEN_QUEUE) {
            t_wire_open o;
            if (t_wire_decode_open(msg->payload, msg->payload_len, &o) == 0 &&
                t_wire_name_copy(namebuf, sizeof(namebuf), o.name, o.name_len) == 0)
                nm = namebuf;
        } else if (msg->header.type == T_MSG_POST) {
            t_wire_post p;
            if (status == T_ERR_AGAIN) {
                t_cluster *cl = t_broker_cluster(sc->srv->broker);
                t_node *ld = cl ? t_cluster_get_leader(cl) : NULL;
                if (ld && t_node_host(ld)) {
                    int hn = snprintf(namebuf, sizeof(namebuf), "%s_%u",
                                      t_node_host(ld), (unsigned)t_node_port(ld));
                    if (hn > 0 && (size_t)hn < sizeof(namebuf) &&
                        t_wire_name_valid(namebuf, (size_t)hn))
                        nm = namebuf;
                }
            }
            if (!nm && t_wire_decode_post(msg->payload, msg->payload_len, &p) == 0 &&
                t_wire_name_copy(namebuf, sizeof(namebuf), p.name, p.name_len) == 0)
                nm = namebuf;
        }
        (void)send_ack(sc, msg->header.type, status, nm);
    }
    if (status == T_ERR_PROTO) close_conn = 1;

    sc->in_cb--;
    if (close_conn && sc->conn) {
        t_conn_destroy(conn);
        return;
    }
    if (sc->free_pending) server_conn_free(sc);
}

static void server_on_accept(t_tcp_server *tcp, int client_fd, t_sockaddr *peer, void *ud) {
    (void)tcp;
    (void)peer;
    t_server *srv = (t_server *)ud;
    if (!srv || client_fd < 0) {
        if (client_fd >= 0) t_socket_close(client_fd);
        return;
    }
    srv->in_cb++;
    if (srv->stopping || srv->conns.len >= srv->max_conns) {
        t_socket_close(client_fd);
        srv->dropped_conns++;
        srv->in_cb--;
        return;
    }
    t_server_conn *sc = (t_server_conn *)calloc(1, sizeof(*sc));
    if (!sc) {
        t_socket_close(client_fd);
        srv->in_cb--;
        return;
    }
    sc->srv = srv;
    t_map_init(&sc->opened);
    sc->sess = t_session_create((uint64_t)(srv->conns.len + 1));
    sc->rl = t_ratelimit_create(srv->rate_tokens, srv->rate_refill);
    sc->conn = t_conn_create(client_fd, srv->loop);
    if (!sc->sess || !sc->rl || !sc->conn) {
        if (sc->conn) t_conn_destroy(sc->conn);
        else t_socket_close(client_fd);
        if (sc->sess) t_session_destroy(sc->sess);
        t_ratelimit_destroy(sc->rl);
        t_map_destroy(&sc->opened);
        free(sc);
        srv->in_cb--;
        return;
    }
    (void)t_session_connect(sc->sess);
    t_conn_set_on_msg(sc->conn, server_on_msg, sc);
    t_conn_set_on_close(sc->conn, server_on_close, sc);
    if (t_vec_push(&srv->conns, sc) != 0) {
        t_conn_destroy(sc->conn);
        srv->in_cb--;
        return;
    }
    srv->in_cb--;
    if (srv->free_pending) t_server_destroy(srv);
}

static void server_idle_tick(void *ud) {
    t_server *srv = (t_server *)ud;
    if (!srv || srv->stopping || srv->idle_timeout_ms <= 0) return;
    int64_t ns = srv->idle_timeout_ms;
    if (ns > INT64_MAX / 1000000LL) ns = INT64_MAX;
    else ns *= 1000000LL;
    for (size_t i = 0; i < srv->conns.len; ) {
        t_server_conn *sc = (t_server_conn *)srv->conns.items[i];
        if (sc && sc->conn && t_session_check_timeout(sc->sess, ns) == 1) {
            t_conn_destroy(sc->conn);
            continue;
        }
        i++;
    }
}

t_server *t_server_create(t_evloop *loop, t_broker *broker, const t_server_config *cfg) {
    if (!loop || !broker) return NULL;
    t_server_config def;
    t_server_config_init(&def);
    if (!cfg) cfg = &def;
    t_server *srv = (t_server *)calloc(1, sizeof(*srv));
    if (!srv) return NULL;
    const char *host = cfg->host;
    if (!host || !host[0]) host = "127.0.0.1";
    srv->host = strdup(host);
    if (!srv->host) {
        free(srv);
        return NULL;
    }
    srv->loop = loop;
    srv->broker = broker;
    srv->port = cfg->port;
    srv->max_conns = cfg->max_conns ? cfg->max_conns : 1;
    srv->rate_tokens = cfg->rate_tokens;
    srv->rate_refill = cfg->rate_refill;
    srv->idle_timeout_ms = cfg->idle_timeout_ms < 0 ? 0 : cfg->idle_timeout_ms;
    srv->idle_timer_id = -1;
    t_vec_init(&srv->conns);
    t_map_init(&srv->open_refs);
    srv->tcp = t_tcp_server_create(loop);
    if (!srv->tcp) {
        t_vec_destroy(&srv->conns);
        t_map_destroy(&srv->open_refs);
        free(srv->host);
        free(srv);
        return NULL;
    }
    return srv;
}

void t_server_destroy(t_server *srv) {
    if (!srv) return;
    if (srv->in_cb > 0) {
        srv->free_pending = 1;
        return;
    }
    srv->free_pending = 0;
    t_server_stop(srv);
    t_tcp_server_destroy(srv->tcp);
    t_vec_destroy(&srv->conns);
    t_map_destroy(&srv->open_refs);
    free(srv->host);
    free(srv);
}

int t_server_start(t_server *srv) {
    if (!srv || srv->running || !srv->tcp) return -1;
    if (t_tcp_server_listen(srv->tcp, srv->host, srv->port, server_on_accept, srv) != 0)
        return -1;
    srv->bound_port = t_socket_local_port(srv->tcp->fd);
    if (srv->idle_timeout_ms > 0) {
        int64_t tick = srv->idle_timeout_ms < 1000 ? srv->idle_timeout_ms : 1000;
        if (tick < 10) tick = 10;
        srv->idle_timer_id = t_evloop_timer_add(srv->loop, tick, 1, server_idle_tick, srv);
        if (srv->idle_timer_id < 0) {
            t_tcp_server_destroy(srv->tcp);
            srv->tcp = t_tcp_server_create(srv->loop);
            srv->bound_port = 0;
            return -1;
        }
    }
    srv->running = 1;
    srv->stopping = 0;
    return 0;
}

void t_server_stop(t_server *srv) {
    if (!srv || !srv->running) {
        if (srv) srv->stopping = 1;
        return;
    }
    srv->stopping = 1;
    srv->running = 0;
    if (srv->idle_timer_id >= 0) {
        t_evloop_timer_del(srv->loop, srv->idle_timer_id);
        srv->idle_timer_id = -1;
    }
    while (srv->conns.len > 0) {
        t_server_conn *sc = (t_server_conn *)srv->conns.items[0];
        if (sc && sc->conn) {
            t_conn *cn = sc->conn;
            sc->conn = NULL;
            t_conn_destroy(cn);
        } else {
            (void)t_vec_remove(&srv->conns, 0);
            if (sc) server_conn_free(sc);
        }
    }
    if (srv->tcp) {
        t_tcp_server_destroy(srv->tcp);
        srv->tcp = t_tcp_server_create(srv->loop);
    }
    srv->bound_port = 0;
}

int t_server_is_running(const t_server *srv) {
    return srv ? srv->running != 0 : 0;
}

uint16_t t_server_port(const t_server *srv) {
    return srv ? srv->bound_port : 0;
}

const char *t_server_host(const t_server *srv) {
    return srv ? srv->host : NULL;
}

size_t t_server_conn_count(const t_server *srv) {
    return srv ? srv->conns.len : 0;
}

size_t t_server_dropped_conns(const t_server *srv) {
    return srv ? srv->dropped_conns : 0;
}

size_t t_server_msgs_in(const t_server *srv) {
    return srv ? srv->msgs_in : 0;
}

size_t t_server_msgs_dropped(const t_server *srv) {
    return srv ? srv->msgs_dropped : 0;
}
