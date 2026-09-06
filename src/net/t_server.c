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
#include "t_cgroup.h"
#include "t_time.h"
#include "t_hmac.h"
#include "t_flowcontrol.h"
#include "t_raft.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <limits.h>

typedef struct t_push_inf {
    char    *name;
    uint64_t id;
} t_push_inf;

typedef struct t_server_conn t_server_conn;

typedef struct t_server_wait {
    t_server_conn *sc;
    uint16_t       type;
    uint64_t       index;
    uint8_t        mode;
    uint8_t        credit;
    int64_t        expire_at_ms;
    char           name[T_WIRE_MAX_NAME + 1];
    char           group[T_WIRE_MAX_NAME + 1];
    char           consumer[T_WIRE_MAX_NAME + 1];
} t_server_wait;

struct t_server_conn {
    t_server     *srv;
    t_conn       *conn;
    t_session    *sess;
    t_ratelimit  *rl;
    t_map         opened; /* name -> (void*)(uintptr_t)mode */
    t_map         joined; /* queue name -> malloc'd consumer id */
    t_vec         inflight; /* t_push_inf*: pull PUSHes awaiting CONFIRM */
    uint64_t      next_push_id;
    int           in_cb;
    int           freed;
    int           free_pending;
    int           authed;
    t_flowcontrol *fc;
};

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
    int64_t          wait_timer_id;
    t_vec            conns;
    t_vec            waits;     /* t_server_wait*: Raft apply then ACK */
    t_map            open_refs; /* queue name -> open count */
    t_map            groups;    /* queue name -> t_cgroup* */
    size_t           dropped_conns;
    size_t           msgs_in;
    size_t           msgs_dropped;
    int              running;
    int              stopping;
    int              in_cb;
    int              free_pending;
    uint8_t         *psk;
    size_t           psk_len;
    size_t           push_credits;
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
    cfg->push_credits = T_SERVER_PUSH_CREDITS_DEFAULT;
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

static uint64_t server_raft_index(t_broker *b) {
    t_raft *r = t_broker_raft(b);
    return r ? t_raft_last_log_index(r) : 0;
}

static int32_t finish_open(t_server_conn *sc, const char *name, uint8_t omode);
static int32_t finish_join(t_server_conn *sc, const char *queue,
                           const char *group, const char *consumer);
static void server_flush_pull(t_server *srv, const char *name);
static void server_wait_tick(void *ud);
static int server_restore_group(t_server *srv, const char *name);

static int64_t server_wait_ttl_ms(t_broker *b) {
    t_raft *r = b ? t_broker_raft(b) : NULL;
    uint64_t to = r ? t_raft_election_timeout_ms(r) : 150;
    if (to == 0) to = 150;
    if (to > (uint64_t)INT64_MAX) to = (uint64_t)INT64_MAX;
    return (int64_t)to;
}

static void server_wait_disarm(t_server *srv) {
    if (!srv || srv->wait_timer_id < 0) return;
    if (srv->loop) t_evloop_timer_del(srv->loop, srv->wait_timer_id);
    srv->wait_timer_id = -1;
}

static void server_wait_arm(t_server *srv) {
    if (!srv || !srv->loop || srv->wait_timer_id >= 0) return;
    int64_t tick = server_wait_ttl_ms(srv->broker) / 4;
    if (tick < 10) tick = 10;
    if (tick > 50) tick = 50;
    srv->wait_timer_id = t_evloop_timer_add(srv->loop, tick, 1, server_wait_tick, srv);
}

static void server_wait_ack_ok(t_server *srv, t_server_wait *w) {
    t_server_conn *sc = w->sc;
    if (!sc || sc->freed || !sc->conn) return;
    if (w->type == T_MSG_OPEN_QUEUE) {
        int32_t st = finish_open(sc, w->name, w->mode);
        if (st == T_OK_CODE)
            (void)send_ack(sc, T_MSG_OPEN_QUEUE, T_OK_CODE, w->name);
        else
            (void)send_ack(sc, T_MSG_OPEN_QUEUE, st, w->name);
        return;
    }
    if (w->type == T_MSG_JOIN) {
        int32_t st = finish_join(sc, w->name, w->group, w->consumer);
        (void)send_ack(sc, T_MSG_JOIN, st, w->name);
        if (st == T_OK_CODE)
            server_flush_pull(srv, w->name);
        return;
    }
    if (w->credit && sc->fc) t_fc_release(sc->fc, 1);
    (void)send_ack(sc, w->type, T_OK_CODE, w->name);
    if (w->type == T_MSG_POST || w->type == T_MSG_REJECT)
        server_flush_pull(srv, w->name);
}

static void server_wait_ack_again(t_server_wait *w) {
    t_server_conn *sc = w->sc;
    if (!sc || sc->freed || !sc->conn) return;
    if (w->credit && sc->fc) t_fc_release(sc->fc, 1);
    (void)send_ack(sc, w->type, (int32_t)T_ERR_AGAIN, w->name);
}

static void server_wait_flush(t_server *srv, uint64_t applied_hint) {
    if (!srv) return;
    uint64_t applied = applied_hint;
    t_raft *r = t_broker_raft(srv->broker);
    if (r) {
        uint64_t la = t_raft_last_applied(r);
        if (la > applied) applied = la;
    }
    int leader = t_broker_is_leader(srv->broker);
    int64_t now = t_time_now_ms();
    for (size_t i = 0; i < srv->waits.len; ) {
        t_server_wait *w = (t_server_wait *)srv->waits.items[i];
        if (!w) {
            (void)t_vec_remove(&srv->waits, i);
            continue;
        }
        if (w->index <= applied) {
            (void)t_vec_remove(&srv->waits, i);
            server_wait_ack_ok(srv, w);
            free(w);
            continue;
        }
        if (!leader || now >= w->expire_at_ms) {
            (void)t_vec_remove(&srv->waits, i);
            server_wait_ack_again(w);
            free(w);
            continue;
        }
        i++;
    }
    if (srv->waits.len == 0) server_wait_disarm(srv);
}

static void server_wait_tick(void *ud) {
    t_server *srv = (t_server *)ud;
    if (!srv || srv->stopping) return;
    server_wait_flush(srv, 0);
}

static int server_wait_add(t_server_conn *sc, uint16_t type, const char *name,
                           uint64_t index, uint8_t mode, uint8_t credit,
                           const char *group, const char *consumer) {
    if (!sc || !sc->srv || !name || index == 0) return -1;
    t_server_wait *w = (t_server_wait *)calloc(1, sizeof(*w));
    if (!w) return -1;
    w->sc = sc;
    w->type = type;
    w->index = index;
    w->mode = mode;
    w->credit = credit;
    w->expire_at_ms = t_time_now_ms() + server_wait_ttl_ms(sc->srv->broker);
    size_t nlen = strlen(name);
    if (nlen > T_WIRE_MAX_NAME) {
        free(w);
        return -1;
    }
    memcpy(w->name, name, nlen);
    w->name[nlen] = '\0';
    if (group) {
        size_t glen = strlen(group);
        if (glen > T_WIRE_MAX_NAME) {
            free(w);
            return -1;
        }
        memcpy(w->group, group, glen);
        w->group[glen] = '\0';
    }
    if (consumer) {
        size_t clen = strlen(consumer);
        if (clen > T_WIRE_MAX_NAME) {
            free(w);
            return -1;
        }
        memcpy(w->consumer, consumer, clen);
        w->consumer[clen] = '\0';
    }
    if (t_vec_push(&sc->srv->waits, w) != 0) {
        free(w);
        return -1;
    }
    if (sc->srv->wait_timer_id < 0) {
        server_wait_arm(sc->srv);
        if (sc->srv->wait_timer_id < 0) {
            (void)t_vec_pop(&sc->srv->waits);
            free(w);
            return -1;
        }
    }
    return 0;
}

static void server_wait_drop_conn(t_server *srv, t_server_conn *sc) {
    if (!srv) return;
    for (size_t i = 0; i < srv->waits.len; ) {
        t_server_wait *w = (t_server_wait *)srv->waits.items[i];
        if (w && w->sc == sc) {
            (void)t_vec_remove(&srv->waits, i);
            free(w);
        } else {
            i++;
        }
    }
    if (srv->waits.len == 0) server_wait_disarm(srv);
}

static int server_send_push(t_server_conn *sc, const char *queue_name, uint64_t msg_id,
                            uint8_t priority, const uint8_t *data, size_t len, int track) {
    if (!sc || sc->freed || !sc->conn || !queue_name) return -1;
    if (len > T_PROTO_MAX_PAYLOAD) return -1;
    if (sc->fc && t_fc_acquire(sc->fc, 1) != 0) return -1;
    size_t nlen = strlen(queue_name);
    if (nlen > T_WIRE_MAX_NAME) {
        if (sc->fc) t_fc_release(sc->fc, 1);
        return -1;
    }
    if (len > SIZE_MAX - (8u + 1u + 2u + nlen + 4u)) {
        if (sc->fc) t_fc_release(sc->fc, 1);
        return -1;
    }
    size_t need = 8u + 1u + 2u + nlen + 4u + len;
    uint8_t stack[512];
    uint8_t *buf = stack;
    int heap = 0;
    if (need > sizeof(stack)) {
        buf = (uint8_t *)malloc(need);
        if (!buf) {
            if (sc->fc) t_fc_release(sc->fc, 1);
            return -1;
        }
        heap = 1;
    }
    t_push_inf *inf = NULL;
    if (track) {
        inf = (t_push_inf *)malloc(sizeof(*inf));
        if (!inf) {
            if (heap) free(buf);
            if (sc->fc) t_fc_release(sc->fc, 1);
            return -1;
        }
        inf->name = strdup(queue_name);
        inf->id = msg_id;
        if (!inf->name || t_vec_push(&sc->inflight, inf) != 0) {
            free(inf->name);
            free(inf);
            if (heap) free(buf);
            if (sc->fc) t_fc_release(sc->fc, 1);
            return -1;
        }
    }
    int n = t_wire_encode_push(buf, need, msg_id, priority, queue_name, data, (uint32_t)len);
    int rc = -1;
    if (n > 0 && send_frame(sc->conn, T_MSG_PUSH, buf, (size_t)n) == 0) {
        t_session_record_send(sc->sess);
        rc = 0;
    }
    if (rc != 0) {
        if (inf) {
            (void)t_vec_pop(&sc->inflight);
            free(inf->name);
            free(inf);
        }
        if (sc->fc) t_fc_release(sc->fc, 1);
    }
    if (heap) free(buf);
    return rc;
}

static void server_push_cb(const char *queue_name, const uint8_t *data, size_t len, void *ud) {
    t_server_conn *sc = (t_server_conn *)ud;
    if (!sc || sc->freed || !sc->conn || !queue_name) return;
    if (sc->next_push_id == UINT64_MAX) sc->next_push_id = 0;
    uint64_t id = ++sc->next_push_id;
    (void)server_send_push(sc, queue_name, id, 0, data, len, 0);
}

static t_queue *server_lookup_queue(t_broker *b, const char *name) {
    t_domain *d = t_broker_get_domain(b, "default");
    if (!d) return NULL;
    return (t_queue *)t_domain_get_queue(d, name);
}

static size_t server_net_consumers(t_server *srv, const char *name, const t_server_conn *except) {
    size_t n = 0;
    if (!srv || !name) return 0;
    for (size_t i = 0; i < srv->conns.len; i++) {
        t_server_conn *sc = (t_server_conn *)srv->conns.items[i];
        if (!sc || sc == except || sc->freed) continue;
        uintptr_t mode = (uintptr_t)t_map_get(&sc->opened, name);
        if (mode & T_WIRE_MODE_CONSUMER) n++;
    }
    return n;
}

static t_push_inf *server_inflight_take(t_server_conn *sc, const char *name, uint64_t id) {
    if (!sc || !name) return NULL;
    for (size_t i = 0; i < sc->inflight.len; i++) {
        t_push_inf *inf = (t_push_inf *)sc->inflight.items[i];
        if (inf && inf->id == id && inf->name && strcmp(inf->name, name) == 0) {
            (void)t_vec_remove(&sc->inflight, i);
            return inf;
        }
    }
    return NULL;
}

static void server_inflight_nack(t_server_conn *sc, const char *only_name) {
    if (!sc) return;
    for (size_t i = 0; i < sc->inflight.len; ) {
        t_push_inf *inf = (t_push_inf *)sc->inflight.items[i];
        if (!inf) {
            (void)t_vec_remove(&sc->inflight, i);
            continue;
        }
        if (only_name && (!inf->name || strcmp(inf->name, only_name) != 0)) {
            i++;
            continue;
        }
        (void)t_vec_remove(&sc->inflight, i);
        if (sc->srv && inf->name) {
            t_queue *q = server_lookup_queue(sc->srv->broker, inf->name);
            if (q) (void)t_queue_nack(q, inf->id);
        }
        free(inf->name);
        free(inf);
    }
}

static void server_cgroup_noop(const char *topic, const uint8_t *payload, size_t len, void *ud) {
    (void)topic;
    (void)payload;
    (void)len;
    (void)ud;
}

static void server_leave_group(t_server_conn *sc, const char *name) {
    if (!sc || !sc->srv || !name) return;
    char *cid = (char *)t_map_remove(&sc->joined, name);
    if (!cid) return;
    t_cgroup *cg = (t_cgroup *)t_map_get(&sc->srv->groups, name);
    if (cg) (void)t_cgroup_remove_consumer(cg, cid);
    free(cid);
}

static t_server_conn *server_pick_group_consumer(t_cgroup *cg) {
    size_t n = t_cgroup_consumer_count(cg);
    for (size_t i = 0; i < n; i++) {
        t_server_conn *sc = (t_server_conn *)t_cgroup_pick(cg);
        if (!sc || sc->freed || !sc->conn) continue;
        if (sc->fc && t_fc_available(sc->fc) == 0) continue;
        return sc;
    }
    return NULL;
}

static int server_restore_group(t_server *srv, const char *name) {
    if (!srv || !name) return -1;
    if (t_map_get(&srv->groups, name)) return 0;
    t_queue *q = server_lookup_queue(srv->broker, name);
    if (!q) return 0;
    const char *g = t_queue_group(q);
    if (!g) return 0;
    t_cgroup *cg = t_cgroup_create(g);
    if (!cg) return -1;
    if (t_map_insert(&srv->groups, name, cg) != 0) {
        t_cgroup_destroy(cg);
        return -1;
    }
    return 0;
}

static t_server_conn *server_pick_pull_consumer(t_server *srv, const char *name) {
    if (!srv || !name) return NULL;
    (void)server_restore_group(srv, name);
    t_cgroup *cg = (t_cgroup *)t_map_get(&srv->groups, name);
    if (cg)
        return server_pick_group_consumer(cg);
    for (size_t i = 0; i < srv->conns.len; i++) {
        t_server_conn *sc = (t_server_conn *)srv->conns.items[i];
        if (!sc || sc->freed || !sc->conn) continue;
        uintptr_t mode = (uintptr_t)t_map_get(&sc->opened, name);
        if ((mode & T_WIRE_MODE_CONSUMER) == 0) continue;
        if (sc->fc && t_fc_available(sc->fc) == 0) continue;
        return sc;
    }
    return NULL;
}

static void server_flush_pull(t_server *srv, const char *name) {
    if (!srv || !name) return;
    t_queue *q = server_lookup_queue(srv->broker, name);
    if (!q) return;
    if (t_queue_get_type(q) == T_QUEUE_BROADCAST) return;
    if (t_queue_consumer_count(q) > 0) return;
    if (t_queue_group(q) && server_restore_group(srv, name) != 0)
        return;
    if (t_queue_group(q) && !t_map_get(&srv->groups, name))
        return;
    while (t_queue_pending_count(q) > 0) {
        t_server_conn *sc = server_pick_pull_consumer(srv, name);
        if (!sc) break;
        t_msg m;
        if (t_queue_consume(q, &m) != 0) break;
        uint8_t pri = 0;
        if (m.priority > 0)
            pri = (m.priority > 255) ? 255 : (uint8_t)m.priority;
        if (server_send_push(sc, name, m.msg_id, pri, m.data, m.data_len, 1) != 0) {
            (void)t_queue_nack(q, m.msg_id);
            break;
        }
    }
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
    t_cgroup *cg = (t_cgroup *)t_map_remove(&srv->groups, name);
    if (cg) t_cgroup_destroy(cg);
    t_queue *q = server_lookup_queue(srv->broker, name);
    if (q && (t_queue_get_flags(q) & T_QUEUE_FLAG_AUTODELETE))
        (void)t_broker_delete_queue(srv->broker, "default", name);
}

static void server_unsub_all(t_server_conn *sc) {
    if (!sc || !sc->srv || !sc->srv->broker) return;
    server_inflight_nack(sc, NULL);
    t_map_iter it = t_map_iter_begin(&sc->opened);
    const char *k;
    void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        uintptr_t mode = (uintptr_t)v;
        server_leave_group(sc, k);
        if (mode & T_WIRE_MODE_CONSUMER) {
            (void)t_broker_unsubscribe(sc->srv->broker, k, server_push_cb, sc);
            server_flush_pull(sc->srv, k);
        }
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
    if (sc->srv) {
        server_wait_drop_conn(sc->srv, sc);
        server_conn_detach(sc->srv, sc);
    }
    {
        t_map_iter it = t_map_iter_begin(&sc->joined);
        const char *k;
        void *v;
        while (t_map_iter_next(&it, &k, &v))
            free(v);
    }
    t_map_destroy(&sc->joined);
    t_map_destroy(&sc->opened);
    if (sc->sess) {
        (void)t_session_disconnect(sc->sess);
        (void)t_session_destroy(sc->sess);
        sc->sess = NULL;
    }
    t_ratelimit_destroy(sc->rl);
    sc->rl = NULL;
    t_fc_destroy(sc->fc);
    sc->fc = NULL;
    t_vec_destroy(&sc->inflight);
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
    if (!t_broker_is_leader(b))
        return T_ERR_AGAIN;
    if (!queue_exists(b, name)) {
        int cr = t_broker_create_queue(b, "default", name, (int)o.qtype, (int)o.qflags);
        if (cr < 0) {
            if (t_broker_raft(b)) return T_ERR_AGAIN;
            if (o.qflags & T_QUEUE_FLAG_DURABLE) return T_ERR_IO;
            return T_ERR_GENERIC;
        }
        if (cr == 1) {
            if (server_wait_add(sc, T_MSG_OPEN_QUEUE, name, server_raft_index(b),
                                o.mode, 0, NULL, NULL) != 0)
                return T_ERR_GENERIC;
            return T_OK_CODE;
        }
    }
    int32_t st = finish_open(sc, name, o.mode);
    if (st != T_OK_CODE) return st;
    if (send_ack(sc, T_MSG_OPEN_QUEUE, T_OK_CODE, name) != 0)
        return T_ERR_IO;
    return T_OK_CODE;
}

static int32_t finish_open(t_server_conn *sc, const char *name, uint8_t omode) {
    t_broker *b = sc->srv->broker;
    uintptr_t prev = (uintptr_t)t_map_get(&sc->opened, name);
    uintptr_t mode = prev | (uintptr_t)omode;
    if ((mode & T_WIRE_MODE_CONSUMER) && !(prev & T_WIRE_MODE_CONSUMER)) {
        t_queue *q = server_lookup_queue(b, name);
        if (q && (t_queue_get_flags(q) & T_QUEUE_FLAG_EXCLUSIVE) &&
            (t_queue_consumer_count(q) > 0 ||
             server_net_consumers(sc->srv, name, sc) > 0))
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
        t_queue *q = server_lookup_queue(b, name);
        int push = q && t_queue_get_type(q) == T_QUEUE_BROADCAST;
        if (push && t_broker_subscribe(b, name, server_push_cb, sc) != 0) {
            if (prev == 0) {
                (void)t_map_remove(&sc->opened, name);
                server_unref_open(sc->srv, name);
            } else {
                (void)t_map_insert(&sc->opened, name, (void *)prev);
            }
            if (q && (t_queue_get_flags(q) & T_QUEUE_FLAG_EXCLUSIVE))
                return T_ERR_BUSY;
            return T_ERR_GENERIC;
        }
        if (!push) {
            (void)server_restore_group(sc->srv, name);
            server_flush_pull(sc->srv, name);
        }
    }
    return T_OK_CODE;
}

static void server_on_applied(t_broker *b, uint64_t last_applied, void *ud) {
    t_server *srv = (t_server *)ud;
    (void)b;
    if (!srv) return;
    server_wait_flush(srv, last_applied);
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
    if (mode & T_WIRE_MODE_CONSUMER) {
        server_inflight_nack(sc, name);
        (void)t_broker_unsubscribe(sc->srv->broker, name, server_push_cb, sc);
        server_leave_group(sc, name);
        server_flush_pull(sc->srv, name);
    }
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
    int pr = t_broker_publish(sc->srv->broker, name, p.data, p.data_len, (int)p.priority);
    if (pr < 0)
        return t_broker_raft(sc->srv->broker) ? T_ERR_AGAIN : T_ERR_GENERIC;
    if (pr == 1) {
        if (server_wait_add(sc, T_MSG_POST, name, server_raft_index(sc->srv->broker),
                            0, 0, NULL, NULL) != 0)
            return T_ERR_GENERIC;
        return T_OK_CODE;
    }
    server_flush_pull(sc->srv, name);
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
    t_push_inf *inf = server_inflight_take(sc, name, c.msg_id);
    if (inf) {
        t_queue *q = server_lookup_queue(sc->srv->broker, name);
        if (msg->header.type == T_MSG_REJECT) {
            int nr = t_broker_nack(sc->srv->broker, name, inf->id);
            if (nr < 0 && t_broker_raft(sc->srv->broker)) {
                if (t_vec_push(&sc->inflight, inf) != 0) {
                    free(inf->name);
                    free(inf);
                    return T_ERR_GENERIC;
                }
                return T_ERR_AGAIN;
            }
            if (nr == 1) {
                free(inf->name);
                free(inf);
                if (server_wait_add(sc, T_MSG_REJECT, name,
                                    server_raft_index(sc->srv->broker), 0, 1,
                                    NULL, NULL) != 0)
                    return T_ERR_GENERIC;
                return T_OK_CODE;
            }
        } else if (t_broker_raft(sc->srv->broker)) {
            int ar = t_broker_ack(sc->srv->broker, name, inf->id);
            if (ar < 0) {
                if (t_vec_push(&sc->inflight, inf) != 0) {
                    free(inf->name);
                    free(inf);
                    return T_ERR_GENERIC;
                }
                return T_ERR_AGAIN;
            }
            if (ar == 1) {
                free(inf->name);
                free(inf);
                if (server_wait_add(sc, (uint16_t)msg->header.type, name,
                                    server_raft_index(sc->srv->broker), 0, 1,
                                    NULL, NULL) != 0)
                    return T_ERR_GENERIC;
                return T_OK_CODE;
            }
        } else if (q) {
            (void)t_queue_ack(q, inf->id);
        }
        free(inf->name);
        free(inf);
    }
    if (sc->fc) t_fc_release(sc->fc, 1);
    server_flush_pull(sc->srv, name);
    if (send_ack(sc, (uint16_t)msg->header.type, T_OK_CODE, name) != 0)
        return T_ERR_IO;
    return T_OK_CODE;
}

static int32_t handle_auth(t_server_conn *sc, const t_proto_msg *msg) {
    const uint8_t *mac = NULL;
    if (t_wire_decode_auth(msg->payload, msg->payload_len, &mac) != 0)
        return T_ERR_PROTO;
    uint8_t expect[T_HMAC_SHA256_LEN];
    if (t_auth_mac(expect, sc->srv->psk, sc->srv->psk_len) != 0)
        return T_ERR_GENERIC;
    if (!t_hmac_equal(mac, expect, T_HMAC_SHA256_LEN)) {
        t_hmac_wipe(expect, sizeof(expect));
        return T_ERR_PERMISSION;
    }
    t_hmac_wipe(expect, sizeof(expect));
    sc->authed = 1;
    if (send_ack(sc, T_MSG_AUTH, T_OK_CODE, NULL) != 0)
        return T_ERR_IO;
    return T_OK_CODE;
}

static int32_t finish_join(t_server_conn *sc, const char *queue,
                           const char *group, const char *consumer) {
    if (!sc || !queue || !group || !consumer || !group[0] || !consumer[0])
        return T_ERR_INVALID;
    uintptr_t mode = (uintptr_t)t_map_get(&sc->opened, queue);
    if ((mode & T_WIRE_MODE_CONSUMER) == 0)
        return T_ERR_PERMISSION;
    t_queue *q = server_lookup_queue(sc->srv->broker, queue);
    if (!q) return T_ERR_NOTFOUND;
    if (t_queue_get_type(q) == T_QUEUE_BROADCAST)
        return T_ERR_INVALID;
    const char *bound = t_queue_group(q);
    if (bound && strcmp(bound, group) != 0)
        return T_ERR_BUSY;
    if (!bound && t_queue_set_group(q, group) != 0)
        return T_ERR_GENERIC;
    if (server_restore_group(sc->srv, queue) != 0)
        return T_ERR_NOMEM;
    t_cgroup *cg = (t_cgroup *)t_map_get(&sc->srv->groups, queue);
    if (!cg) return T_ERR_GENERIC;
    if (strcmp(t_cgroup_id(cg), group) != 0)
        return T_ERR_BUSY;
    char *prev = (char *)t_map_get(&sc->joined, queue);
    if (prev) {
        if (strcmp(prev, consumer) == 0)
            return T_OK_CODE;
        return T_ERR_BUSY;
    }
    if (t_cgroup_add_consumer(cg, consumer, server_cgroup_noop, sc) != 0)
        return T_ERR_EXISTS;
    char *cid = strdup(consumer);
    if (!cid) {
        (void)t_cgroup_remove_consumer(cg, consumer);
        return T_ERR_NOMEM;
    }
    if (t_map_insert(&sc->joined, queue, cid) != 0) {
        (void)t_cgroup_remove_consumer(cg, consumer);
        free(cid);
        return T_ERR_NOMEM;
    }
    return T_OK_CODE;
}

static int32_t handle_join(t_server_conn *sc, const t_proto_msg *msg) {
    t_wire_join j;
    if (t_wire_decode_join(msg->payload, msg->payload_len, &j) != 0)
        return T_ERR_PROTO;
    char group[T_WIRE_MAX_NAME + 1];
    char consumer[T_WIRE_MAX_NAME + 1];
    char queue[T_WIRE_MAX_NAME + 1];
    if (t_wire_name_copy(group, sizeof(group), j.group, j.group_len) != 0 ||
        t_wire_name_copy(consumer, sizeof(consumer), j.consumer, j.consumer_len) != 0 ||
        t_wire_name_copy(queue, sizeof(queue), j.queue, j.queue_len) != 0)
        return T_ERR_INVALID;
    uintptr_t mode = (uintptr_t)t_map_get(&sc->opened, queue);
    if ((mode & T_WIRE_MODE_CONSUMER) == 0)
        return T_ERR_PERMISSION;
    t_queue *q = server_lookup_queue(sc->srv->broker, queue);
    if (!q) return T_ERR_NOTFOUND;
    if (t_queue_get_type(q) == T_QUEUE_BROADCAST)
        return T_ERR_INVALID;
    if (!t_broker_is_running(sc->srv->broker))
        return T_ERR_CLOSED;
    if (!t_broker_is_leader(sc->srv->broker))
        return T_ERR_AGAIN;

    char *prev = (char *)t_map_get(&sc->joined, queue);
    if (prev) {
        const char *bound = t_queue_group(q);
        if (bound && strcmp(bound, group) == 0 && strcmp(prev, consumer) == 0) {
            if (send_ack(sc, T_MSG_JOIN, T_OK_CODE, queue) != 0)
                return T_ERR_IO;
            return T_OK_CODE;
        }
        return T_ERR_BUSY;
    }

    const char *bound = t_queue_group(q);
    if (bound && strcmp(bound, group) != 0)
        return T_ERR_BUSY;
    if (!bound) {
        int jr = t_broker_join_group(sc->srv->broker, queue, group);
        if (jr < 0)
            return t_broker_raft(sc->srv->broker) ? T_ERR_AGAIN : T_ERR_GENERIC;
        if (jr == 1) {
            if (server_wait_add(sc, T_MSG_JOIN, queue,
                                server_raft_index(sc->srv->broker), 0, 0,
                                group, consumer) != 0)
                return T_ERR_GENERIC;
            return T_OK_CODE;
        }
    }

    int32_t st = finish_join(sc, queue, group, consumer);
    if (st != T_OK_CODE) return st;
    if (send_ack(sc, T_MSG_JOIN, T_OK_CODE, queue) != 0)
        return T_ERR_IO;
    server_flush_pull(sc->srv, queue);
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
    int is_credit = (msg->header.type == T_MSG_CONFIRM ||
                     msg->header.type == T_MSG_REJECT);

    if (sc->srv->psk_len > 0 && !sc->authed) {
        if (msg->header.type != T_MSG_AUTH) {
            (void)send_ack(sc, msg->header.type, (int32_t)T_ERR_PERMISSION, NULL);
            close_conn = 1;
        } else {
            status = handle_auth(sc, msg);
            if (status != T_OK_CODE) {
                (void)send_ack(sc, T_MSG_AUTH, status, NULL);
                close_conn = 1;
            }
        }
        sc->in_cb--;
        if (close_conn && sc->conn) {
            (void)t_conn_flush(sc->conn);
            t_conn_destroy(conn);
            return;
        }
        if (sc->free_pending) server_conn_free(sc);
        return;
    }

    if (is_credit) {
        status = handle_confirm(sc, msg);
        if (status != T_OK_CODE)
            (void)send_ack(sc, msg->header.type, status, NULL);
        sc->in_cb--;
        if (status == T_ERR_PROTO && sc->conn) {
            t_conn_destroy(conn);
            return;
        }
        if (sc->free_pending) server_conn_free(sc);
        return;
    }

    if (msg->header.type != T_MSG_HEARTBEAT && msg->header.type != T_MSG_NOP &&
        !t_ratelimit_allow(sc->rl, now_ms)) {
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
    case T_MSG_JOIN:
        status = handle_join(sc, msg);
        break;
    case T_MSG_AUTH:
        close_conn = 1;
        break;
    default:
        close_conn = 1;
        break;
    }

    if (!close_conn && status != T_OK_CODE) {
        const char *nm = NULL;
        char namebuf[T_WIRE_MAX_NAME + 1];
        if (status == T_ERR_AGAIN) {
            t_cluster *cl = t_broker_cluster(sc->srv->broker);
            t_node *ld = cl ? t_cluster_get_leader(cl) : NULL;
            uint16_t cport = ld ? t_node_client_port(ld) : 0;
            if (ld && t_node_host(ld) && cport != 0) {
                int hn = snprintf(namebuf, sizeof(namebuf), "%s_%u",
                                  t_node_host(ld), (unsigned)cport);
                if (hn > 0 && (size_t)hn < sizeof(namebuf) &&
                    t_wire_name_valid(namebuf, (size_t)hn))
                    nm = namebuf;
            }
        } else if (msg->header.type == T_MSG_OPEN_QUEUE) {
            t_wire_open o;
            if (t_wire_decode_open(msg->payload, msg->payload_len, &o) == 0 &&
                t_wire_name_copy(namebuf, sizeof(namebuf), o.name, o.name_len) == 0)
                nm = namebuf;
        } else if (msg->header.type == T_MSG_POST) {
            t_wire_post p;
            if (t_wire_decode_post(msg->payload, msg->payload_len, &p) == 0 &&
                t_wire_name_copy(namebuf, sizeof(namebuf), p.name, p.name_len) == 0)
                nm = namebuf;
        } else if (msg->header.type == T_MSG_JOIN) {
            t_wire_join jn;
            if (t_wire_decode_join(msg->payload, msg->payload_len, &jn) == 0 &&
                t_wire_name_copy(namebuf, sizeof(namebuf), jn.queue, jn.queue_len) == 0)
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
    t_map_init(&sc->joined);
    t_vec_init(&sc->inflight);
    sc->sess = t_session_create((uint64_t)(srv->conns.len + 1));
    sc->rl = t_ratelimit_create(srv->rate_tokens, srv->rate_refill);
    if (srv->push_credits > 0)
        sc->fc = t_fc_create(srv->push_credits, 0);
    sc->conn = t_conn_create(client_fd, srv->loop);
    if (!sc->sess || !sc->rl || !sc->conn ||
        (srv->push_credits > 0 && !sc->fc)) {
        if (sc->conn) t_conn_destroy(sc->conn);
        else t_socket_close(client_fd);
        if (sc->sess) t_session_destroy(sc->sess);
        t_ratelimit_destroy(sc->rl);
        t_fc_destroy(sc->fc);
        t_map_destroy(&sc->opened);
        t_map_destroy(&sc->joined);
        t_vec_destroy(&sc->inflight);
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
    srv->push_credits = cfg->push_credits;
    srv->idle_timer_id = -1;
    srv->wait_timer_id = -1;
    t_vec_init(&srv->conns);
    t_vec_init(&srv->waits);
    t_broker_set_applied_cb(broker, server_on_applied, srv);
    t_map_init(&srv->open_refs);
    t_map_init(&srv->groups);
    srv->tcp = t_tcp_server_create(loop);
    if (!srv->tcp) {
        t_broker_set_applied_cb(broker, NULL, NULL);
        t_vec_destroy(&srv->conns);
        t_vec_destroy(&srv->waits);
        t_map_destroy(&srv->open_refs);
        t_map_destroy(&srv->groups);
        free(srv->host);
        free(srv);
        return NULL;
    }
    if (cfg->psk_len > T_AUTH_PSK_MAX || (cfg->psk_len > 0 && !cfg->psk)) {
        t_tcp_server_destroy(srv->tcp);
        t_broker_set_applied_cb(broker, NULL, NULL);
        t_vec_destroy(&srv->conns);
        t_vec_destroy(&srv->waits);
        t_map_destroy(&srv->open_refs);
        t_map_destroy(&srv->groups);
        free(srv->host);
        free(srv);
        return NULL;
    }
    if (cfg->psk_len > 0) {
        srv->psk = (uint8_t *)malloc(cfg->psk_len);
        if (!srv->psk) {
            t_tcp_server_destroy(srv->tcp);
            t_broker_set_applied_cb(broker, NULL, NULL);
            t_vec_destroy(&srv->conns);
            t_vec_destroy(&srv->waits);
            t_map_destroy(&srv->open_refs);
            t_map_destroy(&srv->groups);
            free(srv->host);
            free(srv);
            return NULL;
        }
        memcpy(srv->psk, cfg->psk, cfg->psk_len);
        srv->psk_len = cfg->psk_len;
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
    if (srv->broker) t_broker_set_applied_cb(srv->broker, NULL, NULL);
    t_tcp_server_destroy(srv->tcp);
    t_vec_destroy(&srv->conns);
    while (srv->waits.len > 0) {
        t_server_wait *w = (t_server_wait *)t_vec_pop(&srv->waits);
        free(w);
    }
    t_vec_destroy(&srv->waits);
    t_map_destroy(&srv->open_refs);
    {
        t_map_iter it = t_map_iter_begin(&srv->groups);
        const char *k;
        void *v;
        while (t_map_iter_next(&it, &k, &v))
            t_cgroup_destroy((t_cgroup *)v);
    }
    t_map_destroy(&srv->groups);
    if (srv->psk) {
        t_hmac_wipe(srv->psk, srv->psk_len);
        free(srv->psk);
    }
    free(srv->host);
    free(srv);
}

int t_server_start(t_server *srv) {
    if (!srv || srv->running || !srv->tcp) return -1;
    if (srv->psk_len == 0) {
        unsigned a = 0, b = 0, c = 0, d = 0;
        char extra;
        if (!srv->host ||
            sscanf(srv->host, "%u.%u.%u.%u%c", &a, &b, &c, &d, &extra) != 4 ||
            a != 127 || b > 255 || c > 255 || d > 255)
            return -1;
    }
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
    server_wait_disarm(srv);
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
