#include "t_peer.h"
#include "t_tcp.h"
#include "t_conn.h"
#include "t_socket.h"
#include "t_proto.h"
#include "t_wire.h"
#include "t_vec.h"
#include "t_time.h"
#include <stdlib.h>
#include <string.h>

typedef struct t_peer_conn {
    t_peer *peer;
    t_conn *conn;
    int     outbound;
    int     in_cb;
    int     freed;
    int     free_pending;
} t_peer_conn;

struct t_peer {
    t_evloop     *loop;
    t_raft       *raft;
    t_cluster    *cluster;
    t_tcp_server *tcp;
    char         *host;
    uint16_t      port;
    uint16_t      bound_port;
    t_vec         conns;
    uint64_t      votes;
    int64_t       last_activity_ms;
    int64_t       timer_id;
    int           running;
    int           stopping;
    int           in_cb;
    int           free_pending;
};

static void peer_on_accept(t_tcp_server *tcp, int client_fd, t_sockaddr *sa, void *ud);
static void peer_on_msg(t_conn *conn, const t_proto_msg *msg, void *ud);
static void peer_on_close(t_conn *conn, void *ud);
static void peer_conn_free(t_peer_conn *pc);
static void peer_tick(void *ud);

void t_peer_config_init(t_peer_config *cfg) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->host = "127.0.0.1";
    cfg->port = 4223;
}

static int peer_send(t_conn *conn, const uint8_t *payload, size_t plen) {
    if (!conn) return -1;
    t_proto_msg msg;
    t_proto_header_init(&msg.header, T_MSG_CLUSTER, (uint32_t)plen);
    msg.payload = (uint8_t *)payload;
    msg.payload_len = plen;
    return t_conn_send(conn, &msg);
}

static size_t peer_quorum(const t_peer *p) {
    if (!p || !p->cluster) return 1;
    size_t n = t_cluster_node_count(p->cluster);
    uint64_t self = t_cluster_self_id(p->cluster);
    if (!t_cluster_get_node((t_cluster *)p->cluster, self)) n++;
    if (n == 0) n = 1;
    return n / 2 + 1;
}

static void peer_note_leader(t_peer *p) {
    if (!p || !p->cluster) return;
    if (t_raft_state(p->raft) != T_NODE_LEADER) return;
    uint64_t id = t_raft_id(p->raft);
    if (t_cluster_get_node(p->cluster, id))
        (void)t_cluster_set_leader(p->cluster, id);
}

static void peer_conn_detach(t_peer *p, t_peer_conn *pc) {
    if (!p || !pc) return;
    for (size_t i = 0; i < p->conns.len; i++) {
        if (p->conns.items[i] == pc) {
            (void)t_vec_remove(&p->conns, i);
            return;
        }
    }
}

static void peer_conn_free(t_peer_conn *pc) {
    if (!pc || pc->freed) return;
    if (pc->in_cb > 0) {
        pc->free_pending = 1;
        return;
    }
    pc->freed = 1;
    if (pc->peer) peer_conn_detach(pc->peer, pc);
    pc->conn = NULL;
    free(pc);
}

static void peer_conn_close(t_peer_conn *pc) {
    if (!pc || !pc->conn) return;
    t_conn *cn = pc->conn;
    pc->conn = NULL;
    t_conn_destroy(cn);
}

static void peer_on_close(t_conn *conn, void *ud) {
    t_peer_conn *pc = (t_peer_conn *)ud;
    if (!pc) return;
    int owned = (pc->conn == conn);
    pc->conn = NULL;
    peer_conn_free(pc);
    if (owned) t_conn_destroy(conn);
}

static t_peer_conn *peer_conn_new(t_peer *p, int fd, int outbound) {
    t_peer_conn *pc = (t_peer_conn *)calloc(1, sizeof(*pc));
    if (!pc) {
        t_socket_close(fd);
        return NULL;
    }
    pc->peer = p;
    pc->outbound = outbound;
    pc->conn = t_conn_create(fd, p->loop);
    if (!pc->conn) {
        free(pc);
        return NULL;
    }
    t_conn_set_on_msg(pc->conn, peer_on_msg, pc);
    t_conn_set_on_close(pc->conn, peer_on_close, pc);
    if (t_vec_push(&p->conns, pc) != 0) {
        t_conn_destroy(pc->conn);
        return NULL;
    }
    return pc;
}

static int peer_dial_payload(t_peer *p, const char *host, uint16_t port,
                             const uint8_t *payload, size_t plen) {
    if (!p || !host || port == 0 || !payload) return -1;
    int fd = t_socket_dial_ipv4(host, port);
    if (fd < 0) return -1;
    t_peer_conn *pc = peer_conn_new(p, fd, 1);
    if (!pc) return -1;
    if (peer_send(pc->conn, payload, plen) != 0) {
        peer_conn_close(pc);
        return -1;
    }
    return 0;
}

typedef struct {
    t_peer *peer;
    const uint8_t *payload;
    size_t plen;
    uint64_t skip_id;
} peer_bcast;

static void peer_send_one(t_node *n, void *ud) {
    peer_bcast *b = (peer_bcast *)ud;
    if (!n || t_node_id(n) == b->skip_id) return;
    if (!t_node_is_alive(n)) return;
    (void)peer_dial_payload(b->peer, t_node_host(n), t_node_port(n),
                            b->payload, b->plen);
}

static void peer_maybe_win(t_peer *p) {
    if (!p || t_raft_state(p->raft) != T_NODE_CANDIDATE) return;
    if (p->votes < peer_quorum(p)) return;
    if (t_raft_become_leader(p->raft) != 0) return;
    peer_note_leader(p);
}

int t_peer_campaign(t_peer *p) {
    if (!p || !p->running || !p->raft) return -1;
    if (t_raft_become_candidate(p->raft) != 0) return -1;
    p->votes = 1;
    p->last_activity_ms = t_time_now_ms();
    peer_maybe_win(p);
    if (t_raft_state(p->raft) == T_NODE_LEADER) return 0;

    uint8_t buf[64];
    t_wire_vote_req vr;
    memset(&vr, 0, sizeof(vr));
    vr.term = t_raft_current_term(p->raft);
    vr.candidate_id = t_raft_id(p->raft);
    vr.last_log_index = t_raft_last_log_index(p->raft);
    vr.last_log_term = t_raft_last_log_term(p->raft);
    int n = t_wire_encode_vote_req(buf, sizeof(buf), &vr);
    if (n < 0) return -1;
    peer_bcast b = { p, buf, (size_t)n, t_raft_id(p->raft) };
    t_cluster_foreach(p->cluster, peer_send_one, &b);
    return 0;
}

static void peer_heartbeat(t_peer *p) {
    if (!p || t_raft_state(p->raft) != T_NODE_LEADER) return;
    t_wire_cluster_entry ents[T_WIRE_CLUSTER_MAX_ENTS];
    uint32_t nent = 0;
    size_t lc = t_raft_log_count(p->raft);
    if (lc > T_WIRE_CLUSTER_MAX_ENTS) lc = T_WIRE_CLUSTER_MAX_ENTS;
    for (size_t i = 1; i <= lc; i++) {
        const t_raft_entry *e = t_raft_get_entry(p->raft, (uint64_t)i);
        if (!e) break;
        ents[nent].index = e->index;
        ents[nent].term = e->term;
        ents[nent].type = e->type;
        ents[nent].data = e->data;
        ents[nent].data_len = (uint32_t)e->data_len;
        nent++;
    }
    t_wire_append_req ar;
    memset(&ar, 0, sizeof(ar));
    ar.term = t_raft_current_term(p->raft);
    ar.leader_id = t_raft_id(p->raft);
    ar.leader_commit = t_raft_commit_index(p->raft);
    uint8_t buf[8192];
    int n = t_wire_encode_append_req(buf, sizeof(buf), &ar, ents, nent);
    if (n < 0) return;
    peer_bcast b = { p, buf, (size_t)n, t_raft_id(p->raft) };
    t_cluster_foreach(p->cluster, peer_send_one, &b);
}

static void peer_on_inbound(t_peer *p, t_peer_conn *pc, const t_proto_msg *msg) {
    uint8_t resp[512];
    int n = t_raft_rpc(p->raft, msg->payload, msg->payload_len, resp, sizeof(resp));
    if (n < 0) {
        peer_conn_close(pc);
        return;
    }
    p->last_activity_ms = t_time_now_ms();
    if (msg->payload_len > 0 && msg->payload[0] == T_WIRE_CLUSTER_APPEND_REQ) {
        t_wire_append_req ar;
        t_wire_cluster_entry ents[T_WIRE_CLUSTER_MAX_ENTS];
        if (t_wire_decode_append_req(msg->payload, msg->payload_len, &ar,
                                     ents, T_WIRE_CLUSTER_MAX_ENTS) == 0) {
            if (ar.leader_id && t_cluster_get_node(p->cluster, ar.leader_id))
                (void)t_cluster_set_leader(p->cluster, ar.leader_id);
        }
    }
    (void)peer_send(pc->conn, resp, (size_t)n);
}

static void peer_on_outbound(t_peer *p, t_peer_conn *pc, const t_proto_msg *msg) {
    if (!msg->payload || msg->payload_len == 0) {
        peer_conn_close(pc);
        return;
    }
    uint8_t rpc = msg->payload[0];
    if (rpc == T_WIRE_CLUSTER_VOTE_RESP) {
        t_wire_vote_resp vr;
        if (t_wire_decode_vote_resp(msg->payload, msg->payload_len, &vr) == 0) {
            if (vr.term > t_raft_current_term(p->raft))
                (void)t_raft_become_follower(p->raft, vr.term);
            else if (vr.granted && t_raft_state(p->raft) == T_NODE_CANDIDATE &&
                     vr.term == t_raft_current_term(p->raft)) {
                p->votes++;
                peer_maybe_win(p);
            }
        }
    } else if (rpc == T_WIRE_CLUSTER_APPEND_RESP) {
        t_wire_append_resp ar;
        if (t_wire_decode_append_resp(msg->payload, msg->payload_len, &ar) == 0 &&
            ar.term > t_raft_current_term(p->raft))
            (void)t_raft_become_follower(p->raft, ar.term);
    }
    peer_conn_close(pc);
}

static void peer_on_msg(t_conn *conn, const t_proto_msg *msg, void *ud) {
    (void)conn;
    t_peer_conn *pc = (t_peer_conn *)ud;
    if (!pc || pc->freed || !msg) return;
    pc->in_cb++;
    if (msg->header.type != T_MSG_CLUSTER) {
        peer_conn_close(pc);
        pc->in_cb--;
        if (pc->free_pending) peer_conn_free(pc);
        return;
    }
    if (pc->outbound)
        peer_on_outbound(pc->peer, pc, msg);
    else
        peer_on_inbound(pc->peer, pc, msg);
    pc->in_cb--;
    if (pc->free_pending) peer_conn_free(pc);
}

static void peer_on_accept(t_tcp_server *tcp, int client_fd, t_sockaddr *sa, void *ud) {
    (void)tcp;
    (void)sa;
    t_peer *p = (t_peer *)ud;
    if (!p || client_fd < 0) {
        if (client_fd >= 0) t_socket_close(client_fd);
        return;
    }
    p->in_cb++;
    if (p->stopping || p->conns.len >= 64) {
        t_socket_close(client_fd);
        p->in_cb--;
        return;
    }
    (void)peer_conn_new(p, client_fd, 0);
    p->in_cb--;
    if (p->free_pending) t_peer_destroy(p);
}

static void peer_tick(void *ud) {
    t_peer *p = (t_peer *)ud;
    if (!p || p->stopping) return;
    int64_t now = t_time_now_ms();
    if (t_raft_state(p->raft) == T_NODE_LEADER) {
        peer_heartbeat(p);
        return;
    }
    int64_t to = (int64_t)t_raft_election_timeout_ms(p->raft);
    to += (int64_t)(t_raft_id(p->raft) % 17);
    if (to < 10) to = 10;
    if (now - p->last_activity_ms >= to)
        (void)t_peer_campaign(p);
}

t_peer *t_peer_create(t_evloop *loop, t_raft *raft, t_cluster *cluster,
                      const t_peer_config *cfg) {
    if (!loop || !raft || !cluster) return NULL;
    t_peer_config def;
    t_peer_config_init(&def);
    if (!cfg) cfg = &def;
    t_peer *p = (t_peer *)calloc(1, sizeof(*p));
    if (!p) return NULL;
    const char *host = cfg->host;
    if (!host || !host[0]) host = "127.0.0.1";
    p->host = strdup(host);
    if (!p->host) {
        free(p);
        return NULL;
    }
    p->loop = loop;
    p->raft = raft;
    p->cluster = cluster;
    p->port = cfg->port;
    p->timer_id = -1;
    t_vec_init(&p->conns);
    p->tcp = t_tcp_server_create(loop);
    if (!p->tcp) {
        t_vec_destroy(&p->conns);
        free(p->host);
        free(p);
        return NULL;
    }
    return p;
}

void t_peer_stop(t_peer *p) {
    if (!p) return;
    p->stopping = 1;
    p->running = 0;
    if (p->timer_id >= 0) {
        t_evloop_timer_del(p->loop, p->timer_id);
        p->timer_id = -1;
    }
    while (p->conns.len > 0) {
        t_peer_conn *pc = (t_peer_conn *)p->conns.items[0];
        if (pc && pc->conn) peer_conn_close(pc);
        else {
            (void)t_vec_remove(&p->conns, 0);
            peer_conn_free(pc);
        }
    }
    if (p->tcp) t_tcp_server_destroy(p->tcp);
    p->tcp = NULL;
}

void t_peer_destroy(t_peer *p) {
    if (!p) return;
    if (p->in_cb > 0) {
        p->free_pending = 1;
        return;
    }
    p->free_pending = 0;
    t_peer_stop(p);
    t_vec_destroy(&p->conns);
    free(p->host);
    free(p);
}

int t_peer_start(t_peer *p) {
    if (!p || p->running || !p->tcp) return -1;
    if (t_tcp_server_listen(p->tcp, p->host, p->port, peer_on_accept, p) != 0)
        return -1;
    p->bound_port = t_socket_local_port(p->tcp->fd);
    p->last_activity_ms = t_time_now_ms();
    int64_t tick = (int64_t)t_raft_heartbeat_interval_ms(p->raft);
    if (tick < 10) tick = 10;
    p->timer_id = t_evloop_timer_add(p->loop, tick, 1, peer_tick, p);
    if (p->timer_id < 0) {
        t_tcp_server_destroy(p->tcp);
        p->tcp = t_tcp_server_create(p->loop);
        p->bound_port = 0;
        return -1;
    }
    p->running = 1;
    p->stopping = 0;
    return 0;
}

int t_peer_is_running(const t_peer *p) {
    return p ? p->running : 0;
}

uint16_t t_peer_port(const t_peer *p) {
    return p ? p->bound_port : 0;
}

const char *t_peer_host(const t_peer *p) {
    return p ? p->host : NULL;
}

int t_peer_is_leader(const t_peer *p) {
    return p && t_raft_state(p->raft) == T_NODE_LEADER;
}
