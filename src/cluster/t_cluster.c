#include "t_cluster.h"
#include "t_node.h"
#include "t_map.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>

struct t_cluster {
    uint64_t self_id;
    t_map nodes;
    t_node *leader;
};

static void make_key(char *buf, size_t sz, uint64_t id) {
    snprintf(buf, sz, "%llu", (unsigned long long)id);
}

t_cluster *t_cluster_create(uint64_t self_id) {
    t_cluster *c = (t_cluster *)calloc(1, sizeof(t_cluster));
    if (!c) return NULL;
    c->self_id = self_id;
    c->leader = NULL;
    t_map_init(&c->nodes);
    return c;
}

void t_cluster_destroy(t_cluster *cluster) {
    if (!cluster) return;
    t_map_iter it = t_map_iter_begin(&cluster->nodes);
    const char *k;
    void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        t_node_destroy((t_node *)v);
    }
    t_map_destroy(&cluster->nodes);
    free(cluster);
}

int t_cluster_add_node(t_cluster *cluster, uint64_t node_id,
                        const char *host, uint16_t port) {
    if (!cluster || !host) return -1;
    char key[32];
    make_key(key, sizeof(key), node_id);
    if (t_map_contains(&cluster->nodes, key)) return -1;
    t_node *n = t_node_create(node_id, host, port);
    if (!n) return -1;
    if (t_map_insert(&cluster->nodes, key, n) != 0) {
        t_node_destroy(n);
        return -1;
    }
    return 0;
}

int t_cluster_remove_node(t_cluster *cluster, uint64_t node_id) {
    if (!cluster) return -1;
    char key[32];
    make_key(key, sizeof(key), node_id);
    void *p = t_map_remove(&cluster->nodes, key);
    if (!p) return -1;
    if (cluster->leader == (t_node *)p) {
        cluster->leader = NULL;
        /* Demote any leftover LEADER roles on surviving nodes. */
        t_map_iter it = t_map_iter_begin(&cluster->nodes);
        const char *k;
        void *v;
        while (t_map_iter_next(&it, &k, &v)) {
            if (t_node_is_leader((t_node *)v))
                t_node_set_role((t_node *)v, T_NODE_FOLLOWER);
        }
    }
    t_node_destroy((t_node *)p);
    return 0;
}

size_t t_cluster_node_count(const t_cluster *cluster) {
    return cluster ? t_map_len(&cluster->nodes) : 0;
}

t_node *t_cluster_get_node(t_cluster *cluster, uint64_t node_id) {
    if (!cluster) return NULL;
    char key[32];
    make_key(key, sizeof(key), node_id);
    return (t_node *)t_map_get(&cluster->nodes, key);
}

t_node *t_cluster_get_leader(t_cluster *cluster) {
    if (!cluster || !cluster->leader) return NULL;
    if (!t_node_is_alive(cluster->leader)) {
        t_node_set_role(cluster->leader, T_NODE_FOLLOWER);
        cluster->leader = NULL;
        return NULL;
    }
    return cluster->leader;
}

int t_cluster_set_leader(t_cluster *cluster, uint64_t node_id) {
    if (!cluster) return -1;
    t_node *n = t_cluster_get_node(cluster, node_id);
    if (!n || !t_node_is_alive(n)) return -1;
    if (cluster->leader && cluster->leader != n) {
        t_node_set_role(cluster->leader, T_NODE_FOLLOWER);
    }
    t_node_set_role(n, T_NODE_LEADER);
    cluster->leader = n;
    return 0;
}

uint64_t t_cluster_self_id(const t_cluster *cluster) {
    return cluster ? cluster->self_id : 0;
}

int t_cluster_is_leader(const t_cluster *cluster) {
    if (!cluster || !cluster->leader) return 0;
    if (!t_node_is_alive(cluster->leader)) return 0;
    return t_node_id(cluster->leader) == cluster->self_id;
}

size_t t_cluster_alive_count(const t_cluster *cluster) {
    if (!cluster) return 0;
    size_t count = 0;
    t_map_iter it = t_map_iter_begin(&cluster->nodes);
    const char *k;
    void *v;
    while (t_map_iter_next(&it, &k, &v)) {
        if (t_node_is_alive((t_node *)v)) count++;
    }
    return count;
}

void t_cluster_foreach(t_cluster *cluster, t_cluster_node_fn fn, void *ud) {
    if (!cluster || !fn) return;
    t_map_iter it = t_map_iter_begin(&cluster->nodes);
    const char *k;
    void *v;
    while (t_map_iter_next(&it, &k, &v))
        fn((t_node *)v, ud);
}

static int peer_host_char(unsigned char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
           (c >= '0' && c <= '9') || c == '.' || c == '-';
}

int t_cluster_parse_peers(const char *list, t_cluster_peer_spec *out,
                          size_t cap, size_t *n) {
    if (!n) return -1;
    *n = 0;
    if (!list || !list[0]) return 0;
    if (cap > 0 && !out) return -1;
    size_t len = strlen(list);
    if (list[0] == ',' || list[len - 1] == ',') return -1;
    const char *p = list;
    while (*p) {
        if (*n >= cap) return -1;
        if (*p < '1' || *p > '9') return -1;
        uint64_t id = 0;
        while (*p >= '0' && *p <= '9') {
            if (id > (UINT64_MAX - (uint64_t)(*p - '0')) / 10) return -1;
            id = id * 10 + (uint64_t)(*p - '0');
            p++;
        }
        if (id == 0 || *p != '@') return -1;
        p++;
        const char *h0 = p;
        while (*p && *p != ':' && *p != ',') {
            if (!peer_host_char((unsigned char)*p)) return -1;
            p++;
        }
        size_t hlen = (size_t)(p - h0);
        if (hlen == 0 || hlen > T_CLUSTER_PEER_HOST_MAX) return -1;
        if (*p != ':') return -1;
        p++;
        if (*p < '1' || *p > '9') return -1;
        unsigned long port = 0;
        while (*p >= '0' && *p <= '9') {
            port = port * 10 + (unsigned long)(*p - '0');
            if (port > 65535ul) return -1;
            p++;
        }
        if (port < 1 || port > 65535ul) return -1;
        unsigned long client_port = 0;
        if (*p == '/') {
            p++;
            if (*p < '1' || *p > '9') return -1;
            while (*p >= '0' && *p <= '9') {
                client_port = client_port * 10 + (unsigned long)(*p - '0');
                if (client_port > 65535ul) return -1;
                p++;
            }
            if (client_port < 1 || client_port > 65535ul) return -1;
        }
        if (*p && *p != ',') return -1;
        for (size_t i = 0; i < *n; i++) {
            if (out[i].id == id) return -1;
        }
        out[*n].id = id;
        memcpy(out[*n].host, h0, hlen);
        out[*n].host[hlen] = '\0';
        out[*n].port = (uint16_t)port;
        out[*n].client_port = (uint16_t)client_port;
        (*n)++;
        if (*p == ',') p++;
    }
    return 0;
}

int t_cluster_add_peers(t_cluster *cluster, const t_cluster_peer_spec *peers,
                        size_t n) {
    if (!cluster) return -1;
    if (n > 0 && !peers) return -1;
    for (size_t i = 0; i < n; i++) {
        if (peers[i].id == 0 || !peers[i].host[0] || peers[i].port == 0)
            return -1;
        if (t_cluster_add_node(cluster, peers[i].id, peers[i].host,
                               peers[i].port) != 0)
            return -1;
        if (peers[i].client_port) {
            t_node *n = t_cluster_get_node(cluster, peers[i].id);
            if (!n || t_node_set_client_port(n, peers[i].client_port) != 0)
                return -1;
        }
    }
    return 0;
}
