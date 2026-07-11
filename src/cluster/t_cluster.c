#include "t_cluster.h"
#include "t_node.h"
#include "t_map.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

struct t_cluster {
    uint64_t self_id;
    t_map nodes;
    t_node *leader;
};

static void make_key(char *buf, size_t sz, uint64_t id) {
    snprintf(buf, sz, "%lu", (unsigned long)id);
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
    if (cluster->leader == (t_node *)p) cluster->leader = NULL;
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
    return cluster ? cluster->leader : NULL;
}

int t_cluster_set_leader(t_cluster *cluster, uint64_t node_id) {
    if (!cluster) return -1;
    t_node *n = t_cluster_get_node(cluster, node_id);
    if (!n) return -1;
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
    if (!cluster) return 0;
    return cluster->leader && t_node_id(cluster->leader) == cluster->self_id;
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
