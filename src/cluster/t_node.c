#include "t_node.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Internal definition of a cluster node */
struct t_node {
    uint64_t id;
    char *host;
    uint16_t port;
    t_nrole role;
    int alive;
    uint64_t term;
};

t_node *t_node_create(uint64_t node_id, const char *host, uint16_t port) {
    if (host == NULL) {
        return NULL;
    }
    t_node *n = (t_node *)calloc(1, sizeof(t_node));
    if (!n) return NULL;
    n->id = node_id;
    n->port = port;
    n->host = strdup(host);
    if (!n->host) {
        free(n);
        return NULL;
    }
    n->role = T_NODE_FOLLOWER;
    n->alive = 1;
    n->term = 0;
    return n;
}

void t_node_destroy(t_node *node) {
    if (!node) return;
    free(node->host);
    free(node);
}

uint64_t t_node_id(const t_node *node) {
    return node ? node->id : 0;
}

const char *t_node_host(const t_node *node) {
    return node ? node->host : NULL;
}

uint16_t t_node_port(const t_node *node) {
    return node ? node->port : 0;
}

t_nrole t_node_get_role(const t_node *node) {
    return node ? node->role : T_NODE_FOLLOWER;
}

int t_node_is_leader(const t_node *node) {
    return node ? (node->role == T_NODE_LEADER) : 0;
}

void t_node_set_role(t_node *node, t_nrole role) {
    if (node) node->role = role;
}

int t_node_is_alive(const t_node *node) {
    return node ? node->alive : 0;
}

void t_node_set_alive(t_node *node, int alive) {
    if (node) node->alive = alive ? 1 : 0;
}

uint64_t t_node_term(const t_node *node) {
    return node ? node->term : 0;
}

void t_node_set_term(t_node *node, uint64_t term) {
    if (node) node->term = term;
}
