#ifndef T_CLUSTER_H
#define T_CLUSTER_H

#include "t_node.h"
#include <stdint.h>

typedef struct t_cluster t_cluster;

t_cluster *t_cluster_create(uint64_t self_id);
void       t_cluster_destroy(t_cluster *cluster);
int        t_cluster_add_node(t_cluster *cluster, uint64_t node_id,
                            const char *host, uint16_t port);
int        t_cluster_remove_node(t_cluster *cluster, uint64_t node_id);
size_t     t_cluster_node_count(const t_cluster *cluster);
t_node    *t_cluster_get_node(t_cluster *cluster, uint64_t node_id);
t_node    *t_cluster_get_leader(t_cluster *cluster);
int        t_cluster_set_leader(t_cluster *cluster, uint64_t node_id);
uint64_t   t_cluster_self_id(const t_cluster *cluster);
int        t_cluster_is_leader(const t_cluster *cluster);
size_t     t_cluster_alive_count(const t_cluster *cluster);
typedef void (*t_cluster_node_fn)(t_node *node, void *ud);
void       t_cluster_foreach(t_cluster *cluster, t_cluster_node_fn fn, void *ud);

/* Static membership: "id@host:peer[/client],...". Fail closed on junk. */
#define T_CLUSTER_PEER_HOST_MAX 63
#define T_CLUSTER_PEERS_MAX     64

typedef struct t_cluster_peer_spec {
    uint64_t id;
    char     host[T_CLUSTER_PEER_HOST_MAX + 1];
    uint16_t port;
    uint16_t client_port; /* 0 = unknown; do not use as a client hint */
} t_cluster_peer_spec;

int t_cluster_parse_peers(const char *list, t_cluster_peer_spec *out,
                          size_t cap, size_t *n);
int t_cluster_add_peers(t_cluster *cluster, const t_cluster_peer_spec *peers,
                        size_t n);

#endif
