#ifndef T_NODE_H
#define T_NODE_H

#include "t_compiler.h"
#include <stdint.h>

typedef enum t_nrole {
    T_NODE_FOLLOWER = 0,
    T_NODE_CANDIDATE,
    T_NODE_LEADER
} t_nrole;

typedef struct t_node t_node;

t_node     *t_node_create(uint64_t node_id, const char *host, uint16_t port);
void        t_node_destroy(t_node *node);
uint64_t    t_node_id(const t_node *node);
const char *t_node_host(const t_node *node);
uint16_t    t_node_port(const t_node *node);
t_nrole     t_node_get_role(const t_node *node);
int         t_node_is_leader(const t_node *node);
void        t_node_set_role(t_node *node, t_nrole role);
int         t_node_is_alive(const t_node *node);
void        t_node_set_alive(t_node *node, int alive);
uint64_t    t_node_term(const t_node *node);
void        t_node_set_term(t_node *node, uint64_t term);

#endif /* T_NODE_H */
