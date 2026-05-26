#ifndef T_ADMIN_H
#define T_ADMIN_H

#include "t_evloop.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_admin t_admin;

typedef struct {
    size_t connections;
    size_t messages_in;
    size_t messages_out;
    size_t bytes_in;
    size_t bytes_out;
    size_t uptime_ms;
    size_t queues;
    size_t subscriptions;
    size_t cluster_nodes;
    const char *version;
    const char *node_id;
    const char *cluster_leader;
    const char *cluster_role;
} t_admin_stats;

typedef void (*t_admin_stats_cb)(t_admin_stats *stats, void *ud);

t_admin *t_admin_create(t_evloop *loop, const char *host, int port);
void     t_admin_destroy(t_admin *admin);
int      t_admin_start(t_admin *admin);
void     t_admin_stop(t_admin *admin);

void t_admin_set_stats_cb(t_admin *admin, t_admin_stats_cb cb, void *ud);

const char *t_admin_host(const t_admin *admin);
int         t_admin_port(const t_admin *admin);
int         t_admin_is_running(const t_admin *admin);

#endif
