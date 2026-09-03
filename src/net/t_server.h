#ifndef T_SERVER_H
#define T_SERVER_H

#include "t_compiler.h"
#include "t_evloop.h"
#include "t_broker.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_server t_server;

typedef struct t_server_config {
    const char *host;          /* default 127.0.0.1; IPv4 only */
    uint16_t    port;          /* 0 = ephemeral */
    size_t      max_conns;     /* default 1024 */
    size_t      rate_tokens;   /* burst; default 128; 0 = always busy */
    double      rate_refill;   /* tokens per millisecond */
    int64_t     idle_timeout_ms; /* 0 = disabled */
    const uint8_t *psk;        /* NULL = no AUTH; required off loopback */
    size_t         psk_len;
} t_server_config;

void t_server_config_init(t_server_config *cfg);

t_server *t_server_create(t_evloop *loop, t_broker *broker, const t_server_config *cfg);
void      t_server_destroy(t_server *srv);
int       t_server_start(t_server *srv);
void      t_server_stop(t_server *srv);

int       t_server_is_running(const t_server *srv);
uint16_t  t_server_port(const t_server *srv);
const char *t_server_host(const t_server *srv);
size_t    t_server_conn_count(const t_server *srv);
size_t    t_server_dropped_conns(const t_server *srv);
size_t    t_server_msgs_in(const t_server *srv);
size_t    t_server_msgs_dropped(const t_server *srv);

#endif /* T_SERVER_H */
