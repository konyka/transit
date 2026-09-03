#ifndef T_PEER_H
#define T_PEER_H

#include "t_compiler.h"
#include "t_evloop.h"
#include "t_raft.h"
#include "t_cluster.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_peer t_peer;

typedef struct t_peer_config {
    const char *host; /* default 127.0.0.1 */
    uint16_t    port; /* 0 = ephemeral; default 4223 */
} t_peer_config;

void t_peer_config_init(t_peer_config *cfg);

t_peer   *t_peer_create(t_evloop *loop, t_raft *raft, t_cluster *cluster,
                        const t_peer_config *cfg);
void      t_peer_destroy(t_peer *peer);
int       t_peer_start(t_peer *peer);
void      t_peer_stop(t_peer *peer);
int       t_peer_campaign(t_peer *peer);

int       t_peer_is_running(const t_peer *peer);
uint16_t  t_peer_port(const t_peer *peer);
const char *t_peer_host(const t_peer *peer);
int       t_peer_is_leader(const t_peer *peer);

#endif /* T_PEER_H */
