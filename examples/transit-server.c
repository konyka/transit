#include "t_config.h"
#include "t_admin.h"
#include "t_signal.h"
#include "t_evloop.h"
#include "t_broker.h"
#include "t_server.h"
#include "t_dispatch.h"
#include "t_version.h"
#include "t_log.h"
#include "t_cluster.h"
#include "t_raft.h"
#include "t_peer.h"
#include "t_hmac.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

static t_evloop *g_loop;
static t_broker *g_broker;
static t_server *g_server;
static t_admin  *g_admin;
static t_config *g_config;
static t_cluster *g_cluster;
static t_raft   *g_raft;
static t_peer   *g_peer;

static void on_stats(t_admin_stats *stats, void *ud) {
    (void)ud;
    stats->version = t_version();
    stats->node_id = "transit-0";
    stats->cluster_role = "leader";
    stats->cluster_leader = "transit-0";
}

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s [options]\n", prog);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -c <file>   Configuration file (INI format)\n");
    fprintf(stderr, "  -h <host>   Listen host (default: 127.0.0.1)\n");
    fprintf(stderr, "  -p <port>   Client listen port (default: 4222)\n");
    fprintf(stderr, "  -a <port>   Admin stats port (default: 8222)\n");
    fprintf(stderr, "  -d <dir>    Durable WAL directory (mkdir 0700)\n");
    fprintf(stderr, "  -C <port>   Cluster peer port (opt-in; default bind 127.0.0.1)\n");
    fprintf(stderr, "  -k <psk>    Client AUTH pre-shared key (required off loopback)\n");
    fprintf(stderr, "  -v          Print version and exit\n");
    fprintf(stderr, "  --help      Show this help\n");
}

int main(int argc, char **argv) {
    const char *config_file = NULL;
    const char *host = "127.0.0.1";
    const char *datadir = NULL;
    int client_port = 4222;
    int admin_port = 8222;
    int cluster_port = -1;
    const char *psk = NULL;
    int push_credits = -1;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
            config_file = argv[++i];
        } else if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            client_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-a") == 0 && i + 1 < argc) {
            admin_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
            datadir = argv[++i];
        } else if (strcmp(argv[i], "-C") == 0 && i + 1 < argc) {
            cluster_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-k") == 0 && i + 1 < argc) {
            psk = argv[++i];
        } else if (strcmp(argv[i], "-v") == 0) {
            printf("transit %s\n", t_version());
            return 0;
        } else if (strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage(argv[0]);
            return 1;
        }
    }

    g_config = t_config_create();
    if (config_file) {
        if (t_config_parse_file(g_config, config_file) != 0) {
            fprintf(stderr, "Failed to parse config: %s\n", config_file);
            return 1;
        }
        const char *ch = t_config_get(g_config, "server", "host");
        if (ch) host = ch;
        int cp = t_config_get_int(g_config, "server", "port", 0);
        if (cp > 0) client_port = cp;
        int ap = t_config_get_int(g_config, "admin", "port", 0);
        if (ap > 0) admin_port = ap;
        const char *dd = t_config_get(g_config, "storage", "datadir");
        if (dd && dd[0]) datadir = dd;
        int kp = t_config_get_int(g_config, "cluster", "port", -1);
        if (cluster_port < 0 && kp >= 0) cluster_port = kp;
        const char *ak = t_config_get(g_config, "auth", "psk");
        if (ak && ak[0] && !psk) psk = ak;
        int pc = t_config_get_int(g_config, "server", "push_credits", -1);
        if (pc >= 0) push_credits = pc;
    }

    t_signal_install();

    g_loop = t_evloop_create();
    if (!g_loop) {
        fprintf(stderr, "Failed to create event loop\n");
        return 1;
    }

    g_broker = t_broker_create("transit-0");
    if (!g_broker) {
        fprintf(stderr, "Failed to create broker\n");
        return 1;
    }
    if (datadir && t_broker_set_datadir(g_broker, datadir) != 0) {
        fprintf(stderr, "Failed to use datadir: %s\n", datadir);
        t_broker_destroy(g_broker);
        t_evloop_destroy(g_loop);
        t_config_destroy(g_config);
        return 1;
    }
    if (t_broker_start(g_broker) != 0) {
        fprintf(stderr, "Failed to start broker\n");
        t_broker_destroy(g_broker);
        t_evloop_destroy(g_loop);
        t_config_destroy(g_config);
        return 1;
    }

    if (client_port < 0 || client_port > 65535) {
        fprintf(stderr, "Invalid client port\n");
        t_broker_destroy(g_broker);
        t_evloop_destroy(g_loop);
        t_config_destroy(g_config);
        return 1;
    }
    t_server_config scfg;
    t_server_config_init(&scfg);
    scfg.host = host;
    scfg.port = (uint16_t)client_port;
    if (push_credits >= 0) scfg.push_credits = (size_t)push_credits;
    if (psk) {
        size_t n = strlen(psk);
        if (n == 0 || n > T_AUTH_PSK_MAX) {
            fprintf(stderr, "Invalid AUTH psk length\n");
            t_broker_destroy(g_broker);
            t_evloop_destroy(g_loop);
            t_config_destroy(g_config);
            return 1;
        }
        scfg.psk = (const uint8_t *)psk;
        scfg.psk_len = n;
    }
    g_server = t_server_create(g_loop, g_broker, &scfg);
    if (!g_server || t_server_start(g_server) != 0) {
        fprintf(stderr, "Failed to listen on %s:%d (non-loopback requires -k / [auth] psk=)\n",
                host, client_port);
        if (g_server) t_server_destroy(g_server);
        t_broker_destroy(g_broker);
        t_evloop_destroy(g_loop);
        t_config_destroy(g_config);
        return 1;
    }

    if (cluster_port >= 0) {
        if (cluster_port > 65535) {
            fprintf(stderr, "Invalid cluster port\n");
            t_server_destroy(g_server);
            t_broker_destroy(g_broker);
            t_evloop_destroy(g_loop);
            t_config_destroy(g_config);
            return 1;
        }
        g_cluster = t_cluster_create(1);
        t_raft_config rcfg;
        memset(&rcfg, 0, sizeof(rcfg));
        rcfg.node_id = 1;
        rcfg.election_timeout_ms = 150;
        rcfg.heartbeat_interval_ms = 50;
        g_raft = t_raft_create(&rcfg);
        if (g_raft && datadir) {
            char rpath[1024];
            int rn = snprintf(rpath, sizeof(rpath), "%s/raft.log", datadir);
            if (rn < 0 || (size_t)rn >= sizeof(rpath) ||
                t_raft_open_log(g_raft, rpath, 1) != 0) {
                fprintf(stderr, "Failed to open raft log in %s\n", datadir);
                if (g_raft) t_raft_destroy(g_raft);
                t_server_destroy(g_server);
                t_broker_destroy(g_broker);
                t_evloop_destroy(g_loop);
                t_config_destroy(g_config);
                return 1;
            }
        }
        t_peer_config pcfg;
        t_peer_config_init(&pcfg);
        pcfg.host = host;
        pcfg.port = (uint16_t)cluster_port;
        g_peer = t_peer_create(g_loop, g_raft, g_cluster, &pcfg);
        if (!g_cluster || !g_raft || !g_peer || t_peer_start(g_peer) != 0) {
            fprintf(stderr, "Failed to listen for cluster peers on %s:%d\n",
                    host, cluster_port);
            if (g_peer) t_peer_destroy(g_peer);
            if (g_raft) t_raft_destroy(g_raft);
            if (g_cluster) t_cluster_destroy(g_cluster);
            t_server_destroy(g_server);
            t_broker_destroy(g_broker);
            t_evloop_destroy(g_loop);
            t_config_destroy(g_config);
            return 1;
        }
        if (t_cluster_add_node(g_cluster, 1, t_peer_host(g_peer),
                               t_peer_port(g_peer)) != 0 ||
            t_broker_set_cluster(g_broker, g_cluster) != 0 ||
            t_broker_set_raft(g_broker, g_raft) != 0 ||
            t_raft_apply_entries(g_raft) != 0 ||
            t_peer_campaign(g_peer) != 0) {
            fprintf(stderr, "Failed to start cluster peer\n");
            t_peer_destroy(g_peer);
            t_raft_destroy(g_raft);
            t_cluster_destroy(g_cluster);
            t_server_destroy(g_server);
            t_broker_destroy(g_broker);
            t_evloop_destroy(g_loop);
            t_config_destroy(g_config);
            return 1;
        }
        fprintf(stdout, "transit cluster peer on %s:%u\n",
                t_peer_host(g_peer), (unsigned)t_peer_port(g_peer));
    }

    g_admin = t_admin_create(g_loop, "127.0.0.1", admin_port);
    if (g_admin) {
        t_admin_set_stats_cb(g_admin, on_stats, NULL);
        if (t_admin_start(g_admin) == 0) {
            fprintf(stdout, "transit %s admin listening on 127.0.0.1:%d\n",
                    t_version(), t_admin_port(g_admin));
        }
    }

    fprintf(stdout, "transit %s ready on %s:%u\n",
            t_version(), t_server_host(g_server), (unsigned)t_server_port(g_server));
    fflush(stdout);

    t_evloop_run(g_loop, 1000);

    while (!t_signal_is_shutdown()) {
        t_evloop_run(g_loop, 1000);
    }

    fprintf(stdout, "\nShutting down...\n");
    if (g_admin) {
        t_admin_stop(g_admin);
        t_admin_destroy(g_admin);
    }
    if (g_server) t_server_destroy(g_server);
    if (g_peer) t_peer_destroy(g_peer);
    t_broker_stop(g_broker);
    t_broker_destroy(g_broker);
    if (g_raft) t_raft_destroy(g_raft);
    if (g_cluster) t_cluster_destroy(g_cluster);
    t_evloop_destroy(g_loop);
    t_config_destroy(g_config);

    fprintf(stdout, "Bye.\n");
    return 0;
}
