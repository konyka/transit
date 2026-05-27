#include "t_config.h"
#include "t_admin.h"
#include "t_signal.h"
#include "t_evloop.h"
#include "t_broker.h"
#include "t_dispatch.h"
#include "t_version.h"
#include "t_log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

static t_evloop *g_loop;
static t_broker *g_broker;
static t_admin  *g_admin;
static t_config *g_config;

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
    fprintf(stderr, "  -h <host>   Listen host (default: 0.0.0.0)\n");
    fprintf(stderr, "  -p <port>   Client listen port (default: 4222)\n");
    fprintf(stderr, "  -a <port>   Admin stats port (default: 8222)\n");
    fprintf(stderr, "  -v          Print version and exit\n");
    fprintf(stderr, "  --help      Show this help\n");
}

int main(int argc, char **argv) {
    const char *config_file = NULL;
    const char *host = "0.0.0.0";
    int client_port = 4222;
    int admin_port = 8222;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
            config_file = argv[++i];
        } else if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            client_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-a") == 0 && i + 1 < argc) {
            admin_port = atoi(argv[++i]);
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

    g_admin = t_admin_create(g_loop, "127.0.0.1", admin_port);
    if (g_admin) {
        t_admin_set_stats_cb(g_admin, on_stats, NULL);
        if (t_admin_start(g_admin) == 0) {
            fprintf(stdout, "transit %s admin listening on 127.0.0.1:%d\n",
                    t_version(), t_admin_port(g_admin));
        }
    }

    fprintf(stdout, "transit %s ready on %s:%d\n", t_version(), host, client_port);
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
    t_broker_destroy(g_broker);
    t_evloop_destroy(g_loop);
    t_config_destroy(g_config);

    fprintf(stdout, "Bye.\n");
    return 0;
}
