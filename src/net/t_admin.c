#include "t_admin.h"
#include "t_socket.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define T_ADMIN_VERSION "0.1.0"
#define T_ADMIN_MAX_CLIENTS 64
#define T_ADMIN_BUF_SIZE 4096
#define T_ADMIN_RESP_SIZE 2048

typedef struct {
    int       fd;
    t_evio    io;
    t_admin  *admin;
    char      buf[T_ADMIN_BUF_SIZE];
    size_t    len;
    char      resp[T_ADMIN_RESP_SIZE];
    size_t    resp_len;
    size_t    resp_sent;
    int       in_io_cb;
    int       free_pending;
} t_admin_client;

struct t_admin {
    t_evloop       *loop;
    char           *host;
    int             port;
    int             listen_fd;
    t_evio          listen_io;
    int             running;
    t_admin_stats_cb stats_cb;
    void           *stats_ud;
    t_admin_client *clients[T_ADMIN_MAX_CLIENTS];
    size_t          client_count;
    int             in_cb;        /* accept / client callback nesting */
    int             free_pending; /* destroy deferred until in_cb == 0 */
};

static void admin_accept(t_evio *io, int events, void *ud);
static void admin_client_cb(t_evio *io, int events, void *ud);
static void admin_remove_client(t_admin *admin, t_admin_client *c);

t_admin *t_admin_create(t_evloop *loop, const char *host, int port) {
    t_admin *a = (t_admin *)calloc(1, sizeof(*a));
    if (!a) return NULL;
    a->loop = loop;
    a->host = host ? strdup(host) : strdup("127.0.0.1");
    if (!a->host) {
        free(a);
        return NULL;
    }
    a->port = port;
    a->listen_fd = -1;
    a->running = 0;
    a->stats_cb = NULL;
    a->stats_ud = NULL;
    a->client_count = 0;
    return a;
}

void t_admin_destroy(t_admin *admin) {
    if (!admin) return;
    if (admin->in_cb > 0) {
        admin->free_pending = 1;
        return;
    }
    admin->free_pending = 0;
    t_admin_stop(admin);
    free(admin->host);
    free(admin);
}

int t_admin_start(t_admin *admin) {
    if (!admin || admin->running) return -1;
    if (admin->port < 0 || admin->port > 65535) return -1;
    int fd = t_socket_create(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    t_socket_set_reuseaddr(fd);

    t_sockaddr addr;
    if (t_sockaddr_init_ipv4(&addr, admin->host, (uint16_t)admin->port) != 0) {
        t_socket_close(fd);
        return -1;
    }
    if (t_socket_bind(fd, &addr) != 0) {
        t_socket_close(fd);
        return -1;
    }
    if (t_socket_listen(fd, 16) != 0) {
        t_socket_close(fd);
        return -1;
    }

    int actual = (int)t_socket_local_port(fd);
    if (actual <= 0) {
        t_socket_close(fd);
        return -1;
    }
    admin->listen_fd = fd;
    admin->port = actual;
    admin->listen_io.fd = fd;
    admin->listen_io.callback = admin_accept;
    admin->listen_io.user_data = admin;
    admin->listen_io.loop = admin->loop;
    admin->listen_io.events = T_EV_READ;

    if (t_evloop_add(admin->loop, &admin->listen_io, T_EV_READ) != 0) {
        t_socket_close(fd);
        admin->listen_fd = -1;
        return -1;
    }
    admin->running = 1;
    return 0;
}

void t_admin_stop(t_admin *admin) {
    if (!admin || !admin->running) return;
    if (admin->listen_fd >= 0) {
        t_evloop_del(admin->loop, &admin->listen_io);
        t_socket_close(admin->listen_fd);
        admin->listen_fd = -1;
    }
    while (admin->client_count > 0) {
        t_admin_client *c = admin->clients[0];
        if (!c) break;
        admin_remove_client(admin, c);
    }
    admin->running = 0;
}

void t_admin_set_stats_cb(t_admin *admin, t_admin_stats_cb cb, void *ud) {
    if (admin) {
        admin->stats_cb = cb;
        admin->stats_ud = ud;
    }
}

const char *t_admin_host(const t_admin *admin) {
    return admin ? admin->host : NULL;
}

int t_admin_port(const t_admin *admin) {
    return admin ? admin->port : 0;
}

int t_admin_is_running(const t_admin *admin) {
    return admin ? admin->running : 0;
}

static void admin_accept(t_evio *io, int events, void *ud) {
    (void)events;
    t_admin *admin = (t_admin *)ud;
    if (!admin) return;
    admin->in_cb++;
    int fd = t_socket_accept(io->fd, NULL);
    if (fd < 0) {
        admin->in_cb--;
        if (admin->free_pending) t_admin_destroy(admin);
        return;
    }

    if (admin->client_count >= T_ADMIN_MAX_CLIENTS) {
        t_socket_close(fd);
        admin->in_cb--;
        if (admin->free_pending) t_admin_destroy(admin);
        return;
    }

    t_admin_client *c = (t_admin_client *)calloc(1, sizeof(*c));
    if (!c) {
        t_socket_close(fd);
        admin->in_cb--;
        if (admin->free_pending) t_admin_destroy(admin);
        return;
    }
    c->fd = fd;
    c->admin = admin;
    c->io.fd = fd;
    c->io.callback = admin_client_cb;
    c->io.user_data = c;
    c->io.loop = admin->loop;
    c->io.events = T_EV_READ;
    c->len = 0;
    c->resp_len = 0;
    c->resp_sent = 0;

    if (t_evloop_add(admin->loop, &c->io, T_EV_READ) != 0) {
        t_socket_close(fd);
        free(c);
        admin->in_cb--;
        if (admin->free_pending) t_admin_destroy(admin);
        return;
    }

    size_t slot = admin->client_count;
    admin->clients[slot] = c;
    admin->client_count++;
    admin->in_cb--;
    if (admin->free_pending) t_admin_destroy(admin);
}

static void admin_remove_client(t_admin *admin, t_admin_client *c) {
    if (!admin || !c) return;
    t_evloop_del(admin->loop, &c->io);
    c->io.callback = NULL;
    c->io.user_data = NULL;
    if (c->fd >= 0) {
        t_socket_close(c->fd);
        c->fd = -1;
    }
    int found = 0;
    for (size_t i = 0; i < admin->client_count; i++) {
        if (admin->clients[i] == c) {
            admin->clients[i] = admin->clients[admin->client_count - 1];
            admin->clients[admin->client_count - 1] = NULL;
            admin->client_count--;
            found = 1;
            break;
        }
    }
    if (!found) return; /* already removed: avoid count skew / double free */
    if (c->in_io_cb) {
        c->free_pending = 1;
        return;
    }
    t_evloop_defer_free(admin->loop, c);
}

static void admin_set_error_resp(t_admin_client *c, int code) {
    int n = snprintf(c->resp, sizeof(c->resp),
        "HTTP/1.1 %d Error\r\n"
        "Content-Length: 0\r\n"
        "Connection: close\r\n"
        "\r\n",
        code);
    if (n < 0 || (size_t)n >= sizeof(c->resp)) {
        c->resp_len = 0;
        c->resp_sent = 0;
        return;
    }
    c->resp_len = (size_t)n;
    c->resp_sent = 0;
}

static int headers_complete(const char *buf, size_t len) {
    if (len < 4) return 0;
    for (size_t i = 0; i + 3 < len; i++) {
        if (buf[i] == '\r' && buf[i+1] == '\n' && buf[i+2] == '\r' && buf[i+3] == '\n') {
            return (int)(i + 4);
        }
    }
    return 0;
}

/* Escape JSON string into dst; returns bytes written (excl NUL) or -1. */
static int json_escape(char *dst, size_t dst_cap, const char *src) {
    if (!dst || dst_cap == 0) return -1;
    if (!src) src = "";
    size_t o = 0;
    for (const unsigned char *p = (const unsigned char *)src; *p; ++p) {
        const char *esc = NULL;
        char hex[7];
        if (*p == '"' || *p == '\\') {
            hex[0] = '\\'; hex[1] = (char)*p; hex[2] = '\0';
            esc = hex;
        } else if (*p == '\n') {
            esc = "\\n";
        } else if (*p == '\r') {
            esc = "\\r";
        } else if (*p == '\t') {
            esc = "\\t";
        } else if (*p < 0x20) {
            snprintf(hex, sizeof(hex), "\\u%04x", *p);
            esc = hex;
        }
        if (esc) {
            size_t el = strlen(esc);
            if (o + el >= dst_cap) return -1;
            memcpy(dst + o, esc, el);
            o += el;
        } else {
            if (o + 1 >= dst_cap) return -1;
            dst[o++] = (char)*p;
        }
    }
    dst[o] = '\0';
    return (int)o;
}

static void admin_json_resp(t_admin_client *c, int code, const char *reason,
                            const char *json) {
    if (!c || !reason || !json) {
        if (c) admin_set_error_resp(c, 500);
        return;
    }
    int jlen = (int)strlen(json);
    int hlen = snprintf(c->resp, sizeof(c->resp),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n%s",
        code, reason, jlen, json);
    if (hlen < 0 || (size_t)hlen >= sizeof(c->resp)) {
        admin_set_error_resp(c, 500);
        return;
    }
    c->resp_len = (size_t)hlen;
    c->resp_sent = 0;
}

static void admin_fill_stats(t_admin *admin, t_admin_stats *stats) {
    memset(stats, 0, sizeof(*stats));
    stats->version = T_ADMIN_VERSION;
    if (admin && admin->stats_cb)
        admin->stats_cb(stats, admin->stats_ud);
}

static void build_health(t_admin_client *c) {
    admin_json_resp(c, 200, "OK", "{\"status\":\"ok\"}");
}

static void build_ready(t_admin *admin, t_admin_client *c) {
    t_admin_stats stats;
    admin_fill_stats(admin, &stats);
    if (stats.ready)
        admin_json_resp(c, 200, "OK", "{\"ready\":true}");
    else
        admin_json_resp(c, 503, "Service Unavailable", "{\"ready\":false}");
}

static void build_response(t_admin *admin, t_admin_client *c) {
    t_admin_stats stats;
    admin_fill_stats(admin, &stats);

    char esc_ver[64], esc_role[64], esc_leader[64], esc_node[64];
    if (json_escape(esc_ver, sizeof(esc_ver), stats.version ? stats.version : T_ADMIN_VERSION) < 0 ||
        json_escape(esc_role, sizeof(esc_role), stats.cluster_role ? stats.cluster_role : "") < 0 ||
        json_escape(esc_leader, sizeof(esc_leader), stats.cluster_leader ? stats.cluster_leader : "") < 0 ||
        json_escape(esc_node, sizeof(esc_node), stats.node_id ? stats.node_id : "") < 0) {
        admin_set_error_resp(c, 500);
        return;
    }

    char json[1024];
    int jlen = snprintf(json, sizeof(json),
        "{\"name\":\"transit\",\"version\":\"%s\","
        "\"uptime_ms\":%zu,"
        "\"stats\":{"
        "\"connections\":%zu,"
        "\"messages_in\":%zu,"
        "\"messages_out\":%zu,"
        "\"bytes_in\":%zu,"
        "\"bytes_out\":%zu,"
        "\"queues\":%zu,"
        "\"subscriptions\":%zu,"
        "\"cluster_nodes\":%zu,"
        "\"cluster_role\":\"%s\","
        "\"cluster_leader\":\"%s\","
        "\"node_id\":\"%s\","
        "\"ready\":%s"
        "}}",
        esc_ver,
        stats.uptime_ms,
        stats.connections, stats.messages_in, stats.messages_out,
        stats.bytes_in, stats.bytes_out,
        stats.queues, stats.subscriptions, stats.cluster_nodes,
        esc_role, esc_leader, esc_node,
        stats.ready ? "true" : "false"
    );
    if (jlen < 0) jlen = 0;
    if ((size_t)jlen >= sizeof(json)) jlen = (int)(sizeof(json) - 1);

    /* Keep Content-Length consistent with the body that actually fits. */
    char header[160];
    int hlen = snprintf(header, sizeof(header),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n",
        jlen);
    if (hlen < 0 || (size_t)hlen >= sizeof(header)) {
        admin_set_error_resp(c, 500);
        return;
    }
    if ((size_t)hlen + (size_t)jlen >= sizeof(c->resp)) {
        /* Oversized body: fail closed instead of truncating JSON mid-token. */
        admin_set_error_resp(c, 500);
        return;
    }
    memcpy(c->resp, header, (size_t)hlen);
    memcpy(c->resp + hlen, json, (size_t)jlen);
    c->resp_len = (size_t)hlen + (size_t)jlen;
    c->resp[c->resp_len] = '\0';
    c->resp_sent = 0;
}

static void admin_client_cb(t_evio *io, int events, void *ud) {
    (void)io;
    t_admin_client *c = (t_admin_client *)ud;
    t_admin *admin = c->admin;
    if (!admin) return;
    admin->in_cb++;
    c->in_io_cb = 1;
    if ((events & T_EV_ERROR) && !c->free_pending) {
        admin_remove_client(admin, c);
        goto out;
    }
    if (events & T_EV_READ) {
        if (c->fd < 0 || c->free_pending) goto out;
        /* Do not parse a new request while a response is still flushing. */
        if (c->resp_len > 0 && c->resp_sent < c->resp_len) goto out;
        ssize_t r = t_socket_read(c->fd, c->buf + c->len, T_ADMIN_BUF_SIZE - c->len - 1);
        if (r < 0) {
            if (t_socket_again() || t_socket_intr()) goto out;
            admin_remove_client(admin, c);
            goto out;
        }
        if (r == 0) {
            admin_remove_client(admin, c);
            goto out;
        }
        c->len += (size_t)r;
        c->buf[c->len] = '\0';

        if (headers_complete(c->buf, c->len) > 0) {
            if (c->len >= 3 && c->buf[0] == 'G' && c->buf[1] == 'E' && c->buf[2] == 'T') {
                char path[128] = {0};
                sscanf(c->buf, "GET %127s", path);
                if (strcmp(path, "/health") == 0) {
                    build_health(c);
                    if (!c->free_pending && c->fd >= 0 &&
                        t_evloop_mod(admin->loop, &c->io, T_EV_WRITE) != 0)
                        admin_remove_client(admin, c);
                    goto out;
                }
                if (strcmp(path, "/ready") == 0) {
                    build_ready(admin, c);
                    if (!c->free_pending && c->fd >= 0 &&
                        t_evloop_mod(admin->loop, &c->io, T_EV_WRITE) != 0)
                        admin_remove_client(admin, c);
                    goto out;
                }
                if (strcmp(path, "/") == 0 || strcmp(path, "/stats") == 0) {
                    build_response(admin, c);
                    if (!c->free_pending && c->fd >= 0 &&
                        t_evloop_mod(admin->loop, &c->io, T_EV_WRITE) != 0)
                        admin_remove_client(admin, c);
                    goto out;
                }
            }
            const char *resp404 = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            strncpy(c->resp, resp404, sizeof(c->resp) - 1);
            c->resp_len = strlen(resp404);
            c->resp_sent = 0;
            if (!c->free_pending && c->fd >= 0 &&
                t_evloop_mod(admin->loop, &c->io, T_EV_WRITE) != 0)
                admin_remove_client(admin, c);
            goto out;
        }
        if (c->len >= T_ADMIN_BUF_SIZE - 1) {
            admin_remove_client(admin, c);
            goto out;
        }
    }
    if ((events & T_EV_WRITE) && !c->free_pending && c->fd >= 0) {
        while (c->resp_sent < c->resp_len) {
            ssize_t w = t_socket_write(c->fd, c->resp + c->resp_sent, c->resp_len - c->resp_sent);
            if (w > 0) {
                c->resp_sent += (size_t)w;
            } else if (w < 0 && (t_socket_again() || t_socket_intr())) {
                goto out;
            } else {
                admin_remove_client(admin, c);
                goto out;
            }
        }
        admin_remove_client(admin, c);
    }
out:
    c->in_io_cb = 0;
    if (c->free_pending) t_evloop_defer_free(admin->loop, c);
    admin->in_cb--;
    if (admin->free_pending) t_admin_destroy(admin);
}
