#include "t_admin.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

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
};

static void admin_accept(t_evio *io, int events, void *ud);
static void admin_client_cb(t_evio *io, int events, void *ud);
static void admin_remove_client(t_admin *admin, t_admin_client *c);

static void set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) flags = 0;
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int find_actual_port(int fd) {
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);
    if (getsockname(fd, (struct sockaddr *)&addr, &addrlen) == 0) {
        return ntohs(addr.sin_port);
    }
    return 0;
}

t_admin *t_admin_create(t_evloop *loop, const char *host, int port) {
    t_admin *a = (t_admin *)calloc(1, sizeof(*a));
    if (!a) return NULL;
    a->loop = loop;
    a->host = host ? strdup(host) : strdup("127.0.0.1");
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
    t_admin_stop(admin);
    free(admin->host);
    free(admin);
}

int t_admin_start(t_admin *admin) {
    if (!admin || admin->running) return -1;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_nonblock(fd);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)admin->port);
    inet_pton(AF_INET, admin->host, &addr.sin_addr);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 16) != 0) {
        close(fd);
        return -1;
    }

    admin->listen_fd = fd;
    admin->port = find_actual_port(fd);
    admin->listen_io.fd = fd;
    admin->listen_io.callback = admin_accept;
    admin->listen_io.user_data = admin;
    admin->listen_io.loop = admin->loop;
    admin->listen_io.events = T_EV_READ;

    t_evloop_add(admin->loop, &admin->listen_io, T_EV_READ);
    admin->running = 1;
    return 0;
}

void t_admin_stop(t_admin *admin) {
    if (!admin || !admin->running) return;
    if (admin->listen_fd >= 0) {
        t_evloop_del(admin->loop, &admin->listen_io);
        close(admin->listen_fd);
        admin->listen_fd = -1;
    }
    for (size_t i = 0; i < admin->client_count; i++) {
        t_admin_client *c = admin->clients[i];
        if (c) {
            t_evloop_del(admin->loop, &c->io);
            close(c->fd);
            free(c);
            admin->clients[i] = NULL;
        }
    }
    admin->client_count = 0;
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
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);
    int fd = accept(io->fd, (struct sockaddr *)&addr, &addrlen);
    if (fd < 0) return;
    set_nonblock(fd);

    if (admin->client_count >= T_ADMIN_MAX_CLIENTS) {
        close(fd);
        return;
    }

    t_admin_client *c = (t_admin_client *)calloc(1, sizeof(*c));
    if (!c) { close(fd); return; }
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

    t_evloop_add(admin->loop, &c->io, T_EV_READ);

    size_t slot = admin->client_count;
    admin->clients[slot] = c;
    admin->client_count++;
}

static void admin_remove_client(t_admin *admin, t_admin_client *c) {
    t_evloop_del(admin->loop, &c->io);
    close(c->fd);
    for (size_t i = 0; i < admin->client_count; i++) {
        if (admin->clients[i] == c) {
            admin->clients[i] = admin->clients[admin->client_count - 1];
            admin->clients[admin->client_count - 1] = NULL;
            break;
        }
    }
    admin->client_count--;
    free(c);
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

static void build_response(t_admin *admin, t_admin_client *c) {
    t_admin_stats stats;
    memset(&stats, 0, sizeof(stats));
    stats.version = T_ADMIN_VERSION;
    if (admin->stats_cb) {
        admin->stats_cb(&stats, admin->stats_ud);
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
        "\"node_id\":\"%s\""
        "}}",
        stats.version ? stats.version : T_ADMIN_VERSION,
        stats.uptime_ms,
        stats.connections, stats.messages_in, stats.messages_out,
        stats.bytes_in, stats.bytes_out,
        stats.queues, stats.subscriptions, stats.cluster_nodes,
        stats.cluster_role ? stats.cluster_role : "",
        stats.cluster_leader ? stats.cluster_leader : "",
        stats.node_id ? stats.node_id : ""
    );
    if (jlen < 0) jlen = 0;
    if ((size_t)jlen >= sizeof(json)) jlen = (int)(sizeof(json) - 1);

    c->resp_len = (size_t)snprintf(c->resp, sizeof(c->resp),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n"
        "%s",
        jlen, json);
    c->resp_sent = 0;
}

static void admin_client_cb(t_evio *io, int events, void *ud) {
    (void)io;
    t_admin_client *c = (t_admin_client *)ud;
    t_admin *admin = c->admin;
    if (events & T_EV_READ) {
        ssize_t r = read(c->fd, c->buf + c->len, T_ADMIN_BUF_SIZE - c->len - 1);
        if (r <= 0) {
            admin_remove_client(admin, c);
            return;
        }
        c->len += (size_t)r;
        c->buf[c->len] = '\0';

        if (headers_complete(c->buf, c->len) > 0) {
            if (c->len >= 3 && c->buf[0] == 'G' && c->buf[1] == 'E' && c->buf[2] == 'T') {
                char path[128] = {0};
                sscanf(c->buf, "GET %127s", path);
                if (strcmp(path, "/") == 0 || strcmp(path, "/stats") == 0) {
                    build_response(admin, c);
                    t_evloop_mod(admin->loop, &c->io, T_EV_WRITE);
                    return;
                }
            }
            const char *resp404 = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            strncpy(c->resp, resp404, sizeof(c->resp) - 1);
            c->resp_len = strlen(resp404);
            c->resp_sent = 0;
            t_evloop_mod(admin->loop, &c->io, T_EV_WRITE);
            return;
        }
        if (c->len >= T_ADMIN_BUF_SIZE - 1) {
            admin_remove_client(admin, c);
        }
    }
    if (events & T_EV_WRITE) {
        while (c->resp_sent < c->resp_len) {
            ssize_t w = write(c->fd, c->resp + c->resp_sent, c->resp_len - c->resp_sent);
            if (w > 0) {
                c->resp_sent += (size_t)w;
            } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return;
            } else {
                admin_remove_client(admin, c);
                return;
            }
        }
        admin_remove_client(admin, c);
    }
}
