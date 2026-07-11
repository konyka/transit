#include "t_tcp.h"
#include "t_socket.h"
#include "t_evloop.h"
#include "t_time.h"
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* Forward declarations */
static void t_tcp_server_accept_cb(t_evio *io, int flags, void *ud);
static void t_tcp_conn_read_cb(t_evio *io, int flags, void *ud);

/* t_tcp_server implementations */
t_tcp_server *t_tcp_server_create(t_evloop *loop) {
    t_tcp_server *srv = (t_tcp_server*)calloc(1, sizeof(*srv));
    if (!srv) return NULL;
    srv->fd = -1;
    srv->loop = loop;
    srv->on_accept = NULL;
    srv->user_data = NULL;
    return srv;
}

void t_tcp_server_destroy(t_tcp_server *srv) {
    if (!srv) return;
    if (srv->fd >= 0) {
        t_socket_close(srv->fd);
        srv->fd = -1;
    }
    // Remove accept io from loop if registered
    t_evloop_del(srv->loop, &srv->accept_io);
    free(srv);
}

static int server_bind_listen(t_tcp_server *srv, const char *ip, uint16_t port) {
    int fd = t_socket_create(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    t_socket_set_reuseaddr(fd);
    t_socket_set_nonblock(fd);

    t_sockaddr addr;
    if (t_sockaddr_init_ipv4(&addr, ip ? ip : "127.0.0.1", port) != 0) {
        t_socket_close(fd);
        return -1;
    }
    if (t_socket_bind(fd, &addr) != 0) {
        t_socket_close(fd);
        return -1;
    }
    if (t_socket_listen(fd, 128) != 0) {
        t_socket_close(fd);
        return -1;
    }
    srv->fd = fd;
    return 0;
}

static void t_tcp_server_accept_cb(t_evio *io, int flags, void *ud) {
    (void)flags;
    (void)ud;
    t_tcp_server *srv = (t_tcp_server *)io->user_data;
    if (!srv || srv->fd < 0) return;
    while (1) {
        t_sockaddr peer;
        int client = t_socket_accept(srv->fd, &peer);
        if (client < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            else break;
        }
        if (srv->on_accept) {
            srv->on_accept(srv, client, &peer, srv->user_data);
        } else {
            t_socket_close(client);
        }
    }
}

int t_tcp_server_listen(t_tcp_server *srv, const char *ip, uint16_t port,
                        t_tcp_accept_cb cb, void *ud) {
    if (!srv) return -1;
    if (srv->fd >= 0) {
        if (srv->loop) t_evloop_del(srv->loop, &srv->accept_io);
        t_socket_close(srv->fd);
        srv->fd = -1;
    }
    if (server_bind_listen(srv, ip, port) != 0) return -1;
    srv->on_accept = cb;
    srv->user_data = ud;
    /* Register accept callback on the event loop */
    srv->accept_io.fd = srv->fd;
    srv->accept_io.events = T_EV_READ;
    srv->accept_io.callback = t_tcp_server_accept_cb;
    srv->accept_io.user_data = srv;
    srv->accept_io.loop = srv->loop;
    int r = t_evloop_add(srv->loop, &srv->accept_io, T_EV_READ);
    if (r != 0) {
        t_socket_close(srv->fd);
        srv->fd = -1;
        return -1;
    }
    return 0;
}

t_tcp_conn *t_tcp_dial(t_evloop *loop, const char *ip, uint16_t port) {
    int fd = t_socket_create(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return NULL;
    t_socket_set_nonblock(fd);
    t_sockaddr addr;
    if (t_sockaddr_init_ipv4(&addr, ip, port) != 0) {
        t_socket_close(fd);
        return NULL;
    }
    int rc = t_socket_connect_async(fd, &addr);
    (void)rc; /* allow both blocking and non-blocking in tests */
    t_tcp_conn *conn = t_tcp_conn_create(fd, loop);
    return conn;
}

/* t_tcp_conn implementations */
t_tcp_conn *t_tcp_conn_create(int fd, t_evloop *loop) {
    if (fd < 0) return NULL;
    t_tcp_conn *conn = (t_tcp_conn*)calloc(1, sizeof(*conn));
    if (!conn) {
        t_socket_close(fd);
        return NULL;
    }
    conn->fd = fd;
    conn->loop = loop;
    conn->on_read = NULL;
    conn->on_close = NULL;
    // Keep fd non-blocking and delay options
    t_socket_set_nonblock(fd);
    t_socket_set_nodelay(fd);
    return conn;
}

void t_tcp_conn_destroy(t_tcp_conn *conn) {
    if (!conn) return;
    if (conn->loop) {
        t_evloop_del(conn->loop, &conn->read_io);
    }
    if (conn->fd >= 0) t_socket_close(conn->fd);
    free(conn);
}

static void t_tcp_conn_read_cb(t_evio *io, int flags, void *ud) {
    (void)flags;
    (void)ud;
    t_tcp_conn *conn = (t_tcp_conn *)io->user_data;
    if (!conn || conn->closed) return;
    unsigned char buf[4096];
    ssize_t r = t_socket_read(conn->fd, buf, sizeof(buf));
    if (r > 0) {
        if (conn->on_read) conn->on_read(conn, buf, (size_t)r, conn->user_data);
        return;
    }
    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return;
    conn->closed = 1;
    if (conn->loop) t_evloop_del(conn->loop, &conn->read_io);
    if (conn->fd >= 0) {
        t_socket_close(conn->fd);
        conn->fd = -1;
    }
    if (conn->on_close) conn->on_close(conn, conn->user_data);
}

int t_tcp_conn_start_read(t_tcp_conn *conn, t_tcp_read_cb cb, void *ud) {
    if (!conn) return -1;
    conn->on_read = cb;
    conn->user_data = ud;
    conn->read_io.fd = conn->fd;
    conn->read_io.events = T_EV_READ;
    conn->read_io.callback = t_tcp_conn_read_cb;
    conn->read_io.user_data = conn;
    conn->read_io.loop = conn->loop;
    return t_evloop_add(conn->loop, &conn->read_io, T_EV_READ);
}

int t_tcp_conn_stop_read(t_tcp_conn *conn) {
    if (!conn) return -1;
    return t_evloop_del(conn->loop, &conn->read_io);
}

ssize_t t_tcp_conn_write(t_tcp_conn *conn, const void *buf, size_t len) {
    if (!conn || conn->closed || conn->fd < 0) return -1;
    return t_socket_write(conn->fd, buf, len);
}
