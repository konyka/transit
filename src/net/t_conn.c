#include "t_conn.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>

/* Simple, self-contained TCP connection with wire-protocol framing.
 * This module relies on the generic event loop (t_evloop) API and the
 * wire protocol API (t_proto).
 */

struct t_conn {
    int fd;
    t_evloop *loop;
    t_evio io; /* registered IO handle for the event loop */

    /* Read buffer for incoming protocol frames */
    uint8_t *recv_buf;
    size_t   recv_len;
    size_t   recv_cap;

    /* Write buffer for outgoing data */
    uint8_t *send_buf;
    size_t   send_len;
    size_t   send_cap;

    /* Callbacks */
    t_conn_msg_cb on_msg;
    void           *on_msg_ud;
    t_conn_close_cb on_close;
    void           *on_close_ud;

    /* Stats */
    size_t bytes_sent;
    size_t bytes_recv;
    size_t msgs_sent;
    size_t msgs_recv;

    int closed;
};

/* Forward declarations for internal helpers */
static void t_conn_handle_read(t_conn *conn);
static void t_conn_handle_write(t_conn *conn);
static void t_conn_io_cb(t_evio *io, int events, void *user_data);

#define T_CONN_INIT_CAP 4096
#ifndef T_EV_READ
#define T_EV_READ 1
#endif
#ifndef T_EV_WRITE
#define T_EV_WRITE 2
#endif

t_conn *t_conn_create(int fd, t_evloop *loop)
{
    if (fd < 0) return NULL;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) flags = 0;
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    t_conn *conn = (t_conn *)calloc(1, sizeof(*conn));
    if (!conn) return NULL;
    conn->fd = fd;
    conn->loop = loop;
    conn->recv_cap = T_CONN_INIT_CAP;
    conn->recv_buf = (uint8_t *)malloc(conn->recv_cap);
    conn->send_cap = T_CONN_INIT_CAP;
    conn->send_buf = (uint8_t *)malloc(conn->send_cap);
    conn->io.fd = fd;
    /* The event loop expects a callback taking (t_evio*, int events) */
    conn->io.callback = t_conn_io_cb;
    conn->io.user_data = conn;
    conn->on_msg = NULL;
    conn->on_close = NULL;
    conn->closed = 0;
    conn->bytes_sent = conn->bytes_recv = 0;
    conn->msgs_sent = conn->msgs_recv = 0;

    if (loop) {
        t_evloop_add(loop, &conn->io, T_EV_READ);
    }
    return conn;
}

void t_conn_destroy(t_conn *conn)
{
    if (!conn) return;
    if (conn->loop) {
        t_evloop_del(conn->loop, &conn->io);
    }
    if (conn->recv_buf) free(conn->recv_buf);
    if (conn->send_buf) free(conn->send_buf);
    if (conn->fd >= 0) close(conn->fd);
    free(conn);
}

int t_conn_send(t_conn *conn, const t_proto_msg *msg)
{
    if (!conn || conn->closed) return -1;
    /* Encode the message into the send buffer */
    size_t frame_len = T_PROTO_HEADER_SIZE; /* header */
    frame_len += msg->payload_len;
    if (conn->send_cap < frame_len) {
        size_t new_cap = conn->send_cap ? conn->send_cap * 2 : 1024;
        while (new_cap < frame_len) new_cap *= 2;
        uint8_t *new_buf = (uint8_t *)realloc(conn->send_buf, new_cap);
        if (!new_buf) return -1;
        conn->send_buf = new_buf;
        conn->send_cap = new_cap;
    }
    /* Build header */
    uint8_t header_buf[T_PROTO_HEADER_SIZE];
    t_proto_header_init(&((t_proto_header){0}),  msg->header.type, msg->payload_len);
    t_proto_header_encode(&msg->header, header_buf, sizeof(header_buf));
    memcpy(conn->send_buf, header_buf, T_PROTO_HEADER_SIZE);
    if (msg->payload_len > 0 && msg->payload) {
        memcpy(conn->send_buf + T_PROTO_HEADER_SIZE, msg->payload, msg->payload_len);
    }

    conn->send_len = frame_len;
    conn->bytes_sent += conn->send_len;
    conn->msgs_sent += 1;

    if (conn->loop) {
        /* Ensure we are watching for write events to flush data */
        t_evloop_mod(conn->loop, &conn->io, T_EV_READ | T_EV_WRITE);
    }
    return 0;
}

void t_conn_set_on_msg(t_conn *conn, t_conn_msg_cb cb, void *ud)
{
    if (conn) {
        conn->on_msg = cb;
        conn->on_msg_ud = ud;
    }
}

void t_conn_set_on_close(t_conn *conn, t_conn_close_cb cb, void *ud)
{
    if (conn) {
        conn->on_close = cb;
        conn->on_close_ud = ud;
    }
}

int t_conn_fd(const t_conn *conn)
{
    return conn ? conn->fd : -1;
}

int t_conn_is_closed(const t_conn *conn)
{
    return conn ? !!conn->closed : 1;
}

size_t t_conn_bytes_sent(const t_conn *conn)
{
    return conn ? conn->bytes_sent : 0;
}

size_t t_conn_bytes_recv(const t_conn *conn)
{
    return conn ? conn->bytes_recv : 0;
}

size_t t_conn_msgs_sent(const t_conn *conn)
{
    return conn ? conn->msgs_sent : 0;
}

size_t t_conn_msgs_recv(const t_conn *conn)
{
    return conn ? conn->msgs_recv : 0;
}

/* Internal helpers */
static void t_conn_handle_read(t_conn *conn)
{
    if (!conn || conn->closed) return;
    for (;;) {
        ssize_t n = read(conn->fd, conn->recv_buf + conn->recv_len, conn->recv_cap - conn->recv_len);
        if (n > 0) {
            conn->recv_len += (size_t)n;
            conn->bytes_recv += (size_t)n;
            /* Try to decode frames from buffer */
            for (;;) {
                if (conn->recv_len < T_PROTO_HEADER_SIZE) break;
                t_proto_header hdr;
                if (t_proto_header_decode(&hdr, conn->recv_buf, T_PROTO_HEADER_SIZE) != 0) {
                    /* header decode failed -> close connection */
                    conn->closed = 1;
                    if (conn->on_close) conn->on_close(conn, conn->on_close_ud);
                    return;
                }
                size_t payload_len = hdr.payload_len;
                size_t total = T_PROTO_HEADER_SIZE + payload_len;
                if (conn->recv_len < total) break; /* wait for more data */
                /* Copy payload to a temp buffer to avoid buffer mutation issues */
                uint8_t *payload = NULL;
                if (payload_len > 0) {
                    payload = (uint8_t *)malloc(payload_len);
                    if (!payload) {
                        conn->closed = 1;
                        if (conn->on_close) conn->on_close(conn, conn->on_close_ud);
                        return;
                    }
                    memcpy(payload, conn->recv_buf + T_PROTO_HEADER_SIZE, payload_len);
                }
                t_proto_msg msg;
                msg.header = hdr;
                msg.payload = payload;
                msg.payload_len = payload_len;
                if (conn->on_msg) conn->on_msg(conn, &msg, conn->on_msg_ud);
                if (payload) free(payload);
                /* Consume the frame from buffer */
                size_t consumed = total;
                if (consumed < conn->recv_len) {
                    memmove(conn->recv_buf, conn->recv_buf + consumed, conn->recv_len - consumed);
                }
                conn->recv_len -= consumed;
                conn->msgs_recv += 1;
            }
        } else if (n == 0) {
            /* EOF */
            conn->closed = 1;
            if (conn->on_close) conn->on_close(conn, conn->on_close_ud);
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return; /* no more data now */
            }
            /* Real error */
            conn->closed = 1;
            if (conn->on_close) conn->on_close(conn, conn->on_close_ud);
            return;
        }
    }
}

static void t_conn_handle_write(t_conn *conn)
{
    if (!conn || conn->closed) return;
    while (conn->send_len > 0) {
        ssize_t n = write(conn->fd, conn->send_buf, conn->send_len);
        if (n > 0) {
            conn->send_len -= (size_t)n;
            if (conn->send_len > 0) {
                memmove(conn->send_buf, conn->send_buf + n, conn->send_len);
            }
            conn->bytes_sent += (size_t)n;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return; /* can't write now, wait for next event */
            }
            /* error */
            conn->closed = 1;
            if (conn->on_close) conn->on_close(conn, conn->on_close_ud);
            return;
        } else {
            /* EOF */
            conn->closed = 1;
            if (conn->on_close) conn->on_close(conn, conn->on_close_ud);
            return;
        }
    }
    /* All data sent, stop watching for write events */
    if (conn->loop) {
        t_evloop_mod(conn->loop, &conn->io, T_EV_READ);
    }
}

static void t_conn_io_cb(t_evio *io, int events, void *user_data)
{
    (void)user_data;
    if (!io) return;
    t_conn *conn = (t_conn *)io->user_data;
    if (!conn) return;
    if (events & T_EV_READ) {
        t_conn_handle_read(conn);
    }
    if (events & T_EV_WRITE) {
        t_conn_handle_write(conn);
    }
}
