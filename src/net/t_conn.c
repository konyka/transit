#include "t_conn.h"
#include "t_crc32c.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <arpa/inet.h>

/* TCP connection with wire-protocol framing over t_evloop. */

struct t_conn {
    int fd;
    t_evloop *loop;
    t_evio io;

    uint8_t *recv_buf;
    size_t   recv_len;
    size_t   recv_cap;

    uint8_t *send_buf;
    size_t   send_len;
    size_t   send_cap;

    t_conn_msg_cb on_msg;
    void           *on_msg_ud;
    t_conn_close_cb on_close;
    void           *on_close_ud;

    size_t bytes_sent;
    size_t bytes_recv;
    size_t msgs_sent;
    size_t msgs_recv;

    int closed;
    int in_io_cb;
    int free_pending;
};

static void t_conn_handle_read(t_conn *conn);
static void t_conn_handle_write(t_conn *conn);
static void t_conn_io_cb(t_evio *io, int events, void *user_data);
static void t_conn_free(t_conn *conn);

#define T_CONN_INIT_CAP 4096

static int t_conn_ensure_recv(t_conn *conn, size_t needed) {
    if (conn->recv_cap >= needed) return 0;
    size_t new_cap = conn->recv_cap ? conn->recv_cap * 2 : T_CONN_INIT_CAP;
    while (new_cap < needed) {
        if (new_cap > SIZE_MAX / 2) return -1;
        new_cap *= 2;
    }
    uint8_t *nb = (uint8_t *)realloc(conn->recv_buf, new_cap);
    if (!nb) return -1;
    conn->recv_buf = nb;
    conn->recv_cap = new_cap;
    return 0;
}

static void t_conn_close_now(t_conn *conn) {
    if (conn->closed) return;
    conn->closed = 1;
    if (conn->loop) t_evloop_del(conn->loop, &conn->io);
    conn->io.user_data = NULL;
    if (conn->fd >= 0) {
        close(conn->fd);
        conn->fd = -1;
    }
    if (conn->on_close) conn->on_close(conn, conn->on_close_ud);
}

static void t_conn_free(t_conn *conn) {
    if (!conn) return;
    free(conn->recv_buf);
    free(conn->send_buf);
    if (conn->fd >= 0) close(conn->fd);
    free(conn);
}

t_conn *t_conn_create(int fd, t_evloop *loop)
{
    if (fd < 0) return NULL;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) flags = 0;
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    t_conn *conn = (t_conn *)calloc(1, sizeof(*conn));
    if (!conn) {
        close(fd);
        return NULL;
    }
    conn->fd = fd;
    conn->loop = loop;
    conn->recv_cap = T_CONN_INIT_CAP;
    conn->recv_buf = (uint8_t *)malloc(conn->recv_cap);
    conn->send_cap = T_CONN_INIT_CAP;
    conn->send_buf = (uint8_t *)malloc(conn->send_cap);
    if (!conn->recv_buf || !conn->send_buf) {
        free(conn->recv_buf);
        free(conn->send_buf);
        free(conn);
        close(fd);
        return NULL;
    }
    conn->io.fd = fd;
    conn->io.callback = t_conn_io_cb;
    conn->io.user_data = conn;

    if (loop) {
        if (t_evloop_add(loop, &conn->io, T_EV_READ) != 0) {
            t_conn_destroy(conn);
            return NULL;
        }
    }
    return conn;
}

void t_conn_destroy(t_conn *conn)
{
    if (!conn) return;
    if (!conn->closed) t_conn_close_now(conn);
    if (conn->in_io_cb) {
        conn->free_pending = 1;
        return;
    }
    t_conn_free(conn);
}

int t_conn_send(t_conn *conn, const t_proto_msg *msg)
{
    if (!conn || !msg || conn->closed) return -1;
    if (msg->payload_len > T_PROTO_MAX_PAYLOAD) return -1;
    if (msg->payload_len > SIZE_MAX - T_PROTO_HEADER_SIZE) return -1;
    size_t frame_len = T_PROTO_HEADER_SIZE + msg->payload_len;
    if (frame_len > SIZE_MAX - conn->send_len) return -1;

    size_t needed = conn->send_len + frame_len;
    if (conn->send_cap < needed) {
        size_t new_cap = conn->send_cap ? conn->send_cap * 2 : T_CONN_INIT_CAP;
        while (new_cap < needed) {
            if (new_cap > SIZE_MAX / 2) return -1;
            new_cap *= 2;
        }
        uint8_t *new_buf = (uint8_t *)realloc(conn->send_buf, new_cap);
        if (!new_buf) return -1;
        conn->send_buf = new_buf;
        conn->send_cap = new_cap;
    }

    /* Encode with CRC via t_proto_encode_msg into the send buffer. */
    t_proto_msg enc = *msg;
    if (enc.header.magic == 0) {
        t_proto_header_init(&enc.header, (t_msg_type)msg->header.type, (uint32_t)msg->payload_len);
    } else {
        enc.header.payload_len = (uint32_t)msg->payload_len;
    }
    int n = t_proto_encode_msg(&enc, conn->send_buf + conn->send_len, conn->send_cap - conn->send_len);
    if (n < 0) return -1;

    conn->send_len += (size_t)n;
    conn->msgs_sent += 1;
    /* bytes_sent counted only on successful write in handle_write */

    if (conn->loop) {
        if (t_evloop_mod(conn->loop, &conn->io, T_EV_READ | T_EV_WRITE) != 0) {
            conn->send_len -= (size_t)n;
            conn->msgs_sent -= 1;
            return -1;
        }
    } else {
        /* No event loop: briefly block so the frame is fully flushed. */
        int flags = fcntl(conn->fd, F_GETFL, 0);
        if (flags >= 0) (void)fcntl(conn->fd, F_SETFL, flags & ~O_NONBLOCK);
        t_conn_handle_write(conn);
        if (flags >= 0) (void)fcntl(conn->fd, F_SETFL, flags);
        if (conn->closed || conn->send_len > 0) {
            if (!conn->closed) {
                /* Drop any unsent remnant of this frame to avoid splicing. */
                if (conn->send_len >= (size_t)n) conn->send_len -= (size_t)n;
                else conn->send_len = 0;
            }
            conn->msgs_sent -= 1;
            return -1;
        }
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

static void t_conn_handle_read(t_conn *conn)
{
    if (!conn || conn->closed) return;
    for (;;) {
        if (conn->recv_len == conn->recv_cap) {
            if (t_conn_ensure_recv(conn, conn->recv_cap + T_CONN_INIT_CAP) != 0) {
                t_conn_close_now(conn);
                return;
            }
        }

        ssize_t n = read(conn->fd, conn->recv_buf + conn->recv_len,
                         conn->recv_cap - conn->recv_len);
        if (n > 0) {
            conn->recv_len += (size_t)n;
            conn->bytes_recv += (size_t)n;

            for (;;) {
                if (conn->recv_len < T_PROTO_HEADER_SIZE) break;

                t_proto_header hdr;
                if (t_proto_header_decode(&hdr, conn->recv_buf, T_PROTO_HEADER_SIZE) != 0) {
                    t_conn_close_now(conn);
                    return;
                }
                if (hdr.magic != T_PROTO_MAGIC ||
                    hdr.version != T_PROTO_VERSION ||
                    hdr.type >= T_MSG_MAX ||
                    hdr.payload_len > T_PROTO_MAX_PAYLOAD) {
                    t_conn_close_now(conn);
                    return;
                }

                size_t total = T_PROTO_HEADER_SIZE + (size_t)hdr.payload_len;
                if (t_conn_ensure_recv(conn, total) != 0) {
                    t_conn_close_now(conn);
                    return;
                }
                if (conn->recv_len < total) break;

                /* Verify CRC over header(with CRC=0) + payload in-place. */
                uint32_t stored_crc = hdr.crc32c;
                uint32_t zero = 0;
                memcpy(conn->recv_buf + 4, &zero, 4);
                uint32_t crc_calc = t_crc32c(conn->recv_buf, total);
                uint32_t crc_be = htonl(stored_crc);
                memcpy(conn->recv_buf + 4, &crc_be, 4);
                if (crc_calc != stored_crc) {
                    t_conn_close_now(conn);
                    return;
                }

                uint8_t *payload = NULL;
                if (hdr.payload_len > 0) {
                    payload = (uint8_t *)malloc(hdr.payload_len);
                    if (!payload) {
                        t_conn_close_now(conn);
                        return;
                    }
                    memcpy(payload, conn->recv_buf + T_PROTO_HEADER_SIZE, hdr.payload_len);
                }

                t_proto_msg msg;
                msg.header = hdr;
                msg.payload = payload;
                msg.payload_len = hdr.payload_len;
                if (conn->on_msg) conn->on_msg(conn, &msg, conn->on_msg_ud);
                free(payload);
                if (conn->closed) return;

                size_t consumed = total;
                if (consumed < conn->recv_len) {
                    memmove(conn->recv_buf, conn->recv_buf + consumed, conn->recv_len - consumed);
                }
                conn->recv_len -= consumed;
                conn->msgs_recv += 1;
            }
        } else if (n == 0) {
            t_conn_close_now(conn);
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
            t_conn_close_now(conn);
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
            conn->bytes_sent += (size_t)n;
            conn->send_len -= (size_t)n;
            if (conn->send_len > 0) {
                memmove(conn->send_buf, conn->send_buf + n, conn->send_len);
            }
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
            t_conn_close_now(conn);
            return;
        } else {
            t_conn_close_now(conn);
            return;
        }
    }
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
    conn->in_io_cb = 1;
    if ((events & T_EV_READ) && !conn->closed) t_conn_handle_read(conn);
    if ((events & T_EV_WRITE) && !conn->closed) t_conn_handle_write(conn);
    conn->in_io_cb = 0;
    if (conn->free_pending) t_conn_free(conn);
}
