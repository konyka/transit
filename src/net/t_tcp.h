#ifndef T_TCP_H
#define T_TCP_H

#include "t_socket.h"
#include "t_evloop.h"
#include <stdint.h>

typedef struct t_tcp_server t_tcp_server;
typedef struct t_tcp_conn   t_tcp_conn;

typedef void (*t_tcp_accept_cb)(t_tcp_server *srv, int client_fd, t_sockaddr *peer, void *ud);
typedef void (*t_tcp_read_cb)(t_tcp_conn *conn, void *buf, size_t len, void *ud);
typedef void (*t_tcp_close_cb)(t_tcp_conn *conn, void *ud);

struct t_tcp_conn {
    int       fd;
    t_evio    read_io;
    t_evio    write_io;
    t_tcp_read_cb  on_read;
    t_tcp_close_cb on_close;
    void     *user_data;
    t_evloop *loop;
    int       closed;
    int       in_io_cb;
    int       free_pending;
};

t_tcp_conn *t_tcp_conn_create(int fd, t_evloop *loop);
void        t_tcp_conn_destroy(t_tcp_conn *conn);
int         t_tcp_conn_start_read(t_tcp_conn *conn, t_tcp_read_cb cb, void *ud);
int         t_tcp_conn_stop_read(t_tcp_conn *conn);
ssize_t     t_tcp_conn_write(t_tcp_conn *conn, const void *buf, size_t len);

struct t_tcp_server {
    int              fd;
    t_evio           accept_io;
    t_evloop        *loop;
    t_tcp_accept_cb  on_accept;
    void            *user_data;
};

t_tcp_server *t_tcp_server_create(t_evloop *loop);
void          t_tcp_server_destroy(t_tcp_server *srv);
int           t_tcp_server_listen(t_tcp_server *srv, const char *ip, uint16_t port,
                                  t_tcp_accept_cb cb, void *ud);

t_tcp_conn   *t_tcp_dial(t_evloop *loop, const char *ip, uint16_t port);

#endif /* T_TCP_H */
