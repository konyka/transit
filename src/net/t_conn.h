#ifndef T_CONN_H
#define T_CONN_H

#include "t_compiler.h"
#include "t_evloop.h"
#include "t_proto.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_conn t_conn;

typedef void (*t_conn_msg_cb)(t_conn *conn, const t_proto_msg *msg, void *ud);
typedef void (*t_conn_close_cb)(t_conn *conn, void *ud);

t_conn *t_conn_create(int fd, t_evloop *loop);
void     t_conn_destroy(t_conn *conn);

int      t_conn_send(t_conn *conn, const t_proto_msg *msg);
int      t_conn_flush(t_conn *conn);
void     t_conn_set_on_msg(t_conn *conn, t_conn_msg_cb cb, void *ud);
void     t_conn_set_on_close(t_conn *conn, t_conn_close_cb cb, void *ud);

int      t_conn_fd(const t_conn *conn);
int      t_conn_is_closed(const t_conn *conn);

size_t   t_conn_bytes_sent(const t_conn *conn);
size_t   t_conn_bytes_recv(const t_conn *conn);
size_t   t_conn_msgs_sent(const t_conn *conn);
size_t   t_conn_msgs_recv(const t_conn *conn);

#endif /* T_CONN_H */
