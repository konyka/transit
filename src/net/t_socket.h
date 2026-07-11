#ifndef T_SOCKET_H
#define T_SOCKET_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

/* Platform-agnostic socket helpers */
typedef struct t_sockaddr {
    union {
        struct { uint32_t addr; uint16_t port; uint16_t family; } ipv4;
        uint8_t raw[128];
    } u;
} t_sockaddr;

int  t_socket_create(int domain, int type, int protocol);
void t_socket_close(int fd);
int  t_socket_set_nonblock(int fd);
int  t_socket_set_reuseaddr(int fd);
int  t_socket_set_nodelay(int fd);
int  t_socket_bind(int fd, const t_sockaddr *addr);
int  t_socket_listen(int fd, int backlog);
int  t_socket_accept(int fd, t_sockaddr *peer_addr);
int  t_socket_connect(int fd, const t_sockaddr *addr);
/* Returns 0 connected, 1 in progress (EINPROGRESS/EALREADY), -1 hard error. */
int  t_socket_connect_async(int fd, const t_sockaddr *addr);

ssize_t t_socket_read(int fd, void *buf, size_t len);
ssize_t t_socket_write(int fd, const void *buf, size_t len);

int t_sockaddr_init_ipv4(t_sockaddr *addr, const char *ip, uint16_t port);
uint16_t t_sockaddr_port(const t_sockaddr *addr);

#endif /* T_SOCKET_H */
