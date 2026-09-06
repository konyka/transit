#ifndef T_SOCKET_H
#define T_SOCKET_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

#if T_PLATFORM_WINDOWS
#include <winsock2.h>
#include <ws2tcpip.h>
#if defined(_MSC_VER)
#include <BaseTsd.h>
#ifndef _SSIZE_T_DEFINED
typedef SSIZE_T ssize_t;
#define _SSIZE_T_DEFINED
#endif
#endif
#else
#include <sys/types.h>
#include <sys/socket.h>
#endif

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
int  t_socket_set_block(int fd);
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
/* Last t_socket_* error: would-block / interrupted (errno or WSAGetLastError). */
int t_socket_again(void);
int t_socket_intr(void);

int t_sockaddr_init_ipv4(t_sockaddr *addr, const char *ip, uint16_t port);
uint16_t t_sockaddr_port(const t_sockaddr *addr);
uint16_t t_socket_local_port(int fd);
/* Blocking IPv4 connect; returns a non-blocking fd, or -1. */
int t_socket_dial_ipv4(const char *ip, uint16_t port);
/* Connected stream pair. POSIX: AF_UNIX socketpair. Windows: loopback TCP
 * with a self-token so a stolen accept cannot be returned. Both ends
 * are non-blocking. fds[0] and fds[1] are -1 on failure. */
int t_socket_pair(int fds[2]);

#endif /* T_SOCKET_H */
