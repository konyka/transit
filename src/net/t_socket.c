#include "t_socket.h"
#include "t_compiler.h"
#include <string.h>
#include <errno.h>
#include <limits.h>

#if T_PLATFORM_WINDOWS
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#endif

#if T_PLATFORM_WINDOWS
static BOOL CALLBACK wsa_once_fn(PINIT_ONCE once, PVOID param, PVOID *ctx) {
    WSADATA wsa;
    (void)once;
    (void)param;
    (void)ctx;
    return WSAStartup(MAKEWORD(2, 2), &wsa) == 0 ? TRUE : FALSE;
}

static int wsa_ensure(void) {
    static INIT_ONCE once = INIT_ONCE_STATIC_INIT;
    return InitOnceExecuteOnce(&once, wsa_once_fn, NULL, NULL) ? 0 : -1;
}

static int sock_err(void) {
    return WSAGetLastError();
}

static int sock_in_progress(int err) {
    return err == WSAEWOULDBLOCK || err == WSAEINPROGRESS || err == WSAEALREADY;
}

static int sock_interrupted(int err) {
    return err == WSAEINTR;
}

static void sock_close(int fd) {
    if (fd >= 0) (void)closesocket((SOCKET)fd);
}

static int set_nonblock(int fd) {
    u_long flags = 1;
    return ioctlsocket((SOCKET)fd, FIONBIO, &flags) == 0 ? 0 : -1;
}

static int set_block(int fd) {
    u_long flags = 0;
    return ioctlsocket((SOCKET)fd, FIONBIO, &flags) == 0 ? 0 : -1;
}

static int sock_io_len(size_t len) {
    if (len > (size_t)INT_MAX) return INT_MAX;
    return (int)len;
}
#else
static int sock_err(void) {
    return errno;
}

static int sock_in_progress(int err) {
    return err == EINPROGRESS || err == EALREADY;
}

static int sock_interrupted(int err) {
    return err == EINTR;
}

static void sock_close(int fd) {
    if (fd >= 0) (void)close(fd);
}

static int set_nonblock(int fd) {
#if defined(O_NONBLOCK)
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    if (!(flags & O_NONBLOCK)) {
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) return -1;
    }
    return 0;
#else
    int flags = 1;
    return ioctl(fd, FIONBIO, &flags);
#endif
}
#endif

int t_socket_create(int domain, int type, int protocol) {
#if T_PLATFORM_WINDOWS
    if (wsa_ensure() != 0) return -1;
#endif
    int fd = (int)socket(domain, type, protocol);
    if (fd < 0) return -1;
    if (set_nonblock(fd) < 0) {
        sock_close(fd);
        return -1;
    }
    return fd;
}

void t_socket_close(int fd) {
    sock_close(fd);
}

int t_socket_set_nonblock(int fd) {
    return set_nonblock(fd);
}

int t_socket_set_reuseaddr(int fd) {
    int opt = 1;
    return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt, sizeof(opt));
}

int t_socket_set_nodelay(int fd) {
    int opt = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char *)&opt, sizeof(opt));
}

int t_socket_bind(int fd, const t_sockaddr *addr) {
    if (!addr) return -1;
    if (addr->u.ipv4.family != AF_INET) return -1;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(addr->u.ipv4.addr);
    sa.sin_port = addr->u.ipv4.port;
    return bind(fd, (struct sockaddr *)&sa, sizeof(sa));
}

int t_socket_listen(int fd, int backlog) {
    return listen(fd, backlog);
}

int t_socket_accept(int fd, t_sockaddr *peer_addr) {
    struct sockaddr_in sa;
    socklen_t slen = sizeof(sa);
    int nfd;
    do {
        nfd = (int)accept(fd, (struct sockaddr *)&sa, &slen);
    } while (nfd < 0 && sock_interrupted(sock_err()));
    if (nfd < 0) return -1;
    /* Match t_socket_create: accepted fds are non-blocking for the evloop. */
    if (set_nonblock(nfd) < 0) {
        sock_close(nfd);
        return -1;
    }
    if (peer_addr) {
        peer_addr->u.ipv4.family = AF_INET;
        peer_addr->u.ipv4.addr = ntohl(sa.sin_addr.s_addr);
        peer_addr->u.ipv4.port = sa.sin_port; /* network order */
    }
    return nfd;
}

int t_socket_connect(int fd, const t_sockaddr *addr) {
    if (!addr) return -1;
    if (addr->u.ipv4.family != AF_INET) return -1;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(addr->u.ipv4.addr);
    sa.sin_port = addr->u.ipv4.port;
    return connect(fd, (struct sockaddr *)&sa, sizeof(sa));
}

int t_socket_connect_async(int fd, const t_sockaddr *addr) {
    int rc = t_socket_connect(fd, addr);
    if (rc == 0) return 0;
    if (sock_in_progress(sock_err())) return 1; /* still in progress */
    return -1;
}

ssize_t t_socket_read(int fd, void *buf, size_t len) {
#if T_PLATFORM_WINDOWS
    return (ssize_t)recv((SOCKET)fd, (char *)buf, sock_io_len(len), 0);
#else
    return read(fd, buf, len);
#endif
}

ssize_t t_socket_write(int fd, const void *buf, size_t len) {
#if T_PLATFORM_WINDOWS
    return (ssize_t)send((SOCKET)fd, (const char *)buf, sock_io_len(len), 0);
#elif defined(MSG_NOSIGNAL)
    return send(fd, buf, len, MSG_NOSIGNAL);
#else
    return write(fd, buf, len);
#endif
}

int t_sockaddr_init_ipv4(t_sockaddr *addr, const char *ip, uint16_t port) {
    if (!addr || !ip) return -1;
#if T_PLATFORM_WINDOWS
    if (wsa_ensure() != 0) return -1;
#endif
    struct in_addr a;
    if (inet_pton(AF_INET, ip, &a) != 1) return -1;
    addr->u.ipv4.family = AF_INET;
    addr->u.ipv4.addr = ntohl(a.s_addr); /* store host-order */
    addr->u.ipv4.port = htons(port);
    return 0;
}

uint16_t t_sockaddr_port(const t_sockaddr *addr) {
    if (!addr) return 0;
    return ntohs(addr->u.ipv4.port);
}

uint16_t t_socket_local_port(int fd) {
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);
    if (fd < 0) return 0;
#if T_PLATFORM_WINDOWS
    if (wsa_ensure() != 0) return 0;
#endif
    memset(&addr, 0, sizeof(addr));
    if (getsockname(fd, (struct sockaddr *)&addr, &len) != 0) return 0;
    if (addr.sin_family != AF_INET) return 0;
    return ntohs(addr.sin_port);
}

int t_socket_dial_ipv4(const char *ip, uint16_t port) {
    if (!ip || port == 0) return -1;
    int fd = t_socket_create(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    t_sockaddr addr;
    if (t_sockaddr_init_ipv4(&addr, ip, port) != 0) {
        t_socket_close(fd);
        return -1;
    }
#if T_PLATFORM_WINDOWS
    if (set_block(fd) != 0) {
        t_socket_close(fd);
        return -1;
    }
#else
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) (void)fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
#endif
    if (t_socket_connect(fd, &addr) != 0) {
        t_socket_close(fd);
        return -1;
    }
#if T_PLATFORM_WINDOWS
    if (set_nonblock(fd) != 0) {
        t_socket_close(fd);
        return -1;
    }
#else
    if (flags >= 0) (void)fcntl(fd, F_SETFL, flags);
#endif
    (void)t_socket_set_nodelay(fd);
    return fd;
}
