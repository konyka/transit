#include "t_socket.h"
#include "t_compiler.h"
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/* Helper: set non-blocking flag on fd */
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

int t_socket_create(int domain, int type, int protocol) {
    int fd = socket(domain, type, protocol);
    if (fd < 0) return -1;
    // Ensure non-blocking by default
    if (set_nonblock(fd) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

void t_socket_close(int fd) {
    if (fd >= 0) close(fd);
}

int t_socket_set_nonblock(int fd) {
    return set_nonblock(fd);
}

int t_socket_set_reuseaddr(int fd) {
    int opt = 1;
    return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
}

int t_socket_set_nodelay(int fd) {
    int opt = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
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
        nfd = accept(fd, (struct sockaddr *)&sa, &slen);
    } while (nfd < 0 && errno == EINTR);
    if (nfd < 0) return -1;
    /* Match t_socket_create: accepted fds are non-blocking for the evloop. */
    if (set_nonblock(nfd) < 0) {
        close(nfd);
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
    if (errno == EINPROGRESS || errno == EALREADY) return 1; /* still in progress */
    return -1;
}

ssize_t t_socket_read(int fd, void *buf, size_t len) {
    return read(fd, buf, len);
}

ssize_t t_socket_write(int fd, const void *buf, size_t len) {
    return write(fd, buf, len);
}

int t_sockaddr_init_ipv4(t_sockaddr *addr, const char *ip, uint16_t port) {
    if (!addr || !ip) return -1;
    struct in_addr a;
    if (inet_aton(ip, &a) == 0) {
        if (inet_pton(AF_INET, ip, &a) != 1) return -1;
    }
    addr->u.ipv4.family = AF_INET;
    addr->u.ipv4.addr = ntohl(a.s_addr); /* store host-order */
    addr->u.ipv4.port = htons(port);
    return 0;
}

uint16_t t_sockaddr_port(const t_sockaddr *addr) {
    if (!addr) return 0;
    return ntohs(addr->u.ipv4.port);
}
