#include "t_evloop_internal.h"

#ifdef T_HAVE_EPOLL

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/epoll.h>

typedef struct {
    int epoll_fd;
} t_evloop_epoll_state;

static int t_epoll_create(t_evloop *loop) {
    t_evloop_epoll_state *st = (t_evloop_epoll_state *)calloc(1, sizeof(*st));
    if (!st) return -1;
    st->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (st->epoll_fd < 0) {
        free(st);
        return -1;
    }
    loop->backend_state = st;
    return 0;
}

static void t_epoll_destroy(t_evloop *loop) {
    t_evloop_epoll_state *st = (t_evloop_epoll_state *)loop->backend_state;
    if (st) {
        if (st->epoll_fd >= 0) close(st->epoll_fd);
        free(st);
        loop->backend_state = NULL;
    }
}

static int t_epoll_add(t_evloop *loop, t_evio *io, int events) {
    t_evloop_epoll_state *st = (t_evloop_epoll_state *)loop->backend_state;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.ptr = io;
    int ep = 0;
    if (events & T_EV_READ)  ep |= EPOLLIN;
    if (events & T_EV_WRITE) ep |= EPOLLOUT;
    if (events & T_EV_ONCE)  ep |= EPOLLONESHOT;
    ev.events = (ep != 0) ? (uint32_t)ep : EPOLLIN;
    return epoll_ctl(st->epoll_fd, EPOLL_CTL_ADD, io->fd, &ev);
}

static int t_epoll_mod(t_evloop *loop, t_evio *io, int events) {
    t_evloop_epoll_state *st = (t_evloop_epoll_state *)loop->backend_state;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.ptr = io;
    int ep = 0;
    if (events & T_EV_READ)  ep |= EPOLLIN;
    if (events & T_EV_WRITE) ep |= EPOLLOUT;
    if (events & T_EV_ONCE)  ep |= EPOLLONESHOT;
    ev.events = (ep != 0) ? (uint32_t)ep : EPOLLIN;
    return epoll_ctl(st->epoll_fd, EPOLL_CTL_MOD, io->fd, &ev);
}

static int t_epoll_del(t_evloop *loop, t_evio *io) {
    t_evloop_epoll_state *st = (t_evloop_epoll_state *)loop->backend_state;
    return epoll_ctl(st->epoll_fd, EPOLL_CTL_DEL, io->fd, NULL);
}

static int t_epoll_poll(t_evloop *loop, int timeout_ms) {
    t_evloop_epoll_state *st = (t_evloop_epoll_state *)loop->backend_state;
    struct epoll_event events[128];
    int nfds;
    do {
        nfds = epoll_wait(st->epoll_fd, events, 128, timeout_ms);
    } while (nfds < 0 && errno == EINTR);
    if (nfds > 0) {
        for (int i = 0; i < nfds; ++i) {
            t_evio *io = (t_evio *)events[i].data.ptr;
            if (io == &loop->wakeup_io) {
                char buf[64];
                while (read(loop->wakeup_fds[0], buf, sizeof(buf)) > 0) {}
                continue;
            }
            if (io && io->callback) {
                int flags = 0;
                if (events[i].events & EPOLLIN)  flags |= T_EV_READ;
                if (events[i].events & EPOLLOUT) flags |= T_EV_WRITE;
                if (events[i].events & (EPOLLERR | EPOLLHUP)) flags |= T_EV_ERROR;
                io->callback(io, flags, io->user_data);
            }
        }
    }
    return nfds;
}

t_evloop_backend const t_epoll_backend = {
    t_epoll_create,
    t_epoll_destroy,
    t_epoll_add,
    t_epoll_mod,
    t_epoll_del,
    t_epoll_poll,
};

#else

t_evloop_backend const t_epoll_backend = {0};

#endif
