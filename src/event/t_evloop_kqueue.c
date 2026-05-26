#include "t_evloop_internal.h"

#ifdef T_HAVE_KQUEUE

#include "t_time.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/event.h>
#include <stdint.h>

typedef struct {
    int kq_fd;
} t_evloop_kqueue_state;

static int t_kqueue_create(t_evloop *loop) {
    t_evloop_kqueue_state *st = (t_evloop_kqueue_state *)calloc(1, sizeof(*st));
    if (!st) return -1;
    st->kq_fd = kqueue();
    if (st->kq_fd < 0) {
        free(st);
        return -1;
    }
    loop->backend_state = st;
    return 0;
}

static void t_kqueue_destroy(t_evloop *loop) {
    t_evloop_kqueue_state *st = (t_evloop_kqueue_state *)loop->backend_state;
    if (st) {
        if (st->kq_fd >= 0) close(st->kq_fd);
        free(st);
        loop->backend_state = NULL;
    }
}

static int t_kqueue_add(t_evloop *loop, t_evio *io, int events) {
    t_evloop_kqueue_state *st = (t_evloop_kqueue_state *)loop->backend_state;
    struct kevent ke;
    short filter = (events & T_EV_WRITE) ? EVFILT_WRITE : EVFILT_READ;
    u_short flags = EV_ADD;
    if (events & T_EV_ONCE) flags |= EV_ONESHOT;
    EV_SET(&ke, io->fd, filter, flags, 0, 0, io);
    return kevent(st->kq_fd, &ke, 1, NULL, 0, NULL);
}

static int t_kqueue_mod(t_evloop *loop, t_evio *io, int events) {
    t_evloop_kqueue_state *st = (t_evloop_kqueue_state *)loop->backend_state;
    struct kevent ke;
    short filter = (events & T_EV_WRITE) ? EVFILT_WRITE : EVFILT_READ;
    u_short flags = EV_ADD;
    if (events & T_EV_ONCE) flags |= EV_ONESHOT;
    EV_SET(&ke, io->fd, filter, flags, 0, 0, io);
    return kevent(st->kq_fd, &ke, 1, NULL, 0, NULL);
}

static int t_kqueue_del(t_evloop *loop, t_evio *io) {
    t_evloop_kqueue_state *st = (t_evloop_kqueue_state *)loop->backend_state;
    struct kevent ke[2];
    EV_SET(&ke[0], io->fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    EV_SET(&ke[1], io->fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    kevent(st->kq_fd, ke, 2, NULL, 0, NULL);
    return 0;
}

static int t_kqueue_poll(t_evloop *loop, int timeout_ms) {
    t_evloop_kqueue_state *st = (t_evloop_kqueue_state *)loop->backend_state;
    struct timespec ts;
    struct timespec *tsp = NULL;
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
        tsp = &ts;
    }
    struct kevent events[128];
    int nfds = kevent(st->kq_fd, NULL, 0, events, 128, tsp);
    if (nfds > 0) {
        for (int i = 0; i < nfds; i++) {
            t_evio *io = (t_evio *)events[i].udata;
            if (io == &loop->wakeup_io) {
                char buf[64];
                while (read(loop->wakeup_fds[0], buf, sizeof(buf)) > 0) {}
                continue;
            }
            if (io && io->callback) {
                int flags = 0;
                if (events[i].filter == EVFILT_READ) flags |= T_EV_READ;
                if (events[i].filter == EVFILT_WRITE) flags |= T_EV_WRITE;
                if (events[i].flags & EV_ERROR) flags |= T_EV_ERROR;
                if (events[i].flags & EV_EOF) flags |= T_EV_ERROR;
                io->callback(io, flags, io->user_data);
            }
        }
    }
    return nfds;
}

t_evloop_backend const t_kqueue_backend = {
    t_kqueue_create,
    t_kqueue_destroy,
    t_kqueue_add,
    t_kqueue_mod,
    t_kqueue_del,
    t_kqueue_poll,
};

#else

t_evloop_backend const t_kqueue_backend = {0};

#endif
