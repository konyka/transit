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
    struct kevent ke[4];
    int n = 0;
    /* Clear any prior filters so mod(READ) does not leave WRITE armed. */
    EV_SET(&ke[n++], io->fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    EV_SET(&ke[n++], io->fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    (void)kevent(st->kq_fd, ke, n, NULL, 0, NULL);
    n = 0;
    u_short flags = EV_ADD;
    if (events & T_EV_ONCE) flags |= EV_ONESHOT;
    if (events & T_EV_READ) {
        EV_SET(&ke[n++], io->fd, EVFILT_READ, flags, 0, 0, io);
    }
    if (events & T_EV_WRITE) {
        EV_SET(&ke[n++], io->fd, EVFILT_WRITE, flags, 0, 0, io);
    }
    if (n == 0) return 0;
    return kevent(st->kq_fd, ke, n, NULL, 0, NULL);
}

static int t_kqueue_mod(t_evloop *loop, t_evio *io, int events) {
    return t_kqueue_add(loop, io, events);
}

static int t_kqueue_del(t_evloop *loop, t_evio *io) {
    t_evloop_kqueue_state *st = (t_evloop_kqueue_state *)loop->backend_state;
    struct kevent ke[2];
    EV_SET(&ke[0], io->fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    EV_SET(&ke[1], io->fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    kevent(st->kq_fd, ke, 2, NULL, 0, NULL);
    return 0;
}

static void t_kqueue_wakeup(t_evloop *loop) {
    if (!loop || loop->wakeup_fds[1] < 0) return;
    char b = 1;
    ssize_t w;
    do {
        w = write(loop->wakeup_fds[1], &b, 1);
    } while (w < 0 && errno == EINTR);
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
    int nfds;
    do {
        nfds = kevent(st->kq_fd, NULL, 0, events, 128, tsp);
    } while (nfds < 0 && errno == EINTR);
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
    t_kqueue_wakeup,
};

#else

t_evloop_backend const t_kqueue_backend = {0};

#endif
