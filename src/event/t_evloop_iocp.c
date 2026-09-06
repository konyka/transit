#include "t_evloop_internal.h"

#ifdef T_HAVE_IOCP

#include "t_socket.h"
#include "t_mutex.h"
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <winsock2.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

/* Readiness backend (WSAPoll). IOCP completions are not used: the rest of
 * Transit is readiness-based (epoll/kqueue + recv/send). WSAPoll matches
 * that model without extra copies.
 *
 * The registration table is mutated from other threads (t_conn_create/send).
 * Lock only the snapshot copy; never hold the lock across WSAPoll. */

typedef struct {
    t_evio   **ios;
    size_t     n;
    size_t     cap;
    WSAPOLLFD *pfds;
    t_evio   **snap;
    size_t     snap_cap;
    t_mutex    mu;
} t_evloop_iocp_state;

static void t_iocp_wakeup(t_evloop *loop);

static int win_grow(t_evloop_iocp_state *st) {
    size_t ncap = st->cap ? st->cap * 2 : 16;
    t_evio **nios = (t_evio **)realloc(st->ios, ncap * sizeof(*nios));
    if (!nios) return -1;
    st->ios = nios;
    st->cap = ncap;
    return 0;
}

static int t_iocp_create(t_evloop *loop)
{
    t_evloop_iocp_state *st = (t_evloop_iocp_state *)calloc(1, sizeof *st);
    if (!st) return -1;
    t_mutex_init(&st->mu);
    loop->backend_state = st;
    return 0;
}

static void t_iocp_destroy(t_evloop *loop)
{
    t_evloop_iocp_state *st = (t_evloop_iocp_state *)loop->backend_state;
    if (!st) return;
    t_mutex_destroy(&st->mu);
    free(st->ios);
    free(st->pfds);
    free(st->snap);
    free(st);
    loop->backend_state = NULL;
}

static int t_iocp_add(t_evloop *loop, t_evio *io, int events)
{
    t_evloop_iocp_state *st = (t_evloop_iocp_state *)loop->backend_state;
    size_t i;
    int rc = 0;
    if (!st || !io) return -1;
    io->events = events;
    if (io->fd < 0) return 0;
    t_mutex_lock(&st->mu);
    for (i = 0; i < st->n; i++) {
        if (st->ios[i] == io) {
            t_mutex_unlock(&st->mu);
            return 0;
        }
    }
    if (st->n == st->cap && win_grow(st) != 0) rc = -1;
    else st->ios[st->n++] = io;
    t_mutex_unlock(&st->mu);
    if (rc == 0) t_iocp_wakeup(loop);
    return rc;
}

static int t_iocp_mod(t_evloop *loop, t_evio *io, int events)
{
    if (!io) return -1;
    io->events = events;
    t_iocp_wakeup(loop);
    return 0;
}

static int t_iocp_del(t_evloop *loop, t_evio *io)
{
    t_evloop_iocp_state *st = (t_evloop_iocp_state *)loop->backend_state;
    size_t i;
    if (!st || !io) return -1;
    t_mutex_lock(&st->mu);
    for (i = 0; i < st->n; i++) {
        if (st->ios[i] == io) {
            st->ios[i] = st->ios[st->n - 1];
            st->n--;
            break;
        }
    }
    t_mutex_unlock(&st->mu);
    t_iocp_wakeup(loop);
    return 0;
}

static void t_iocp_wakeup(t_evloop *loop)
{
    char b = 1;
    if (!loop || loop->wakeup_fds[1] < 0) return;
    (void)t_socket_write(loop->wakeup_fds[1], &b, 1);
}

static void win_drain_wakeup(t_evloop *loop)
{
    char buf[64];
    if (!loop || loop->wakeup_fds[0] < 0) return;
    while (t_socket_read(loop->wakeup_fds[0], buf, sizeof(buf)) > 0) {}
}

static int t_iocp_poll(t_evloop *loop, int timeout_ms)
{
    t_evloop_iocp_state *st = (t_evloop_iocp_state *)loop->backend_state;
    t_evio *fired[128];
    int flags[128];
    int nf = 0;
    int rc;
    size_t i;
    size_t n;
    if (!st) return -1;
    t_mutex_lock(&st->mu);
    n = st->n;
    if (n == 0) {
        t_mutex_unlock(&st->mu);
        if (timeout_ms < 0) timeout_ms = 0;
        Sleep((DWORD)timeout_ms);
        return 0;
    }
    if (st->snap_cap < n) {
        WSAPOLLFD *np = (WSAPOLLFD *)realloc(st->pfds, n * sizeof(*np));
        t_evio **ns = (t_evio **)realloc(st->snap, n * sizeof(*ns));
        if (!np || !ns) {
            if (np) st->pfds = np;
            if (ns) st->snap = ns;
            t_mutex_unlock(&st->mu);
            return -1;
        }
        st->pfds = np;
        st->snap = ns;
        st->snap_cap = n;
    }
    for (i = 0; i < n; i++) {
        SHORT ev = 0;
        t_evio *io = st->ios[i];
        st->snap[i] = io;
        if (io->events & T_EV_READ) ev = (SHORT)(ev | POLLIN);
        if (io->events & T_EV_WRITE) ev = (SHORT)(ev | POLLOUT);
        st->pfds[i].fd = (SOCKET)io->fd;
        st->pfds[i].events = ev;
        st->pfds[i].revents = 0;
    }
    t_mutex_unlock(&st->mu);
    rc = WSAPoll(st->pfds, (ULONG)n, timeout_ms < 0 ? -1 : timeout_ms);
    if (rc == SOCKET_ERROR) {
        if (WSAGetLastError() == WSAEINTR) return 0;
        return -1;
    }
    if (rc == 0) return 0;
    for (i = 0; i < n && nf < 128; i++) {
        SHORT re = st->pfds[i].revents;
        int fl = 0;
        if (!re) continue;
        if (re & (POLLIN | POLLHUP | POLLERR)) fl |= T_EV_READ;
        if (re & POLLOUT) fl |= T_EV_WRITE;
        if (re & (POLLERR | POLLHUP)) fl |= T_EV_ERROR;
        fired[nf] = st->snap[i];
        flags[nf] = fl;
        nf++;
    }
    for (i = 0; i < (size_t)nf; i++) {
        if (fired[i] == &loop->wakeup_io) {
            win_drain_wakeup(loop);
            continue;
        }
        if (fired[i]->callback)
            fired[i]->callback(fired[i], flags[i], fired[i]->user_data);
    }
    return nf;
}

t_evloop_backend const t_iocp_backend = {
    t_iocp_create,
    t_iocp_destroy,
    t_iocp_add,
    t_iocp_mod,
    t_iocp_del,
    t_iocp_poll,
    t_iocp_wakeup,
};

#else

t_evloop_backend const t_iocp_backend = {0};

#endif
