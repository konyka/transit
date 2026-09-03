#include "t_evloop_internal.h"

#ifdef T_HAVE_IOCP

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <winsock2.h>
#include <stdlib.h>
#include <stdint.h>

typedef struct {
    HANDLE iocp_handle;
} t_evloop_iocp_state;

static int t_iocp_create(t_evloop *loop)
{
    t_evloop_iocp_state *st = (t_evloop_iocp_state *)malloc(sizeof *st);
    if (!st) return -1;
    st->iocp_handle = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (st->iocp_handle == NULL) {
        free(st);
        return -1;
    }
    loop->backend_state = st;
    return 0;
}

static void t_iocp_destroy(t_evloop *loop)
{
    t_evloop_iocp_state *st = (t_evloop_iocp_state *)loop->backend_state;
    if (!st) return;
    if (st->iocp_handle) CloseHandle(st->iocp_handle);
    free(st);
    loop->backend_state = NULL;
}

static int t_iocp_add(t_evloop *loop, t_evio *io, int events)
{
    t_evloop_iocp_state *st = (t_evloop_iocp_state *)loop->backend_state;
    if (!st || !io) return -1;
    io->events = events;
    if (io->fd < 0) return 0; /* wakeup has no HANDLE */
    HANDLE res = CreateIoCompletionPort((HANDLE)(intptr_t)io->fd, st->iocp_handle, (ULONG_PTR)io, 0);
    if (res == NULL) return -1;
    return 0;
}

static int t_iocp_mod(t_evloop *loop, t_evio *io, int events)
{
    if (!io) return -1;
    io->events = events;
    return 0;
}

static int t_iocp_del(t_evloop *loop, t_evio *io)
{
    if (!io) return -1;
    if (io->fd >= 0) (void)CancelIo((HANDLE)(intptr_t)io->fd);
    return 0;
}

static void t_iocp_wakeup(t_evloop *loop)
{
    t_evloop_iocp_state *st;
    if (!loop) return;
    st = (t_evloop_iocp_state *)loop->backend_state;
    if (!st || !st->iocp_handle) return;
    (void)PostQueuedCompletionStatus(st->iocp_handle, 0, (ULONG_PTR)&loop->wakeup_io, NULL);
}

static int t_iocp_poll(t_evloop *loop, int timeout_ms)
{
    t_evloop_iocp_state *st = (t_evloop_iocp_state *)loop->backend_state;
    if (!st) return -1;

    DWORD timeout = (timeout_ms < 0) ? INFINITE : (DWORD)timeout_ms;
    DWORD bytesTransferred;
    ULONG_PTR completionKey;
    LPOVERLAPPED overlapped;
    int n = 0;
    const int max_batch = 128;
    for (;;) {
        DWORD wait_ms = (n == 0) ? timeout : 0;
        BOOL ok = GetQueuedCompletionStatus(st->iocp_handle, &bytesTransferred, &completionKey, &overlapped, wait_ms);
        if (!ok) {
            DWORD err = GetLastError();
            if (err == WAIT_TIMEOUT) {
                return n;
            }
            return -1;
        }
        t_evio *io = (t_evio *)(ULONG_PTR)completionKey;
        if (io == &loop->wakeup_io) {
            /* Wakeup event; ignore payload */
            if (++n >= max_batch) return n;
            continue;
        }
        if (io && io->callback) {
            io->callback(io, io->events, io->user_data);
        }
        if (++n >= max_batch) return n;
    }
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
