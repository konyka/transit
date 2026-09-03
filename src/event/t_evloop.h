#ifndef T_EVLOOP_H
#define T_EVLOOP_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

#define T_EV_READ   0x01
#define T_EV_WRITE  0x02
#define T_EV_ERROR  0x04
#define T_EV_ONCE   0x08

typedef struct t_evloop t_evloop;
typedef struct t_evio   t_evio;

// Internal backend vtable used to swap IO backends (epoll, kqueue, etc.).
// This is intentionally kept as an internal abstraction; public API remains unchanged.
typedef struct t_evloop_backend {
    int  (*create)(t_evloop *loop);
    void (*destroy)(t_evloop *loop);
    int  (*add)(t_evloop *loop, t_evio *io, int events);
    int  (*mod)(t_evloop *loop, t_evio *io, int events);
    int  (*del)(t_evloop *loop, t_evio *io);
    int  (*poll)(t_evloop *loop, int timeout_ms);
    void (*wakeup)(t_evloop *loop);
} t_evloop_backend;

// Backend symbols provided by platform-specific implementations
extern t_evloop_backend const t_epoll_backend;
extern t_evloop_backend const t_kqueue_backend;
extern t_evloop_backend const t_iocp_backend;

typedef void (*t_ev_cb)(t_evio *io, int flags, void *user_data);
typedef void (*t_timer_cb)(void *user_data);

struct t_evio {
    int       fd;
    int       events;
    t_ev_cb   callback;
    void     *user_data;
    t_evloop *loop;
};

t_evloop *t_evloop_create(void);
void      t_evloop_destroy(t_evloop *loop);

int  t_evloop_add(t_evloop *loop, t_evio *io, int events);
int  t_evloop_mod(t_evloop *loop, t_evio *io, int events);
int  t_evloop_del(t_evloop *loop, t_evio *io);

int64_t t_evloop_timer_add(t_evloop *loop, int64_t timeout_ms, int repeat,
                           t_timer_cb callback, void *user_data);
void    t_evloop_timer_del(t_evloop *loop, int64_t timer_id);

/* Returns 0 when stopped normally; 1 if loop was destroyed from a poll/timer
 * callback (caller must not call t_evloop_destroy on the same pointer). */
int  t_evloop_run(t_evloop *loop, int timeout_ms);
void t_evloop_stop(t_evloop *loop);
int  t_evloop_is_running(const t_evloop *loop);

/* Free ptr after the current poll batch (safe while processing_poll).
 * If loop is NULL or not inside poll, frees immediately. */
void t_evloop_defer_free(t_evloop *loop, void *ptr);

#endif /* T_EVLOOP_H */
