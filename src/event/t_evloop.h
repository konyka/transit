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

int  t_evloop_run(t_evloop *loop, int timeout_ms);
void t_evloop_stop(t_evloop *loop);
int  t_evloop_is_running(const t_evloop *loop);

#endif /* T_EVLOOP_H */
