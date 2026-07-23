#ifndef T_EVLOOP_INTERNAL_H
#define T_EVLOOP_INTERNAL_H

#include "t_evloop.h"

typedef struct {
    int64_t     id;
    int64_t     expire_ms;
    int64_t     repeat_ms;
    t_timer_cb  callback;
    void       *user_data;
    int         active;
} t_timer_entry;

struct t_evloop {
    t_evloop_backend const *backend;
    void                  *backend_state;
    int         running;
    int64_t     next_timer_id;
    t_timer_entry *timers;
    size_t         timer_count;
    size_t         timer_cap;
    int         wakeup_fds[2];
    t_evio      wakeup_io;
    int         processing_timers;
    int         processing_poll;
    int         destroy_pending;
    void      **deferred_free;
    size_t      deferred_free_len;
    size_t      deferred_free_cap;
    /* Overflow chain when deferred_free realloc fails under a poll batch. */
    struct t_defer_ovf {
        void *ptr;
        struct t_defer_ovf *next;
    } *deferred_free_overflow;
    /* Last-resort single slot when overflow node malloc also fails. */
    void       *deferred_free_oom;
};

#endif
