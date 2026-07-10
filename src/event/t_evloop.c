#include "t_evloop_internal.h"
#include "t_time.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>

static void timer_heap_swap(t_timer_entry *a, t_timer_entry *b) {
    t_timer_entry tmp = *a; *a = *b; *b = tmp;
}

static void timer_heap_sift_up(t_timer_entry *arr, size_t idx) {
    while (idx > 0) {
        size_t p = (idx - 1) / 2;
        if (arr[p].expire_ms <= arr[idx].expire_ms) break;
        timer_heap_swap(&arr[p], &arr[idx]);
        idx = p;
    }
}

static void timer_heap_sift_down(t_timer_entry *arr, size_t idx, size_t n) {
    for (;;) {
        size_t l = idx * 2 + 1;
        if (l >= n) break;
        size_t r = l + 1;
        size_t smallest = l;
        if (r < n && arr[r].expire_ms < arr[l].expire_ms) smallest = r;
        if (arr[idx].expire_ms <= arr[smallest].expire_ms) break;
        timer_heap_swap(&arr[idx], &arr[smallest]);
        idx = smallest;
    }
}

static void timer_heap_push(t_evloop *loop, t_timer_entry entry) {
    if (loop->timers == NULL) {
        loop->timer_cap = 16;
        loop->timers = (t_timer_entry *)calloc(loop->timer_cap, sizeof(t_timer_entry));
    }
    if (loop->timer_count >= loop->timer_cap) {
        loop->timer_cap *= 2;
        loop->timers = (t_timer_entry *)realloc(loop->timers, loop->timer_cap * sizeof(t_timer_entry));
    }
    size_t idx = loop->timer_count;
    loop->timers[idx] = entry;
    timer_heap_sift_up(loop->timers, idx);
    loop->timer_count++;
}

static t_timer_entry timer_heap_pop(t_evloop *loop) {
    t_timer_entry res = loop->timers[0];
    loop->timer_count--;
    if (loop->timer_count > 0) {
        loop->timers[0] = loop->timers[loop->timer_count];
        timer_heap_sift_down(loop->timers, 0, loop->timer_count);
    }
    return res;
}

static void evloop_process_timers(t_evloop *loop) {
    int64_t now = t_time_now_ms();
    while (loop->timer_count > 0) {
        t_timer_entry *top = &loop->timers[0];
        if (!top->active) {
            timer_heap_pop(loop);
            continue;
        }
        if (top->expire_ms <= now) {
            t_timer_entry cur = timer_heap_pop(loop);
            if (cur.active && cur.callback) {
                cur.callback(cur.user_data);
            }
            if (cur.active && cur.repeat_ms > 0) {
                cur.expire_ms = now + cur.repeat_ms;
                timer_heap_push(loop, cur);
            }
            now = t_time_now_ms();
            continue;
        }
        break;
    }
}

t_evloop *t_evloop_create(void) {
    t_evloop *loop = (t_evloop *)calloc(1, sizeof(t_evloop));
    if (!loop) return NULL;
    loop->wakeup_fds[0] = loop->wakeup_fds[1] = -1;
    if (pipe(loop->wakeup_fds) != 0) {
        free(loop);
        return NULL;
    }
    loop->wakeup_io.fd = loop->wakeup_fds[0];
    loop->wakeup_io.loop = loop;
    loop->wakeup_io.events = T_EV_READ;
    loop->wakeup_io.callback = NULL;
    loop->wakeup_io.user_data = NULL;

#ifdef T_HAVE_KQUEUE
    loop->backend = &t_kqueue_backend;
#elif defined(T_HAVE_IOCP)
    loop->backend = &t_iocp_backend;
#elif defined(T_HAVE_EPOLL)
    loop->backend = &t_epoll_backend;
#else
    free(loop);
    return NULL;
#endif
    if (loop->backend->create(loop) != 0) {
        close(loop->wakeup_fds[0]);
        close(loop->wakeup_fds[1]);
        free(loop);
        return NULL;
    }
    loop->backend->add(loop, &loop->wakeup_io, T_EV_READ);

    loop->next_timer_id = 1;
    loop->running = 0;
    return loop;
}

void t_evloop_destroy(t_evloop *loop) {
    if (!loop) return;
    loop->backend->destroy(loop);
    if (loop->wakeup_fds[0] >= 0) close(loop->wakeup_fds[0]);
    if (loop->wakeup_fds[1] >= 0) close(loop->wakeup_fds[1]);
    free(loop->timers);
    free(loop);
}

int t_evloop_add(t_evloop *loop, t_evio *io, int events) {
    if (!loop || !io) return -1;
    io->loop = loop;
    io->events = events;
    return loop->backend->add(loop, io, events);
}

int t_evloop_mod(t_evloop *loop, t_evio *io, int events) {
    if (!loop || !io) return -1;
    io->events = events;
    return loop->backend->mod(loop, io, events);
}

int t_evloop_del(t_evloop *loop, t_evio *io) {
    if (!loop || !io) return -1;
    return loop->backend->del(loop, io);
}

int64_t t_evloop_timer_add(t_evloop *loop, int64_t timeout_ms, int repeat,
                           t_timer_cb callback, void *user_data) {
    if (!loop) return -1;
    t_timer_entry e;
    e.id = loop->next_timer_id++;
    e.expire_ms = t_time_now_ms() + timeout_ms;
    e.repeat_ms = repeat;
    e.callback = callback;
    e.user_data = user_data;
    e.active = 1;
    timer_heap_push(loop, e);
    return e.id;
}

void t_evloop_timer_del(t_evloop *loop, int64_t timer_id) {
    if (!loop || !loop->timers) return;
    for (size_t i = 0; i < loop->timer_count; ++i) {
        if (loop->timers[i].id == timer_id) {
            loop->timers[i].active = 0;
            break;
        }
    }
}

int t_evloop_run(t_evloop *loop, int timeout_ms) {
    if (!loop) return -1;
    loop->running = 1;
    while (loop->running) {
        int wait_ms = timeout_ms;
        if (loop->timer_count > 0) {
            while (loop->timer_count > 0 && !loop->timers[0].active) {
                timer_heap_pop(loop);
            }
            if (loop->timer_count > 0) {
                int64_t now = t_time_now_ms();
                int64_t diff = loop->timers[0].expire_ms - now;
                if (diff < 0) diff = 0;
                wait_ms = (timeout_ms < 0) ? (int)diff :
                          ((diff < timeout_ms) ? (int)diff : timeout_ms);
            }
        }
        loop->backend->poll(loop, wait_ms);
        evloop_process_timers(loop);
    }
    return 0;
}

void t_evloop_stop(t_evloop *loop) {
    if (!loop) return;
    loop->running = 0;
    if (loop->wakeup_fds[1] >= 0) {
        char b = 1;
        write(loop->wakeup_fds[1], &b, 1);
    }
}

int t_evloop_is_running(const t_evloop *loop) {
    return loop ? loop->running : 0;
}
