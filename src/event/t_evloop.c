#include "t_evloop_internal.h"
#include "t_time.h"
#include "t_shutdown.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <limits.h>

static void timer_heap_swap(t_timer_entry *a, t_timer_entry *b) {
    t_timer_entry tmp = *a; *a = *b; *b = tmp;
}

static int evloop_process_timers(t_evloop *loop); /* uses t_evloop_destroy */

static void evloop_flush_deferred_free(t_evloop *loop) {
    if (!loop) return;
    if (loop->deferred_free) {
        for (size_t i = 0; i < loop->deferred_free_len; ++i) {
            free(loop->deferred_free[i]);
            loop->deferred_free[i] = NULL;
        }
        loop->deferred_free_len = 0;
    }
    while (loop->deferred_free_overflow) {
        struct t_defer_ovf *n = loop->deferred_free_overflow;
        loop->deferred_free_overflow = n->next;
        free(n->ptr);
        free(n);
    }
}

void t_evloop_defer_free(t_evloop *loop, void *ptr) {
    if (!ptr) return;
    if (!loop || !loop->processing_poll) {
        free(ptr);
        return;
    }
    if (loop->deferred_free_len >= loop->deferred_free_cap) {
        size_t ncap = loop->deferred_free_cap ? loop->deferred_free_cap * 2 : 8;
        if (ncap <= SIZE_MAX / sizeof(void *)) {
            void **n = (void **)realloc(loop->deferred_free, ncap * sizeof(void *));
            if (n) {
                loop->deferred_free = n;
                loop->deferred_free_cap = ncap;
            }
        }
    }
    if (loop->deferred_free_len < loop->deferred_free_cap) {
        loop->deferred_free[loop->deferred_free_len++] = ptr;
        return;
    }
    /* Array full and grow failed: chain a node so we never drop the pointer. */
    struct t_defer_ovf *node = (struct t_defer_ovf *)malloc(sizeof(*node));
    if (!node) return; /* true OOM: cannot defer without UAF risk */
    node->ptr = ptr;
    node->next = loop->deferred_free_overflow;
    loop->deferred_free_overflow = node;
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

static int timer_heap_push(t_evloop *loop, t_timer_entry entry) {
    if (loop->timers == NULL) {
        size_t cap = 16;
        t_timer_entry *timers = (t_timer_entry *)calloc(cap, sizeof(t_timer_entry));
        if (!timers) return -1;
        loop->timers = timers;
        loop->timer_cap = cap;
    }
    if (loop->timer_count >= loop->timer_cap) {
        if (loop->timer_cap > SIZE_MAX / 2 / sizeof(t_timer_entry)) return -1;
        size_t new_cap = loop->timer_cap * 2;
        t_timer_entry *timers = (t_timer_entry *)realloc(loop->timers, new_cap * sizeof(t_timer_entry));
        if (!timers) return -1;
        loop->timers = timers;
        loop->timer_cap = new_cap;
    }
    size_t idx = loop->timer_count;
    loop->timers[idx] = entry;
    timer_heap_sift_up(loop->timers, idx);
    loop->timer_count++;
    return 0;
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

static int evloop_process_timers(t_evloop *loop) {
    loop->processing_timers = 1;
    int64_t now = t_time_now_ms();
    while (loop->timer_count > 0 && !loop->destroy_pending) {
        t_timer_entry *top = &loop->timers[0];
        if (!top->active) {
            timer_heap_pop(loop);
            continue;
        }
        if (top->expire_ms <= now) {
            void (*cb)(void *) = top->callback;
            void *ud = top->user_data;
            int64_t repeat = top->repeat_ms;
            /* Rearm repeating timers in place (no alloc) before the callback. */
            if (repeat > 0) {
                if (now > INT64_MAX - repeat)
                    top->expire_ms = INT64_MAX;
                else
                    top->expire_ms = now + repeat;
                timer_heap_sift_down(loop->timers, 0, loop->timer_count);
            } else {
                timer_heap_pop(loop);
            }
            if (cb) cb(ud);
            if (loop->destroy_pending) break;
            now = t_time_now_ms();
            continue;
        }
        break;
    }
    loop->processing_timers = 0;
    if (loop->destroy_pending) {
        t_evloop_destroy(loop);
        return 1;
    }
    return 0;
}

t_evloop *t_evloop_create(void) {
    t_evloop *loop = (t_evloop *)calloc(1, sizeof(t_evloop));
    if (!loop) return NULL;
    loop->wakeup_fds[0] = loop->wakeup_fds[1] = -1;
    if (pipe(loop->wakeup_fds) != 0) {
        free(loop);
        return NULL;
    }
    /* Non-blocking: stop() must not hang if the wake pipe fills. */
    for (int i = 0; i < 2; ++i) {
        int fl = fcntl(loop->wakeup_fds[i], F_GETFL, 0);
        if (fl < 0 || fcntl(loop->wakeup_fds[i], F_SETFL, fl | O_NONBLOCK) < 0) {
            close(loop->wakeup_fds[0]);
            close(loop->wakeup_fds[1]);
            free(loop);
            return NULL;
        }
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
    if (loop->backend->add(loop, &loop->wakeup_io, T_EV_READ) != 0) {
        loop->backend->destroy(loop);
        close(loop->wakeup_fds[0]);
        close(loop->wakeup_fds[1]);
        free(loop);
        return NULL;
    }

    /* Pre-size defer list so poll-batch frees rarely hit realloc OOM. */
    loop->deferred_free_cap = 256;
    loop->deferred_free = (void **)calloc(loop->deferred_free_cap, sizeof(void *));
    if (!loop->deferred_free) {
        loop->backend->destroy(loop);
        close(loop->wakeup_fds[0]);
        close(loop->wakeup_fds[1]);
        free(loop);
        return NULL;
    }
    loop->next_timer_id = 1;
    loop->running = 0;
    return loop;
}

void t_evloop_destroy(t_evloop *loop) {
    if (!loop) return;
    if (loop->processing_timers || loop->processing_poll) {
        loop->destroy_pending = 1;
        t_evloop_stop(loop);
        return;
    }
    evloop_flush_deferred_free(loop);
    free(loop->deferred_free);
    loop->deferred_free = NULL;
    loop->deferred_free_cap = 0;
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
    /* Disarm so same-batch kevents/epolls cannot invoke a freed owner. */
    io->callback = NULL;
    io->user_data = NULL;
    return loop->backend->del(loop, io);
}

int64_t t_evloop_timer_add(t_evloop *loop, int64_t timeout_ms, int repeat,
                           t_timer_cb callback, void *user_data) {
    if (!loop || timeout_ms < 0) return -1;
    if (loop->next_timer_id == INT64_MAX) return -1;
    int64_t now = t_time_now_ms();
    if (timeout_ms > 0 && now > INT64_MAX - timeout_ms) return -1;
    t_timer_entry e;
    e.id = loop->next_timer_id;
    e.expire_ms = now + timeout_ms;
    e.repeat_ms = repeat;
    e.callback = callback;
    e.user_data = user_data;
    e.active = 1;
    if (timer_heap_push(loop, e) != 0) return -1;
    loop->next_timer_id++;
    return e.id;
}

void t_evloop_timer_del(t_evloop *loop, int64_t timer_id) {
    if (!loop || !loop->timers) return;
    for (size_t i = 0; i < loop->timer_count; ++i) {
        if (loop->timers[i].id == timer_id) {
            loop->timers[i].active = 0;
            /* Bubble to root so run() can pop cancelled entries promptly. */
            loop->timers[i].expire_ms = 0;
            timer_heap_sift_up(loop->timers, i);
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
                if (diff > INT_MAX) diff = INT_MAX;
                wait_ms = (timeout_ms < 0) ? (int)diff :
                          ((diff < timeout_ms) ? (int)diff : timeout_ms);
            }
        }
        loop->processing_poll = 1;
        loop->backend->poll(loop, wait_ms);
        loop->processing_poll = 0;
        evloop_flush_deferred_free(loop);
        if (loop->destroy_pending) {
            t_evloop_destroy(loop);
            return 1;
        }
        /* 1 => loop freed inside a timer callback; caller must not destroy. */
        if (evloop_process_timers(loop)) return 1;
        t_shutdown_on_signal(loop);
    }
    return 0;
}

void t_evloop_stop(t_evloop *loop) {
    if (!loop) return;
    loop->running = 0;
    if (loop->wakeup_fds[1] >= 0) {
        char b = 1;
        ssize_t w;
        do {
            w = write(loop->wakeup_fds[1], &b, 1);
        } while (w < 0 && errno == EINTR);
        /* EAGAIN: a wake byte is already queued; running=0 is enough. */
    }
}

int t_evloop_is_running(const t_evloop *loop) {
    return loop ? loop->running : 0;
}
