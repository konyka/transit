#include "t_evloop.h"
#include "t_time.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <stdint.h>

typedef struct {
    int64_t     id;
    int64_t     expire_ms;
    int64_t     repeat_ms;
    t_timer_cb  callback;
    void       *user_data;
    int         active;
} t_timer_entry;

struct t_evloop {
    int         epoll_fd;
    int         running;
    int64_t     next_timer_id;
    t_timer_entry *timers;
    size_t         timer_count;
    size_t         timer_cap;
    int         wakeup_fds[2];
    t_evio      wakeup_io;
};

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
    loop->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (loop->epoll_fd < 0) {
        free(loop);
        return NULL;
    }
    loop->wakeup_fds[0] = loop->wakeup_fds[1] = -1;
    if (pipe(loop->wakeup_fds) != 0) {
        close(loop->epoll_fd);
        free(loop);
        return NULL;
    }
    loop->wakeup_io.fd = loop->wakeup_fds[0];
    loop->wakeup_io.loop = loop;
    loop->wakeup_io.events = T_EV_READ;
    loop->wakeup_io.callback = NULL;
    loop->wakeup_io.user_data = NULL;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.ptr = &loop->wakeup_io;
    ev.events = EPOLLIN;
    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_ADD, loop->wakeup_fds[0], &ev) != 0) {
        close(loop->wakeup_fds[0]);
        close(loop->wakeup_fds[1]);
        close(loop->epoll_fd);
        free(loop);
        return NULL;
    }
    loop->next_timer_id = 1;
    loop->timers = NULL;
    loop->timer_count = 0;
    loop->timer_cap = 0;
    loop->running = 0;
    return loop;
}

void t_evloop_destroy(t_evloop *loop) {
    if (!loop) return;
    if (loop->epoll_fd >= 0) close(loop->epoll_fd);
    if (loop->wakeup_fds[0] >= 0) close(loop->wakeup_fds[0]);
    if (loop->wakeup_fds[1] >= 0) close(loop->wakeup_fds[1]);
    free(loop->timers);
    free(loop);
}

int t_evloop_add(t_evloop *loop, t_evio *io, int events) {
    if (!loop || !io) return -1;
    io->loop = loop;
    io->events = events;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.ptr = io;
    int ep_events = 0;
    if (events & T_EV_READ) ep_events |= EPOLLIN;
    if (events & T_EV_WRITE) ep_events |= EPOLLOUT;
    if (events & T_EV_ONCE) ep_events |= EPOLLONESHOT;
    ev.events = (ep_events != 0) ? (uint32_t)ep_events : EPOLLIN;
    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_ADD, io->fd, &ev) != 0) return -1;
    return 0;
}

int t_evloop_mod(t_evloop *loop, t_evio *io, int events) {
    if (!loop || !io) return -1;
    io->events = events;
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.ptr = io;
    int ep_events = 0;
    if (events & T_EV_READ) ep_events |= EPOLLIN;
    if (events & T_EV_WRITE) ep_events |= EPOLLOUT;
    if (events & T_EV_ONCE) ep_events |= EPOLLONESHOT;
    ev.events = (ep_events != 0) ? (uint32_t)ep_events : EPOLLIN;
    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_MOD, io->fd, &ev) != 0) return -1;
    return 0;
}

int t_evloop_del(t_evloop *loop, t_evio *io) {
    if (!loop || !io) return -1;
    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_DEL, io->fd, NULL) != 0) return -1;
    return 0;
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
    struct epoll_event events[128];
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
        int nfds = epoll_wait(loop->epoll_fd, events, 128, wait_ms);
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
                    if (events[i].events & EPOLLIN) flags |= T_EV_READ;
                    if (events[i].events & EPOLLOUT) flags |= T_EV_WRITE;
                    if (events[i].events & (EPOLLERR | EPOLLHUP)) flags |= T_EV_ERROR;
                    io->callback(io, flags, io->user_data);
                }
            }
        }
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
