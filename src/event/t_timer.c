#include "t_timer.h"
#include "t_time.h"
#include <stdlib.h>
#include <string.h>

typedef struct timer_node {
    int64_t id;
    int64_t expire_ms;
    int64_t repeat_ms;
    t_timer_fn cb;
    void *user_data;
    int active;
} timer_node;

struct t_timer {
    timer_node *heap;
    size_t count;
    size_t cap;
    int64_t next_id;
};

static void swap_nodes(timer_node *a, timer_node *b) {
    timer_node t = *a; *a = *b; *b = t;
}

static void sift_up(timer_node *arr, size_t idx) {
    while (idx > 0) {
        size_t p = (idx - 1) / 2;
        if (arr[p].expire_ms <= arr[idx].expire_ms) break;
        swap_nodes(&arr[p], &arr[idx]);
        idx = p;
    }
}

static void sift_down(timer_node *arr, size_t idx, size_t n) {
    while (1) {
        size_t l = idx * 2 + 1;
        if (l >= n) break;
        size_t r = l + 1;
        size_t smallest = l;
        if (r < n && arr[r].expire_ms < arr[l].expire_ms) smallest = r;
        if (arr[idx].expire_ms <= arr[smallest].expire_ms) break;
        swap_nodes(&arr[idx], &arr[smallest]);
        idx = smallest;
    }
}

static int heap_push(t_timer *t, timer_node node) {
    if (t->heap == NULL) {
        size_t cap = 4;
        timer_node *heap = (timer_node *)calloc(cap, sizeof(timer_node));
        if (!heap) return -1;
        t->heap = heap;
        t->cap = cap;
    }
    if (t->count >= t->cap) {
        size_t new_cap = t->cap * 2;
        timer_node *heap = (timer_node *)realloc(t->heap, new_cap * sizeof(timer_node));
        if (!heap) return -1;
        t->heap = heap;
        t->cap = new_cap;
    }
    size_t idx = t->count;
    t->heap[idx] = node;
    sift_up(t->heap, idx);
    t->count++;
    return 0;
}

static timer_node heap_pop(t_timer *t) {
    timer_node res = t->heap[0];
    t->count--;
    if (t->count > 0) {
        t->heap[0] = t->heap[t->count];
        sift_down(t->heap, 0, t->count);
    }
    return res;
}

t_timer *t_timer_create(void) {
    t_timer *t = (t_timer *)calloc(1, sizeof(t_timer));
    if (!t) return NULL;
    t->heap = NULL;
    t->count = 0;
    t->cap = 0;
    t->next_id = 1;
    return t;
}

void t_timer_destroy(t_timer *t) {
    if (!t) return;
    free(t->heap);
    free(t);
}

int64_t t_timer_add(t_timer *t, int64_t delay_ms, int64_t repeat_ms,
                    t_timer_fn fn, void *user_data) {
    if (!t || !fn) return -1;
    timer_node node;
    node.id = t->next_id;
    node.expire_ms = t_time_now_ms() + delay_ms;
    node.repeat_ms = repeat_ms;
    node.cb = fn;
    node.user_data = user_data;
    node.active = 1;
    if (heap_push(t, node) != 0) return -1;
    t->next_id++;
    return node.id;
}

void t_timer_cancel(t_timer *t, int64_t id) {
    if (!t) return;
    for (size_t i = 0; i < t->count; ++i) {
        if (t->heap[i].id == id) {
            t->heap[i].active = 0;
            break;
        }
    }
}

int64_t t_timer_process(t_timer *t) {
    if (!t) return -1;
    int64_t now = t_time_now_ms();
    while (t->count > 0) {
        timer_node *top = &t->heap[0];
        if (!top->active) {
            heap_pop(t);
            continue;
        }
        if (top->expire_ms <= now) {
            timer_node cur = heap_pop(t);
            if (cur.cb) cur.cb(cur.user_data);
            if (cur.active && cur.repeat_ms > 0) {
                cur.expire_ms = now + cur.repeat_ms;
                (void)heap_push(t, cur); /* drop repeat on OOM */
            }
            now = t_time_now_ms();
        } else {
            break;
        }
    }
    if (t->count == 0) return -1;
    int64_t next = t->heap[0].expire_ms - t_time_now_ms();
    return (next < 0) ? 0 : next;
}

size_t t_timer_count(const t_timer *t) {
    if (!t) return 0;
    size_t c = 0;
    for (size_t i = 0; i < t->count; ++i)
        if (t->heap[i].active) c++;
    return c;
}
