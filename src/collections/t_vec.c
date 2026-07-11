#include <stdlib.h>
#include <string.h> /* for memcpy if needed */
#include <stddef.h>
#include <stdint.h>
#include "t_vec.h"

static int t_vec_grow(t_vec *v, size_t min_cap) {
    size_t new_cap = v->cap ? v->cap * 2 : 8;
    if (v->cap && new_cap / 2 != v->cap) return -1;
    if (new_cap < min_cap) new_cap = min_cap;
    if (new_cap > SIZE_MAX / sizeof(void *)) return -1;
    void **new_items = (void **)realloc(v->items, new_cap * sizeof(void *));
    if (!new_items) return -1;
    v->items = new_items;
    v->cap = new_cap;
    return 0;
}

void t_vec_init(t_vec *v) {
    if (!v) return;
    v->items = NULL;
    v->len = 0;
    v->cap = 0;
}

void t_vec_destroy(t_vec *v) {
    if (!v) return;
    free(v->items);
    v->items = NULL;
    v->len = 0;
    v->cap = 0;
}

size_t t_vec_len(const t_vec *v) {
    return v ? v->len : 0;
}

size_t t_vec_cap(const t_vec *v) {
    return v ? v->cap : 0;
}

int t_vec_push(t_vec *v, void *item) {
    if (!v) return -1;
    if (v->len >= SIZE_MAX) return -1;
    size_t need = v->len + 1;
    if (need < v->len) return -1;
    if (v->len >= v->cap) {
        if (t_vec_grow(v, need) != 0) return -1;
    }
    v->items[v->len++] = item;
    return 0;
}

void *t_vec_pop(t_vec *v) {
    if (!v || v->len == 0) return NULL;
    return v->items[--v->len];
}

void *t_vec_get(const t_vec *v, size_t index) {
    if (!v || index >= v->len) return NULL;
    return v->items[index];
}

int t_vec_set(t_vec *v, size_t index, void *item) {
    if (!v || index >= v->len) return -1;
    v->items[index] = item;
    return 0;
}

int t_vec_insert(t_vec *v, size_t index, void *item) {
    if (!v || index > v->len) return -1;
    if (v->len >= SIZE_MAX) return -1;
    size_t need = v->len + 1;
    if (need < v->len) return -1;
    if (v->len >= v->cap) {
        if (t_vec_grow(v, need) != 0) return -1;
    }
    /* move tail */
    for (size_t i = v->len; i > index; --i) {
        v->items[i] = v->items[i - 1];
    }
    v->items[index] = item;
    v->len++;
    return 0;
}

void *t_vec_remove(t_vec *v, size_t index) {
    if (!v || index >= v->len) return NULL;
    void *rem = v->items[index];
    for (size_t i = index; i + 1 < v->len; ++i) {
        v->items[i] = v->items[i + 1];
    }
    v->len--;
    return rem;
}

void t_vec_clear(t_vec *v) {
    if (!v) return;
    free(v->items);
    v->items = NULL;
    v->len = 0;
    v->cap = 0;
}

int t_vec_reserve(t_vec *v, size_t capacity) {
    if (!v) return -1;
    if (capacity <= v->cap) return 0;
    size_t new_cap = capacity;
    if (new_cap < 8) new_cap = 8;
    if (new_cap > SIZE_MAX / sizeof(void *)) return -1;
    void **new_items = (void **)realloc(v->items, new_cap * sizeof(void *));
    if (!new_items) return -1;
    v->items = new_items;
    v->cap = new_cap;
    return 0;
}

int t_vec_find(const t_vec *v, const void *item) {
    if (!v) return -1;
    for (size_t i = 0; i < v->len; ++i) {
        if (v->items[i] == item) return (int)i;
    }
    return -1;
}
