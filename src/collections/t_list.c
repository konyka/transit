#include <stddef.h>
#include "t_list.h"

void t_list_init(t_list *l) {
    if (!l) return;
    l->head = NULL;
    l->tail = NULL;
    l->count = 0;
}

int t_list_empty(const t_list *l) {
    return l ? l->count == 0 : 1;
}

size_t t_list_count(const t_list *l) {
    return l ? l->count : 0;
}

void t_list_push_front(t_list *l, t_list_node *node) {
    if (!l || !node) return;
    node->prev = NULL;
    node->next = l->head;
    if (l->head) l->head->prev = node; else l->tail = node;
    l->head = node;
    l->count++;
}

void t_list_push_back(t_list *l, t_list_node *node) {
    if (!l || !node) return;
    node->next = NULL;
    node->prev = l->tail;
    if (l->tail) l->tail->next = node; else l->head = node;
    l->tail = node;
    l->count++;
}

t_list_node *t_list_pop_front(t_list *l) {
    if (!l || !l->head) return NULL;
    t_list_node *node = l->head;
    l->head = node->next;
    if (l->head) l->head->prev = NULL; else l->tail = NULL;
    node->next = NULL;
    node->prev = NULL;
    l->count--;
    return node;
}

t_list_node *t_list_pop_back(t_list *l) {
    if (!l || !l->tail) return NULL;
    t_list_node *node = l->tail;
    l->tail = node->prev;
    if (l->tail) l->tail->next = NULL; else l->head = NULL;
    node->next = NULL;
    node->prev = NULL;
    l->count--;
    return node;
}

void t_list_remove(t_list *l, t_list_node *node) {
    if (!l || !node) return;
    /* Detached node (not sole element): ignore to avoid clearing head/count. */
    if (!node->prev && !node->next && l->head != node) return;
    if (node->prev) node->prev->next = node->next; else l->head = node->next;
    if (node->next) node->next->prev = node->prev; else l->tail = node->prev;
    node->next = NULL;
    node->prev = NULL;
    if (l->count) l->count--;
}

void t_list_splice(t_list *dst, t_list *src) {
    if (!dst || !src || dst == src) return;
    if (!src->head) return;
    if (!dst->head) {
        dst->head = src->head;
        dst->tail = src->tail;
        dst->count = src->count;
    } else {
        dst->tail->next = src->head;
        src->head->prev = dst->tail;
        dst->tail = src->tail;
        dst->count += src->count;
    }
    src->head = NULL;
    src->tail = NULL;
    src->count = 0;
}
