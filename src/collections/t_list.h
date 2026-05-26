#ifndef T_LIST_H
#define T_LIST_H

#include "t_compiler.h"
#include <stddef.h>

/* Intrusive list node - embed this in your struct */
typedef struct t_list_node {
    struct t_list_node *prev;
    struct t_list_node *next;
} t_list_node;

/* List head */
typedef struct t_list {
    t_list_node *head;
    t_list_node *tail;
    size_t       count;
} t_list;

#define T_LIST_INIT { NULL, NULL, 0 }
#define T_LIST_NODE_INIT { NULL, NULL }

void        t_list_init(t_list *l);
int         t_list_empty(const t_list *l);
size_t      t_list_count(const t_list *l);
void        t_list_push_front(t_list *l, t_list_node *node);
void        t_list_push_back(t_list *l, t_list_node *node);
t_list_node *t_list_pop_front(t_list *l);
t_list_node *t_list_pop_back(t_list *l);
void        t_list_remove(t_list *l, t_list_node *node);
void        t_list_splice(t_list *dst, t_list *src);

#define T_LIST_ENTRY(node, type, member) \
    ((type *)((char *)(node) - offsetof(type, member)))

#define T_LIST_FOR_EACH(l, cursor) \
    for ((cursor) = (l)->head; (cursor) != NULL; (cursor) = (cursor)->next)

#define T_LIST_FOR_EACH_SAFE(l, cursor, next) \
    for ((cursor) = (l)->head, (next) = (cursor) ? (cursor)->next : NULL; \
         (cursor) != NULL; \
         (cursor) = (next), (next) = (cursor) ? (cursor)->next : NULL)

#endif /* T_LIST_H */
