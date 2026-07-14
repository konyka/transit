#ifndef T_ROUTER_H
#define T_ROUTER_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_router t_router;

t_router *t_router_create(void);
void      t_router_destroy(t_router *router);

int       t_router_bind(t_router *router, const char *pattern, void *target);
int       t_router_unbind(t_router *router, const char *pattern);
/* If return == max_targets, results may be truncated; enlarge buffer to fan out fully. */
size_t    t_router_route(t_router *router, const char *topic, void **targets, size_t max_targets);
int       t_router_has_binding(t_router *router, const char *pattern);

#endif /* T_ROUTER_H */
