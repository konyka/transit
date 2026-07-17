#include <stdlib.h>
#include <string.h>
#include "t_router.h"
#include "../collections/t_vec.h"

typedef struct t_binding {
    char *pattern;
    void *target;
} t_binding;

typedef struct t_router {
    t_vec bindings; /* array of t_binding* */
} t_router;

t_router *t_router_create(void) {
    t_router *r = (t_router *)malloc(sizeof(t_router));
    if (!r) return NULL;
    t_vec_init(&r->bindings);
    return r;
}

void t_router_destroy(t_router *router) {
    if (!router) return;
    for (size_t i = 0; i < router->bindings.len; ++i) {
        t_binding *b = (t_binding *)router->bindings.items[i];
        if (b) {
            free(b->pattern);
            free(b);
        }
    }
    t_vec_destroy(&router->bindings);
    free(router);
}

static int topic_match_pattern(const char *pattern, const char *topic) {
    if (!pattern || !topic) return 0;
    const char *p = pattern;
    const char *t = topic;
    for (;;) {
        size_t plen = 0;
        while (p[plen] && p[plen] != '.') plen++;
        size_t tlen = 0;
        while (t[tlen] && t[tlen] != '.') tlen++;
        int p_end = (p[plen] == '\0');
        int t_end = (t[tlen] == '\0');

        if (p_end && plen == 0 && t_end && tlen == 0) return 1;
        if (plen == 1 && p[0] == '#') {
            return p_end; /* '#' must be the final pattern segment */
        }
        if (t_end && tlen == 0) {
            /* Topic exhausted: only a trailing '#' may remain. */
            return (plen == 1 && p[0] == '#' && p_end);
        }
        if (p_end && plen == 0) {
            return 0;
        }
        if (!(plen == 1 && p[0] == '*') &&
            !(plen == tlen && memcmp(p, t, plen) == 0)) {
            return 0;
        }
        if (p_end && t_end) return 1;
        if (p_end) return 0;
        if (t_end) {
            p += plen + 1;
            return (p[0] == '#' && p[1] == '\0');
        }
        p += plen + 1;
        t += tlen + 1;
    }
}

int t_router_bind(t_router *router, const char *pattern, void *target) {
    if (!router || !pattern) return -1;
    /* Idempotent: replace target for an existing pattern. */
    for (size_t i = 0; i < router->bindings.len; ++i) {
        t_binding *existing = (t_binding *)router->bindings.items[i];
        if (existing && existing->pattern && strcmp(existing->pattern, pattern) == 0) {
            existing->target = target;
            return 0;
        }
    }
    t_binding *b = (t_binding *)malloc(sizeof(t_binding));
    if (!b) return -1;
    b->pattern = strdup(pattern);
    if (!b->pattern) {
        free(b);
        return -1;
    }
    b->target = target;
    if (t_vec_push(&router->bindings, b) != 0) {
        free(b->pattern);
        free(b);
        return -1;
    }
    return 0;
}

int t_router_unbind(t_router *router, const char *pattern) {
    if (!router || !pattern) return -1;
    int removed = 0;
    for (size_t i = 0; i < router->bindings.len; ) {
        t_binding *b = (t_binding *)router->bindings.items[i];
        if (b && b->pattern && strcmp(b->pattern, pattern) == 0) {
            free(b->pattern);
            free(b);
            for (size_t j = i; j + 1 < router->bindings.len; ++j) {
                router->bindings.items[j] = router->bindings.items[j + 1];
            }
            router->bindings.len--;
            removed++;
            continue;
        }
        ++i;
    }
    return removed ? 0 : -1;
}

size_t t_router_route(t_router *router, const char *topic, void **targets, size_t max_targets) {
    if (!router || !topic || !targets || max_targets == 0) return 0;
    size_t count = 0;
    for (size_t i = 0; i < router->bindings.len; ++i) {
        t_binding *b = (t_binding *)router->bindings.items[i];
        if (!b) continue;
        if (topic_match_pattern(b->pattern, topic)) {
            targets[count++] = b->target;
            if (count >= max_targets) break;
        }
    }
    return count;
}

int t_router_has_binding(t_router *router, const char *pattern) {
    if (!router || !pattern) return 0;
    for (size_t i = 0; i < router->bindings.len; ++i) {
        t_binding *b = (t_binding *)router->bindings.items[i];
        if (b && b->pattern && strcmp(b->pattern, pattern) == 0) return 1;
    }
    return 0;
}
