#include "t_ratelimit.h"
#include "t_mutex.h"
#include <stdlib.h>

struct t_ratelimit {
    size_t   max_tokens;
    double   refill_rate;
    double   tokens;
    uint64_t last_refill_ms;
    int      initialized;
    uint64_t total_allowed;
    uint64_t total_rejected;
    t_mutex  mu;
};

t_ratelimit *t_ratelimit_create(size_t max_tokens, double refill_rate) {
    t_ratelimit *rl = (t_ratelimit *)calloc(1, sizeof(*rl));
    if (!rl) return NULL;
    t_mutex_init(&rl->mu);
    /* Reject NaN/Inf/negative so tokens cannot become permanently unusable. */
    if (!(refill_rate >= 0.0) || refill_rate > 1e18) refill_rate = 0.0;
    rl->max_tokens = max_tokens;
    rl->refill_rate = refill_rate;
    rl->tokens = (double)max_tokens;
    rl->initialized = 0;
    return rl;
}

void t_ratelimit_destroy(t_ratelimit *rl) {
    if (!rl) return;
    t_mutex_destroy(&rl->mu);
    free(rl);
}

static void refill_unlocked(t_ratelimit *rl, uint64_t now_ms) {
    if (!rl->initialized) {
        rl->last_refill_ms = now_ms;
        rl->initialized = 1;
        return;
    }
    if (now_ms < rl->last_refill_ms) {
        /* Clock rewind: reset baseline so tokens can recover. */
        rl->last_refill_ms = now_ms;
        return;
    }
    if (now_ms == rl->last_refill_ms) return;
    double elapsed = (double)(now_ms - rl->last_refill_ms);
    rl->tokens += elapsed * rl->refill_rate;
    if (rl->tokens > (double)rl->max_tokens) {
        rl->tokens = (double)rl->max_tokens;
    }
    if (rl->tokens < 0.0) rl->tokens = 0.0;
    rl->last_refill_ms = now_ms;
}

int t_ratelimit_allow(t_ratelimit *rl, uint64_t now_ms) {
    if (!rl) return 0;
    t_mutex_lock(&rl->mu);
    refill_unlocked(rl, now_ms);
    int ok = 0;
    if (rl->tokens >= 1.0) {
        rl->tokens -= 1.0;
        rl->total_allowed++;
        ok = 1;
    } else {
        rl->total_rejected++;
    }
    t_mutex_unlock(&rl->mu);
    return ok;
}

size_t t_ratelimit_available(t_ratelimit *rl, uint64_t now_ms) {
    if (!rl) return 0;
    t_mutex_lock(&rl->mu);
    refill_unlocked(rl, now_ms);
    size_t n = 0;
    if (rl->tokens > 0.0) n = (size_t)rl->tokens;
    t_mutex_unlock(&rl->mu);
    return n;
}

uint64_t t_ratelimit_total_allowed(const t_ratelimit *rl) {
    if (!rl) return 0;
    t_ratelimit *m = (t_ratelimit *)rl;
    t_mutex_lock(&m->mu);
    uint64_t v = m->total_allowed;
    t_mutex_unlock(&m->mu);
    return v;
}

uint64_t t_ratelimit_total_rejected(const t_ratelimit *rl) {
    if (!rl) return 0;
    t_ratelimit *m = (t_ratelimit *)rl;
    t_mutex_lock(&m->mu);
    uint64_t v = m->total_rejected;
    t_mutex_unlock(&m->mu);
    return v;
}

void t_ratelimit_reset(t_ratelimit *rl) {
    if (!rl) return;
    t_mutex_lock(&rl->mu);
    rl->tokens = (double)rl->max_tokens;
    rl->initialized = 0;
    rl->total_allowed = 0;
    rl->total_rejected = 0;
    t_mutex_unlock(&rl->mu);
}
