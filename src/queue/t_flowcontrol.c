#include "t_flowcontrol.h"
#include "t_atomic.h"
#include "t_time.h"
#include <stdlib.h>

struct t_flowcontrol {
    size_t    max_credits;
    size_t    credits;
    int64_t   refill_interval_ms;
    int64_t   last_refill_ms;
    t_atomic_int total_acquired;
    t_atomic_int total_released;
    t_atomic_int total_rejected;
    int       blocked;
};

static void fc_maybe_refill(t_flowcontrol *fc) {
    if (!fc || fc->refill_interval_ms <= 0) return;
    int64_t now = t_time_now_ms();
    if (fc->last_refill_ms == 0) {
        fc->last_refill_ms = now;
        return;
    }
    /* Clock rewind: reset baseline so credits can recover. */
    if (now < fc->last_refill_ms) {
        fc->last_refill_ms = now;
        return;
    }
    if (now - fc->last_refill_ms >= fc->refill_interval_ms) {
        fc->credits = fc->max_credits;
        fc->blocked = 0;
        fc->last_refill_ms = now;
    }
}

t_flowcontrol *t_fc_create(size_t max_credits, int64_t refill_interval_ms) {
    t_flowcontrol *fc = (t_flowcontrol *)calloc(1, sizeof(*fc));
    if (!fc) return NULL;
    fc->max_credits = max_credits;
    fc->credits = max_credits;
    fc->refill_interval_ms = refill_interval_ms;
    fc->last_refill_ms = t_time_now_ms();
    return fc;
}

void t_fc_destroy(t_flowcontrol *fc) {
    free(fc);
}

int t_fc_acquire(t_flowcontrol *fc, size_t count) {
    if (!fc) return -1;
    fc_maybe_refill(fc);
    if (count == 0) return 0; /* no-op: do not clear blocked */
    if (fc->credits >= count) {
        fc->credits -= count;
        fc->blocked = 0;
        t_atomic_fetch_add_int(&fc->total_acquired, (int)count);
        return 0;
    }
    fc->blocked = 1;
    t_atomic_fetch_add_int(&fc->total_rejected, (int)count);
    return -1;
}

void t_fc_release(t_flowcontrol *fc, size_t count) {
    if (!fc) return;
    if (fc->credits >= fc->max_credits) {
        fc->credits = fc->max_credits;
    } else if (count >= fc->max_credits - fc->credits) {
        fc->credits = fc->max_credits;
    } else {
        fc->credits += count;
    }
    if (fc->credits > 0) fc->blocked = 0;
    t_atomic_fetch_add_int(&fc->total_released, (int)count);
}

void t_fc_refill(t_flowcontrol *fc) {
    if (!fc) return;
    fc->credits = fc->max_credits;
    fc->blocked = 0;
    fc->last_refill_ms = t_time_now_ms();
}

size_t t_fc_available(const t_flowcontrol *fc) {
    if (!fc) return 0;
    /* Cast away const for lazy refill of mutable counters. */
    fc_maybe_refill((t_flowcontrol *)fc);
    return fc->credits;
}

size_t t_fc_max(const t_flowcontrol *fc) {
    return fc ? fc->max_credits : 0;
}

int t_fc_is_blocked(const t_flowcontrol *fc) {
    if (!fc) return 1;
    fc_maybe_refill((t_flowcontrol *)fc);
    return fc->blocked;
}

uint64_t t_fc_total_acquired(const t_flowcontrol *fc) {
    return fc ? (uint64_t)t_atomic_load_int((t_atomic_int *)&fc->total_acquired) : 0;
}

uint64_t t_fc_total_released(const t_flowcontrol *fc) {
    return fc ? (uint64_t)t_atomic_load_int((t_atomic_int *)&fc->total_released) : 0;
}

uint64_t t_fc_total_rejected(const t_flowcontrol *fc) {
    return fc ? (uint64_t)t_atomic_load_int((t_atomic_int *)&fc->total_rejected) : 0;
}
