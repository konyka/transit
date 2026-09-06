#include "t_flowcontrol.h"
#include "t_atomic.h"
#include "t_time.h"
#include "t_mutex.h"
#include <stdlib.h>
#include <limits.h>

/* Stats counters are int atomics; clamp at INT_MAX so adds cannot wrap negative. */
static void fc_add_stat(t_atomic_int *counter, size_t count) {
    while (count > 0) {
        int cur = t_atomic_load_int(counter);
        if (cur < 0 || cur >= INT_MAX) return;
        int room = INT_MAX - cur;
        int chunk = (count > (size_t)room) ? room : (int)count;
        if (chunk <= 0) return;
        if (t_atomic_cas_int(counter, cur, cur + chunk))
            count -= (size_t)chunk;
    }
}

struct t_flowcontrol {
    size_t    max_credits;
    size_t    credits;
    int64_t   refill_interval_ms;
    int64_t   last_refill_ms;
    t_atomic_int total_acquired;
    t_atomic_int total_released;
    t_atomic_int total_rejected;
    int       blocked;
    t_mutex   mu;
};

static void fc_maybe_refill_unlocked(t_flowcontrol *fc) {
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
    t_mutex_init(&fc->mu);
    fc->max_credits = max_credits;
    fc->credits = max_credits;
    fc->refill_interval_ms = refill_interval_ms;
    fc->last_refill_ms = t_time_now_ms();
    return fc;
}

void t_fc_destroy(t_flowcontrol *fc) {
    if (!fc) return;
    t_mutex_destroy(&fc->mu);
    free(fc);
}

int t_fc_acquire(t_flowcontrol *fc, size_t count) {
    if (!fc) return -1;
    t_mutex_lock(&fc->mu);
    fc_maybe_refill_unlocked(fc);
    if (count == 0) {
        t_mutex_unlock(&fc->mu);
        return 0; /* no-op: do not clear blocked */
    }
    /* Impossible request: reject without latching blocked forever. */
    if (count > fc->max_credits) {
        fc_add_stat(&fc->total_rejected, count);
        t_mutex_unlock(&fc->mu);
        return -1;
    }
    if (fc->credits >= count) {
        fc->credits -= count;
        fc->blocked = 0;
        fc_add_stat(&fc->total_acquired, count);
        t_mutex_unlock(&fc->mu);
        return 0;
    }
    fc->blocked = 1;
    fc_add_stat(&fc->total_rejected, count);
    t_mutex_unlock(&fc->mu);
    return -1;
}

void t_fc_release(t_flowcontrol *fc, size_t count) {
    if (!fc) return;
    t_mutex_lock(&fc->mu);
    if (fc->credits >= fc->max_credits) {
        fc->credits = fc->max_credits;
    } else if (count >= fc->max_credits - fc->credits) {
        fc->credits = fc->max_credits;
    } else {
        fc->credits += count;
    }
    if (fc->credits > 0) fc->blocked = 0;
    fc_add_stat(&fc->total_released, count);
    t_mutex_unlock(&fc->mu);
}

void t_fc_refill(t_flowcontrol *fc) {
    if (!fc) return;
    t_mutex_lock(&fc->mu);
    fc->credits = fc->max_credits;
    fc->blocked = 0;
    fc->last_refill_ms = t_time_now_ms();
    t_mutex_unlock(&fc->mu);
}

size_t t_fc_available(const t_flowcontrol *fc) {
    if (!fc) return 0;
    t_flowcontrol *m = (t_flowcontrol *)fc;
    t_mutex_lock(&m->mu);
    fc_maybe_refill_unlocked(m);
    size_t credits = m->credits;
    t_mutex_unlock(&m->mu);
    return credits;
}

size_t t_fc_max(const t_flowcontrol *fc) {
    return fc ? fc->max_credits : 0;
}

int t_fc_is_blocked(const t_flowcontrol *fc) {
    if (!fc) return 1;
    t_flowcontrol *m = (t_flowcontrol *)fc;
    t_mutex_lock(&m->mu);
    fc_maybe_refill_unlocked(m);
    int blocked = m->blocked;
    t_mutex_unlock(&m->mu);
    return blocked;
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
