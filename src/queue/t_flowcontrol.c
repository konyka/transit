#include "t_flowcontrol.h"
#include "t_atomic.h"
#include <stdlib.h>

struct t_flowcontrol {
    size_t    max_credits;
    size_t    credits;
    int64_t   refill_interval_ms;
    t_atomic_int total_acquired;
    t_atomic_int total_released;
    t_atomic_int total_rejected;
    int       blocked;
};

t_flowcontrol *t_fc_create(size_t max_credits, int64_t refill_interval_ms) {
    t_flowcontrol *fc = (t_flowcontrol *)calloc(1, sizeof(*fc));
    if (!fc) return NULL;
    fc->max_credits = max_credits;
    fc->credits = max_credits;
    fc->refill_interval_ms = refill_interval_ms;
    return fc;
}

void t_fc_destroy(t_flowcontrol *fc) {
    free(fc);
}

int t_fc_acquire(t_flowcontrol *fc, size_t count) {
    if (!fc) return -1;
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
    fc->credits += count;
    if (fc->credits > fc->max_credits) fc->credits = fc->max_credits;
    if (fc->credits > 0) fc->blocked = 0;
    t_atomic_fetch_add_int(&fc->total_released, (int)count);
}

void t_fc_refill(t_flowcontrol *fc) {
    if (!fc) return;
    fc->credits = fc->max_credits;
    fc->blocked = 0;
}

size_t t_fc_available(const t_flowcontrol *fc) {
    return fc ? fc->credits : 0;
}

size_t t_fc_max(const t_flowcontrol *fc) {
    return fc ? fc->max_credits : 0;
}

int t_fc_is_blocked(const t_flowcontrol *fc) {
    return fc ? fc->blocked : 1;
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
