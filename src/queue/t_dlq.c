#include "t_dlq.h"
#include "t_time.h"
#include <stdlib.h>
#include <string.h>

struct t_dlq {
    t_dlq_entry *entries;
    size_t       head;
    size_t       tail;
    size_t       count;
    size_t       capacity;
    uint64_t     total_pushed;
    uint64_t     total_popped;
    uint64_t     total_dropped;
};

t_dlq *t_dlq_create(size_t max_entries) {
    t_dlq *dlq = (t_dlq *)calloc(1, sizeof(*dlq));
    if (!dlq) return NULL;
    dlq->capacity = max_entries > 0 ? max_entries : 1024;
    dlq->entries = (t_dlq_entry *)calloc(dlq->capacity, sizeof(t_dlq_entry));
    if (!dlq->entries) { free(dlq); return NULL; }
    return dlq;
}

void t_dlq_destroy(t_dlq *dlq) {
    if (!dlq) return;
    for (size_t i = 0; i < dlq->capacity; i++) {
        free(dlq->entries[i].topic);
        free(dlq->entries[i].payload);
        free(dlq->entries[i].reason);
    }
    free(dlq->entries);
    free(dlq);
}

int t_dlq_push(t_dlq *dlq, const char *topic, const uint8_t *payload, size_t len, const char *reason) {
    if (!dlq || (len > 0 && !payload)) return -1;

    /* Allocate first so a full queue is not evicted on OOM. */
    char *topic_copy = topic ? strdup(topic) : NULL;
    if (topic && !topic_copy) return -1;
    uint8_t *payload_copy = NULL;
    if (len > 0 && payload) {
        payload_copy = (uint8_t *)malloc(len);
        if (!payload_copy) {
            free(topic_copy);
            return -1;
        }
        memcpy(payload_copy, payload, len);
    }
    char *reason_copy = reason ? strdup(reason) : NULL;
    if (reason && !reason_copy) {
        free(topic_copy);
        free(payload_copy);
        return -1;
    }

    if (dlq->count >= dlq->capacity) {
        t_dlq_entry *oldest = &dlq->entries[dlq->head];
        free(oldest->topic);
        free(oldest->payload);
        free(oldest->reason);
        memset(oldest, 0, sizeof(*oldest));
        dlq->head = (dlq->head + 1) % dlq->capacity;
        dlq->count--;
        dlq->total_dropped++;
    }
    size_t idx = (dlq->tail) % dlq->capacity;
    t_dlq_entry *e = &dlq->entries[idx];
    e->topic = topic_copy;
    e->payload = payload_copy;
    e->payload_len = (len > 0 && payload) ? len : 0;
    e->reason = reason_copy;
    e->timestamp_ms = (uint64_t)t_time_now_ms();
    e->retry_count = 0;
    dlq->tail = (dlq->tail + 1) % dlq->capacity;
    dlq->count++;
    dlq->total_pushed++;
    return 0;
}

int t_dlq_pop(t_dlq *dlq, t_dlq_entry *out) {
    if (!dlq || !out || dlq->count == 0) return -1;
    t_dlq_entry *e = &dlq->entries[dlq->head];
    *out = *e;
    memset(e, 0, sizeof(*e));
    dlq->head = (dlq->head + 1) % dlq->capacity;
    dlq->count--;
    dlq->total_popped++;
    return 0;
}

void t_dlq_clear(t_dlq *dlq) {
    if (!dlq) return;
    while (dlq->count > 0) {
        t_dlq_entry *e = &dlq->entries[dlq->head];
        free(e->topic);
        free(e->payload);
        free(e->reason);
        memset(e, 0, sizeof(*e));
        dlq->head = (dlq->head + 1) % dlq->capacity;
        dlq->count--;
    }
}

size_t t_dlq_size(const t_dlq *dlq) {
    return dlq ? dlq->count : 0;
}

size_t t_dlq_capacity(const t_dlq *dlq) {
    return dlq ? dlq->capacity : 0;
}

int t_dlq_is_empty(const t_dlq *dlq) {
    return dlq ? dlq->count == 0 : 1;
}

int t_dlq_is_full(const t_dlq *dlq) {
    return dlq ? dlq->count >= dlq->capacity : 1;
}

uint64_t t_dlq_total_pushed(const t_dlq *dlq) {
    return dlq ? dlq->total_pushed : 0;
}

uint64_t t_dlq_total_popped(const t_dlq *dlq) {
    return dlq ? dlq->total_popped : 0;
}

uint64_t t_dlq_total_dropped(const t_dlq *dlq) {
    return dlq ? dlq->total_dropped : 0;
}
