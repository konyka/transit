#ifndef T_DLQ_H
#define T_DLQ_H

#include <stddef.h>
#include <stdint.h>

typedef struct t_dlq t_dlq;
typedef struct t_dlq_entry t_dlq_entry;

struct t_dlq_entry {
    char     *topic;
    uint8_t  *payload;
    size_t    payload_len;
    char     *reason;
    uint64_t  timestamp_ms;
    int       retry_count;
};

/* Align with T_QUEUE_MAX_PAYLOAD / wire max. */
#define T_DLQ_MAX_PAYLOAD (16 * 1024 * 1024)

t_dlq *t_dlq_create(size_t max_entries);
void   t_dlq_destroy(t_dlq *dlq);

int  t_dlq_push(t_dlq *dlq, const char *topic, const uint8_t *payload, size_t len, const char *reason);
int  t_dlq_pop(t_dlq *dlq, t_dlq_entry *out);
void t_dlq_clear(t_dlq *dlq);

size_t t_dlq_size(const t_dlq *dlq);
size_t t_dlq_capacity(const t_dlq *dlq);
int    t_dlq_is_empty(const t_dlq *dlq);
int    t_dlq_is_full(const t_dlq *dlq);

uint64_t t_dlq_total_pushed(const t_dlq *dlq);
uint64_t t_dlq_total_popped(const t_dlq *dlq);
uint64_t t_dlq_total_dropped(const t_dlq *dlq);

#endif
