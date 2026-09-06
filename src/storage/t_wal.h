#ifndef T_WAL_H
#define T_WAL_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

#define T_WAL_PUT  1
#define T_WAL_DEL  2
#define T_WAL_JOIN 3
#define T_WAL_MAX_FILE (256 * 1024 * 1024)

typedef struct t_wal t_wal;

typedef struct t_wal_rec {
    uint8_t        op;
    uint8_t        priority;
    uint64_t       msg_id;
    const uint8_t *data;
    uint32_t       data_len;
} t_wal_rec;

typedef void (*t_wal_replay_fn)(const t_wal_rec *rec, void *ud);

/* sync_every: 0 = only on flush/close; N = fsync every N records. */
t_wal *t_wal_open(const char *path, int sync_every);
void   t_wal_close(t_wal *w);
int    t_wal_append(t_wal *w, uint8_t op, uint8_t priority, uint64_t msg_id,
                    const uint8_t *data, uint32_t len);
int    t_wal_flush(t_wal *w);
int    t_wal_replay(t_wal *w, t_wal_replay_fn fn, void *ud);
int    t_wal_unlink(const char *path);
const char *t_wal_path(const t_wal *w);
size_t t_wal_appends(const t_wal *w);

#endif /* T_WAL_H */
