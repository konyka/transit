#include "t_raft.h"
#include "t_wire.h"
#include "t_crc32c.h"
#include "t_proto.h"
#include "t_file.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <limits.h>

#define T_RAFT_HDR_V1 24
#define T_RAFT_HDR_V2 32
#define T_RAFT_REC 25
#define T_RAFT_VER 2
#define T_RAFT_SNAP_VER 1
#define T_RAFT_COMPACT_MIN_DEFAULT 64

/* Internal raft representation (opaque to users) */
struct t_raft {
    uint64_t self_id;
    uint64_t current_term;
    t_nrole state;
    uint64_t commit_index;
    uint64_t last_applied;
    uint64_t voted_for; /* 0 means none, else node id */
    t_raft_entry *log;
    size_t log_cap;
    size_t log_count;
    t_raft_apply_cb apply_cb;
    void *apply_ud;
    t_raft_replicate_cb replicate_cb;
    void *replicate_ud;
    t_raft_snap_encode_cb snap_encode_cb;
    t_raft_snap_apply_cb snap_apply_cb;
    void *snap_ud;
    uint64_t snap_index;
    uint64_t snap_term;
    uint8_t *snap_payload;
    size_t snap_payload_len;
    int snap_applied;
    size_t compact_min;
    int applying; /* nest count while apply_cb runs */
    int free_pending;
    char *log_path;
    t_file logf;
    int sync_every;
    int unsynced;
    uint64_t election_timeout_ms;
    uint64_t heartbeat_interval_ms;
};

static int ensure_log_cap(t_raft *r, size_t needed) {
    if (r->log_cap >= needed) return 0;
    size_t newcap = r->log_cap ? r->log_cap * 2 : 4;
    if (r->log_cap && newcap / 2 != r->log_cap) return -1;
    while (newcap < needed) {
        if (newcap > SIZE_MAX / 2) return -1;
        newcap *= 2;
    }
    if (newcap > SIZE_MAX / sizeof(t_raft_entry)) return -1;
    t_raft_entry *newlog = (t_raft_entry *)realloc(r->log, newcap * sizeof(t_raft_entry));
    if (!newlog) return -1;
    r->log = newlog;
    r->log_cap = newcap;
    return 0;
}

static void write_le32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static void write_le64(uint8_t *p, uint64_t v) {
    write_le32(p, (uint32_t)v);
    write_le32(p + 4, (uint32_t)(v >> 32));
}

static uint32_t read_le32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t read_le64(const uint8_t *p) {
    return (uint64_t)read_le32(p) | ((uint64_t)read_le32(p + 4) << 32);
}

static int raft_sync_meta(t_raft *r) {
    if (!r || !t_file_is_open(&r->logf)) return 0;
    uint8_t hdr[T_RAFT_HDR_V2];
    memset(hdr, 0, sizeof(hdr));
    hdr[0] = 'T'; hdr[1] = 'R'; hdr[2] = 'F'; hdr[3] = 'T';
    hdr[4] = T_RAFT_VER;
    write_le64(hdr + 8, r->current_term);
    write_le64(hdr + 16, r->voted_for);
    write_le64(hdr + 24, r->commit_index);
    if (t_file_seek(&r->logf, 0, SEEK_SET) != 0) return -1;
    if (t_file_write(&r->logf, hdr, sizeof(hdr)) != 0) return -1;
    if (t_file_sync(&r->logf) != 0) return -1;
    if (t_file_seek(&r->logf, 0, SEEK_END) != 0) return -1;
    r->unsynced = 0;
    return 0;
}

static int raft_write_entry(t_raft *r, const t_raft_entry *e) {
    if (!r || !t_file_is_open(&r->logf) || !e) return 0;
    if (e->data_len > T_PROTO_MAX_PAYLOAD) return -1;
    size_t rec = (size_t)T_RAFT_REC + e->data_len;
    uint8_t stack[256];
    uint8_t *buf = stack;
    int heap = 0;
    if (rec > sizeof(stack)) {
        buf = (uint8_t *)malloc(rec);
        if (!buf) return -1;
        heap = 1;
    }
    write_le64(buf + 4, e->term);
    write_le64(buf + 12, e->index);
    buf[20] = e->type;
    write_le32(buf + 21, (uint32_t)e->data_len);
    if (e->data_len) memcpy(buf + T_RAFT_REC, e->data, e->data_len);
    uint32_t crc = t_crc32c(buf + 4, rec - 4);
    write_le32(buf, crc);
    int rc = t_file_write(&r->logf, buf, rec);
    if (heap) free(buf);
    if (rc != 0) return -1;
    r->unsynced++;
    if (r->sync_every > 0 && r->unsynced >= r->sync_every) {
        if (t_file_sync(&r->logf) != 0) return -1;
        r->unsynced = 0;
    }
    return 0;
}

static int raft_rewrite_log(t_raft *r) {
    if (!r || !t_file_is_open(&r->logf) || !r->log_path) return 0;
    char tmp[1024];
    int n = snprintf(tmp, sizeof(tmp), "%s.tmp", r->log_path);
    if (n < 0 || (size_t)n >= sizeof(tmp)) return -1;
    t_file neu;
    t_file_init(&neu);
    if (t_file_open(&neu, tmp, T_FILE_READ | T_FILE_WRITE | T_FILE_CREAT | T_FILE_TRUNC) != 0)
        return -1;
    t_file saved = r->logf;
    r->logf = neu;
    if (raft_sync_meta(r) != 0) {
        r->logf = saved;
        t_file_close(&neu);
        (void)t_file_unlink(tmp);
        return -1;
    }
    for (size_t i = 0; i < r->log_count; i++) {
        if (raft_write_entry(r, &r->log[i]) != 0) {
            r->logf = saved;
            t_file_close(&neu);
            (void)t_file_unlink(tmp);
            return -1;
        }
    }
    if (t_file_sync(&r->logf) != 0) {
        r->logf = saved;
        t_file_close(&neu);
        (void)t_file_unlink(tmp);
        return -1;
    }
    t_file_close(&saved);
    t_file_close(&r->logf);
    if (t_file_rename(tmp, r->log_path) != 0) {
        (void)t_file_unlink(tmp);
        (void)t_file_open(&r->logf, r->log_path, T_FILE_READ | T_FILE_WRITE);
        return -1;
    }
    if (t_file_open(&r->logf, r->log_path, T_FILE_READ | T_FILE_WRITE) != 0) return -1;
    if (t_file_seek(&r->logf, 0, SEEK_END) != 0) return -1;
    r->unsynced = 0;
    return 0;
}

static void raft_truncate_from(t_raft *r, uint64_t index) {
    if (!r) return;
    size_t keep = 0;
    while (keep < r->log_count && r->log[keep].index < index)
        keep++;
    for (size_t i = keep; i < r->log_count; i++)
        free(r->log[i].data);
    r->log_count = keep;
}

static uint64_t raft_last_index(const t_raft *r) {
    if (!r) return 0;
    if (r->log_count) return r->log[r->log_count - 1].index;
    return r->snap_index;
}

static uint64_t raft_last_term(const t_raft *r) {
    if (!r) return 0;
    if (r->log_count) return r->log[r->log_count - 1].term;
    return r->snap_term;
}

t_raft *t_raft_create(const t_raft_config *cfg) {
    t_raft *r = (t_raft *)calloc(1, sizeof(t_raft));
    if (!r) return NULL;
    r->self_id = cfg ? cfg->node_id : 0;
    r->current_term = 0;
    r->state = T_NODE_FOLLOWER;
    r->commit_index = 0;
    r->last_applied = 0;
    r->voted_for = 0;
    r->log = NULL;
    r->log_cap = 0;
    r->log_count = 0;
    r->apply_cb = NULL;
    r->apply_ud = NULL;
    r->snap_applied = 1;
    r->compact_min = T_RAFT_COMPACT_MIN_DEFAULT;
    r->log_path = NULL;
    t_file_init(&r->logf);
    r->election_timeout_ms = (cfg && cfg->election_timeout_ms) ? cfg->election_timeout_ms : 150;
    r->heartbeat_interval_ms = (cfg && cfg->heartbeat_interval_ms) ? cfg->heartbeat_interval_ms : 50;
    return r;
}

void t_raft_destroy(t_raft *raft) {
    if (!raft) return;
    if (raft->applying > 0) {
        raft->free_pending = 1;
        return;
    }
    raft->free_pending = 0;
    for (size_t i = 0; i < raft->log_count; i++) {
        free(raft->log[i].data);
    }
    free(raft->log);
    free(raft->snap_payload);
    t_file_close(&raft->logf);
    free(raft->log_path);
    free(raft);
}

uint64_t t_raft_current_term(const t_raft *raft) {
    return raft ? raft->current_term : 0;
}

t_nrole t_raft_state(const t_raft *raft) {
    return raft ? raft->state : T_NODE_FOLLOWER;
}

uint64_t t_raft_commit_index(const t_raft *raft) {
    return raft ? raft->commit_index : 0;
}

uint64_t t_raft_last_applied(const t_raft *raft) {
    return raft ? raft->last_applied : 0;
}

int t_raft_become_candidate(t_raft *raft) {
    if (!raft) return -1;
    /* Each election (including retries while already candidate) needs a new term. */
    if (raft->current_term == UINT64_MAX) return -1;
    raft->current_term += 1;
    raft->state = T_NODE_CANDIDATE;
    raft->voted_for = raft->self_id; /* vote for self */
    return raft_sync_meta(raft);
}

int t_raft_become_leader(t_raft *raft) {
    if (!raft) return -1;
    if (raft->state != T_NODE_CANDIDATE) return -1;
    raft->state = T_NODE_LEADER;
    return 0;
}

int t_raft_become_follower(t_raft *raft, uint64_t term) {
    if (!raft) return -1;
    if (term < raft->current_term) return -1;
    if (term > raft->current_term) {
        raft->current_term = term;
        raft->voted_for = 0;
    }
    raft->state = T_NODE_FOLLOWER;
    return raft_sync_meta(raft);
}

int t_raft_append_entry(t_raft *raft, uint8_t type,
                        const uint8_t *data, size_t len) {
    if (!raft) return -1;
    if (raft->applying > 0 || raft->free_pending) return -1;
    if (len > 0 && !data) return -1;
    if (raft->log_count >= SIZE_MAX) return -1;
    if (ensure_log_cap(raft, raft->log_count + 1) != 0) return -1;
    t_raft_entry *e = &raft->log[raft->log_count];
    e->index = raft_last_index(raft) + 1;
    e->term = raft->current_term;
    e->type = type;
    if (len && data) {
        e->data = (uint8_t *)malloc(len);
        if (!e->data) return -1;
        memcpy(e->data, data, len);
        e->data_len = len;
    } else {
        e->data = NULL;
        e->data_len = 0;
    }
    raft->log_count++;
    if (raft_write_entry(raft, e) != 0) {
        raft->log_count--;
        free(e->data);
        e->data = NULL;
        return -1;
    }
    return 0;
}

size_t t_raft_log_count(const t_raft *raft) {
    return raft ? raft->log_count : 0;
}

const t_raft_entry *t_raft_get_entry(const t_raft *raft, uint64_t index) {
    if (!raft || index == 0 || !raft->log_count) return NULL;
    uint64_t first = raft->log[0].index;
    uint64_t last = raft->log[raft->log_count - 1].index;
    if (index < first || index > last) return NULL;
    return &raft->log[index - first];
}

int t_raft_advance_commit(t_raft *raft, uint64_t commit_idx) {
    if (!raft) return -1;
    uint64_t last = raft_last_index(raft);
    if (commit_idx > last) commit_idx = last;
    /* commit_index must be monotonic — never retreat past last_applied. */
    if (commit_idx < raft->commit_index) return -1;
    if (commit_idx != raft->commit_index) {
        raft->commit_index = commit_idx;
        if (raft_sync_meta(raft) != 0) return -1;
    }
    return 0;
}

static int raft_apply_pending_snap(t_raft *raft) {
    if (!raft || raft->snap_applied || !raft->snap_payload) return 0;
    if (raft->snap_apply_cb &&
        raft->snap_apply_cb(raft->snap_payload, raft->snap_payload_len,
                            raft->snap_ud) != 0)
        return -1;
    raft->snap_applied = 1;
    return 0;
}

int t_raft_apply_entries(t_raft *raft) {
    if (!raft) return -1;
    raft->applying++;
    if (raft_apply_pending_snap(raft) != 0) {
        if (--raft->applying == 0 && raft->free_pending) {
            t_raft_destroy(raft);
            return -2;
        }
        return -1;
    }
    while (!raft->free_pending && raft->last_applied < raft->commit_index) {
        uint64_t next = raft->last_applied + 1;
        const t_raft_entry *e = t_raft_get_entry(raft, next);
        if (!e) {
            if (--raft->applying == 0 && raft->free_pending) {
                t_raft_destroy(raft);
                return -2;
            }
            return -1;
        }
        if (raft->apply_cb) raft->apply_cb(e, raft->apply_ud);
        if (raft->free_pending) break;
        raft->last_applied = next;
    }
    if (--raft->applying == 0 && raft->free_pending) {
        t_raft_destroy(raft);
        return -2; /* raft freed; caller must not touch it */
    }
    return 0;
}

void t_raft_set_apply_cb(t_raft *raft, t_raft_apply_cb cb, void *ud) {
    if (!raft) return;
    raft->apply_cb = cb;
    raft->apply_ud = ud;
}

void t_raft_set_replicate_cb(t_raft *raft, t_raft_replicate_cb cb, void *ud) {
    if (!raft) return;
    raft->replicate_cb = cb;
    raft->replicate_ud = ud;
}

int t_raft_has_replicate_cb(const t_raft *raft) {
    return raft && raft->replicate_cb != NULL;
}

void t_raft_set_snapshot_cb(t_raft *raft, t_raft_snap_encode_cb enc,
                            t_raft_snap_apply_cb apply, void *ud) {
    if (!raft) return;
    raft->snap_encode_cb = enc;
    raft->snap_apply_cb = apply;
    raft->snap_ud = ud;
}

void t_raft_set_compact_min(t_raft *raft, size_t n) {
    if (raft) raft->compact_min = n;
}

uint64_t t_raft_snapshot_index(const t_raft *raft) {
    return raft ? raft->snap_index : 0;
}

uint64_t t_raft_snapshot_term(const t_raft *raft) {
    return raft ? raft->snap_term : 0;
}

static int raft_snap_path(const t_raft *r, char *out, size_t cap) {
    if (!r || !r->log_path || !out || cap < 8) return -1;
    int n = snprintf(out, cap, "%s.snap", r->log_path);
    if (n < 0 || (size_t)n >= cap) return -1;
    return 0;
}

static int raft_write_snap_file(t_raft *r, uint64_t index, uint64_t term,
                                const uint8_t *data, size_t len) {
    if (!r || !r->log_path) return -1;
    if (len > 0 && !data) return -1;
    if (len > (size_t)UINT32_MAX) return -1;
    char path[1024], tmp[1024];
    if (raft_snap_path(r, path, sizeof(path)) != 0) return -1;
    int n = snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    if (n < 0 || (size_t)n >= sizeof(tmp)) return -1;
    t_file f;
    t_file_init(&f);
    if (t_file_open(&f, tmp, T_FILE_READ | T_FILE_WRITE | T_FILE_CREAT | T_FILE_TRUNC) != 0)
        return -1;
    uint8_t hdr[28];
    memset(hdr, 0, sizeof(hdr));
    hdr[0] = 'T'; hdr[1] = 'R'; hdr[2] = 'F'; hdr[3] = 'S';
    hdr[4] = T_RAFT_SNAP_VER;
    write_le64(hdr + 8, index);
    write_le64(hdr + 16, term);
    write_le32(hdr + 24, (uint32_t)len);
    uint32_t crc = t_crc32c_update(0xFFFFFFFFu, hdr + 4, 24);
    if (len) crc = t_crc32c_update(crc, data, len);
    crc ^= 0xFFFFFFFFu;
    uint8_t crcb[4];
    write_le32(crcb, crc);
    int rc = -1;
    if (t_file_write(&f, hdr, sizeof(hdr)) == 0 &&
        (len == 0 || t_file_write(&f, data, len) == 0) &&
        t_file_write(&f, crcb, 4) == 0 &&
        t_file_sync(&f) == 0) {
        rc = 0;
    }
    t_file_close(&f);
    if (rc != 0) {
        (void)t_file_unlink(tmp);
        return -1;
    }
    if (t_file_rename(tmp, path) != 0) {
        (void)t_file_unlink(tmp);
        return -1;
    }
    return 0;
}

static int raft_load_snap_file(t_raft *r) {
    char path[1024];
    if (raft_snap_path(r, path, sizeof(path)) != 0) return -1;
    t_file f;
    t_file_init(&f);
    if (t_file_open(&f, path, T_FILE_READ) != 0)
        return t_file_not_found() ? 0 : -1;
    uint64_t sz = 0;
    if (t_file_size(&f, &sz) != 0 || sz < 32) {
        t_file_close(&f);
        return -1;
    }
    uint8_t hdr[28];
    if (t_file_read(&f, hdr, sizeof(hdr)) != 0) {
        t_file_close(&f);
        return -1;
    }
    if (hdr[0] != 'T' || hdr[1] != 'R' || hdr[2] != 'F' || hdr[3] != 'S' ||
        hdr[4] != T_RAFT_SNAP_VER) {
        t_file_close(&f);
        return -1;
    }
    uint64_t index = read_le64(hdr + 8);
    uint64_t term = read_le64(hdr + 16);
    uint32_t plen = read_le32(hdr + 24);
    if (sz != 32ull + (uint64_t)plen) {
        t_file_close(&f);
        return -1;
    }
    uint8_t *payload = NULL;
    if (plen) {
        payload = (uint8_t *)malloc(plen);
        if (!payload) {
            t_file_close(&f);
            return -1;
        }
        if (t_file_read(&f, payload, plen) != 0) {
            free(payload);
            t_file_close(&f);
            return -1;
        }
    }
    uint8_t crcb[4];
    if (t_file_read(&f, crcb, 4) != 0) {
        free(payload);
        t_file_close(&f);
        return -1;
    }
    t_file_close(&f);
    uint32_t crc = t_crc32c_update(0xFFFFFFFFu, hdr + 4, 24);
    if (plen) crc = t_crc32c_update(crc, payload, plen);
    crc ^= 0xFFFFFFFFu;
    if (crc != read_le32(crcb)) {
        free(payload);
        return -1;
    }
    r->snap_index = index;
    r->snap_term = term;
    r->snap_payload = payload;
    r->snap_payload_len = plen;
    r->snap_applied = 0;
    r->last_applied = index;
    if (r->commit_index < index) r->commit_index = index;
    return 0;
}

static int raft_compact_prefix(t_raft *r, uint64_t through) {
    if (!r || through == 0) return -1;
    if (through > r->last_applied) return -1;
    size_t drop = 0;
    while (drop < r->log_count && r->log[drop].index <= through)
        drop++;
    for (size_t i = 0; i < drop; i++)
        free(r->log[i].data);
    if (drop && drop < r->log_count)
        memmove(r->log, r->log + drop,
                (r->log_count - drop) * sizeof(t_raft_entry));
    r->log_count -= drop;
    if (t_file_is_open(&r->logf) && raft_rewrite_log(r) != 0) return -1;
    return 0;
}

int t_raft_snapshot(t_raft *raft, const uint8_t *data, size_t len) {
    if (!raft || raft->last_applied == 0) return -1;
    if (len > 0 && !data) return -1;
    uint64_t idx = raft->last_applied;
    uint64_t term = 0;
    const t_raft_entry *e = t_raft_get_entry(raft, idx);
    if (e) term = e->term;
    else if (idx == raft->snap_index) term = raft->snap_term;
    else return -1;
    if (raft->log_path && raft_write_snap_file(raft, idx, term, data, len) != 0)
        return -1;
    raft->snap_index = idx;
    raft->snap_term = term;
    free(raft->snap_payload);
    raft->snap_payload = NULL;
    raft->snap_payload_len = 0;
    if (len) {
        uint8_t *copy = (uint8_t *)malloc(len);
        if (!copy) return -1;
        memcpy(copy, data, len);
        raft->snap_payload = copy;
        raft->snap_payload_len = len;
    }
    raft->snap_applied = 1;
    return raft_compact_prefix(raft, idx);
}

int t_raft_snapshot_bytes(t_raft *raft, const uint8_t **data, size_t *len) {
    if (!raft || !data || !len || raft->snap_index == 0) return -1;
    *data = raft->snap_payload;
    *len = raft->snap_payload_len;
    return 0;
}

int t_raft_install_snapshot(t_raft *raft, uint64_t term,
                            uint64_t last_index, uint64_t last_term,
                            const uint8_t *data, size_t len) {
    if (!raft || last_index == 0) return -1;
    if (len > 0 && !data) return -1;
    if (term < raft->current_term) return 0;
    if (term > raft->current_term) {
        if (t_raft_become_follower(raft, term) != 0) return -1;
    } else {
        raft->state = T_NODE_FOLLOWER;
    }
    if (last_index <= raft->snap_index && raft->last_applied >= last_index)
        return 1;
    if (raft->snap_apply_cb) {
        if (raft->snap_apply_cb(data, len, raft->snap_ud) != 0)
            return -1;
    }
    uint8_t *copy = NULL;
    if (len) {
        copy = (uint8_t *)malloc(len);
        if (!copy) return -1;
        memcpy(copy, data, len);
    }
    free(raft->snap_payload);
    raft->snap_payload = copy;
    raft->snap_payload_len = len;
    raft->snap_index = last_index;
    raft->snap_term = last_term;
    raft->snap_applied = 1;
    raft->last_applied = last_index;
    if (raft->commit_index < last_index) raft->commit_index = last_index;
    for (size_t i = 0; i < raft->log_count; i++)
        free(raft->log[i].data);
    raft->log_count = 0;
    if (raft->log_path &&
        raft_write_snap_file(raft, last_index, last_term, data, len) != 0)
        return -1;
    if (t_file_is_open(&raft->logf) && raft_rewrite_log(raft) != 0) return -1;
    if (raft_sync_meta(raft) != 0) return -1;
    return 1;
}

int t_raft_maybe_snapshot(t_raft *raft, const uint64_t *matches, size_t nmatches,
                          size_t cluster_n) {
    if (!raft || raft->last_applied == 0) return 0;
    if (raft->last_applied <= raft->snap_index) return 0;
    if (raft->log_count < raft->compact_min) return 0;
    if (cluster_n == 0) cluster_n = 1;
    if (cluster_n > 1) {
        if (!matches || nmatches < cluster_n - 1) return 0;
        for (size_t i = 0; i < nmatches; i++) {
            if (matches[i] < raft->last_applied) return 0;
        }
    }
    if (!raft->snap_encode_cb) return 0;
    uint8_t *data = NULL;
    size_t len = 0;
    if (raft->snap_encode_cb(&data, &len, raft->snap_ud) != 0) return -1;
    int rc = t_raft_snapshot(raft, data, len);
    free(data);
    return rc;
}

int t_raft_replicate(t_raft *raft, uint64_t index) {
    if (!raft || !raft->replicate_cb) return 0;
    return raft->replicate_cb(raft, index, raft->replicate_ud);
}

int t_raft_majority_commit(t_raft *raft, const uint64_t *matches, size_t nmatches,
                           size_t cluster_n) {
    if (!raft) return -1;
    if (nmatches > 0 && !matches) return -1;
    if (cluster_n == 0) cluster_n = 1;
    size_t quorum = cluster_n / 2 + 1;
    uint64_t last = raft_last_index(raft);
    uint64_t best = raft->commit_index;
    for (uint64_t idx = last; idx > raft->commit_index; idx--) {
        const t_raft_entry *e = t_raft_get_entry(raft, idx);
        if (!e || e->term != raft->current_term) continue;
        size_t votes = 1;
        for (size_t i = 0; i < nmatches; i++) {
            if (matches[i] >= idx) votes++;
        }
        if (votes >= quorum) {
            best = idx;
            break;
        }
    }
    if (best != raft->commit_index) {
        raft->commit_index = best;
        if (raft_sync_meta(raft) != 0) return -1;
    }
    return t_raft_apply_entries(raft);
}

size_t t_raft_applied_count(const t_raft *raft) {
    return raft ? raft->last_applied : 0;
}

int t_raft_request_vote(t_raft *raft, uint64_t candidate_id, uint64_t term) {
    if (!raft) return -1;
    if (term < raft->current_term) return 0;
    if (term > raft->current_term) {
        raft->current_term = term;
        raft->voted_for = 0;
        raft->state = T_NODE_FOLLOWER;
    }
    if (raft->voted_for != 0 && raft->voted_for != candidate_id) return 0;
    raft->voted_for = candidate_id;
    raft->state = T_NODE_FOLLOWER;
    if (raft_sync_meta(raft) != 0) return 0;
    return 1;
}

int t_raft_grant_vote(t_raft *raft, uint64_t candidate_id) {
    if (!raft) return -1;
    if (raft->voted_for != 0 && raft->voted_for != candidate_id) return -1;
    raft->voted_for = candidate_id;
    raft->state = T_NODE_FOLLOWER;
    return raft_sync_meta(raft);
}

uint64_t t_raft_voted_for(const t_raft *raft) {
    return raft ? raft->voted_for : 0;
}

uint64_t t_raft_id(const t_raft *raft) {
    return raft ? raft->self_id : 0;
}

uint64_t t_raft_election_timeout_ms(const t_raft *raft) {
    return raft ? raft->election_timeout_ms : 0;
}

uint64_t t_raft_heartbeat_interval_ms(const t_raft *raft) {
    return raft ? raft->heartbeat_interval_ms : 0;
}

uint64_t t_raft_last_log_index(const t_raft *raft) {
    return raft_last_index(raft);
}

uint64_t t_raft_last_log_term(const t_raft *raft) {
    return raft_last_term(raft);
}

static int raft_append_copy(t_raft *r, uint64_t index, uint64_t term, uint8_t type,
                            const uint8_t *data, size_t len) {
    if (ensure_log_cap(r, r->log_count + 1) != 0) return -1;
    t_raft_entry *e = &r->log[r->log_count];
    e->index = index;
    e->term = term;
    e->type = type;
    e->data = NULL;
    e->data_len = 0;
    if (len) {
        e->data = (uint8_t *)malloc(len);
        if (!e->data) return -1;
        memcpy(e->data, data, len);
        e->data_len = len;
    }
    r->log_count++;
    return 0;
}

int t_raft_open_log(t_raft *raft, const char *path, int sync_every) {
    if (!raft || t_file_is_open(&raft->logf) || !path || !path[0]) return -1;
    if (raft->log_count != 0) return -1;
    char *dup = strdup(path);
    if (!dup) return -1;
    if (t_file_open(&raft->logf, path, T_FILE_READ | T_FILE_WRITE | T_FILE_CREAT) != 0) {
        free(dup);
        return -1;
    }
    raft->log_path = dup;
    raft->sync_every = sync_every < 0 ? 0 : sync_every;
    if (raft_load_snap_file(raft) != 0) return -1;
    uint64_t sz = 0;
    if (t_file_size(&raft->logf, &sz) != 0) {
        t_file_close(&raft->logf);
        free(raft->log_path);
        raft->log_path = NULL;
        return -1;
    }
    if (sz == 0) {
        if (raft_sync_meta(raft) != 0) return -1;
        return 0;
    }
    if (sz < T_RAFT_HDR_V1) return -1;
    uint8_t hdr[T_RAFT_HDR_V2];
    if (t_file_seek(&raft->logf, 0, SEEK_SET) != 0) return -1;
    if (t_file_read(&raft->logf, hdr, T_RAFT_HDR_V1) != 0) return -1;
    if (hdr[0] != 'T' || hdr[1] != 'R' || hdr[2] != 'F' || hdr[3] != 'T')
        return -1;
    uint8_t ver = hdr[4];
    if (ver != 1 && ver != T_RAFT_VER) return -1;
    raft->current_term = read_le64(hdr + 8);
    raft->voted_for = read_le64(hdr + 16);
    raft->commit_index = 0;
    if (ver == T_RAFT_VER) {
        if (sz < T_RAFT_HDR_V2) return -1;
        if (t_file_read(&raft->logf, hdr + T_RAFT_HDR_V1, 8) != 0) return -1;
        raft->commit_index = read_le64(hdr + 24);
    }
    raft->state = T_NODE_FOLLOWER;
    for (;;) {
        uint8_t rec[T_RAFT_REC];
        int rr = t_file_read(&raft->logf, rec, sizeof(rec));
        if (rr == 1) break;
        if (rr != 0) break;
        uint32_t crc = read_le32(rec);
        uint64_t term = read_le64(rec + 4);
        uint64_t index = read_le64(rec + 12);
        uint8_t type = rec[20];
        uint32_t len = read_le32(rec + 21);
        if (len > T_PROTO_MAX_PAYLOAD) return -1;
        uint8_t *data = NULL;
        if (len) {
            data = (uint8_t *)malloc(len);
            if (!data) return -1;
            rr = t_file_read(&raft->logf, data, len);
            if (rr != 0) {
                free(data);
                break;
            }
        }
        uint32_t calc = t_crc32c_update(0xFFFFFFFFu, rec + 4, (size_t)T_RAFT_REC - 4);
        if (len) calc = t_crc32c_update(calc, data, len);
        calc ^= 0xFFFFFFFFu;
        if (calc != crc) {
            free(data);
            return -1;
        }
        if (index <= raft->snap_index) {
            free(data);
            continue;
        }
        if (index != raft_last_index(raft) + 1) {
            free(data);
            return -1;
        }
        if (raft_append_copy(raft, index, term, type, data, len) != 0) {
            free(data);
            return -1;
        }
        if (term > raft->current_term) raft->current_term = term;
        free(data);
    }
    if (ver == 1) {
        if (raft_rewrite_log(raft) != 0) return -1;
        return 0;
    }
    if (t_file_seek(&raft->logf, 0, SEEK_END) != 0) return -1;
    return 0;
}

static int raft_on_vote_req(t_raft *r, const t_wire_vote_req *req,
                            uint64_t *out_term, uint8_t *granted) {
    *granted = 0;
    *out_term = r->current_term;
    if (req->term < r->current_term) return 0;
    if (req->term > r->current_term) {
        r->current_term = req->term;
        r->voted_for = 0;
        r->state = T_NODE_FOLLOWER;
    }
    *out_term = r->current_term;
    uint64_t our_idx = raft_last_index(r);
    uint64_t our_term = raft_last_term(r);
    int up = (req->last_log_term > our_term) ||
             (req->last_log_term == our_term && req->last_log_index >= our_idx);
    if ((r->voted_for == 0 || r->voted_for == req->candidate_id) && up) {
        r->voted_for = req->candidate_id;
        r->state = T_NODE_FOLLOWER;
        *granted = 1;
    }
    if (raft_sync_meta(r) != 0) {
        *granted = 0;
        return -1;
    }
    return 0;
}

static int raft_on_append_req(t_raft *r, const t_wire_append_req *req,
                              const t_wire_cluster_entry *ents, uint32_t n,
                              uint64_t *out_term, uint8_t *ok, uint64_t *match) {
    *ok = 0;
    *match = raft_last_index(r);
    *out_term = r->current_term;
    if (req->term < r->current_term) return 0;
    if (req->term > r->current_term) {
        r->current_term = req->term;
        r->voted_for = 0;
    }
    r->state = T_NODE_FOLLOWER;
    *out_term = r->current_term;
    if (req->prev_log_index > 0) {
        if (req->prev_log_index == r->snap_index) {
            if (req->prev_log_term != r->snap_term) {
                (void)raft_sync_meta(r);
                return 0;
            }
        } else {
            const t_raft_entry *prev = t_raft_get_entry(r, req->prev_log_index);
            if (!prev || prev->term != req->prev_log_term) {
                (void)raft_sync_meta(r);
                return 0;
            }
        }
    } else if (req->prev_log_term != 0) {
        (void)raft_sync_meta(r);
        return 0;
    }
    int dirty = 0;
    for (uint32_t i = 0; i < n; i++) {
        const t_wire_cluster_entry *e = &ents[i];
        if (e->index == 0) return -1;
        if (e->index <= r->snap_index) continue;
        const t_raft_entry *have = t_raft_get_entry(r, e->index);
        if (have) {
            if (have->term != e->term) {
                if (e->index <= r->commit_index) return 0;
                raft_truncate_from(r, e->index);
                dirty = 1;
            } else {
                continue;
            }
        } else if (e->index != raft_last_index(r) + 1) {
            (void)raft_sync_meta(r);
            return 0;
        }
        if (raft_append_copy(r, e->index, e->term, e->type, e->data, e->data_len) != 0)
            return -1;
        dirty = 1;
    }
    if (dirty && raft_rewrite_log(r) != 0) return -1;
    uint64_t last = raft_last_index(r);
    if (req->leader_commit > r->commit_index) {
        uint64_t c = req->leader_commit;
        if (c > last) c = last;
        r->commit_index = c;
    }
    *ok = 1;
    *match = last;
    if (raft_sync_meta(r) != 0) return -1;
    return 0;
}

int t_raft_rpc(t_raft *raft, const uint8_t *req, size_t req_len,
               uint8_t *resp, size_t resp_cap) {
    if (!raft || !req || req_len == 0 || !resp) return -1;
    uint8_t rpc = req[0];
    if (rpc == T_WIRE_CLUSTER_VOTE_REQ) {
        t_wire_vote_req v;
        if (t_wire_decode_vote_req(req, req_len, &v) != 0) return -1;
        uint64_t term = 0;
        uint8_t granted = 0;
        if (raft_on_vote_req(raft, &v, &term, &granted) != 0) return -1;
        return t_wire_encode_vote_resp(resp, resp_cap, term, granted);
    }
    if (rpc == T_WIRE_CLUSTER_APPEND_REQ) {
        t_wire_append_req a;
        t_wire_cluster_entry ents[T_WIRE_CLUSTER_MAX_ENTS];
        if (t_wire_decode_append_req(req, req_len, &a, ents, T_WIRE_CLUSTER_MAX_ENTS) != 0)
            return -1;
        uint64_t term = 0, match = 0;
        uint8_t ok = 0;
        if (raft_on_append_req(raft, &a, ents, a.nentries, &term, &ok, &match) != 0)
            return -1;
        int n = t_wire_encode_append_resp(resp, resp_cap, term, ok, match);
        if (n < 0) return -1;
        if (t_raft_apply_entries(raft) == -2) return n;
        return n;
    }
    if (rpc == T_WIRE_CLUSTER_SNAP_REQ) {
        t_wire_snap_req s;
        if (t_wire_decode_snap_req(req, req_len, &s) != 0) return -1;
        int inst = t_raft_install_snapshot(raft, s.term, s.last_index, s.last_term,
                                           s.data, s.data_len);
        if (inst < 0) return -1;
        uint8_t ok = inst > 0 ? 1 : 0;
        uint64_t match = ok ? s.last_index : t_raft_last_log_index(raft);
        return t_wire_encode_snap_resp(resp, resp_cap, t_raft_current_term(raft),
                                       ok, match);
    }
    return -1;
}
