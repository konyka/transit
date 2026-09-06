#ifndef T_RAFT_H
#define T_RAFT_H

#include "t_node.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_raft t_raft;

typedef struct t_raft_entry {
    uint64_t index;
    uint64_t term;
    uint8_t  type;
    uint8_t *data;
    size_t   data_len;
} t_raft_entry;

typedef struct t_raft_config {
    uint64_t node_id;
    uint64_t election_timeout_ms;
    uint64_t heartbeat_interval_ms;
} t_raft_config;

typedef void (*t_raft_apply_cb)(const t_raft_entry *entry, void *ud);
/* Optional: after a leader append, replicate index to peers and majority-commit. */
typedef int  (*t_raft_replicate_cb)(t_raft *raft, uint64_t index, void *ud);
typedef int  (*t_raft_snap_encode_cb)(uint8_t **out, size_t *len, void *ud);
typedef int  (*t_raft_snap_apply_cb)(const uint8_t *data, size_t len, void *ud);

t_raft            *t_raft_create(const t_raft_config *cfg);
void               t_raft_destroy(t_raft *raft);
void               t_raft_set_apply_cb(t_raft *raft, t_raft_apply_cb cb, void *ud);
void               t_raft_set_replicate_cb(t_raft *raft, t_raft_replicate_cb cb, void *ud);
void               t_raft_set_snapshot_cb(t_raft *raft, t_raft_snap_encode_cb enc,
                                          t_raft_snap_apply_cb apply, void *ud);
void               t_raft_set_compact_min(t_raft *raft, size_t n);
uint64_t           t_raft_snapshot_index(const t_raft *raft);
uint64_t           t_raft_snapshot_term(const t_raft *raft);
/* Persist broker state at last_applied and drop log prefix through that index. */
int                t_raft_snapshot(t_raft *raft, const uint8_t *data, size_t len);
/* Snapshot+compact when the log is long enough and every peer has the prefix. */
int                t_raft_maybe_snapshot(t_raft *raft, const uint64_t *matches,
                                          size_t nmatches, size_t cluster_n);
int                t_raft_snapshot_bytes(t_raft *raft, const uint8_t **data, size_t *len);
/* Install a leader snapshot. 1 = ok, 0 = rejected, -1 = error. */
int                t_raft_install_snapshot(t_raft *raft, uint64_t term,
                                           uint64_t last_index, uint64_t last_term,
                                           const uint8_t *data, size_t len);
int                t_raft_replicate(t_raft *raft, uint64_t index);
/* Commit the highest current-term index replicated on a majority (self + matches).
 * Applies newly committed entries. cluster_n is the voting membership. */
int                t_raft_majority_commit(t_raft *raft, const uint64_t *matches,
                                          size_t nmatches, size_t cluster_n);
uint64_t           t_raft_current_term(const t_raft *raft);
t_nrole            t_raft_state(const t_raft *raft);
uint64_t           t_raft_commit_index(const t_raft *raft);
uint64_t           t_raft_last_applied(const t_raft *raft);
int                t_raft_become_candidate(t_raft *raft);
int                t_raft_become_leader(t_raft *raft);
int                t_raft_become_follower(t_raft *raft, uint64_t term);
int                t_raft_append_entry(t_raft *raft, uint8_t type,
                                        const uint8_t *data, size_t len);
size_t             t_raft_log_count(const t_raft *raft);
const t_raft_entry *t_raft_get_entry(const t_raft *raft, uint64_t index);
int                t_raft_advance_commit(t_raft *raft, uint64_t commit_idx);
/* Returns 0 on success, -1 on error, -2 if a callback destroyed raft. */
int                t_raft_apply_entries(t_raft *raft);
size_t             t_raft_applied_count(const t_raft *raft);
int                t_raft_request_vote(t_raft *raft, uint64_t candidate_id,
                                        uint64_t term);
int                t_raft_grant_vote(t_raft *raft, uint64_t candidate_id);
uint64_t           t_raft_voted_for(const t_raft *raft);
uint64_t           t_raft_id(const t_raft *raft);
uint64_t           t_raft_election_timeout_ms(const t_raft *raft);
uint64_t           t_raft_heartbeat_interval_ms(const t_raft *raft);
uint64_t           t_raft_last_log_index(const t_raft *raft);
uint64_t           t_raft_last_log_term(const t_raft *raft);
/* Durable log (term + votedFor + commit_index in the v2 header). */
int                t_raft_open_log(t_raft *raft, const char *path, int sync_every);
/* Handle a T_MSG_CLUSTER payload; writes a response into resp. Returns length. */
int                t_raft_rpc(t_raft *raft, const uint8_t *req, size_t req_len,
                              uint8_t *resp, size_t resp_cap);

#endif
