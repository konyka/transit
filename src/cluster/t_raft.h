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

t_raft            *t_raft_create(const t_raft_config *cfg);
void               t_raft_destroy(t_raft *raft);
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
int                t_raft_apply_entries(t_raft *raft);
size_t             t_raft_applied_count(const t_raft *raft);
int                t_raft_request_vote(t_raft *raft, uint64_t candidate_id,
                                        uint64_t term);
int                t_raft_grant_vote(t_raft *raft, uint64_t candidate_id);
uint64_t           t_raft_voted_for(const t_raft *raft);

#endif
