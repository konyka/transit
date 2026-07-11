#include "t_raft.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>

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

t_raft *t_raft_create(const t_raft_config *cfg) {
    (void)cfg;
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
    return r;
}

void t_raft_destroy(t_raft *raft) {
    if (!raft) return;
    for (size_t i = 0; i < raft->log_count; i++) {
        free(raft->log[i].data);
    }
    free(raft->log);
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
    /* Only start a new election term when entering candidacy. */
    if (raft->state != T_NODE_CANDIDATE) {
        raft->current_term += 1;
    }
    raft->state = T_NODE_CANDIDATE;
    raft->voted_for = raft->self_id; /* vote for self */
    return 0;
}

int t_raft_become_leader(t_raft *raft) {
    if (!raft) return -1;
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
    return 0;
}

int t_raft_append_entry(t_raft *raft, uint8_t type,
                        const uint8_t *data, size_t len) {
    if (!raft) return -1;
    if (len > 0 && !data) return -1;
    if (ensure_log_cap(raft, raft->log_count + 1) != 0) return -1;
    t_raft_entry *e = &raft->log[raft->log_count];
    e->index = raft->log_count + 1;
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
    return 0;
}

size_t t_raft_log_count(const t_raft *raft) {
    return raft ? raft->log_count : 0;
}

const t_raft_entry *t_raft_get_entry(const t_raft *raft, uint64_t index) {
    if (!raft || index == 0 || index > raft->log_count) return NULL;
    return &raft->log[index - 1];
}

int t_raft_advance_commit(t_raft *raft, uint64_t commit_idx) {
    if (!raft) return -1;
    if (commit_idx > raft->log_count) commit_idx = raft->log_count;
    raft->commit_index = commit_idx;
    return 0;
}

int t_raft_apply_entries(t_raft *raft) {
    if (!raft) return -1;
    while (raft->last_applied < raft->commit_index) {
        uint64_t next = raft->last_applied + 1;
        if (!t_raft_get_entry(raft, next)) return -1;
        raft->last_applied = next;
    }
    return 0;
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
    return 1;
}

int t_raft_grant_vote(t_raft *raft, uint64_t candidate_id) {
    if (!raft) return -1;
    if (raft->voted_for != 0 && raft->voted_for != candidate_id) return -1;
    raft->voted_for = candidate_id;
    raft->state = T_NODE_FOLLOWER;
    return 0;
}

uint64_t t_raft_voted_for(const t_raft *raft) {
    return raft ? raft->voted_for : 0;
}
