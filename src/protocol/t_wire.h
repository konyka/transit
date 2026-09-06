#ifndef T_WIRE_H
#define T_WIRE_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

/* Compact big-endian payloads for t_proto frames. Decode aliases `buf`. */

#define T_WIRE_MAX_NAME 255

#define T_WIRE_MODE_PRODUCER 0x01u
#define T_WIRE_MODE_CONSUMER 0x02u

typedef struct t_wire_open {
    uint8_t     qtype;
    uint8_t     qflags;
    uint8_t     mode;
    const char *name;
    uint16_t    name_len;
} t_wire_open;

typedef struct t_wire_close {
    const char *name;
    uint16_t    name_len;
} t_wire_close;

typedef struct t_wire_post {
    uint8_t        priority;
    const char    *name;
    uint16_t       name_len;
    const uint8_t *data;
    uint32_t       data_len;
} t_wire_post;

typedef struct t_wire_push {
    uint64_t       msg_id;
    uint8_t        priority;
    const char    *name;
    uint16_t       name_len;
    const uint8_t *data;
    uint32_t       data_len;
} t_wire_push;

typedef struct t_wire_ack {
    uint16_t    req_type;
    int32_t     status;
    const char *name;
    uint16_t    name_len;
} t_wire_ack;

typedef struct t_wire_confirm {
    uint64_t    msg_id;
    const char *name;
    uint16_t    name_len;
} t_wire_confirm;

typedef struct t_wire_join {
    const char *group;
    uint16_t    group_len;
    const char *consumer;
    uint16_t    consumer_len;
    const char *queue;
    uint16_t    queue_len;
} t_wire_join;

int t_wire_name_valid(const char *name, size_t len);
int t_wire_name_copy(char *dest, size_t dest_cap, const char *name, uint16_t name_len);

int t_wire_encode_open(uint8_t *buf, size_t cap, uint8_t qtype, uint8_t qflags,
                       uint8_t mode, const char *name);
int t_wire_decode_open(const uint8_t *buf, size_t len, t_wire_open *out);

int t_wire_encode_close(uint8_t *buf, size_t cap, const char *name);
int t_wire_decode_close(const uint8_t *buf, size_t len, t_wire_close *out);

int t_wire_encode_post(uint8_t *buf, size_t cap, uint8_t priority,
                       const char *name, const uint8_t *data, uint32_t data_len);
int t_wire_decode_post(const uint8_t *buf, size_t len, t_wire_post *out);

int t_wire_encode_push(uint8_t *buf, size_t cap, uint64_t msg_id, uint8_t priority,
                       const char *name, const uint8_t *data, uint32_t data_len);
int t_wire_decode_push(const uint8_t *buf, size_t len, t_wire_push *out);

int t_wire_encode_ack(uint8_t *buf, size_t cap, uint16_t req_type, int32_t status,
                      const char *name);
int t_wire_decode_ack(const uint8_t *buf, size_t len, t_wire_ack *out);

int t_wire_encode_confirm(uint8_t *buf, size_t cap, uint64_t msg_id, const char *name);
int t_wire_decode_confirm(const uint8_t *buf, size_t len, t_wire_confirm *out);

int t_wire_encode_join(uint8_t *buf, size_t cap, const char *group,
                       const char *consumer, const char *queue);
int t_wire_decode_join(const uint8_t *buf, size_t len, t_wire_join *out);

/* T_MSG_CLUSTER payloads (peer port, not the client port). */
#define T_WIRE_CLUSTER_VOTE_REQ    1
#define T_WIRE_CLUSTER_VOTE_RESP   2
#define T_WIRE_CLUSTER_APPEND_REQ  3
#define T_WIRE_CLUSTER_APPEND_RESP 4
#define T_WIRE_CLUSTER_SNAP_REQ    5
#define T_WIRE_CLUSTER_SNAP_RESP   6
#define T_WIRE_CLUSTER_MAX_ENTS    256

typedef struct t_wire_cluster_entry {
    uint64_t       index;
    uint64_t       term;
    uint8_t        type;
    const uint8_t *data;
    uint32_t       data_len;
} t_wire_cluster_entry;

typedef struct t_wire_vote_req {
    uint64_t term;
    uint64_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
} t_wire_vote_req;

typedef struct t_wire_vote_resp {
    uint64_t term;
    uint8_t  granted;
} t_wire_vote_resp;

typedef struct t_wire_append_req {
    uint64_t term;
    uint64_t leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint64_t leader_commit;
    uint32_t nentries;
} t_wire_append_req;

typedef struct t_wire_append_resp {
    uint64_t term;
    uint8_t  success;
    uint64_t match_index;
} t_wire_append_resp;

int t_wire_encode_vote_req(uint8_t *buf, size_t cap, const t_wire_vote_req *req);
int t_wire_decode_vote_req(const uint8_t *buf, size_t len, t_wire_vote_req *out);
int t_wire_encode_vote_resp(uint8_t *buf, size_t cap, uint64_t term, uint8_t granted);
int t_wire_decode_vote_resp(const uint8_t *buf, size_t len, t_wire_vote_resp *out);
int t_wire_encode_append_req(uint8_t *buf, size_t cap, const t_wire_append_req *req,
                             const t_wire_cluster_entry *ents, uint32_t n);
int t_wire_decode_append_req(const uint8_t *buf, size_t len, t_wire_append_req *out,
                             t_wire_cluster_entry *ents, uint32_t ent_cap);
int t_wire_encode_append_resp(uint8_t *buf, size_t cap, uint64_t term, uint8_t success,
                              uint64_t match_index);
int t_wire_decode_append_resp(const uint8_t *buf, size_t len, t_wire_append_resp *out);

typedef struct t_wire_snap_req {
    uint64_t       term;
    uint64_t       leader_id;
    uint64_t       last_index;
    uint64_t       last_term;
    const uint8_t *data;
    uint32_t       data_len;
} t_wire_snap_req;

typedef struct t_wire_snap_resp {
    uint64_t term;
    uint8_t  success;
    uint64_t match_index;
} t_wire_snap_resp;

int t_wire_encode_snap_req(uint8_t *buf, size_t cap, const t_wire_snap_req *req);
int t_wire_decode_snap_req(const uint8_t *buf, size_t len, t_wire_snap_req *out);
int t_wire_encode_snap_resp(uint8_t *buf, size_t cap, uint64_t term, uint8_t success,
                            uint64_t match_index);
int t_wire_decode_snap_resp(const uint8_t *buf, size_t len, t_wire_snap_resp *out);

#define T_WIRE_AUTH_MAC_LEN 32
int t_wire_encode_auth(uint8_t *buf, size_t cap, const uint8_t mac[T_WIRE_AUTH_MAC_LEN]);
int t_wire_decode_auth(const uint8_t *buf, size_t len, const uint8_t **mac);

#endif /* T_WIRE_H */
