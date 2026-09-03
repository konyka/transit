#include "t_wire.h"
#include "t_proto.h"
#include <string.h>
#include <arpa/inet.h>

int t_wire_name_valid(const char *name, size_t len) {
    if (!name || len == 0 || len > T_WIRE_MAX_NAME) return 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)name[i];
        int ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                 (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
        if (!ok) return 0;
    }
    return 1;
}

int t_wire_name_copy(char *dest, size_t dest_cap, const char *name, uint16_t name_len) {
    if (!dest || dest_cap == 0) return -1;
    if (name_len > 0 && !name) return -1;
    if ((size_t)name_len + 1 > dest_cap) return -1;
    if (name_len > 0 && !t_wire_name_valid(name, name_len)) return -1;
    if (name_len > 0) memcpy(dest, name, name_len);
    dest[name_len] = '\0';
    return 0;
}

static int put_u8(uint8_t **p, const uint8_t *end, uint8_t v) {
    if (*p >= end) return -1;
    **p = v;
    (*p)++;
    return 0;
}

static int put_u16(uint8_t **p, const uint8_t *end, uint16_t v) {
    if ((size_t)(end - *p) < 2) return -1;
    uint16_t be = htons(v);
    memcpy(*p, &be, 2);
    *p += 2;
    return 0;
}

static int put_u32(uint8_t **p, const uint8_t *end, uint32_t v) {
    if ((size_t)(end - *p) < 4) return -1;
    uint32_t be = htonl(v);
    memcpy(*p, &be, 4);
    *p += 4;
    return 0;
}

static int put_u64(uint8_t **p, const uint8_t *end, uint64_t v) {
    if ((size_t)(end - *p) < 8) return -1;
    uint32_t hi = htonl((uint32_t)(v >> 32));
    uint32_t lo = htonl((uint32_t)v);
    memcpy(*p, &hi, 4);
    memcpy(*p + 4, &lo, 4);
    *p += 8;
    return 0;
}

static int put_bytes(uint8_t **p, const uint8_t *end, const void *src, size_t n) {
    if (n == 0) return 0;
    if (!src || (size_t)(end - *p) < n) return -1;
    memcpy(*p, src, n);
    *p += n;
    return 0;
}

static int get_u8(const uint8_t **p, const uint8_t *end, uint8_t *out) {
    if (*p >= end) return -1;
    *out = **p;
    (*p)++;
    return 0;
}

static int get_u16(const uint8_t **p, const uint8_t *end, uint16_t *out) {
    if ((size_t)(end - *p) < 2) return -1;
    uint16_t be;
    memcpy(&be, *p, 2);
    *out = ntohs(be);
    *p += 2;
    return 0;
}

static int get_u32(const uint8_t **p, const uint8_t *end, uint32_t *out) {
    if ((size_t)(end - *p) < 4) return -1;
    uint32_t be;
    memcpy(&be, *p, 4);
    *out = ntohl(be);
    *p += 4;
    return 0;
}

static int get_u64(const uint8_t **p, const uint8_t *end, uint64_t *out) {
    if ((size_t)(end - *p) < 8) return -1;
    uint32_t hi_be, lo_be;
    memcpy(&hi_be, *p, 4);
    memcpy(&lo_be, *p + 4, 4);
    *out = ((uint64_t)ntohl(hi_be) << 32) | (uint64_t)ntohl(lo_be);
    *p += 8;
    return 0;
}

static int get_name(const uint8_t **p, const uint8_t *end, const char **name, uint16_t *name_len) {
    uint16_t nlen;
    if (get_u16(p, end, &nlen) != 0) return -1;
    if (nlen > T_WIRE_MAX_NAME) return -1;
    if ((size_t)(end - *p) < nlen) return -1;
    if (nlen > 0 && !t_wire_name_valid((const char *)*p, nlen)) return -1;
    *name = nlen ? (const char *)*p : NULL;
    *name_len = nlen;
    *p += nlen;
    return 0;
}

static int require_exact(const uint8_t *p, const uint8_t *end) {
    return p == end ? 0 : -1;
}

static int encode_name_field(uint8_t **p, const uint8_t *end, const char *name, int allow_empty) {
    if (!name || !*name) {
        if (!allow_empty) return -1;
        return put_u16(p, end, 0);
    }
    size_t n = strlen(name);
    if (!t_wire_name_valid(name, n)) return -1;
    if (put_u16(p, end, (uint16_t)n) != 0) return -1;
    return put_bytes(p, end, name, n);
}

int t_wire_encode_open(uint8_t *buf, size_t cap, uint8_t qtype, uint8_t qflags,
                       uint8_t mode, const char *name) {
    if (!buf || cap == 0 || !name) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u8(&p, end, qtype) != 0) return -1;
    if (put_u8(&p, end, qflags) != 0) return -1;
    if (put_u8(&p, end, mode) != 0) return -1;
    if (encode_name_field(&p, end, name, 0) != 0) return -1;
    return (int)(p - buf);
}

int t_wire_decode_open(const uint8_t *buf, size_t len, t_wire_open *out) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    t_wire_open tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_u8(&p, end, &tmp.qtype) != 0) return -1;
    if (get_u8(&p, end, &tmp.qflags) != 0) return -1;
    if (get_u8(&p, end, &tmp.mode) != 0) return -1;
    if (get_name(&p, end, &tmp.name, &tmp.name_len) != 0) return -1;
    if (tmp.name_len == 0) return -1;
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_close(uint8_t *buf, size_t cap, const char *name) {
    if (!buf || cap == 0) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (encode_name_field(&p, end, name, 0) != 0) return -1;
    return (int)(p - buf);
}

int t_wire_decode_close(const uint8_t *buf, size_t len, t_wire_close *out) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    t_wire_close tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_name(&p, end, &tmp.name, &tmp.name_len) != 0) return -1;
    if (tmp.name_len == 0) return -1;
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_post(uint8_t *buf, size_t cap, uint8_t priority,
                       const char *name, const uint8_t *data, uint32_t data_len) {
    if (!buf || cap == 0) return -1;
    if (data_len > 0 && !data) return -1;
    if (data_len > T_PROTO_MAX_PAYLOAD) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u8(&p, end, priority) != 0) return -1;
    if (encode_name_field(&p, end, name, 0) != 0) return -1;
    if (put_u32(&p, end, data_len) != 0) return -1;
    if (put_bytes(&p, end, data, data_len) != 0) return -1;
    return (int)(p - buf);
}

int t_wire_decode_post(const uint8_t *buf, size_t len, t_wire_post *out) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    t_wire_post tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_u8(&p, end, &tmp.priority) != 0) return -1;
    if (get_name(&p, end, &tmp.name, &tmp.name_len) != 0) return -1;
    if (tmp.name_len == 0) return -1;
    if (get_u32(&p, end, &tmp.data_len) != 0) return -1;
    if (tmp.data_len > T_PROTO_MAX_PAYLOAD) return -1;
    if ((size_t)(end - p) < tmp.data_len) return -1;
    tmp.data = tmp.data_len ? p : NULL;
    p += tmp.data_len;
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_push(uint8_t *buf, size_t cap, uint64_t msg_id, uint8_t priority,
                       const char *name, const uint8_t *data, uint32_t data_len) {
    if (!buf || cap == 0) return -1;
    if (data_len > 0 && !data) return -1;
    if (data_len > T_PROTO_MAX_PAYLOAD) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u64(&p, end, msg_id) != 0) return -1;
    if (put_u8(&p, end, priority) != 0) return -1;
    if (encode_name_field(&p, end, name, 0) != 0) return -1;
    if (put_u32(&p, end, data_len) != 0) return -1;
    if (put_bytes(&p, end, data, data_len) != 0) return -1;
    return (int)(p - buf);
}

int t_wire_decode_push(const uint8_t *buf, size_t len, t_wire_push *out) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    t_wire_push tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_u64(&p, end, &tmp.msg_id) != 0) return -1;
    if (get_u8(&p, end, &tmp.priority) != 0) return -1;
    if (get_name(&p, end, &tmp.name, &tmp.name_len) != 0) return -1;
    if (tmp.name_len == 0) return -1;
    if (get_u32(&p, end, &tmp.data_len) != 0) return -1;
    if (tmp.data_len > T_PROTO_MAX_PAYLOAD) return -1;
    if ((size_t)(end - p) < tmp.data_len) return -1;
    tmp.data = tmp.data_len ? p : NULL;
    p += tmp.data_len;
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_ack(uint8_t *buf, size_t cap, uint16_t req_type, int32_t status,
                      const char *name) {
    if (!buf || cap == 0) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u16(&p, end, req_type) != 0) return -1;
    if (put_u32(&p, end, (uint32_t)status) != 0) return -1;
    if (encode_name_field(&p, end, name, 1) != 0) return -1;
    return (int)(p - buf);
}

int t_wire_decode_ack(const uint8_t *buf, size_t len, t_wire_ack *out) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    t_wire_ack tmp;
    memset(&tmp, 0, sizeof(tmp));
    uint32_t st;
    if (get_u16(&p, end, &tmp.req_type) != 0) return -1;
    if (get_u32(&p, end, &st) != 0) return -1;
    tmp.status = (int32_t)st;
    if (get_name(&p, end, &tmp.name, &tmp.name_len) != 0) return -1;
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_confirm(uint8_t *buf, size_t cap, uint64_t msg_id, const char *name) {
    if (!buf || cap == 0) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u64(&p, end, msg_id) != 0) return -1;
    if (encode_name_field(&p, end, name, 0) != 0) return -1;
    return (int)(p - buf);
}

int t_wire_decode_confirm(const uint8_t *buf, size_t len, t_wire_confirm *out) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    t_wire_confirm tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_u64(&p, end, &tmp.msg_id) != 0) return -1;
    if (get_name(&p, end, &tmp.name, &tmp.name_len) != 0) return -1;
    if (tmp.name_len == 0) return -1;
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_vote_req(uint8_t *buf, size_t cap, const t_wire_vote_req *req) {
    if (!buf || !req) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u8(&p, end, T_WIRE_CLUSTER_VOTE_REQ) != 0) return -1;
    if (put_u64(&p, end, req->term) != 0) return -1;
    if (put_u64(&p, end, req->candidate_id) != 0) return -1;
    if (put_u64(&p, end, req->last_log_index) != 0) return -1;
    if (put_u64(&p, end, req->last_log_term) != 0) return -1;
    return (int)(p - buf);
}

int t_wire_decode_vote_req(const uint8_t *buf, size_t len, t_wire_vote_req *out) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    uint8_t rpc;
    t_wire_vote_req tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_u8(&p, end, &rpc) != 0 || rpc != T_WIRE_CLUSTER_VOTE_REQ) return -1;
    if (get_u64(&p, end, &tmp.term) != 0) return -1;
    if (get_u64(&p, end, &tmp.candidate_id) != 0) return -1;
    if (get_u64(&p, end, &tmp.last_log_index) != 0) return -1;
    if (get_u64(&p, end, &tmp.last_log_term) != 0) return -1;
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_vote_resp(uint8_t *buf, size_t cap, uint64_t term, uint8_t granted) {
    if (!buf) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u8(&p, end, T_WIRE_CLUSTER_VOTE_RESP) != 0) return -1;
    if (put_u64(&p, end, term) != 0) return -1;
    if (put_u8(&p, end, granted ? 1 : 0) != 0) return -1;
    return (int)(p - buf);
}

int t_wire_decode_vote_resp(const uint8_t *buf, size_t len, t_wire_vote_resp *out) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    uint8_t rpc;
    t_wire_vote_resp tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_u8(&p, end, &rpc) != 0 || rpc != T_WIRE_CLUSTER_VOTE_RESP) return -1;
    if (get_u64(&p, end, &tmp.term) != 0) return -1;
    if (get_u8(&p, end, &tmp.granted) != 0) return -1;
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_append_req(uint8_t *buf, size_t cap, const t_wire_append_req *req,
                             const t_wire_cluster_entry *ents, uint32_t n) {
    if (!buf || !req) return -1;
    if (n > T_WIRE_CLUSTER_MAX_ENTS) return -1;
    if (n > 0 && !ents) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u8(&p, end, T_WIRE_CLUSTER_APPEND_REQ) != 0) return -1;
    if (put_u64(&p, end, req->term) != 0) return -1;
    if (put_u64(&p, end, req->leader_id) != 0) return -1;
    if (put_u64(&p, end, req->prev_log_index) != 0) return -1;
    if (put_u64(&p, end, req->prev_log_term) != 0) return -1;
    if (put_u64(&p, end, req->leader_commit) != 0) return -1;
    if (put_u32(&p, end, n) != 0) return -1;
    for (uint32_t i = 0; i < n; i++) {
        if (ents[i].data_len > 0 && !ents[i].data) return -1;
        if (ents[i].data_len > T_PROTO_MAX_PAYLOAD) return -1;
        if (put_u64(&p, end, ents[i].index) != 0) return -1;
        if (put_u64(&p, end, ents[i].term) != 0) return -1;
        if (put_u8(&p, end, ents[i].type) != 0) return -1;
        if (put_u32(&p, end, ents[i].data_len) != 0) return -1;
        if (put_bytes(&p, end, ents[i].data, ents[i].data_len) != 0) return -1;
    }
    return (int)(p - buf);
}

int t_wire_decode_append_req(const uint8_t *buf, size_t len, t_wire_append_req *out,
                             t_wire_cluster_entry *ents, uint32_t ent_cap) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    uint8_t rpc;
    t_wire_append_req tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_u8(&p, end, &rpc) != 0 || rpc != T_WIRE_CLUSTER_APPEND_REQ) return -1;
    if (get_u64(&p, end, &tmp.term) != 0) return -1;
    if (get_u64(&p, end, &tmp.leader_id) != 0) return -1;
    if (get_u64(&p, end, &tmp.prev_log_index) != 0) return -1;
    if (get_u64(&p, end, &tmp.prev_log_term) != 0) return -1;
    if (get_u64(&p, end, &tmp.leader_commit) != 0) return -1;
    if (get_u32(&p, end, &tmp.nentries) != 0) return -1;
    if (tmp.nentries > T_WIRE_CLUSTER_MAX_ENTS) return -1;
    if (tmp.nentries > ent_cap) return -1;
    if (tmp.nentries > 0 && !ents) return -1;
    for (uint32_t i = 0; i < tmp.nentries; i++) {
        t_wire_cluster_entry e;
        memset(&e, 0, sizeof(e));
        if (get_u64(&p, end, &e.index) != 0) return -1;
        if (get_u64(&p, end, &e.term) != 0) return -1;
        if (get_u8(&p, end, &e.type) != 0) return -1;
        if (get_u32(&p, end, &e.data_len) != 0) return -1;
        if (e.data_len > T_PROTO_MAX_PAYLOAD) return -1;
        if ((size_t)(end - p) < e.data_len) return -1;
        e.data = e.data_len ? p : NULL;
        p += e.data_len;
        ents[i] = e;
    }
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_append_resp(uint8_t *buf, size_t cap, uint64_t term, uint8_t success,
                              uint64_t match_index) {
    if (!buf) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u8(&p, end, T_WIRE_CLUSTER_APPEND_RESP) != 0) return -1;
    if (put_u64(&p, end, term) != 0) return -1;
    if (put_u8(&p, end, success ? 1 : 0) != 0) return -1;
    if (put_u64(&p, end, match_index) != 0) return -1;
    return (int)(p - buf);
}

int t_wire_decode_append_resp(const uint8_t *buf, size_t len, t_wire_append_resp *out) {
    if (!buf || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    uint8_t rpc;
    t_wire_append_resp tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_u8(&p, end, &rpc) != 0 || rpc != T_WIRE_CLUSTER_APPEND_RESP) return -1;
    if (get_u64(&p, end, &tmp.term) != 0) return -1;
    if (get_u8(&p, end, &tmp.success) != 0) return -1;
    if (get_u64(&p, end, &tmp.match_index) != 0) return -1;
    if (require_exact(p, end) != 0) return -1;
    *out = tmp;
    return 0;
}

int t_wire_encode_auth(uint8_t *buf, size_t cap, const uint8_t mac[T_WIRE_AUTH_MAC_LEN]) {
    if (!buf || !mac || cap < T_WIRE_AUTH_MAC_LEN) return -1;
    memcpy(buf, mac, T_WIRE_AUTH_MAC_LEN);
    return T_WIRE_AUTH_MAC_LEN;
}

int t_wire_decode_auth(const uint8_t *buf, size_t len, const uint8_t **mac) {
    if (!buf || !mac || len != T_WIRE_AUTH_MAC_LEN) return -1;
    *mac = buf;
    return 0;
}
