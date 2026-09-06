#include "t_raft_cmd.h"
#include "t_wire.h"
#include "t_endian.h"
#include "t_proto.h"
#include <string.h>

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

int t_raft_cmd_encode_put(uint8_t *buf, size_t cap, const t_raft_cmd *cmd) {
    if (!buf || !cmd || cmd->type != T_RAFT_CMD_PUT) return -1;
    if (cmd->name_len > 0 && !cmd->name) return -1;
    if (cmd->data_len > 0 && !cmd->data) return -1;
    if (cmd->data_len > T_PROTO_MAX_PAYLOAD) return -1;
    if (cmd->name_len > 0 && !t_wire_name_valid(cmd->name, cmd->name_len)) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u8(&p, end, T_RAFT_CMD_PUT) != 0) return -1;
    if (put_u8(&p, end, cmd->qtype) != 0) return -1;
    if (put_u8(&p, end, cmd->qflags) != 0) return -1;
    if (put_u8(&p, end, cmd->priority) != 0) return -1;
    if (put_u64(&p, end, cmd->msg_id) != 0) return -1;
    if (put_u16(&p, end, cmd->name_len) != 0) return -1;
    if (cmd->name_len) {
        if ((size_t)(end - p) < cmd->name_len) return -1;
        memcpy(p, cmd->name, cmd->name_len);
        p += cmd->name_len;
    }
    if (put_u32(&p, end, cmd->data_len) != 0) return -1;
    if (cmd->data_len) {
        if ((size_t)(end - p) < cmd->data_len) return -1;
        memcpy(p, cmd->data, cmd->data_len);
        p += cmd->data_len;
    }
    return (int)(p - buf);
}

int t_raft_cmd_encode_ack(uint8_t *buf, size_t cap, const t_raft_cmd *cmd) {
    if (!buf || !cmd || cmd->type != T_RAFT_CMD_ACK) return -1;
    if (cmd->name_len > 0 && !cmd->name) return -1;
    if (cmd->name_len > 0 && !t_wire_name_valid(cmd->name, cmd->name_len)) return -1;
    uint8_t *p = buf;
    const uint8_t *end = buf + cap;
    if (put_u8(&p, end, T_RAFT_CMD_ACK) != 0) return -1;
    if (put_u64(&p, end, cmd->msg_id) != 0) return -1;
    if (put_u16(&p, end, cmd->name_len) != 0) return -1;
    if (cmd->name_len) {
        if ((size_t)(end - p) < cmd->name_len) return -1;
        memcpy(p, cmd->name, cmd->name_len);
        p += cmd->name_len;
    }
    return (int)(p - buf);
}

int t_raft_cmd_decode(const uint8_t *buf, size_t len, t_raft_cmd *out) {
    if (!buf || len == 0 || !out) return -1;
    const uint8_t *p = buf;
    const uint8_t *end = buf + len;
    t_raft_cmd tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (get_u8(&p, end, &tmp.type) != 0) return -1;
    if (tmp.type == T_RAFT_CMD_PUT) {
        if (get_u8(&p, end, &tmp.qtype) != 0) return -1;
        if (get_u8(&p, end, &tmp.qflags) != 0) return -1;
        if (get_u8(&p, end, &tmp.priority) != 0) return -1;
        if (get_u64(&p, end, &tmp.msg_id) != 0) return -1;
        if (get_u16(&p, end, &tmp.name_len) != 0) return -1;
        if (tmp.name_len) {
            if ((size_t)(end - p) < tmp.name_len) return -1;
            tmp.name = (const char *)p;
            if (!t_wire_name_valid(tmp.name, tmp.name_len)) return -1;
            p += tmp.name_len;
        }
        if (get_u32(&p, end, &tmp.data_len) != 0) return -1;
        if (tmp.data_len > T_PROTO_MAX_PAYLOAD) return -1;
        if (tmp.data_len) {
            if ((size_t)(end - p) < tmp.data_len) return -1;
            tmp.data = p;
            p += tmp.data_len;
        }
    } else if (tmp.type == T_RAFT_CMD_ACK) {
        if (get_u64(&p, end, &tmp.msg_id) != 0) return -1;
        if (get_u16(&p, end, &tmp.name_len) != 0) return -1;
        if (tmp.name_len) {
            if ((size_t)(end - p) < tmp.name_len) return -1;
            tmp.name = (const char *)p;
            if (!t_wire_name_valid(tmp.name, tmp.name_len)) return -1;
            p += tmp.name_len;
        }
    } else {
        return -1;
    }
    if (p != end) return -1;
    *out = tmp;
    return 0;
}
