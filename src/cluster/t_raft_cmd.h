#ifndef T_RAFT_CMD_H
#define T_RAFT_CMD_H

#include <stdint.h>
#include <stddef.h>

/* Compact Raft log commands for durable queue mutations.
 * Big-endian, exact length. Names use the client wire charset. */

#define T_RAFT_CMD_PUT    1
#define T_RAFT_CMD_ACK    2
#define T_RAFT_CMD_CREATE 3
#define T_RAFT_CMD_DELETE 4
#define T_RAFT_CMD_NACK   5

typedef struct t_raft_cmd {
    uint8_t        type;
    uint8_t        qtype;
    uint8_t        qflags;
    uint8_t        priority;
    uint64_t       msg_id;
    const char    *name;
    uint16_t       name_len;
    const uint8_t *data;
    uint32_t       data_len;
} t_raft_cmd;

int t_raft_cmd_encode_put(uint8_t *buf, size_t cap, const t_raft_cmd *cmd);
int t_raft_cmd_encode_ack(uint8_t *buf, size_t cap, const t_raft_cmd *cmd);
int t_raft_cmd_encode_nack(uint8_t *buf, size_t cap, const t_raft_cmd *cmd);
int t_raft_cmd_encode_create(uint8_t *buf, size_t cap, const t_raft_cmd *cmd);
int t_raft_cmd_encode_delete(uint8_t *buf, size_t cap, const t_raft_cmd *cmd);
int t_raft_cmd_decode(const uint8_t *buf, size_t len, t_raft_cmd *out);

#endif
