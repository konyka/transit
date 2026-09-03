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

#endif /* T_WIRE_H */
