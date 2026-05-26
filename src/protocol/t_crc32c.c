#include "t_crc32c.h"
#include <stdlib.h>
#include <stdint.h>

static uint32_t crc32c_table[256];
static int crc32c_table_inited = 0;

static void build_crc32c_table(void) {
    const uint32_t polynomial = 0x82F63B78u; /* Castagnoli */
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            if (crc & 1)
                crc = (crc >> 1) ^ polynomial;
            else
                crc = crc >> 1;
        }
        crc32c_table[i] = crc;
    }
    crc32c_table_inited = 1;
}

uint32_t t_crc32c_update(uint32_t crc, const void *data, size_t len) {
    if (!crc32c_table_inited) build_crc32c_table();
    const uint8_t *p = (const uint8_t *)data;
    for (size_t i = 0; i < len; i++) {
        crc = crc32c_table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc;
}

uint32_t t_crc32c(const void *data, size_t len) {
    return t_crc32c_update(0xFFFFFFFFu, data, len) ^ 0xFFFFFFFFu;
}
