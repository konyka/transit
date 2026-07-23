#include "t_crc32c.h"
#include "t_compiler.h"
#include <stdlib.h>
#include <stdint.h>

#if T_PLATFORM_WINDOWS
#include <windows.h>
#else
#include <pthread.h>
#endif

static uint32_t crc32c_table[256];

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
}

#if T_PLATFORM_WINDOWS
static INIT_ONCE crc32c_once = INIT_ONCE_STATIC_INIT;

static BOOL CALLBACK crc32c_once_fn(PINIT_ONCE once, PVOID param, PVOID *ctx) {
    (void)once;
    (void)param;
    (void)ctx;
    build_crc32c_table();
    return TRUE;
}

static void crc32c_ensure_table(void) {
    (void)InitOnceExecuteOnce(&crc32c_once, crc32c_once_fn, NULL, NULL);
}
#else
static pthread_once_t crc32c_once = PTHREAD_ONCE_INIT;

static void crc32c_ensure_table(void) {
    (void)pthread_once(&crc32c_once, build_crc32c_table);
}
#endif

uint32_t t_crc32c_update(uint32_t crc, const void *data, size_t len) {
    if (len > 0 && !data) return crc;
    crc32c_ensure_table();
    const uint8_t *p = (const uint8_t *)data;
    for (size_t i = 0; i < len; i++) {
        crc = crc32c_table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc;
}

uint32_t t_crc32c(const void *data, size_t len) {
    return t_crc32c_update(0xFFFFFFFFu, data, len) ^ 0xFFFFFFFFu;
}
