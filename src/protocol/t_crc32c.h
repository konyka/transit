#ifndef T_CRC32C_H
#define T_CRC32C_H

#include <stdint.h>
#include <stddef.h>

uint32_t t_crc32c(const void *data, size_t len);
uint32_t t_crc32c_update(uint32_t crc, const void *data, size_t len);

#endif /* T_CRC32C_H */
