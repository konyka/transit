#ifndef T_MMAP_H
#define T_MMAP_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

typedef struct t_mmap {
    void   *addr;
    size_t  size;
    int     fd;
} t_mmap;

int     t_mmap_create(t_mmap *mm, const char *path, size_t size);
int     t_mmap_open(t_mmap *mm, const char *path);
/* Returns 0 on success, -1 if msync failed (mapping kept for retry). */
int     t_mmap_close(t_mmap *mm);
int     t_mmap_sync(t_mmap *mm);
void   *t_mmap_data(t_mmap *mm);
size_t  t_mmap_size(t_mmap *mm);

#endif /* T_MMAP_H */
