#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>

#include "t_mmap.h"

/* Match storage file-backed dump limit. */
#define T_MMAP_MAX_SIZE (256 * 1024 * 1024)

/* Create a new mmap-backed region for a file. Truncates to 'size'. */
int t_mmap_create(t_mmap *mm, const char *path, size_t size) {
    if (!mm || !path || size == 0) return -1;
    if (size > T_MMAP_MAX_SIZE) return -1;
    /* Ensure size fits in off_t for ftruncate. */
    if ((off_t)size < 0 || (size_t)(off_t)size != size) return -1;
    mm->addr = NULL;
    mm->size = 0;
    mm->fd = -1;
    /* Build via *.tmp + rename so mmap/ftruncate failure keeps the old file. */
    size_t plen = strlen(path);
    if (plen > SIZE_MAX - 5) return -1;
    char *tmp = (char *)malloc(plen + 5);
    if (!tmp) return -1;
    memcpy(tmp, path, plen);
    memcpy(tmp + plen, ".tmp", 5);
    int fd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        free(tmp);
        return -1;
    }
    if (ftruncate(fd, (off_t)size) != 0) {
        close(fd);
        unlink(tmp);
        free(tmp);
        return -1;
    }
    void *addr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        unlink(tmp);
        free(tmp);
        return -1;
    }
    if (rename(tmp, path) != 0) {
        munmap(addr, size);
        close(fd);
        unlink(tmp);
        free(tmp);
        return -1;
    }
    free(tmp);
    mm->addr = addr;
    mm->size = size;
    mm->fd = fd;
    return 0;
}

/* Open an existing mmap-backed file read-write. */
int t_mmap_open(t_mmap *mm, const char *path) {
    if (!mm || !path) return -1;
    mm->addr = NULL;
    mm->size = 0;
    mm->fd = -1;
    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return -1;
    }
    if (st.st_size <= 0 ||
        (uint64_t)st.st_size > (uint64_t)SIZE_MAX ||
        (uint64_t)st.st_size > T_MMAP_MAX_SIZE) {
        close(fd);
        return -1;
    }
    size_t size = (size_t)st.st_size;
    void *addr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        return -1;
    }
    mm->addr = addr;
    mm->size = size;
    mm->fd = fd;
    return 0;
}

int t_mmap_close(t_mmap *mm) {
    if (!mm) return 0;
    if (mm->addr && mm->size) {
        if (msync(mm->addr, mm->size, MS_SYNC) != 0) return -1;
        munmap(mm->addr, mm->size);
        mm->addr = NULL;
        mm->size = 0;
    }
    if (mm->fd >= 0) {
        close(mm->fd);
        mm->fd = -1;
    }
    return 0;
}

int t_mmap_sync(t_mmap *mm) {
    if (!mm || !mm->addr) return -1;
    if (msync(mm->addr, mm->size, MS_SYNC) != 0) return -1;
    return 0;
}

void *t_mmap_data(t_mmap *mm) {
    if (!mm) return NULL;
    return mm->addr;
}

size_t t_mmap_size(t_mmap *mm) {
    return mm ? mm->size : 0;
}
