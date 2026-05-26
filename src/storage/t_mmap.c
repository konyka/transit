#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include "t_mmap.h"

/* Create a new mmap-backed region for a file. Truncates to 'size'. */
int t_mmap_create(t_mmap *mm, const char *path, size_t size) {
    if (!mm || !path) return -1;
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    if (ftruncate(fd, (off_t)size) != 0) {
        close(fd);
        return -1;
    }
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

/* Open an existing mmap-backed file read-write. */
int t_mmap_open(t_mmap *mm, const char *path) {
    if (!mm || !path) return -1;
    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0) {
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

void t_mmap_close(t_mmap *mm) {
    if (!mm) return;
    if (mm->addr && mm->size) {
        munmap(mm->addr, mm->size);
        mm->addr = NULL;
        mm->size = 0;
    }
    if (mm->fd >= 0) {
        close(mm->fd);
        mm->fd = -1;
    }
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
