#include "t_mmap.h"
#include "t_compiler.h"
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

/* Match storage file-backed dump limit. */
#define T_MMAP_MAX_SIZE (256 * 1024 * 1024)

static void mmap_clear(t_mmap *mm) {
    mm->addr = NULL;
    mm->size = 0;
    mm->fd = -1;
    mm->os_file = NULL;
    mm->os_map = NULL;
}

#if T_PLATFORM_WINDOWS

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

static char *mmap_tmp_path(const char *path) {
    size_t plen = strlen(path);
    if (plen > SIZE_MAX - 5) return NULL;
    char *tmp = (char *)malloc(plen + 5);
    if (!tmp) return NULL;
    memcpy(tmp, path, plen);
    memcpy(tmp + plen, ".tmp", 5);
    return tmp;
}

static int mmap_map_file(HANDLE hf, size_t size, HANDLE *out_map, void **out_addr) {
    HANDLE hm = CreateFileMappingA(hf, NULL, PAGE_READWRITE, 0, (DWORD)size, NULL);
    if (!hm) return -1;
    void *addr = MapViewOfFile(hm, FILE_MAP_ALL_ACCESS, 0, 0, size);
    if (!addr) {
        CloseHandle(hm);
        return -1;
    }
    *out_map = hm;
    *out_addr = addr;
    return 0;
}

int t_mmap_create(t_mmap *mm, const char *path, size_t size) {
    if (!mm || !path || size == 0) return -1;
    if (size > T_MMAP_MAX_SIZE) return -1;
    mmap_clear(mm);
    char *tmp = mmap_tmp_path(path);
    if (!tmp) return -1;
    HANDLE hf = CreateFileA(tmp, GENERIC_READ | GENERIC_WRITE,
                            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                            NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hf == INVALID_HANDLE_VALUE) {
        free(tmp);
        return -1;
    }
    LARGE_INTEGER li;
    li.QuadPart = (LONGLONG)size;
    if (!SetFilePointerEx(hf, li, NULL, FILE_BEGIN) || !SetEndOfFile(hf)) {
        CloseHandle(hf);
        DeleteFileA(tmp);
        free(tmp);
        return -1;
    }
    HANDLE hm;
    void *addr;
    if (mmap_map_file(hf, size, &hm, &addr) != 0) {
        CloseHandle(hf);
        DeleteFileA(tmp);
        free(tmp);
        return -1;
    }
    /* Flush and close before rename: MoveFileEx cannot replace a mapped file. */
    if (!FlushViewOfFile(addr, size)) {
        UnmapViewOfFile(addr);
        CloseHandle(hm);
        CloseHandle(hf);
        DeleteFileA(tmp);
        free(tmp);
        return -1;
    }
    UnmapViewOfFile(addr);
    CloseHandle(hm);
    CloseHandle(hf);
    if (!MoveFileExA(tmp, path, MOVEFILE_REPLACE_EXISTING)) {
        DeleteFileA(tmp);
        free(tmp);
        return -1;
    }
    free(tmp);
    return t_mmap_open(mm, path);
}

int t_mmap_open(t_mmap *mm, const char *path) {
    if (!mm || !path) return -1;
    mmap_clear(mm);
    HANDLE hf = CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
                            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                            NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hf == INVALID_HANDLE_VALUE) return -1;
    LARGE_INTEGER sz;
    if (!GetFileSizeEx(hf, &sz) || sz.QuadPart <= 0 ||
        (uint64_t)sz.QuadPart > (uint64_t)SIZE_MAX ||
        (uint64_t)sz.QuadPart > T_MMAP_MAX_SIZE) {
        CloseHandle(hf);
        return -1;
    }
    size_t size = (size_t)sz.QuadPart;
    HANDLE hm;
    void *addr;
    if (mmap_map_file(hf, size, &hm, &addr) != 0) {
        CloseHandle(hf);
        return -1;
    }
    mm->addr = addr;
    mm->size = size;
    mm->os_file = (void *)hf;
    mm->os_map = (void *)hm;
    return 0;
}

int t_mmap_close(t_mmap *mm) {
    if (!mm) return 0;
    if (mm->addr && mm->size) {
        if (!FlushViewOfFile(mm->addr, mm->size)) return -1;
        UnmapViewOfFile(mm->addr);
        mm->addr = NULL;
        mm->size = 0;
    }
    if (mm->os_map) {
        CloseHandle((HANDLE)mm->os_map);
        mm->os_map = NULL;
    }
    if (mm->os_file) {
        CloseHandle((HANDLE)mm->os_file);
        mm->os_file = NULL;
    }
    return 0;
}

int t_mmap_sync(t_mmap *mm) {
    if (!mm || !mm->addr) return -1;
    if (!FlushViewOfFile(mm->addr, mm->size)) return -1;
    if (mm->os_file && !FlushFileBuffers((HANDLE)mm->os_file)) return -1;
    return 0;
}

#else /* POSIX */

#include <stdio.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int t_mmap_create(t_mmap *mm, const char *path, size_t size) {
    if (!mm || !path || size == 0) return -1;
    if (size > T_MMAP_MAX_SIZE) return -1;
    /* Ensure size fits in off_t for ftruncate. */
    if ((off_t)size < 0 || (size_t)(off_t)size != size) return -1;
    mmap_clear(mm);
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

int t_mmap_open(t_mmap *mm, const char *path) {
    if (!mm || !path) return -1;
    mmap_clear(mm);
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

#endif /* T_PLATFORM_WINDOWS */

void *t_mmap_data(t_mmap *mm) {
    if (!mm) return NULL;
    return mm->addr;
}

size_t t_mmap_size(t_mmap *mm) {
    return mm ? mm->size : 0;
}
