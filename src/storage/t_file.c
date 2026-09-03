#include "t_file.h"
#include <string.h>
#include <stdio.h>
#include <stdint.h>

#if T_PLATFORM_WINDOWS

#include <windows.h>

void t_file_init(t_file *f) {
    if (!f) return;
    f->fd = -1;
    f->os = NULL;
}

int t_file_is_open(const t_file *f) {
    return f && f->os != NULL;
}

int t_file_open(t_file *f, const char *path, int flags) {
    DWORD access = 0;
    DWORD disp;
    HANDLE h;
    if (!f || !path) return -1;
    t_file_init(f);
    if (flags & T_FILE_READ) access |= GENERIC_READ;
    if (flags & T_FILE_WRITE) access |= GENERIC_WRITE;
    if (access == 0) return -1;
    if (flags & T_FILE_TRUNC)
        disp = (flags & T_FILE_CREAT) ? CREATE_ALWAYS : TRUNCATE_EXISTING;
    else if (flags & T_FILE_CREAT)
        disp = OPEN_ALWAYS;
    else
        disp = OPEN_EXISTING;
    h = CreateFileA(path, access, FILE_SHARE_READ, NULL, disp,
                    FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE) return -1;
    f->os = (void *)h;
    return 0;
}

void t_file_close(t_file *f) {
    if (!f || !f->os) return;
    CloseHandle((HANDLE)f->os);
    f->os = NULL;
}

int t_file_read(t_file *f, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    HANDLE h;
    size_t off = 0;
    if (!t_file_is_open(f) || (n && !buf)) return -1;
    h = (HANDLE)f->os;
    while (off < n) {
        DWORD chunk = (n - off) > 0x7fffffff ? 0x7fffffff : (DWORD)(n - off);
        DWORD got = 0;
        if (!ReadFile(h, p + off, chunk, &got, NULL)) return -1;
        if (got == 0) return 1;
        off += got;
    }
    return 0;
}

int t_file_write(t_file *f, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    HANDLE h;
    if (!t_file_is_open(f) || (n && !buf)) return -1;
    h = (HANDLE)f->os;
    while (n) {
        DWORD chunk = n > 0x7fffffff ? 0x7fffffff : (DWORD)n;
        DWORD wrote = 0;
        if (!WriteFile(h, p, chunk, &wrote, NULL) || wrote == 0) return -1;
        p += wrote;
        n -= wrote;
    }
    return 0;
}

int t_file_seek(t_file *f, int64_t off, int whence) {
    LARGE_INTEGER li;
    DWORD method;
    if (!t_file_is_open(f)) return -1;
    li.QuadPart = off;
    if (whence == SEEK_END) method = FILE_END;
    else if (whence == SEEK_CUR) method = FILE_CURRENT;
    else method = FILE_BEGIN;
    return SetFilePointerEx((HANDLE)f->os, li, NULL, method) ? 0 : -1;
}

int t_file_size(t_file *f, uint64_t *out) {
    LARGE_INTEGER sz;
    if (!t_file_is_open(f) || !out) return -1;
    if (!GetFileSizeEx((HANDLE)f->os, &sz) || sz.QuadPart < 0) return -1;
    *out = (uint64_t)sz.QuadPart;
    return 0;
}

int t_file_sync(t_file *f) {
    if (!t_file_is_open(f)) return -1;
    return FlushFileBuffers((HANDLE)f->os) ? 0 : -1;
}

int t_file_unlink(const char *path) {
    if (!path) return -1;
    if (DeleteFileA(path)) return 0;
    {
        DWORD err = GetLastError();
        return (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) ? 0 : -1;
    }
}

int t_file_rename(const char *from, const char *to) {
    if (!from || !to) return -1;
    return MoveFileExA(from, to, MOVEFILE_REPLACE_EXISTING) ? 0 : -1;
}

int t_file_not_found(void) {
    DWORD err = GetLastError();
    return err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND;
}

#else /* POSIX */

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

void t_file_init(t_file *f) {
    if (!f) return;
    f->fd = -1;
    f->os = NULL;
}

int t_file_is_open(const t_file *f) {
    return f && f->fd >= 0;
}

int t_file_open(t_file *f, const char *path, int flags) {
    int oflag = 0;
    int mode;
    int fd;
    if (!f || !path) return -1;
    t_file_init(f);
    if ((flags & T_FILE_READ) && (flags & T_FILE_WRITE)) oflag = O_RDWR;
    else if (flags & T_FILE_WRITE) oflag = O_WRONLY;
    else if (flags & T_FILE_READ) oflag = O_RDONLY;
    else return -1;
    if (flags & T_FILE_CREAT) oflag |= O_CREAT;
    if (flags & T_FILE_TRUNC) oflag |= O_TRUNC;
    mode = (oflag & O_WRONLY) ? 0644 : 0600;
    fd = open(path, oflag, mode);
    if (fd < 0) return -1;
    f->fd = fd;
    return 0;
}

void t_file_close(t_file *f) {
    if (!f || f->fd < 0) return;
    close(f->fd);
    f->fd = -1;
}

int t_file_read(t_file *f, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t off = 0;
    if (!t_file_is_open(f) || (n && !buf)) return -1;
    while (off < n) {
        ssize_t r = read(f->fd, p + off, n - off);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) return 1;
        off += (size_t)r;
    }
    return 0;
}

int t_file_write(t_file *f, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t off = 0;
    if (!t_file_is_open(f) || (n && !buf)) return -1;
    while (off < n) {
        ssize_t w = write(f->fd, p + off, n - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) return -1;
        off += (size_t)w;
    }
    return 0;
}

int t_file_seek(t_file *f, int64_t off, int whence) {
    if (!t_file_is_open(f)) return -1;
    return lseek(f->fd, (off_t)off, whence) < 0 ? -1 : 0;
}

int t_file_size(t_file *f, uint64_t *out) {
    struct stat st;
    if (!t_file_is_open(f) || !out) return -1;
    if (fstat(f->fd, &st) != 0 || st.st_size < 0) return -1;
    *out = (uint64_t)st.st_size;
    return 0;
}

int t_file_sync(t_file *f) {
    if (!t_file_is_open(f)) return -1;
    return fsync(f->fd);
}

int t_file_unlink(const char *path) {
    if (!path) return -1;
    return unlink(path) == 0 || errno == ENOENT ? 0 : -1;
}

int t_file_rename(const char *from, const char *to) {
    if (!from || !to) return -1;
    return rename(from, to) == 0 ? 0 : -1;
}

int t_file_not_found(void) {
    return errno == ENOENT;
}

#endif /* T_PLATFORM_WINDOWS */
