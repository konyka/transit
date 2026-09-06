#include "t_wal.h"
#include "t_crc32c.h"
#include "t_queue.h"
#include "t_compiler.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

#define T_WAL_HDR 8
#define T_WAL_REC_HDR 18 /* crc + op + pri + id + len */
#define T_WAL_VER 1

struct t_wal {
    int fd;
    void *os_file; /* Windows HANDLE; unused on POSIX */
    char *path;
    int sync_every;
    int unsynced;
    size_t bytes;
    size_t appends;
};

static void write_le32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

static void write_le64(uint8_t *p, uint64_t v) {
    write_le32(p, (uint32_t)v);
    write_le32(p + 4, (uint32_t)(v >> 32));
}

static uint32_t read_le32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t read_le64(const uint8_t *p) {
    return (uint64_t)read_le32(p) | ((uint64_t)read_le32(p + 4) << 32);
}

#if T_PLATFORM_WINDOWS

#include <windows.h>

static int wal_is_open(const t_wal *w) {
    return w && w->os_file != NULL;
}

static int io_open(t_wal *w, const char *path) {
    HANDLE h = CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
                           FILE_SHARE_READ, NULL, OPEN_ALWAYS,
                           FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE) return -1;
    w->os_file = (void *)h;
    w->fd = -1;
    return 0;
}

static void io_close(t_wal *w) {
    if (w && w->os_file) {
        CloseHandle((HANDLE)w->os_file);
        w->os_file = NULL;
    }
}

static int io_write(t_wal *w, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    HANDLE h = (HANDLE)w->os_file;
    while (n) {
        DWORD chunk = n > 0x7fffffff ? 0x7fffffff : (DWORD)n;
        DWORD wrote = 0;
        if (!WriteFile(h, p, chunk, &wrote, NULL) || wrote == 0) return -1;
        p += wrote;
        n -= wrote;
    }
    return 0;
}

static int io_read(t_wal *w, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    HANDLE h = (HANDLE)w->os_file;
    size_t off = 0;
    while (off < n) {
        DWORD chunk = (n - off) > 0x7fffffff ? 0x7fffffff : (DWORD)(n - off);
        DWORD got = 0;
        if (!ReadFile(h, p + off, chunk, &got, NULL)) return -1;
        if (got == 0) return 1; /* EOF */
        off += got;
    }
    return 0;
}

static int io_seek(t_wal *w, int64_t off, int whence) {
    LARGE_INTEGER li;
    DWORD method;
    li.QuadPart = off;
    if (whence == SEEK_END) method = FILE_END;
    else if (whence == SEEK_CUR) method = FILE_CURRENT;
    else method = FILE_BEGIN;
    return SetFilePointerEx((HANDLE)w->os_file, li, NULL, method) ? 0 : -1;
}

static int io_size(t_wal *w, uint64_t *out) {
    LARGE_INTEGER sz;
    if (!GetFileSizeEx((HANDLE)w->os_file, &sz) || sz.QuadPart < 0) return -1;
    *out = (uint64_t)sz.QuadPart;
    return 0;
}

static int io_sync(t_wal *w) {
    return FlushFileBuffers((HANDLE)w->os_file) ? 0 : -1;
}

static int io_unlink(const char *path) {
    if (DeleteFileA(path)) return 0;
    DWORD err = GetLastError();
    return (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) ? 0 : -1;
}

#else /* POSIX */

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

static int wal_is_open(const t_wal *w) {
    return w && w->fd >= 0;
}

static int io_open(t_wal *w, const char *path) {
    int fd = open(path, O_RDWR | O_CREAT, 0600);
    if (fd < 0) return -1;
    w->fd = fd;
    return 0;
}

static void io_close(t_wal *w) {
    if (w && w->fd >= 0) {
        close(w->fd);
        w->fd = -1;
    }
}

static int io_write(t_wal *w, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t wr = write(w->fd, p + off, n - off);
        if (wr < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (wr == 0) return -1;
        off += (size_t)wr;
    }
    return 0;
}

static int io_read(t_wal *w, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t r = read(w->fd, p + off, n - off);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) return 1; /* EOF */
        off += (size_t)r;
    }
    return 0;
}

static int io_seek(t_wal *w, int64_t off, int whence) {
    return lseek(w->fd, (off_t)off, whence) < 0 ? -1 : 0;
}

static int io_size(t_wal *w, uint64_t *out) {
    struct stat st;
    if (fstat(w->fd, &st) != 0 || st.st_size < 0) return -1;
    *out = (uint64_t)st.st_size;
    return 0;
}

static int io_sync(t_wal *w) {
    return fsync(w->fd);
}

static int io_unlink(const char *path) {
    return unlink(path) == 0 || errno == ENOENT ? 0 : -1;
}

#endif /* T_PLATFORM_WINDOWS */

t_wal *t_wal_open(const char *path, int sync_every) {
    if (!path || !path[0]) return NULL;
    t_wal *w = (t_wal *)calloc(1, sizeof(*w));
    if (!w) return NULL;
    w->fd = -1;
    w->path = strdup(path);
    if (!w->path) {
        free(w);
        return NULL;
    }
    w->sync_every = sync_every < 0 ? 0 : sync_every;
    if (io_open(w, path) != 0) {
        free(w->path);
        free(w);
        return NULL;
    }
    uint64_t sz = 0;
    if (io_size(w, &sz) != 0) {
        io_close(w);
        free(w->path);
        free(w);
        return NULL;
    }
    if (sz == 0) {
        uint8_t hdr[T_WAL_HDR] = { 'T', 'W', 'A', 'L', T_WAL_VER, 0, 0, 0 };
        if (io_write(w, hdr, sizeof(hdr)) != 0) {
            io_close(w);
            (void)io_unlink(path);
            free(w->path);
            free(w);
            return NULL;
        }
        w->bytes = T_WAL_HDR;
        return w;
    }
    if (sz < T_WAL_HDR || sz > (uint64_t)T_WAL_MAX_FILE) {
        io_close(w);
        free(w->path);
        free(w);
        return NULL;
    }
    uint8_t hdr[T_WAL_HDR];
    if (io_seek(w, 0, SEEK_SET) != 0 || io_read(w, hdr, sizeof(hdr)) != 0 ||
        hdr[0] != 'T' || hdr[1] != 'W' || hdr[2] != 'A' || hdr[3] != 'L' ||
        hdr[4] != T_WAL_VER) {
        io_close(w);
        free(w->path);
        free(w);
        return NULL;
    }
    if (io_seek(w, 0, SEEK_END) != 0) {
        io_close(w);
        free(w->path);
        free(w);
        return NULL;
    }
    w->bytes = (size_t)sz;
    return w;
}

void t_wal_close(t_wal *w) {
    if (!w) return;
    if (wal_is_open(w)) {
        (void)io_sync(w);
        io_close(w);
    }
    free(w->path);
    free(w);
}

int t_wal_append(t_wal *w, uint8_t op, uint8_t priority, uint64_t msg_id,
                 const uint8_t *data, uint32_t len) {
    if (!wal_is_open(w)) return -1;
    if (op != T_WAL_PUT && op != T_WAL_DEL && op != T_WAL_JOIN) return -1;
    if (op == T_WAL_JOIN && (len == 0 || !data)) return -1;
    if (len > 0 && !data) return -1;
    if (len > T_QUEUE_MAX_PAYLOAD) return -1;
    size_t rec = (size_t)T_WAL_REC_HDR + (size_t)len;
    if (w->bytes > T_WAL_MAX_FILE || rec > T_WAL_MAX_FILE - w->bytes) return -1;

    uint8_t stack[256];
    uint8_t *buf = stack;
    int heap = 0;
    if (rec > sizeof(stack)) {
        buf = (uint8_t *)malloc(rec);
        if (!buf) return -1;
        heap = 1;
    }
    buf[4] = op;
    buf[5] = priority;
    write_le64(buf + 6, msg_id);
    write_le32(buf + 14, len);
    if (len) memcpy(buf + T_WAL_REC_HDR, data, len);
    uint32_t crc = t_crc32c(buf + 4, rec - 4);
    write_le32(buf, crc);

    int rc = io_write(w, buf, rec);
    if (heap) free(buf);
    if (rc != 0) return -1;
    w->bytes += rec;
    w->appends++;
    w->unsynced++;
    if (w->sync_every > 0 && w->unsynced >= w->sync_every) {
        if (io_sync(w) != 0) return -1;
        w->unsynced = 0;
    }
    return 0;
}

int t_wal_flush(t_wal *w) {
    if (!wal_is_open(w)) return -1;
    if (io_sync(w) != 0) return -1;
    w->unsynced = 0;
    return 0;
}

int t_wal_replay(t_wal *w, t_wal_replay_fn fn, void *ud) {
    if (!wal_is_open(w) || !fn) return -1;
    if (io_seek(w, T_WAL_HDR, SEEK_SET) != 0) return -1;
    for (;;) {
        uint8_t hdr[T_WAL_REC_HDR];
        int r = io_read(w, hdr, sizeof(hdr));
        if (r == 1) break; /* clean EOF */
        if (r != 0) {
            /* short header at EOF = torn write */
            break;
        }
        uint32_t crc = read_le32(hdr);
        uint8_t op = hdr[4];
        uint8_t pri = hdr[5];
        uint64_t id = read_le64(hdr + 6);
        uint32_t len = read_le32(hdr + 14);
        if (len > T_QUEUE_MAX_PAYLOAD) return -1;
        uint8_t *data = NULL;
        if (len > 0) {
            data = (uint8_t *)malloc(len);
            if (!data) return -1;
            r = io_read(w, data, len);
            if (r != 0) {
                free(data);
                break; /* torn payload */
            }
        }
        uint32_t calc = t_crc32c_update(0xFFFFFFFFu, hdr + 4, (size_t)T_WAL_REC_HDR - 4);
        if (len) calc = t_crc32c_update(calc, data, len);
        calc ^= 0xFFFFFFFFu;
        if (calc != crc) {
            free(data);
            return -1;
        }
        if (op != T_WAL_PUT && op != T_WAL_DEL && op != T_WAL_JOIN) {
            free(data);
            return -1;
        }
        if (op == T_WAL_JOIN && (len == 0 || !data)) {
            free(data);
            return -1;
        }
        t_wal_rec rec;
        rec.op = op;
        rec.priority = pri;
        rec.msg_id = id;
        rec.data = data;
        rec.data_len = len;
        fn(&rec, ud);
        free(data);
    }
    if (io_seek(w, 0, SEEK_END) != 0) return -1;
    return 0;
}

int t_wal_unlink(const char *path) {
    if (!path) return -1;
    return io_unlink(path);
}

const char *t_wal_path(const t_wal *w) {
    return w ? w->path : NULL;
}

size_t t_wal_appends(const t_wal *w) {
    return w ? w->appends : 0;
}
