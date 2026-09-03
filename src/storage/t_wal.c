#include "t_wal.h"
#include "t_crc32c.h"
#include "t_queue.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

#define T_WAL_HDR 8
#define T_WAL_REC_HDR 18 /* crc + op + pri + id + len */
#define T_WAL_VER 1

struct t_wal {
    int fd;
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

static int full_write(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(fd, p + off, n - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) return -1;
        off += (size_t)w;
    }
    return 0;
}

static int full_read(int fd, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t r = read(fd, p + off, n - off);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) return 1; /* EOF */
        off += (size_t)r;
    }
    return 0;
}

static int do_fsync(int fd) {
    return fsync(fd);
}

t_wal *t_wal_open(const char *path, int sync_every) {
    if (!path || !path[0]) return NULL;
    t_wal *w = (t_wal *)calloc(1, sizeof(*w));
    if (!w) return NULL;
    w->path = strdup(path);
    if (!w->path) {
        free(w);
        return NULL;
    }
    w->sync_every = sync_every < 0 ? 0 : sync_every;
    w->fd = open(path, O_RDWR | O_CREAT, 0600);
    if (w->fd < 0) {
        free(w->path);
        free(w);
        return NULL;
    }
    struct stat st;
    if (fstat(w->fd, &st) != 0) {
        close(w->fd);
        free(w->path);
        free(w);
        return NULL;
    }
    if (st.st_size == 0) {
        uint8_t hdr[T_WAL_HDR] = { 'T', 'W', 'A', 'L', T_WAL_VER, 0, 0, 0 };
        if (full_write(w->fd, hdr, sizeof(hdr)) != 0) {
            close(w->fd);
            unlink(path);
            free(w->path);
            free(w);
            return NULL;
        }
        w->bytes = T_WAL_HDR;
        return w;
    }
    if (st.st_size < T_WAL_HDR || (uint64_t)st.st_size > (uint64_t)T_WAL_MAX_FILE) {
        close(w->fd);
        free(w->path);
        free(w);
        return NULL;
    }
    uint8_t hdr[T_WAL_HDR];
    if (lseek(w->fd, 0, SEEK_SET) < 0 || full_read(w->fd, hdr, sizeof(hdr)) != 0 ||
        hdr[0] != 'T' || hdr[1] != 'W' || hdr[2] != 'A' || hdr[3] != 'L' ||
        hdr[4] != T_WAL_VER) {
        close(w->fd);
        free(w->path);
        free(w);
        return NULL;
    }
    if (lseek(w->fd, 0, SEEK_END) < 0) {
        close(w->fd);
        free(w->path);
        free(w);
        return NULL;
    }
    w->bytes = (size_t)st.st_size;
    return w;
}

void t_wal_close(t_wal *w) {
    if (!w) return;
    if (w->fd >= 0) {
        (void)do_fsync(w->fd);
        close(w->fd);
    }
    free(w->path);
    free(w);
}

int t_wal_append(t_wal *w, uint8_t op, uint8_t priority, uint64_t msg_id,
                 const uint8_t *data, uint32_t len) {
    if (!w || w->fd < 0) return -1;
    if (op != T_WAL_PUT && op != T_WAL_DEL) return -1;
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

    int rc = full_write(w->fd, buf, rec);
    if (heap) free(buf);
    if (rc != 0) return -1;
    w->bytes += rec;
    w->appends++;
    w->unsynced++;
    if (w->sync_every > 0 && w->unsynced >= w->sync_every) {
        if (do_fsync(w->fd) != 0) return -1;
        w->unsynced = 0;
    }
    return 0;
}

int t_wal_flush(t_wal *w) {
    if (!w || w->fd < 0) return -1;
    if (do_fsync(w->fd) != 0) return -1;
    w->unsynced = 0;
    return 0;
}

int t_wal_replay(t_wal *w, t_wal_replay_fn fn, void *ud) {
    if (!w || w->fd < 0 || !fn) return -1;
    if (lseek(w->fd, T_WAL_HDR, SEEK_SET) < 0) return -1;
    for (;;) {
        uint8_t hdr[T_WAL_REC_HDR];
        int r = full_read(w->fd, hdr, sizeof(hdr));
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
            r = full_read(w->fd, data, len);
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
        if (op != T_WAL_PUT && op != T_WAL_DEL) {
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
    if (lseek(w->fd, 0, SEEK_END) < 0) return -1;
    return 0;
}

int t_wal_unlink(const char *path) {
    if (!path) return -1;
    return unlink(path) == 0 || errno == ENOENT ? 0 : -1;
}

const char *t_wal_path(const t_wal *w) {
    return w ? w->path : NULL;
}

size_t t_wal_appends(const t_wal *w) {
    return w ? w->appends : 0;
}
