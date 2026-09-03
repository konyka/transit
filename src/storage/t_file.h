#ifndef T_FILE_H
#define T_FILE_H

#include "t_compiler.h"
#include <stdint.h>
#include <stddef.h>

#define T_FILE_READ  1
#define T_FILE_WRITE 2
#define T_FILE_CREAT 4
#define T_FILE_TRUNC 8

typedef struct t_file {
    int   fd;
    void *os; /* Windows HANDLE; unused on POSIX */
} t_file;

void t_file_init(t_file *f);
int  t_file_is_open(const t_file *f);
int  t_file_open(t_file *f, const char *path, int flags);
void t_file_close(t_file *f);
/* 0 complete, 1 EOF, -1 error */
int  t_file_read(t_file *f, void *buf, size_t n);
int  t_file_write(t_file *f, const void *buf, size_t n);
int  t_file_seek(t_file *f, int64_t off, int whence);
int  t_file_size(t_file *f, uint64_t *out);
int  t_file_sync(t_file *f);
int  t_file_unlink(const char *path);
int  t_file_rename(const char *from, const char *to);
/* After a failed t_file_open: path was missing. */
int  t_file_not_found(void);

#endif /* T_FILE_H */
