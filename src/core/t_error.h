#ifndef T_ERROR_H
#define T_ERROR_H

#include <stdint.h>

/* Error code type */
typedef int64_t t_error_code;

/* Error with context */
typedef struct t_error {
    t_error_code code;
    const char   *message;
    const char   *file;
    int           line;
    const char   *func;
} t_error;

/* Error code ranges */
#define T_OK_CODE       ((t_error_code)0)
#define T_ERR_GENERIC   ((t_error_code)-1)
#define T_ERR_NOMEM     ((t_error_code)-2)
#define T_ERR_INVALID   ((t_error_code)-3)
#define T_ERR_TIMEOUT   ((t_error_code)-4)
#define T_ERR_BUSY      ((t_error_code)-5)
#define T_ERR_NOTFOUND  ((t_error_code)-6)
#define T_ERR_EXISTS    ((t_error_code)-7)
#define T_ERR_IO        ((t_error_code)-8)
#define T_ERR_CONN      ((t_error_code)-9)
#define T_ERR_CLOSED    ((t_error_code)-10)
#define T_ERR_OVERFLOW  ((t_error_code)-11)
#define T_ERR_PROTO     ((t_error_code)-12)
#define T_ERR_PERMISSION ((t_error_code)-13)
#define T_ERR_AGAIN     ((t_error_code)-14)

/* Explicit zero-value initializer */
#define T_ERROR_NULL { T_OK_CODE, NULL, NULL, 0, NULL }

/* Create error with context */
#define T_ERROR(code, msg) \
    { (code), (msg), __FILE__, __LINE__, __func__ }

/* Check if error is OK */
#define T_OK(err) ((err).code == T_OK_CODE)

/* Check if error is a failure */
#define T_FAIL(err) ((err).code != T_OK_CODE)

/* Return error from function */
#define T_RETURN_IF_FAIL(err) \
    do { if ((err).code != T_OK_CODE) return (err); } while(0)

/* Return error code from function */
#define T_RETURN_CODE_IF_FAIL(err) \
    do { if ((err).code != T_OK_CODE) return (err).code; } while(0)

/* API */
const char *t_error_code_str(t_error_code code);
void t_error_print(const t_error *err);

#endif /* T_ERROR_H */
