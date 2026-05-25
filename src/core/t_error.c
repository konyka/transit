#include "t_error.h"
#include <stdio.h>

static const char* t_error_code_to_str(t_error_code code) {
    switch (code) {
        case T_OK_CODE:         return "OK";
        case T_ERR_GENERIC:     return "EGENERIC";
        case T_ERR_NOMEM:       return "ENOMEM";
        case T_ERR_INVALID:     return "EINVAL";
        case T_ERR_TIMEOUT:     return "ETIMOUT";
        case T_ERR_BUSY:        return "EBUSY";
        case T_ERR_NOTFOUND:    return "ENOENT";
        case T_ERR_EXISTS:      return "EEXIST";
        case T_ERR_IO:          return "EIO";
        case T_ERR_CONN:        return "ECONN";
        case T_ERR_CLOSED:      return "ECLOSED";
        case T_ERR_OVERFLOW:    return "EOVERFLOW";
        case T_ERR_PROTO:       return "EPROTO";
        case T_ERR_PERMISSION:  return "EPERM";
        case T_ERR_AGAIN:       return "EAGAIN";
        default:                  return "EUNKNOWN";
    }
}

const char *t_error_code_str(t_error_code code) {
    return t_error_code_to_str(code);
}

void t_error_print(const t_error *err) {
    if (!err) return;
    const char *file = err->file ? err->file : "(unknown)";
    const char *func = err->func ? err->func : "(unknown)";
    const char *msg  = err->message ? err->message : "";
    fprintf(stderr, "%s:%d:%s: %s: %s\n", file, err->line, func,
            t_error_code_str(err->code), msg);
}
