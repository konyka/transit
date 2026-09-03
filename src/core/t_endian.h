#ifndef T_ENDIAN_H
#define T_ENDIAN_H

#include "t_compiler.h"

#if T_PLATFORM_WINDOWS
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

#endif /* T_ENDIAN_H */
