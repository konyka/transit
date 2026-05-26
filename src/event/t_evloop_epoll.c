#include "t_evloop.h"
#include <stdlib.h>

// Lightweight placeholder for epoll backend bindings.
// The actual epoll logic is implemented in t_evloop.c.

// This file exists to satisfy the build layout and allow future backend
// extensions without changing the public API surface.
