## Detect platform specifics and IO backends

# OS platform detection
if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  set(T_PLATFORM_LINUX ON)
  set(T_PLATFORM_MACOS OFF)
  set(T_PLATFORM_WINDOWS OFF)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  set(T_PLATFORM_LINUX OFF)
  set(T_PLATFORM_MACOS ON)
  set(T_PLATFORM_WINDOWS OFF)
elseif(WIN32)
  set(T_PLATFORM_LINUX OFF)
  set(T_PLATFORM_MACOS OFF)
  set(T_PLATFORM_WINDOWS ON)
else()
  set(T_PLATFORM_LINUX OFF)
  set(T_PLATFORM_MACOS OFF)
  set(T_PLATFORM_WINDOWS OFF)
endif()

# Initialize backend feature flags
set(T_HAVE_EPOLL OFF)
set(T_HAVE_KQUEUE OFF)
set(T_HAVE_IOURING OFF)
set(T_HAVE_IOCP OFF)
set(T_HAVE_PTHREAD OFF)
set(T_HAVE_C11_ATOMICS OFF)

## Heuristic checks (best-effort in this skeleton)
include(CheckCSourceCompiles)

if(T_PLATFORM_LINUX)
  # epoll
  check_c_source_compiles("#include <sys/epoll.h>\nint main(){ epoll_create1(0); return 0; }" HAVE_EPOLL_CODE)
  if(HAVE_EPOLL_CODE)
    set(T_HAVE_EPOLL ON)
  endif()
  # io_uring (optional)
  check_c_source_compiles("#include <liburing.h>\nint main(){ struct io_uring q; return 0; }" HAVE_IOURING_CODE)
  if(HAVE_IOURING_CODE)
    set(T_HAVE_IOURING ON)
  endif()
endif()

if(T_PLATFORM_MACOS)
  # kqueue is available on macOS
  check_c_source_compiles("#include <sys/types.h>\n#include <sys/event.h>\nint main(){ int k=kqueue(); return 0; }" HAVE_KQUEUE_CODE)
  if(HAVE_KQUEUE_CODE)
    set(T_HAVE_KQUEUE ON)
  endif()
endif()

if(T_PLATFORM_WINDOWS)
  # IOCP is a Windows feature; mark as available for skeleton
  set(T_HAVE_IOCP ON)
endif()

# pthreads (POSIX threads) availability
include(CheckSymbolExists)
check_symbol_exists(pthread_create "pthread.h" HAVE_PTHREAD)
if(HAVE_PTHREAD)
  set(T_HAVE_PTHREAD ON)
endif()

# C11 atomics support
include(CheckSymbolExists)
check_symbol_exists(_Atomic "" HAVE_ATOMICS)
if(HAVE_ATOMICS)
  set(T_HAVE_C11_ATOMICS ON)
endif()
