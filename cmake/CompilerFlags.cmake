## Platform- and compiler-specific flags for Transit

include(CheckCCompilerFlag)
include(CheckCSourceCompiles)
include(CheckSymbolExists)
include(CheckIncludeFile)

message(STATUS "Configuring compiler flags for platform: ${CMAKE_SYSTEM_NAME}")

if(MSVC)
    # MSVC flags
    add_compile_options(/W4 /WX)
    # C99 constructs Clang -Werror already accepts; MSVC /W4 still flags them.
    add_compile_options(/wd4204 /wd4221 /wd4244 /wd4267)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS _CRT_NONSTDC_NO_WARNINGS)
    # Use C11 if available, otherwise C99
    if(CMAKE_C_STANDARD LESS 11)
        set(CMAKE_C_STANDARD 11)
        set(CMAKE_C_STANDARD_REQUIRED ON)
    endif()
else()
    # GCC / Clang flags
    add_compile_options(-Wall -Wextra -Werror)
    if(NOT T_PLATFORM_WINDOWS)
        add_compile_options(-fPIC)
    endif()
    # CMake 3.16 does not know C23 dialect flags for newer compilers. Keep the
    # build system on C11 while source remains C99-C23 compatible.
    include(CheckCCompilerFlag)
    check_c_compiler_flag("-std=c11" HAS_C11)
    if(HAS_C11)
        set(CMAKE_C_STANDARD 11)
    else()
        set(CMAKE_C_STANDARD 99)
    endif()
    set(CMAKE_C_STANDARD_REQUIRED ON)
    set(CMAKE_C_EXTENSIONS ON)
endif()

# Sanitizer options
if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

if(ENABLE_UBSAN)
    add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=undefined)
endif()

## Platform defines (set by PlatformDetect.cmake)
if(T_HAVE_EPOLL)
    add_compile_definitions(T_HAVE_EPOLL=1)
endif()
if(T_HAVE_KQUEUE)
    add_compile_definitions(T_HAVE_KQUEUE=1)
endif()
if(T_HAVE_IOCP)
    add_compile_definitions(T_HAVE_IOCP=1)
endif()
if(T_HAVE_IOURING)
    add_compile_definitions(T_HAVE_IOURING=1)
endif()
if(T_HAVE_PTHREAD)
    find_package(Threads REQUIRED)
    add_compile_definitions(T_HAVE_PTHREAD=1)
endif()

# Atomic support (C11 atomics)
if(T_HAVE_C11_ATOMICS)
    add_compile_definitions(T_HAVE_C11_ATOMICS=1)
endif()

# Platform macros for users
if(T_PLATFORM_LINUX)
    add_compile_definitions(T_PLATFORM_LINUX=1)
endif()
if(T_PLATFORM_MACOS)
    add_compile_definitions(T_PLATFORM_MACOS=1)
endif()
if(T_PLATFORM_WINDOWS)
    add_compile_definitions(T_PLATFORM_WINDOWS=1)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS _CRT_NONSTDC_NO_WARNINGS)
endif()
