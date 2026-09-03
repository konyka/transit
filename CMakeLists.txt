cmake_minimum_required(VERSION 3.16)
project(transit C ASM)

# Modern C standard handling - will be refined by CompilerFlags.cmake
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

# Platform detection must run before CompilerFlags so T_HAVE_* macros exist
include(cmake/PlatformDetect.cmake)
include(cmake/CompilerFlags.cmake)

option(ENABLE_DEBUG "Enable debug builds" OFF)
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

if(ENABLE_DEBUG)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
  add_compile_definitions(DEBUG=1)
endif()

# Sub-projects
option(BUILD_TESTS "Build tests" ON)
option(BUILD_EXAMPLES "Build examples" ON)

include(CTest)

# The main transit static library
add_library(transit STATIC)
target_include_directories(transit PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/core
    ${CMAKE_SOURCE_DIR}/src/memory
    ${CMAKE_SOURCE_DIR}/src/collections
    ${CMAKE_SOURCE_DIR}/src/threadpool
    ${CMAKE_SOURCE_DIR}/src/coroutine
    ${CMAKE_SOURCE_DIR}/src/event
    ${CMAKE_SOURCE_DIR}/src/net
    ${CMAKE_SOURCE_DIR}/src/protocol
    ${CMAKE_SOURCE_DIR}/src/storage
    ${CMAKE_SOURCE_DIR}/src/queue
    ${CMAKE_SOURCE_DIR}/src/cluster
    ${CMAKE_SOURCE_DIR}/src/session
    ${CMAKE_SOURCE_DIR}/src/broker
    ${CMAKE_SOURCE_DIR}/src/client
)

message(STATUS "Configuring Transit build (static library).")
add_subdirectory(src)

# Link pthreads if available
if(T_HAVE_PTHREAD)
    find_package(Threads REQUIRED)
    target_link_libraries(transit PUBLIC Threads::Threads)
endif()

if(T_PLATFORM_WINDOWS)
    target_link_libraries(transit PUBLIC ws2_32)
endif()

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

include(CPack)
set(CPACK_PACKAGE_NAME "transit")
set(CPACK_PACKAGE_VERSION "0.1.0")
set(CPACK_GENERATOR "TGZ")
