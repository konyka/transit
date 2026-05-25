cmake_minimum_required(VERSION 3.16)
project(transit C)

# Modern C standard handling - will be refined by CompilerFlags.cmake
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

# Include project-wide flags and platform detection
include(cmake/CompilerFlags.cmake)
include(cmake/PlatformDetect.cmake)

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
)

message(STATUS "Configuring Transit build (static library).")
add_subdirectory(src)

# Link pthreads if available
if(T_HAVE_PTHREAD)
    find_package(Threads REQUIRED)
    target_link_libraries(transit PUBLIC Threads::Threads)
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
