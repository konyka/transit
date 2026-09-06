# Tests for Transit

# Test framework is always built
add_library(t_test STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/framework/t_test.c
)
target_include_directories(t_test PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/framework
)
if(MSVC)
    # Keep .CRT$XCU T_TEST registrars under /OPT:REF (Release).
    target_link_options(t_test INTERFACE /OPT:NOREF)
endif()

# Build and register all unit tests
file(GLOB UNIT_TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/unit/*.c")
foreach(test_file ${UNIT_TEST_SOURCES})
    get_filename_component(test_name ${test_file} NAME_WE)
    add_executable(${test_name} ${test_file})
    target_link_libraries(${test_name} PRIVATE t_test transit)
    target_include_directories(${test_name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/framework
        ${CMAKE_SOURCE_DIR}/src
    )
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()

# Build and register all integration tests
file(GLOB INTEGRATION_TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/integration/*.c")
foreach(test_file ${INTEGRATION_TEST_SOURCES})
    get_filename_component(test_name ${test_file} NAME_WE)
    add_executable(${test_name} ${test_file})
    target_link_libraries(${test_name} PRIVATE t_test transit)
    target_include_directories(${test_name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/framework
        ${CMAKE_SOURCE_DIR}/src
    )
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()

# Build and register all benchmarks
file(GLOB BENCH_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.c")
foreach(bench_file ${BENCH_SOURCES})
    get_filename_component(bench_name ${bench_file} NAME_WE)
    add_executable(${bench_name} ${bench_file})
    target_link_libraries(${bench_name} PRIVATE t_test transit)
    target_include_directories(${bench_name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/framework
        ${CMAKE_SOURCE_DIR}/src
    )
    add_test(NAME ${bench_name} COMMAND ${bench_name} LABELS benchmark)
endforeach()
