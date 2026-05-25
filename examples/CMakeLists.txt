# Examples for Transit

# Build all example programs
file(GLOB EXAMPLE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.c")
foreach(example_file ${EXAMPLE_SOURCES})
    get_filename_component(example_name ${example_file} NAME_WE)
    add_executable(${example_name} ${example_file})
    target_link_libraries(${example_name} PRIVATE transit)
    target_include_directories(${example_name} PRIVATE
        ${CMAKE_SOURCE_DIR}/src
    )
endforeach()
