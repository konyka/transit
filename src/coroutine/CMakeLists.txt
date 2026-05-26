# Module: coroutine
file(GLOB T_MODULE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.c" "${CMAKE_CURRENT_SOURCE_DIR}/*.S")
if(T_MODULE_SOURCES)
  target_sources(transit PRIVATE ${T_MODULE_SOURCES})
endif()
