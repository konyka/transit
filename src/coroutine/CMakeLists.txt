# Module: coroutine
file(GLOB T_MODULE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.c")
if(T_MODULE_SOURCES)
  target_sources(transit PRIVATE ${T_MODULE_SOURCES})
endif()

set(_t_proc "${CMAKE_SYSTEM_PROCESSOR}")
string(TOLOWER "${_t_proc}" _t_proc)
if(_t_proc MATCHES "x86_64|amd64")
  target_sources(transit PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/t_coro_asm_x86_64.S")
elseif(_t_proc MATCHES "aarch64|arm64")
  target_sources(transit PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/t_coro_asm_aarch64.S")
endif()
