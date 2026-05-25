# Root build for all Transit modules.
# Each module adds its sources to the top-level 'transit' library target.

if(NOT TARGET transit)
  message(FATAL_ERROR "transit target not found in parent scope.")
endif()

add_subdirectory(core)
add_subdirectory(memory)
add_subdirectory(collections)
add_subdirectory(threadpool)
add_subdirectory(coroutine)
add_subdirectory(event)
add_subdirectory(net)
add_subdirectory(protocol)
add_subdirectory(storage)
add_subdirectory(queue)
add_subdirectory(cluster)
add_subdirectory(session)
add_subdirectory(broker)
add_subdirectory(client)
