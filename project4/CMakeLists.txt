cmake_minimum_required(VERSION 3.21)

project(test_lock_table VERSION 1.0.0)

# C++ settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

option(USE_LOCK_TABLE "Use the lock table library" ON)

# lock table project library
if(USE_LOCK_TABLE)
  add_subdirectory(lock_table)
  list(APPEND EXTRA_LIBS lock_table)
endif()

add_executable(${CMAKE_PROJECT_NAME} main.cc)

target_link_libraries(${CMAKE_PROJECT_NAME} PUBLIC ${EXTRA_LIBS}
                      Threads::Threads)

target_include_directories(${CMAKE_PROJECT_NAME} PUBLIC
  "${PROJECT_BINARY_DIR}"
  )
