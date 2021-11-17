cmake_minimum_required(VERSION 3.21)

project(disk_based_db VERSION 1.0.0)

configure_file(DbConfig.h.in DbConfig.h)

include(CTest)

# C++ settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Options for libraries
option(USE_DB "Use the DB library" ON)
option(USE_GOOGLE_TEST "Use GoogleTest for testing" ON)

# DB project library
if(USE_DB)
  add_subdirectory(db)
  list(APPEND EXTRA_LIBS db)
endif()

# GoogleTest
if(USE_GOOGLE_TEST)
  add_subdirectory(test)
endif()

add_executable(${CMAKE_PROJECT_NAME} main.cc)

target_link_libraries(${CMAKE_PROJECT_NAME} PUBLIC ${EXTRA_LIBS})

target_include_directories(${CMAKE_PROJECT_NAME} PUBLIC
  "${PROJECT_BINARY_DIR}"
  )

