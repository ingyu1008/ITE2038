# Sources
set(DB_SOURCE_DIR src)
set(DB_SOURCES
  ${DB_SOURCE_DIR}/lock_table.cc
  # Add your sources here
  )

#Headers
set(DB_HEADER_DIR include)
set(DB_HEADERS
  ${DB_HEADER_DIR}/lock_table.h
  # Add your headers here
  )

add_library(lock_table STATIC ${DB_HEADERS} ${DB_SOURCES})

target_include_directories(lock_table
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/${DB_HEADER_DIR}"
  )
