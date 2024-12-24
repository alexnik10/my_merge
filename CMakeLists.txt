cmake_minimum_required(VERSION 3.22)
project(my_project CXX)

set(CMAKE_CXX_STANDARD 23)

link_directories(${CMAKE_SOURCE_DIR}/rocksdb)
set(ROCKSDB_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/rocksdb/include")

find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)

add_executable(merge_operator_example src/merge_operator_example.cpp)
target_include_directories(merge_operator_example PRIVATE ${ROCKSDB_INCLUDE_DIR})
target_link_libraries(merge_operator_example PRIVATE rocksdb z bz2 lz4 zstd snappy)

add_subdirectory(librdx)

add_executable(my_merge src/main.cpp)
target_include_directories(my_merge PRIVATE librdx)
target_include_directories(my_merge PRIVATE librdx/rdx)
target_link_libraries(my_merge PRIVATE librdx)
