cmake_minimum_required(VERSION 3.22)
project(my_project CXX)

set(CMAKE_CXX_STANDARD 23)

link_directories(${CMAKE_SOURCE_DIR}/rocksdb)
set(ROCKSDB_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/rocksdb/include")

find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)

add_subdirectory(librdx)

add_executable(rdx_merge
    src/main.cpp
    src/RdxMerger.cpp
    src/RdxMergeOperator.cpp
)
target_include_directories(rdx_merge PRIVATE librdx)
target_include_directories(rdx_merge PRIVATE librdx/rdx)
target_include_directories(rdx_merge PRIVATE ${ROCKSDB_INCLUDE_DIR})
target_link_libraries(rdx_merge PRIVATE librdx)
target_link_libraries(rdx_merge PRIVATE rocksdb z bz2 lz4 zstd snappy)
