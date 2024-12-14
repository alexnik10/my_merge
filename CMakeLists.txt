cmake_minimum_required(VERSION 3.22)
project(my_project CXX)

set(CMAKE_CXX_STANDARD 23)

#include_directories(rocksdb/include)
add_subdirectory(librdx)
#link_directories(rocksdb)
#find_package(ZLIB REQUIRED)
#find_package(BZip2 REQUIRED)

add_executable(my_merge src/main.cpp)
target_link_libraries(my_merge PRIVATE librdx)

target_include_directories(my_merge PRIVATE librdx)
target_include_directories(my_merge PRIVATE librdx/rdx)
# target_include_directories(my_merge PRIVATE librdx/abc)
