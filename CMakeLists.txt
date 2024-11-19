cmake_minimum_required(VERSION 3.27)
project(my_project CXX)

set(CMAKE_CXX_STANDARD 23)

add_subdirectory(librdx)

add_executable(my_merge src/main.cpp)
target_link_libraries(my_merge PRIVATE librdx)

target_include_directories(my_merge PRIVATE librdx)
target_include_directories(my_merge PRIVATE librdx/rdx)
