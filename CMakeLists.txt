cmake_minimum_required(VERSION 3.14)
project(lsm_engine CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(lsm
    src/memtable.cpp
    src/sstable.cpp
    src/bloom.cpp
    src/db.cpp
    src/wal.cpp
)
target_include_directories(lsm PUBLIC include)

add_executable(lsm_test test/test_main.cpp)
target_link_libraries(lsm_test lsm)
