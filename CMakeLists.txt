cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)

project(karu)
add_executable(karu src/karu.cc src/main.cc src/memory_table.cc src/sstable.h)
