cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)

project(karu)

find_package(AbseilCpp REQUIRED)
target_link_libraries(karu
  absl::status
  absl::statusor
  absl::span
  absl::endian
  absl::strings
  absl::btree
  absl::strings
)

add_executable(karu src/karu.cc src/main.cc src/memory_table.cc src/sstable.h)
