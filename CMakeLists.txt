cmake_minimum_required(VERSION 3.14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)

project("karu")

find_package(absl REQUIRED)

add_executable(
  karu
  src/karu.cc
  src/main.cc
  src/memory_table.cc
  src/sstable.cc
  src/encoder.cc
  src/file_writer.cc
)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)

target_link_libraries(${PROJECT_NAME}
  absl::status
  absl::statusor
  absl::span
  absl::endian
  absl::strings
  absl::btree
  absl::strings
)

enable_testing()
include(GoogleTest)

function(karu_test test_file)
  get_filename_component(test_target_name ${test_file} NAME_WE)

  add_executable(${test_target_name} ${test_file})
  target_link_libraries(${test_target_name} ${PROJECT_NAME} gtest_main)
  gtest_discover_tests(${test_target_name})
endfunction(karu_test)
