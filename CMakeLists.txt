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

add_executable(karu
  src/karu.cc
  src/main.cc
  src/memory_table.cc
  src/sstable.cc
  src/encoder.cc
  src/file_writer.cc


)

find_package(Gtest)
if(Gtest_FOUND)
  enable_testing()

  include(GoogleTest)

  function(karu_test test_file)
    get_filename_component(test_target_name ${test_file} NAME_WE)

    add_executable(${test_target_name} ${test_file})
    target_link_libraries(${test_target_name} ${PROJECT_NAME} gtest_main)

    gtest_discover_tests(${test_target_name})
  endfunction(karu_test)

  karu_test(src/encoder_test.cc)
endif()
