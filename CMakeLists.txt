cmake_minimum_required(VERSION 3.14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)

project("karu")

find_package(absl REQUIRED)

add_executable(
  ${PROJECT_NAME}
  src/tests.cc
  src/file_writer.cc
  src/karu.cc
  src/memory_table.cc
  src/sstable.cc
  src/encoder.cc
  src/hint.cc
  src/bloom.cc
  src/murmurhash3.cc
  src/utils
)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()
target_link_libraries(${PROJECT_NAME}
  absl::status
  absl::statusor
  absl::span
  absl::synchronization
  absl::optional
  absl::endian
  absl::btree
  absl::strings
  gtest_main
)

include(GoogleTest)
gtest_discover_tests(${PROJECT_NAME})
