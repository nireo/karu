#include <filesystem>
#include <iostream>

#include "gtest/gtest.h"
#include "memory_table.h"

void createTestDirectory(const std::string &path) {
  std::filesystem::create_directory(path);
}

TEST(EncoderTest, RawHeader) { EXPECT_EQ(1, 1); }

TEST(MemtableTest, LogFileCreated) {
  const std::string test_dir = "./test";
  createTestDirectory(test_dir);

  karu::memtable::Memtable memtable(test_dir);
  EXPECT_TRUE(std::filesystem::exists(memtable.log_path_));

  std::filesystem::remove_all(test_dir);
}

TEST(MemtableTest, BasicOperations) {
  const std::string test_dir = "./test";
  createTestDirectory(test_dir);

  karu::memtable::Memtable memtable(test_dir);

  auto status = memtable.Insert("key", "value");
  EXPECT_TRUE(status.ok());

  auto value = memtable.Get("key");
  EXPECT_TRUE(value.ok());

  EXPECT_EQ(*value, "key");

  std::filesystem::remove_all(test_dir);
}
