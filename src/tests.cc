#include <absl/container/btree_map.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "gtest/gtest.h"
#include "memory_table.h"
#include "sstable.h"

#define OK EXPECT_TRUE(status.ok())

void createTestDirectory(const std::string &path) {
  std::filesystem::create_directory(path);
}

void createTestFile(const std::string &path) {
  std::ofstream f{path};
  f.close();
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

  EXPECT_EQ(*value, "value");

  std::filesystem::remove_all(test_dir);
}

TEST(SSTableTest, TestBuildFromBTree) {
  const std::string test_dir = "./test";
  createTestDirectory(test_dir);
  absl::btree_map<std::string, std::string> values = {
      {"hello", "world"},
      {"key", "world"},
      {"1", "2"},
  };

  createTestFile("./test/table.data");
  karu::sstable::SSTable sstable("./test/table.data");
  auto status = sstable.InitWriterAndReader();

  status = sstable.BuildFromBTree(values);

  for (const auto &[key, value] : values) {
    auto f_status = sstable.Find(key);

    EXPECT_TRUE(f_status.ok());
    EXPECT_EQ(*f_status, value);
  }

  std::filesystem::remove_all(test_dir);
}

TEST(SSTableTest, TestPopulateFromFile) {
  const std::string test_dir = "./test";
  createTestDirectory(test_dir);
  absl::btree_map<std::string, std::string> values = {
      {"sami", "world"},
      {"hello", "joo"},
  };

  createTestFile("./test/table.data");
  {
    auto sstable =
        std::make_unique<karu::sstable::SSTable>("./test/table.data");
    auto status = sstable->InitWriterAndReader();
    OK;
    status = sstable->BuildFromBTree(values);
    OK;
  }

  {
    auto sstable =
        std::make_unique<karu::sstable::SSTable>("./test/table.data");
    auto status = sstable->InitOnlyReader();
    OK;
    status = sstable->PopulateFromFile();
    OK;

    for (const auto &[key, value] : values) {
      auto f_status = sstable->Find(key);

      EXPECT_TRUE(f_status.ok());
      EXPECT_EQ(*f_status, value);
    }
  }

  std::filesystem::remove_all(test_dir);
}
