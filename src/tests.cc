#include <absl/container/btree_map.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "encoder.h"
#include "gtest/gtest.h"
#include "karu.h"
#include "memory_table.h"
#include "sstable.h"

using namespace karu;

#define OK EXPECT_TRUE(status.ok())

void createTestDirectory(const std::string &path) {
  std::filesystem::create_directory(path);
}

void createTestFile(const std::string &path) {
  std::ofstream f{path};
  f.close();
}

std::string gen_random_str(size_t size) {
  const std::string chars =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::random_device random_device;
  std::mt19937 generator(random_device());
  std::uniform_int_distribution<> distribution(0, chars.size() - 1);
  std::string random_string;

  for (std::size_t i = 0; i < size; ++i) {
    random_string += chars[distribution(generator)];
  }
  return random_string;
}

std::vector<std::string> generate_random_keys(size_t key_count = 10000,
                                              size_t key_size = 10) {
  std::vector<std::string> keys;
  keys.resize(key_count);

  for (std::string &v : keys) {
    v = gen_random_str(key_size);
  }
  return keys;
}

std::vector<std::pair<std::string, std::string>> generate_random_pairs(
    size_t pair_count = 10000, size_t str_size = 10) {
  std::vector<std::pair<std::string, std::string>> keys(pair_count);
  for (auto &[key, value] : keys) {
    key = gen_random_str(str_size);
    value = gen_random_str(str_size);
  }

  return keys;
}

void test_wrapper(std::function<void(std::string)> test_func) {
  const std::string test_dir = "./test";
  createTestDirectory(test_dir);

  test_func(test_dir);

  std::filesystem::remove_all(test_dir);
}

TEST(EncoderTest, RawHeader) { EXPECT_EQ(1, 1); }

TEST(MemtableTest, LogFileCreated) {
  test_wrapper([](std::string test_dir) {
    memtable::Memtable memtable(test_dir);
    EXPECT_TRUE(std::filesystem::exists(memtable.log_path_));
  });
}

TEST(MemtableTest, BasicOperations) {
  test_wrapper([](std::string test_dir) {
    memtable::Memtable memtable(test_dir);

    auto status = memtable.Insert("key", "value");
    EXPECT_TRUE(status.ok());

    auto value = memtable.Get("key");
    EXPECT_TRUE(value.ok());

    EXPECT_EQ(*value, "value");
  });
}

TEST(SSTableTest, TestBuildFromBTree) {
  test_wrapper([](std::string test_dir) {
    absl::btree_map<std::string, std::string> values = {
        {"hello", "world"},
        {"key", "world"},
        {"1", "2"},
    };

    createTestFile("./test/table.data");
    sstable::SSTable sstable("./test/table.data");
    auto status = sstable.InitWriterAndReader();

    status = sstable.BuildFromBTree(values);

    for (const auto &[key, value] : values) {
      auto f_status = sstable.Find(key);

      EXPECT_TRUE(f_status.ok());
      EXPECT_EQ(*f_status, value);
    }
  });
}

TEST(SSTableTest, TestPopulateFromFile) {
  test_wrapper([](std::string test_dir) {
    absl::btree_map<std::string, std::string> values = {
        {"sami", "world"},
        {"hello", "joo"},
    };

    createTestFile("./test/table.data");
    {
      auto sstable =
          std::make_unique<sstable::SSTable>("./test/table.data");
      auto status = sstable->InitWriterAndReader();
      OK;
      status = sstable->BuildFromBTree(values);
      OK;
    }

    {
      auto sstable =
          std::make_unique<sstable::SSTable>("./test/table.data");
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
  });
}

TEST(KaruTest, BasicOperations) {
  test_wrapper([](std::string test_dir) {
    DB db(test_dir);

    // these go into the memtable.
    auto status = db.Insert("hello", "world");
    OK;

    auto get_status = db.Get("hello");
    EXPECT_TRUE(get_status.ok());
    EXPECT_EQ("world", *get_status);
  });
}

TEST(KaruTest, LotsOfPairs) {
  test_wrapper([](std::string test_dir) {
    auto keys = generate_random_keys();
    DB db(test_dir);

    for (const auto &k : keys) {
      auto status = db.Insert(k, k);
      OK;
    }

    for (const auto &key : keys) {
      auto status = db.Get(key);
      OK;

      EXPECT_EQ(key, *status);
    }
  });
}

TEST(KaruTest, MemtableFlush) {
  test_wrapper([](std::string test_dir) {
    auto keys = generate_random_keys(500);
    karu::DB db(test_dir);

    for (const auto &k : keys) {
      auto status = db.Insert(k, k);
      OK;
    }
    auto status = db.FlushMemoryTable();
    OK;

    // let is sleep for a little while
    EXPECT_EQ(1, db.sstable_map_.size());
    for (const auto &k : keys) {
      auto f_status = (*db.sstable_map_.begin()).second->Find(k);
      EXPECT_TRUE(f_status.ok());
    }
  });
}

TEST(EncoderTest, HintHeader) {
  std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[8]);
  encoder::HintHeader hintheader(buffer.get());
}
