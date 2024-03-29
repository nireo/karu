#include <absl/container/btree_map.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "bloom.h"
#include "encoder.h"
#include "gtest/gtest.h"
#include "karu.h"
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

void test_wrapper(const std::function<void(std::string)>& test_func) {
  const std::string test_dir = "./test";
  createTestDirectory(test_dir);

  test_func(test_dir);

  std::filesystem::remove_all(test_dir);
}

TEST(SSTableTest, TestBuildFromBTree) {
  test_wrapper([](const std::string& test_dir) {
    absl::btree_map<std::string, std::string> values = {
        {"hello", "world"},
        {"key", "world"},
        {"keykey", "worldworld"},
        {"xdxd", "1231254125125"},
        {"llonglonglonglonglonglonglonglonglonglonglongong", "verylong"},
        {"verylong", "llonglonglonglonglonglonglonglonglonglonglongong"}};

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
  test_wrapper([](const std::string& test_dir) {
    absl::btree_map<std::string, std::string> values = {
        {"hello", "world"},
        {"key", "world"},
        {"keykey", "worldworld"},
        {"xdxd", "1231254125125"},
        {"llonglonglonglonglonglonglonglonglonglonglongong", "verylong"},
        {"verylong", "llonglonglonglonglonglonglonglonglonglonglongong"}};

    createTestFile("./test/table.data");
    {
      auto sstable = std::make_unique<sstable::SSTable>("./test/table.data");
      auto status = sstable->InitWriterAndReader();
      OK;
      status = sstable->BuildFromBTree(values);
      OK;
    }

    {
      auto sstable = std::make_unique<sstable::SSTable>("./test/table.data");
      auto status = sstable->InitOnlyReader();
      OK;
      status = sstable->PopulateFromFile();
      std::cerr << status.message() << '\n';
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
  test_wrapper([](const std::string& test_dir) {
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
  test_wrapper([](const std::string& test_dir) {
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
  test_wrapper([](const std::string& test_dir) {
    auto keys = generate_random_keys(500);
    karu::DB db(test_dir);

    for (const auto &k : keys) {
      auto status = db.Insert(k, k);
      OK;
    }
    auto status = db.FlushMemoryTable();
    OK;

    // let is sleep for a little while
    for (const auto &k : keys) {
      // now we should still be able to find the key.
      auto get_status = db.Get(k);
      EXPECT_TRUE(get_status.ok());

      EXPECT_EQ(*get_status, k);
    }
  });
}

TEST(EncoderTest, HintHeader) {
  std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[8]);
  encoder::HintHeader hintheader(buffer.get());
  constexpr std::uint8_t key_size = 16;
  constexpr std::uint16_t value_size = 1024;

  hintheader.SetKeyLength(key_size);
  hintheader.SetValueLength(value_size);

  EXPECT_EQ(hintheader.KeyLength(), key_size);
  EXPECT_EQ(hintheader.ValueLength(), value_size);

  hintheader.MakeTombstone();
  EXPECT_TRUE(hintheader.IsTombstoneValue());
}

TEST(EncoderTest, EntryHeader) {
  std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[8]);
  encoder::HintHeader entryheader(buffer.get());
  constexpr std::uint8_t key_size = 16;
  constexpr std::uint16_t value_size = 1024;

  entryheader.SetKeyLength(key_size);
  entryheader.SetValueLength(value_size);

  EXPECT_EQ(entryheader.KeyLength(), key_size);
  EXPECT_EQ(entryheader.ValueLength(), value_size);

  entryheader.MakeTombstone();
  EXPECT_TRUE(entryheader.IsTombstoneValue());
}

TEST(BloomFilterTest, Main) {
  bloom::BloomFilter bloomfilter(60000, 13);
  auto keys = generate_random_keys();
  for (const auto &k : keys) {
    bloomfilter.add(k.c_str(), k.size());
  }

  for (const auto &k : keys) {
    EXPECT_TRUE(bloomfilter.contains(k.c_str(), k.size()));
  }
}

TEST(KaruTest, PersistanceTest) {
  test_wrapper([](std::string test_dir) {
    auto keys = generate_random_keys(500);
    {
      karu::DB db(test_dir);

      for (const auto &k : keys) {
        auto status = db.Insert(k, k);
        OK;
      }

      auto status = db.FlushMemoryTable();
      OK;
    }

    {
      karu::DB db(test_dir);

      for (const auto &k : keys) {
        auto status = db.Get(k);
        OK;
        EXPECT_EQ(*status, k);
      }
    }
  });
}

TEST(SStableTest, InsertTest) {
  test_wrapper([](const std::string& test_dir) {
    std::string value = "world";
    std::string _useless = "hello";

    karu::sstable::SSTable sstable(test_dir + "/test.data", 5129512);
    auto status = sstable.InitWriterAndReader();
    OK;

    auto insert_status = sstable.Insert(_useless, value);
    if (!insert_status.ok()) {
      std::cerr << insert_status.status().message() << '\n';
    }

    EXPECT_TRUE(insert_status.ok());
    auto read_status = sstable.FindValueFromPos(sstable::EntryPosition{
        .pos_ = *insert_status,
        .value_size_ = static_cast<std::uint16_t>(value.size()),
    });
    EXPECT_TRUE(read_status.ok());
    EXPECT_EQ(value, *read_status);
  });
}

TEST(KaruTest, SSTableCreated) {
  test_wrapper([](const std::string& test_dir) {
    karu::DB db(test_dir);

    int sstable_count = 0;
    for (const auto &iter : std::filesystem::directory_iterator(test_dir)) {
      if (iter.path().extension() == ".data") {
        sstable_count++;
      }
    }

    EXPECT_EQ(1, sstable_count);
  });
}

TEST(KaruTest, HintFileStartup) {
  test_wrapper([](const std::string& test_dir) {
    // this is pretty much same as the persistance test, but we test parsing
    // hint files.
    auto keys = generate_random_keys(10, 5);
    karu::DBConfig conf{
        .hint_files_ = true,
        .database_directory_ = test_dir,
    };
    {
      karu::DB db(conf);

      for (const auto &k : keys) {
        auto status = db.Insert(k, k);
        OK;
      }

      auto status = db.FlushMemoryTable();
      OK;
    }
    {
      karu::DB db(conf);

      for (const auto &k : keys) {
        auto status = db.Get(k);
        OK;
        EXPECT_EQ(*status, k);
      }
    }
  });
}
