#include <chrono>
#include <iostream>
#include <random>
#include <string>

#include "karu.h"

static std::string gen_random_str(size_t size) {
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

static std::vector<std::string> generate_random_keys(size_t key_count = 10000,
                                                     size_t key_size = 10) {
  std::vector<std::string> keys;
  keys.resize(key_count);

  for (std::string &v : keys) {
    v = gen_random_str(key_size);
  }
  return keys;
}

static std::vector<std::pair<std::string, std::string>> generate_random_pairs(
    size_t pair_count = 10000, size_t str_size = 10) {
  std::vector<std::pair<std::string, std::string>> keys(pair_count);
  for (auto &[key, value] : keys) {
    key = gen_random_str(str_size);
    value = gen_random_str(str_size);
  }

  return keys;
}

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "need 3 arguments\n";
    std::exit(1);
  }

  int str_lengths = std::stoi(argv[1]);
  int iterations = std::stoi(argv[2]);

  auto keys = generate_random_pairs(iterations, str_lengths);
  auto insert_time_start = std::chrono::high_resolution_clock::now();
  karu::DB db("./test");
  for (const auto &k : keys) {
    db.Insert(k.first, k.second);
  }
  auto insert_time_end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double, std::milli> write_double =
      insert_time_end - insert_time_start;
  std::cout << "reads took: " << write_double.count() << '\n';

  auto read_time_start = std::chrono::high_resolution_clock::now();
  for (const auto &k : keys) {
    db.Get(k.first);
  }
  auto read_time_end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double, std::milli> read_double =
      insert_time_end - insert_time_start;
  std::cout << "reads took: " << read_double.count() << '\n';
}
