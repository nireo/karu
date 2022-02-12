#include "bloom.h"

#include "murmurhash3.h"

namespace karu {
namespace bloom {
BloomFilter::BloomFilter(std::uint64_t size, std::uint8_t hash_count)
    : bits_(size), hash_count_(hash_count) {}

inline std::uint64_t nth_hash(uint8_t n, uint64_t hash_a, uint64_t hash_b,
                              uint64_t filter_size) {
  return (hash_a + n * hash_b) % filter_size;
}

std::array<std::uint64_t, 2> hash(const char *data, std::size_t len) noexcept {
  std::array<std::uint64_t, 2> hash_value;
  MurmurHash3_x64_128(data, len, 0, hash_value.data());
  return hash_value;
}

void BloomFilter::add(const char *data, std::size_t len) noexcept {
  auto hashValues = hash(data, len);
  for (int n = 0; n < hash_count_; n++) {
    bits_[nth_hash(n, hashValues[0], hashValues[1], bits_.size())] = true;
  }
}

bool BloomFilter::contains(const char *data, std::size_t len) const noexcept {
  auto hashValues = hash(data, len);
  for (int n = 0; n < hash_count_; n++) {
    if (!bits_[nth_hash(n, hashValues[0], hashValues[1], bits_.size())]) {
      return false;
    }
  }

  return true;
}
}  // namespace bloom
}  // namespace karu
