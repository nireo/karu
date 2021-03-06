#ifndef _KARU_BLOOM_H
#define _KARU_BLOOM_H

#include <cstdint>
#include <functional>
#include <vector>

namespace karu {
namespace bloom {
struct BloomFilter {
  BloomFilter(std::uint64_t size, uint8_t hash_count);
  void add(const char *data, std::size_t len) noexcept;
  bool contains(const char *data, std::size_t len) const noexcept;

  BloomFilter &operator=(const BloomFilter &) = delete;
  BloomFilter(const BloomFilter &) = delete;

 private:
  std::uint8_t hash_count_;
  std::vector<bool> bits_;
};
}  // namespace bloom
}  // namespace karu

#endif
