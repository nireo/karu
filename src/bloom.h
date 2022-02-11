#ifndef _KARU_BLOOM_H
#define _KARU_BLOOM_H

#include <cstdint>
#include <functional>
#include <vector>

namespace karu {
struct BloomFilter {
  BloomFilter(std::uint64_t size, uint8_t hash_count);
  void add(const char *data, std::size_t len) noexcept;
  bool contains(const char *data, std::size_t len) const noexcept;

 private:
  std::uint8_t hash_count_;
  std::vector<bool> bits_;
};
}  // namespace karu

#endif
