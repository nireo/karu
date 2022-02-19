#ifndef _KARU_TYPES_H
#define _KARU_TYPES_H

#include <cstdint>

namespace karu {
using file_id_t = std::int64_t;

struct DatabaseEntry {
  file_id_t file_id_;
  std::uint32_t pos_;
  std::uint16_t value_size_;
};
};  // namespace karu

#endif
