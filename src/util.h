#ifndef _KARU_UTIL_H
#define _KARU_UTIL_H

#include "absl/status/status.h"

namespace karu {
namespace util {
absl::Span<const std::uint8_t> StringToSpan(const std::string &s) {
  return {reinterpret_cast<const std::uint8_t *>(s.data()), s.size()};
}
}  // namespace util
}  // namespace karu

#endif
