#ifndef _KARU_UTILS_H
#define _KARU_UTILS_H

#include <absl/status/statusor.h>

#include <string>
#include <string_view>
#include <vector>

#include "karu.h"

namespace karu {
namespace utils {
std::vector<std::string> files_with_extension(
    std::string_view ext, std::string_view directory) noexcept;

absl::StatusOr<file_id_t> parse_file_id(std::string_view path) noexcept;
}  // namespace utils
}  // namespace karu

#endif
