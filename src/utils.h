#ifndef _KARU_UTILS_H
#define _KARU_UTILS_H

#include <absl/status/statusor.h>

#include <string>
#include <string_view>
#include <vector>

#include "karu.h"

namespace karu::utils {
std::vector<std::string> files_with_extension(
    std::string_view ext, std::string_view directory) noexcept;

absl::StatusOr<file_id_t> parse_file_id(std::string_view path) noexcept;
file_id_t generate_file_id() noexcept;
}  // namespace karu

#endif
