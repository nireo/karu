#include "utils.h"

#include <cctype>
#include <filesystem>

#include "karu.h"

namespace karu {
namespace utils {
std::vector<std::string> files_with_extension(
    std::string_view ext, std::string_view directory) noexcept {
  std::vector<std::string> result;
  for (const auto &entry : std::filesystem::directory_iterator(directory)) {
    if (entry.path().extension() == ext) {
      result.push_back(entry.path());
    }
  }

  return result;
}

// this function basically finds the last number appearing in a string. We need
// to find the last number, since the database directory or any other directory
// could contain numbers.
absl::StatusOr<file_id_t> parse_file_id(std::string_view path) noexcept {
  file_id_t last = 0;
  file_id_t id = 0;

  // TODO: not so scuffed in the future plese
  for (size_t i = 0; i < path.size(); ++i) {
    if (std::isdigit(path[i])) {
      id += path[i] - '0';  // add the number and a zero the rhs of the number.
      id *= 10;

      last = id;
    } else {
      id = 0;
    }
  }

  return last;
}

file_id_t generate_file_id() noexcept {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}
}  // namespace utils
}  // namespace karu
