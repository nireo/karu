#ifndef _KARU_FILE_WRITER_H
#define _KARU_FILE_WRITER_H

#include <cstdint>
#include <fstream>

#include "absl/status/statusor.h"

namespace karu {
namespace io {
class FileWriter {
 public:
  ~FileWriter() { file_.close(); }
  FileWriter(const std::string &fname, std::ofstream &&file,
             std::uint64_t offset)
      : file_(std::move(file)), filename_(fname), offset_(offset) {}
  [[nodiscard]] absl::StatusOr<std::uint64_t> Append(
      absl::Span<const std::uint64_t> src) noexcept;
  void Sync() noexcept;
  std::uint64_t Size() const noexcept { return offset_; }

 private:
  std::ofstream file_;
  uint64_t offset_;
  std::string filename_;
};
}  // namespace io
}  // namespace karu

#endif
