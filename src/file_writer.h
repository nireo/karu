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

  FileWriter() = default;
  FileWriter(const FileWriter &) = delete;
  FileWriter &operator=(const FileWriter &) = delete;

 private:
  std::ofstream file_;
  uint64_t offset_;
  std::string filename_;
};

class FileReader {
 public:
  FileReader(const int fd) : fd_(fd) {}
  FileReader(const FileReader &) = delete;
  FileReader &operator=(const FileReader &) = delete;
  ~FileReader() { ::close(fd_); }

  [[nodiscard]] absl::StatusOr<std::uint64_t> ReadAt(
      std::uint64_t offset, absl::Span<std::uint8_t> dst) noexcept;

 private:
  const int fd_;
};

absl::StatusOr<std::unique_ptr<FileWriter>> CreateFileWriter(
    const std::string &fname) noexcept;
absl::StatusOr<std::unique_ptr<FileReader>> OpenFileReader(
    const std::string &fname) noexcept;
}  // namespace io
}  // namespace karu

#endif
