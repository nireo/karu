#ifndef _KARU_FILE_WRITER_H
#define _KARU_FILE_WRITER_H

#include <cstdint>
#include <fstream>
#include <utility>

#include "absl/status/statusor.h"

namespace karu::io {
class FileWriter {
 public:
  ~FileWriter() { file_.close(); }
  FileWriter(std::string fname, std::ofstream &&file,
             std::uint32_t offset)
      : file_(std::move(file)), filename_(std::move(fname)), offset_(offset) {}
  [[nodiscard]] absl::StatusOr<std::uint32_t> Append(
      absl::Span<const std::uint8_t> src) noexcept;
  void Sync() noexcept;
  std::uint32_t Size() const noexcept { return offset_; }

  FileWriter() = default;
  FileWriter(const FileWriter &) = delete;
  FileWriter &operator=(const FileWriter &) = delete;
  std::uint64_t LastWritten() const noexcept { return last_written_; };

 private:
  std::uint64_t last_written_ = 0;  // so we can easily append sizes
  std::ofstream file_;
  uint32_t offset_{};
  std::string filename_;
};

class FileReader {
 public:
  explicit FileReader(const int fd) : fd_(fd) {}
  FileReader(const FileReader &) = delete;
  FileReader &operator=(const FileReader &) = delete;
  ~FileReader() { ::close(fd_); }

  [[nodiscard]] absl::StatusOr<std::uint64_t> ReadAt(
      std::uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept;

 private:
  const int fd_;
};

absl::StatusOr<std::unique_ptr<FileWriter>> OpenFileWriter(
    const std::string &fname) noexcept;
absl::StatusOr<std::unique_ptr<FileReader>> OpenFileReader(
    const std::string &fname) noexcept;

}  // namespace karu

#endif
