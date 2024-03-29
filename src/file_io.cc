#include "file_io.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <cerrno>
#include <cstdint>
#include <memory>

#include "absl/status/status.h"

namespace karu::io {
absl::StatusOr<std::uint32_t> FileWriter::Append(
    absl::Span<const std::uint8_t> src) noexcept {
  file_.write(reinterpret_cast<const char *>(src.data()), src.size());
  if (file_.fail()) {
    return absl::InternalError("file write failed.");
  }

  std::uint64_t offset = offset_;  // where it starts.
  offset_ += src.size();

  return offset;
}

void FileWriter::Sync() noexcept {
  // flush the writed buffer onto the disk
  file_.flush();
}

absl::StatusOr<std::unique_ptr<FileWriter>> OpenFileWriter(
    const std::string &fname) noexcept {
  // get size of file.
  struct ::stat fileStat{};
  if (::stat(fname.c_str(), &fileStat) == -1) {
    return absl::InternalError("could not get filesize");
  }
  auto file_size = static_cast<uint64_t>(fileStat.st_size);

  std::ofstream file(
      fname, std::fstream::out | std::fstream::app | std::fstream::binary);

  if (!file.is_open()) {
    return absl::InternalError("could not open file.");
  }

  return std::make_unique<FileWriter>(
      fname, std::move(file), file_size);
}

absl::StatusOr<std::unique_ptr<FileReader>> OpenFileReader(
    const std::string &fname) noexcept {
  int fd = ::open(fname.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return absl::InternalError("could not open file.");
  }

  return std::make_unique<FileReader>(fd);
}

absl::StatusOr<std::uint64_t> FileReader::ReadAt(
    std::uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept {
  ssize_t size =
      ::pread(fd_, dst.data(), dst.size(), static_cast<off_t>(offset));
  if (size < 0) {
    return absl::InternalError("pread failed");
  }

  return size;
}
}  // namespace karu
