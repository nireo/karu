#include "file_writer.h"

namespace karu {
namespace io {
absl::StatusOr<std::uint64_t> FileWriter::Append(
    absl::Span<const std::uint64_t> src) noexcept {
  file_.write(reinterpret_cast<const char *>(src.data()), src.size());
  if (file_.fail()) {
    return absl::InternalError("file write failed.");
  }

  std::uint64_t offset = offset_;  // where it starts.
  offset += src.size();

  return offset;
}
void FileWriter::Sync() noexcept { file_.flush(); }
}  // namespace io
}  // namespace karu
