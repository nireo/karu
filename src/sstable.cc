#include "sstable.h"

#include "file_writer.h"

namespace karu {
namespace sstable {
absl::Status SSTable::InitWriterAndReader() noexcept {
  // this is only called when we are creating a new sstable, such that we don't
  // need the file size. After creating an sstable, we still need to take care
  // of the reader, that is why we initialize it as well.
  auto reader = io::OpenFileReader(fname_);
  if (!reader.ok()) {
    return reader.status();
  }
  reader_ = std::move(reader.value());

  auto writer = io::CreateFileWriter(fname_);
  if (!writer.ok()) {
    return writer.status();
  }
  write_ = std::move(writer.value());

  return absl::OkStatus();
}

absl::Status SSTable::InitOnlyReader() noexcept {
  auto reader = io::OpenFileReader(fname_);
  if (!reader.ok()) {
    return reader.status();
  }
  reader_ = std::move(reader.value());

  return absl::OkStatus();
}
}  // namespace sstable
}  // namespace karu
