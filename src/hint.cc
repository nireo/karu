#include "hint.h"

#include <absl/status/status.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <cstring>
#include <ftream>
#include <memory>

#include "encoder.h"
#include "file_writer.h"
#include "karu.h"
#include "sstable.h"

namespace karu {
namespace hint {

#define STRING_TO_SPAN(str)       \
  absl::Span<const std::uint8_t>{ \
      reinterpret_cast<const std::uint8_t *>(str.data()), str.size()};

HintFile::HintFile(const std::string &path) {
  auto status = io::OpenFileWriter(path_);
  if (status.ok()) {
    file_writer_ = std::move(*status);
  }

  path_ = path;
}

absl::Status HintFile::WriteHint(const std::string &key,
                                 std::uint16_t value_size,
                                 std::uint32_t pos) noexcept {
  if (file_writer_ == nullptr) {
    return absl::InternalError("hint file writer is a nullptr.");
  }

  auto key_span = STRING_TO_SPAN(key);

  // we have already made sure that they key is not too long.
  auto klen = static_cast<std::uint16_t>(key_span.size());
  std::uint32_t buffer_size = encoder::kHintHeader + klen;
  std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[buffer_size]);

  // set header information
  encoder::HintHeader header(buffer.get());
  header.SetPos(pos);
  header.SetKeyLength(klen);
  header.SetValueLength(value_size);

  // copy the key data into the buffer
  std::memcpy(&buffer[encoder::kFullHeader], key_span.data(), key_span.size());

  // write to the hint file
  if (auto status = file_writer_->Append({buffer.get(), buffer_size});
      !status.ok()) {
    return status.status();
  }
  file_writer_->Sync();  // write changes to disk

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<sstable::SSTable>>
HintFile::BuildSSTableHintFile() noexcept {
  if (file_reader_ == nullptr) {
    return absl::InternalError("hint file reader was nullptr.");
  }

  std::unique_ptr<sstable::SSTable> sstable =
      std::make_unique<sstable::SSTable>("TODO");

  if (auto status = sstable->InitOnlyReader(); !status.ok()) {
    return status;
  }

  struct ::stat fileStat;
  if (::stat(path_.c_str(), &fileStat) == -1) {
    return absl::InternalError("could not get filesize");
  }
  uint64_t file_size = static_cast<uint64_t>(fileStat.st_size);

  std::uint32_t offset = 0;
  while (offset < file_size) {
    std::unique_ptr<std::uint8_t[]> header_buffer(
        new std::uint8_t[encoder::kHintHeader]);
    auto status = file_reader_->ReadAt(
        offset, {header_buffer.get(), encoder::kHintHeader});
    if (!status.ok()) {
      return status.status();
    }

    if (*status != encoder::kHintHeader) {
      return absl::InternalError("read wrong amount of bytes");
    }

    encoder::HintHeader hint_header(header_buffer.get());
    std::uint8_t ksize = hint_header.KeyLength();
    std::uint16_t vsize = hint_header.ValueLength();
    std::uint32_t pos = hint_header.ValuePos();
  }

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<sstable::SSTable>> ParseHintFile(
    const std::string &hint_path) noexcept {
  std::ifstream file(hint_path, std::ios::binary | std::ios::in);
  if (!file) {
    return absl::InternalError(
        "could not open file stream to hint file: " + hint_path + ".\n");
  }

  std::map<std::string, sstable::EntryPosition> result_;
  std::uint32_t offset = 0;

  while (true) {
  }

  return std::make_unique<sstable::SSTable>(std::move(result_));
}
}  // namespace hint
}  // namespace karu
