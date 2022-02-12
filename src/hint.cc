#include "hint.h"

#include <cstring>

#include "encoder.h"
#include "file_writer.h"

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
  auto key_span = STRING_TO_SPAN(key);

  // we have already made sure that they key is not too long.
  auto klen = static_cast<std::uint8_t>(key_span.size());
  std::uint32_t buffer_size = encoder::kHintHeader + klen;
  std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[buffer_size]);

  // set header information
  encoder::HintHeader header(buffer.get());
  header.SetPos(pos);
  header.SetKeyLength(klen);
  header.SetValueLength(value_size);

  // copy the key data into the buffer
  std::memcpy(&buffer[encoder::kFullHeader], key_span.data(), klen);

  // write to the hint file
  if (auto status = file_writer_->Append({buffer.get(), buffer_size});
      !status.ok()) {
    return status.status();
  }
  file_writer_->Sync(); // write changes to disk

  return absl::OkStatus();
}
}  // namespace hint
}  // namespace karu
