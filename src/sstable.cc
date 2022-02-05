#include "sstable.h"

#include <absl/status/status.h>

#include "encoder.h"
#include "file_writer.h"
#include "util.h"

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

  auto writer = io::OpenFileWriter(fname_);
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

absl::StatusOr<std::string> SSTable::Find(const std::string &key) noexcept {
  if (!offset_map_.contains(key)) {
    return absl::NotFoundError("could not find key");
  }

  std::uint64_t starting_offset = offset_map_[key];
  std::unique_ptr<std::uint8_t> header_buffer(
      new std::uint8_t[encoder::kFullHeader]);
  auto status = reader_->ReadAt(starting_offset,
                                {header_buffer.get(), encoder::kFullHeader});
  if (!status.ok()) {
    return status.status();
  }

  if (*status != encoder::kFullHeader) {
    return absl::InternalError("read wrong amount of bytes");
  }

  // get value and key lenghts
  std::uint8_t key_length = header_buffer.get()[0];
  std::uint16_t value_length =
      absl::little_endian::Load16(&header_buffer.get()[encoder::kKeyByteCount]);

  std::unique_ptr<std::uint8_t> value_buffer(new std::uint8_t[value_length]);
  std::string result = reinterpret_cast<char *>(value_buffer.get());

  return result;
}

absl::Status SSTable::BuildFromBTree(
    const absl::btree_map<std::string, std::string> &btree) noexcept {
  if (write_ == nullptr) {
    return absl::InternalError("file writen has been initialized");
  }

  for (const auto &[key, value] : btree) {
    auto key_span = util::StringToSpan(key);
    auto value_span = util::StringToSpan(value);

    auto klen = static_cast<std::uint8_t>(key.size());
    auto vlen = static_cast<std::uint16_t>(value.size());
    std::uint32_t buffer_size = encoder::kFullHeader + klen + vlen;

    std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[buffer_size]);
    encoder::EntryHeader header(buffer.get());
    header.SetKeyLength(klen);
    header.SetValueLength(vlen);

    std::memcpy(&buffer[encoder::kFullHeader], key.data(), klen);
    std::memcpy(&buffer[encoder::kFullHeader + klen], value.data(), klen);

    auto status = write_->Append({buffer.get(), buffer_size});
    if (!status.ok()) {
      return status.status();
    }

    std::uint64_t offset = *status;
    offset_map_[key] = offset;
  }

  write_->Sync();  // we don't need to sync after every turn
  return absl::OkStatus();
}
}  // namespace sstable
}  // namespace karu
