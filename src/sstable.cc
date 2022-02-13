#include "sstable.h"

#include <absl/status/status.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <fstream>

#include "encoder.h"
#include "file_writer.h"

namespace karu {
namespace sstable {

#define STRING_TO_SPAN(str)       \
  absl::Span<const std::uint8_t>{ \
      reinterpret_cast<const std::uint8_t *>(str.data()), str.size()};

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

absl::StatusOr<std::string> SSTable::FindValueFromPos(
    const EntryPosition &pos) noexcept {
  std::unique_ptr<std::uint8_t[]> value_buffer(
      new std::uint8_t[pos.value_size_]);

  auto status =
      reader_->ReadAt(pos.pos_, {value_buffer.get(), pos.value_size_});
  if (!status.ok()) {
    return status.status();
  }

  if (*status != pos.value_size_) {
    return absl::InternalError("read wrong amount of bytes from file.");
  }
  std::string result = reinterpret_cast<char *>(value_buffer.get());

  return result;
}

absl::StatusOr<std::string> SSTable::Find(const std::string &key) noexcept {
  if (!bloom_.contains(key.c_str(), key.size())) {
    return absl::NotFoundError("could not find key in bloom map.");
  }

  auto offset_it = offset_map_.find(key);
  if (offset_it == offset_map_.end()) {
    return absl::NotFoundError("could not find offset for key");
  }

  return FindValueFromPos(offset_it->second);
}

absl::Status SSTable::BuildFromBTree(
    const absl::btree_map<std::string, std::string> &btree) noexcept {
  if (write_ == nullptr) {
    return absl::InternalError("file writen has been initialized");
  }

  for (const auto &entry : btree) {
    absl::Span<const std::uint8_t> key_span = STRING_TO_SPAN(entry.first);
    absl::Span<const std::uint8_t> value_span = STRING_TO_SPAN(entry.second);

    if (key_span.size() == 0 || key_span.size() > 0xFF) {
      return absl::InternalError("invalid key length");
    }

    if (value_span.size() >= encoder::kTombstone) {
      return absl::InternalError("invalid value length");
    }

    auto key_len = static_cast<std::uint8_t>(key_span.size());
    auto value_len = static_cast<std::uint16_t>(value_span.size());
    std::uint32_t buffer_size = encoder::kFullHeader + key_len + value_len;
    std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[buffer_size]);

    encoder::EntryHeader header(buffer.get());
    header.SetKeyLength(key_len);
    header.SetValueLength(value_len);

    std::memcpy(&buffer[encoder::kFullHeader], key_span.data(), key_len);
    std::memcpy(&buffer[encoder::kFullHeader + key_span.size()],
                value_span.data(), value_len);

    auto status = write_->Append({buffer.get(), buffer_size});
    if (!status.ok()) {
      return status.status();
    }

    bloom_.add(entry.first.c_str(), entry.first.size());
    auto offset = *status;
    offset_map_[entry.first] =
        EntryPosition{.pos_ = offset + encoder::kFullHeader + key_len,
                      .value_size_ = value_len};
  }

  write_->Sync();  // we don't need to sync after every turn
  return absl::OkStatus();
}

absl::Status SSTable::PopulateFromFile() noexcept {
  if (reader_ == nullptr) {
    return absl::InternalError("reader is nullptr when reading.");
  }

  std::ifstream datafile(fname_, std::ios::binary | std::ios::in);
  if (!datafile) {
    return absl::InternalError("could not initialize data stream.");
  }

  std::uint64_t starting_offset = 0;
  while (true) {
    std::uint8_t header[encoder::kFullHeader]{};
    datafile.read(reinterpret_cast<char *>(header), encoder::kFullHeader);
    if (datafile.eof()) {
      break;
    }

    if (datafile.fail()) {
      return absl::InternalError("failed reading from file stream.");
    }

    encoder::EntryHeader entry_header(header);
    auto klen = entry_header.KeyLength();
    auto vlen = entry_header.ValueLength();

    std::unique_ptr<std::uint8_t[]> key_buffer(new std::uint8_t[klen]);
    datafile.read(reinterpret_cast<char *>(key_buffer.get()), klen);
    if (datafile.eof()) {
      break;
    }

    if (datafile.fail()) {
      return absl::InternalError("failed reading from file stream.");
    }
    datafile.ignore(vlen);

    std::string result = reinterpret_cast<char *>(key_buffer.get());
    bloom_.add(result.c_str(), result.size());
    offset_map_[result] =
        EntryPosition{.pos_ = static_cast<uint32_t>(
                          starting_offset + encoder::kFullHeader + klen),
                      .value_size_ = vlen};
    starting_offset += encoder::kFullHeader + klen + vlen;
  }

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<SSTable>> ParseSSTableFromFile(
    const std::string &fname) noexcept {
  auto sstable = std::make_unique<SSTable>(fname);
  auto status =
      sstable->InitOnlyReader();  // we don't care about the file writer.
  if (!status.ok()) {
    return status;
  }

  status = sstable->PopulateFromFile();
  if (!status.ok()) {
    return status;
  }

  return sstable;
}
}  // namespace sstable
}  // namespace karu
