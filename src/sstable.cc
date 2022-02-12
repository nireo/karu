#include "sstable.h"

#include <absl/status/status.h>
#include <fcntl.h>
#include <sys/stat.h>

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

absl::StatusOr<std::string> SSTable::Find(const std::string &key) noexcept {
  auto offset_it = offset_map_.find(key);
  if (offset_it == offset_map_.end()) {
    return absl::NotFoundError("could not find offset for key");
  }

  std::uint64_t starting_offset = offset_it->second;
  std::unique_ptr<std::uint8_t[]> header_buffer(
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

  std::unique_ptr<std::uint8_t[]> value_buffer(new std::uint8_t[value_length]);
  status = reader_->ReadAt(starting_offset + encoder::kFullHeader + key_length,
                           {
                               value_buffer.get(),
                               value_length,
                           });

  if (!status.ok()) {
    return status.status();
  }
  std::string result = reinterpret_cast<char *>(value_buffer.get());

  return result;
}

absl::Status SSTable::BuildFromBTree(
    const absl::btree_map<std::string, std::string> &btree) noexcept {
  if (write_ == nullptr) {
    return absl::InternalError("file writen has been initialized");
  }

  for (const auto &[key, value] : btree) {
    auto key_span = STRING_TO_SPAN(key);
    auto value_span = STRING_TO_SPAN(value);

    auto klen = static_cast<std::uint8_t>(key_span.size());
    auto vlen = static_cast<std::uint16_t>(value_span.size());
    std::uint32_t buffer_size = encoder::kFullHeader + klen + vlen;

    std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[buffer_size]);
    encoder::EntryHeader header(buffer.get());
    header.SetKeyLength(klen);
    header.SetValueLength(vlen);

    std::memcpy(&buffer[encoder::kFullHeader], key_span.data(), klen);
    std::memcpy(&buffer[encoder::kFullHeader + klen], value.data(), vlen);

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

absl::Status SSTable::PopulateFromFile() noexcept {
  if (reader_ == nullptr) {
    return absl::InternalError("reader is nullptr when reading.");
  }

  // read file size so we know when to stop.
  struct ::stat fileStat;
  if (::stat(fname_.c_str(), &fileStat) == -1) {
    return absl::InternalError("could not get filesize");
  }
  uint64_t file_size = static_cast<uint64_t>(fileStat.st_size);

  std::uint64_t starting_offset = 0;
  while (starting_offset <= file_size) {
    std::unique_ptr<std::uint8_t> header_buffer(
        new std::uint8_t[encoder::kFullHeader]);
    auto status = reader_->ReadAt(starting_offset,
                                  {header_buffer.get(), encoder::kFullHeader});
    if (!status.ok()) {
      break;
    }

    if (*status != encoder::kFullHeader) {
      break;
    }

    // get value and key lenghts
    std::uint8_t key_length = header_buffer.get()[0];
    std::uint16_t value_length = absl::little_endian::Load16(
        &header_buffer.get()[encoder::kKeyByteCount]);

    std::unique_ptr<std::uint8_t[]> key_buffer(new std::uint8_t[key_length]);
    status = reader_->ReadAt(starting_offset + encoder::kFullHeader,
                             {
                                 key_buffer.get(),
                                 key_length,
                             });

    if (!status.ok()) {
      break;
    }

    std::string result = reinterpret_cast<char *>(key_buffer.get());
    offset_map_[result] = starting_offset;
    std::cerr << result << ':' << starting_offset << '\n';
    starting_offset += encoder::kFullHeader + key_length + value_length;
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
