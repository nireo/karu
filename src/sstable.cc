#include "sstable.h"

#include <absl/status/status.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <filesystem>
#include <fstream>
#include <memory>

#include "encoder.h"
#include "file_io.h"
#include "memory_table.h"

namespace karu {
namespace sstable {

#define STRING_TO_SPAN(str)       \
  absl::Span<const std::uint8_t>{ \
      reinterpret_cast<const std::uint8_t *>(str.data()), str.size()};

SSTable::SSTable(std::map<std::string, EntryPosition> &&map,
                 const std::string &path)
    : bloom_(bloom::BloomFilter(30000, 13)),
      write_(nullptr),
      reader_(nullptr),
      fname_(path),
      offset_map_(std::move(map)) {
  // only initialize the reader since we are not going to be writing to this
  // file.
  if (auto status = InitOnlyReader(); !status.ok()) {
    std::cerr << "error initializing sstable reader.\n";
    reader_ = nullptr;
  }
}

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

absl::StatusOr<std::uint32_t> SSTable::Insert(
    const std::string &key, const std::string &value) noexcept {
  if (write_ == nullptr) {
    return absl::InternalError("table writer is nullptr when trying to write");
  }

  auto key_span = STRING_TO_SPAN(key);
  auto value_span = STRING_TO_SPAN(value);

  auto key_len = static_cast<std::uint16_t>(key_span.size());
  auto value_len = static_cast<std::uint16_t>(value_span.size());
  std::uint32_t buffer_size = encoder::kFullHeader + key_len + value_len;
  std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[buffer_size]);
  absl::little_endian::Store16(&buffer.get()[0], key_len);
  absl::little_endian::Store16(&buffer.get()[encoder::kKeyByteCount],
                               value_len);

  std::memcpy(&buffer[encoder::kFullHeader], key_span.data(), key_span.size());
  std::memcpy(&buffer[encoder::kFullHeader + key_span.size()],
              value_span.data(), value_len);

  auto status = write_->Append({buffer.get(), buffer_size});
  if (!status.ok()) {
    return status.status();
  }

  // write changes to disk
  write_->Sync();
  return *status + encoder::kFullHeader + key_len; // where the value starts in the file.
}

// This is exactly same as the Find function but with arguments such that we
// don't need to construct a separate file struct.
absl::StatusOr<std::string> SSTable::Find(std::uint16_t value_size,
                                          std::uint32_t pos) noexcept {
  std::unique_ptr<std::uint8_t[]> value_buffer(new std::uint8_t[value_size]);
  auto status = reader_->ReadAt(pos, {value_buffer.get(), value_size});
  if (!status.ok()) {
    return status.status();
  }

  if (*status != value_size) {
    return absl::InternalError("read wrong amount of bytes from file.");
  }

  std::string result = reinterpret_cast<char *>(value_buffer.get());
  result.resize(value_size);

  return result;
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
  result.resize(pos.value_size_);

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

    auto key_len = static_cast<std::uint16_t>(key_span.size());
    auto value_len = static_cast<std::uint16_t>(value_span.size());
    std::uint32_t buffer_size = encoder::kFullHeader + key_len + value_len;
    std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[buffer_size]);
    absl::little_endian::Store16(&buffer.get()[0], key_len);
    absl::little_endian::Store16(&buffer.get()[encoder::kKeyByteCount],
                                 value_len);

    std::memcpy(&buffer[encoder::kFullHeader], key_span.data(),
                key_span.size());
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

absl::StatusOr<std::unique_ptr<SSTable>> CreateSSTableFromMemtable(
    const memtable::Memtable &memtable,
    const std::string &database_directory) noexcept {
  std::string sstable_path =
      database_directory + '/' + std::to_string(memtable.ID()) + ".data";
  std::unique_ptr<sstable::SSTable> sstable =
      std::make_unique<SSTable>(sstable_path);

  if (auto status = sstable->BuildFromBTree(memtable.map_); !status.ok()) {
    std::cerr << "error writing memtable(" << memtable.ID() << ") to disk.\n";
    return absl::InternalError("error writing memtable to disk.");
  }
  std::filesystem::remove(memtable.log_path_);

  return sstable;  // moves out the sstable
}

absl::Status SSTable::PopulateFromFile() noexcept {
  if (reader_ == nullptr) {
    return absl::InternalError("reader is nullptr when reading.");
  }

  struct ::stat fileStat;
  if (::stat(fname_.c_str(), &fileStat) == -1) {
    return absl::InternalError("could not get filesize");
  }
  uint32_t file_size = static_cast<uint32_t>(fileStat.st_size);

  std::uint32_t starting_offset = 0;
  while (starting_offset <= file_size) {
    std::unique_ptr<std::uint8_t[]> header_buffer(
        new std::uint8_t[encoder::kFullHeader]);
    auto status = reader_->ReadAt(starting_offset,
                                  {header_buffer.get(), encoder::kFullHeader});
    if (!status.ok()) break;
    if (*status == 0) {
      break;
    }

    std::uint16_t key_length =
        absl::little_endian::Load16(&header_buffer.get()[0]);
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
    result.resize(key_length);
    starting_offset += encoder::kFullHeader + key_length;
    offset_map_[result] = EntryPosition{
        .pos_ = starting_offset,
        .value_size_ = value_length,
    };
    starting_offset += value_length;
    bloom_.add(result.c_str(), result.size());
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
