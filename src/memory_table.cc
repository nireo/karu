#include "memory_table.h"

#include <absl/status/status.h>
#include <absl/strings/internal/resize_uninitialized.h>

#include <chrono>
#include <cstring>
#include <string>

#include "absl/synchronization/mutex.h"
#include "encoder.h"
#include "file_writer.h"

namespace karu {
namespace memtable {

#define STRING_TO_SPAN(str)       \
  absl::Span<const std::uint8_t>{ \
      reinterpret_cast<const std::uint8_t *>(str.data()), str.size()};

Memtable::Memtable(const std::string &directory) {
  // other functions already ensure that the directory in fact does exist.
  // get the unix timestamp
  int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

  // make file path
  log_path_ = directory;
  log_path_ += "/" + std::to_string(timestamp) + ".log";

  // create the file
  std::ofstream temp{log_path_};
  temp.close();

  auto writer = io::OpenFileWriter(log_path_);
  if (!writer.ok()) {
    exit(1);  // TODO: Make a function which can fail this
  }

  fw_ = std::move(*writer);
}

absl::Status Memtable::Insert(const std::string &key,
                              const std::string &value) noexcept {
  index_mutex_.WriterLock();
  map_[key] = value;
  index_mutex_.WriterUnlock();
  auto status = AppendToLog(key, value);
  if (!status.ok()) {
    return status;
  }

  return absl::OkStatus();
}

absl::StatusOr<std::string> Memtable::Get(const std::string &key) noexcept {
  absl::ReaderMutexLock guard(&index_mutex_);
  auto it = map_.find(key);
  if (it == map_.end()) {
    return absl::NotFoundError("couldn't find entry in memtable.");
  }

  return it->second;
}

absl::Status Memtable::AppendToLog(const std::string &key_str,
                                   const std::string &value_str) noexcept {
  absl::WriterMutexLock guard(
      &file_lock_);  // when this goes out of scope it releases the mutex
  auto key = STRING_TO_SPAN(key_str);
  auto value = STRING_TO_SPAN(value_str);

  if (key.size() == 0 || key.size() > 255) {
    return absl::InternalError("bad key length");
  }

  auto klen = static_cast<std::uint8_t>(key.size());
  auto vlen = static_cast<std::uint16_t>(value.size());
  std::uint32_t buffer_size = encoder::kFullHeader + klen + vlen;

  std::unique_ptr<std::uint8_t[]> buffer(new std::uint8_t[buffer_size]);
  encoder::EntryHeader header(buffer.get());
  // store the key.
  header.SetKeyLength(klen);
  header.SetValueLength(vlen);

  std::memcpy(&buffer[encoder::kFullHeader], key.data(), klen);
  std::memcpy(&buffer[encoder::kFullHeader + klen], value.data(), klen);

  auto status = fw_->Append({buffer.get(), buffer_size});
  if (!status.ok()) {
    return status.status();
  }

  file_size_ += buffer_size;
  fw_->Sync();  // make sure that files are written to disk.

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<Memtable>> CreateMemtableWithDir(
    const std::string &dir) noexcept {
  auto memtable_ptr = std::make_unique<Memtable>(dir);

  auto writer = io::OpenFileWriter(memtable_ptr->log_path_);
  if (!writer.ok()) {
    return writer.status();
  }

  memtable_ptr->fw_ = std::move(writer.value());
  return memtable_ptr;
}
}  // namespace memtable
}  // namespace karu
