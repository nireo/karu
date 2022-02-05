#include "memory_table.h"

#include <absl/status/status.h>
#include <absl/strings/internal/resize_uninitialized.h>

#include <chrono>
#include <cstring>
#include <string>

#include "encoder.h"
#include "file_writer.h"
#include "util.h"

namespace karu {
namespace memtable {
Memtable::Memtable(const std::string &directory) {
  // other functions already ensure that the directory in fact does exist.
  // get the unix timestamp
  std::time_t res = std::time(nullptr);
  int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

  // make file path
  log_path_ = directory;
  log_path_ += "/" + std::to_string(timestamp) + ".log";

  // create the file
  std::ofstream temp{log_path_};
  temp.close();
}

absl::Status Memtable::Insert(const std::string &key,
                              const std::string &value) noexcept {
  map_[key] = value;
  auto status = AppendToLog(key, value);
  if (!status.ok()) {
    return status;
  }

  return absl::OkStatus();
}

absl::Status Memtable::AppendToLog(const std::string &key_str,
                                   const std::string &value_str) noexcept {
  auto key = util::StringToSpan(key_str);
  auto value = util::StringToSpan(value_str);

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
