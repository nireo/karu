#include "memory_table.h"

#include <chrono>
#include <string>

#include "file_writer.h"

namespace karu {
namespace memory_table {
MemoryTable::MemoryTable(const std::string &directory) {
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

absl::Status MemoryTable::Insert(const std::string &key,
                                 const std::string &value) noexcept {
  // insert into table
  map_.insert(key, value);

  // write into logs.

  return absl::OkStatus();
}

absl::Status MemoryTable::AppendToLog(const std::string &key,
                                      const std::string &value) noexcept {
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<MemoryTable>> CreateMemtableWithDir(
    const std::string &dir) noexcept {
  auto memtable_ptr = std::make_unique<MemoryTable>(dir);

  auto writer = io::CreateFileWriter(memtable_ptr->log_path_);
  if (!writer.ok()) {
    return writer.status();
  }

  memtable_ptr->fw_ = std::move(writer.value());
  return memtable_ptr;
}
}  // namespace memory_table
}  // namespace karu
