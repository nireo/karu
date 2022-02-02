#include "memory_table.h"

#include <chrono>

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
}  // namespace memory_table
}  // namespace karu
