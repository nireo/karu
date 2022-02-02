#ifndef _KARU_MEMORY_TABLE_H
#define _KARU_MEMORY_TABLE_H

#include <fstream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "absl/container/btree_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "file_writer.h"

namespace karu {
namespace memory_table {
constexpr std::uint64_t kMaxMemSize = 8 * 1024 * 1024;  // 8 mb
class MemoryTable {
 public:
  explicit MemoryTable(const std::string &dir);
  MemoryTable(const MemoryTable &) = delete;
  MemoryTable &operator=(const MemoryTable &) = delete;
  [[nodiscard]] absl::Status Insert(const std::string &key,
                                    const std::string &value) noexcept;
  [[nodiscard]] absl::StatusOr<std::string> Get(
      const std::string &key) const noexcept;
  [[nodiscard]] absl::Status AppendToLog(const std::string &key,
                                         const std::string &value) noexcept;

 private:
  std::string log_path_;
  absl::btree_map<std::string, std::string> map_;
  std::unique_ptr<io::FileWriter> fw_;
  absl::Mutex file_lock_;
};
}  // namespace memory_table
}  // namespace karu

#endif
