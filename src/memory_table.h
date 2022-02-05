#ifndef _KARU_MEMORY_TABLE_H
#define _KARU_MEMORY_TABLE_H

#include <cstdint>
#include <fstream>
#include <memory>
#include <shared_mutex>
#include <string>

#include "absl/container/btree_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "file_writer.h"

namespace karu {
namespace memtable {
constexpr std::uint64_t kMaxMemSize = 8 * 1024 * 1024;  // 8 mb
class Memtable {
 public:
  // Constructor only sets the log path and makes sure that a file exists at
  // given log_path_.
  explicit Memtable(const std::string &dir);
  Memtable(const Memtable &) = delete;
  Memtable &operator=(const Memtable &) = delete;

  [[nodiscard]] absl::Status Insert(const std::string &key,
                                    const std::string &value) noexcept;

  [[nodiscard]] absl::StatusOr<std::string> Get(
      const std::string &key) const noexcept;

  [[nodiscard]] absl::Status AppendToLog(const std::string &key_str,
                                         const std::string &value_str) noexcept;
  std::string log_path_;
  std::unique_ptr<io::FileWriter> fw_;

 private:
  absl::btree_map<std::string, std::string> map_;
  absl::Mutex file_lock_;
};

absl::StatusOr<std::unique_ptr<Memtable>> CreateMemtableWithDir(
    const std::string &dir) noexcept;
}  // namespace memtable
}  // namespace karu

#endif
