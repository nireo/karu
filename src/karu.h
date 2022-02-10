#ifndef _KARU_H
#define _KARU_H

#include <cstdint>
#include <string>
#include <unordered_map>

#include "absl/container/btree_map.h"
#include "absl/strings/string_view.h"
#include "memory_table.h"
#include "sstable.h"

namespace karu {
using file_id_t = std::int64_t;
class DB {
  DB(absl::string_view directory);

  // TODO: Implement these functions
  absl::Status InitializeSSTables() noexcept;
  absl::Status FlushMemoryTable() noexcept;
  absl::Status Insert(const std::string &key,
                      const std::string &value) noexcept;  // string_view?
  absl::StatusOr<std::string> Get(
      const std::string &key) noexcept;  // string_view?
  absl::Status Delete(const std::string &key) noexcept;
  absl::Status Shutdown() noexcept;

 private:
  // we hold memtables which we have not yet written to disk in the memtable_list
  std::vector<std::unique_ptr<memtable::Memtable>> memtable_list_;
  std::unique_ptr<memtable::Memtable> current_memtable_ = nullptr;
  absl::btree_map<file_id_t, std::unique_ptr<sstable::SSTable>> sstable_map_;
  std::string database_directory_;

  absl::Mutex sstable_mutex_;
  absl::Mutex memtable_mutex_;
  absl::Mutex memtable_list_mutex_;
};
}  // namespace karu

#endif
