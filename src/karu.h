#ifndef _KARU_H
#define _KARU_H

#include <cstdint>
#include <string>
#include <unordered_map>

#include "absl/container/btree_map.h"
#include "absl/strings/string_view.h"
#include "memory_table.h"
#include "sstable.h"

using file_id_t = std::uint64_t;
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

 private:
  std::unique_ptr<MemoryTable> current_memtable_;
  absl::btree_map<file_id_t, std::unique_ptr<SSTable>> sstable_map_;
  std::string database_directory_;

  absl::Mutex sstable_mutex_;
  absl::Mutex memtable_mutex_;
};

#endif
