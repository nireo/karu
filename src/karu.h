#ifndef _KARU_H
#define _KARU_H

#include <cstdint>
#include <string>
#include <unordered_map>

#include "../third_party/parallel_hashmap/phmap.h"
#include "absl/container/btree_map.h"
#include "absl/strings/string_view.h"
#include "memory_table.h"
#include "sstable.h"

namespace karu {

using file_id_t = std::int64_t;
constexpr const char *sstable_file_suffix = ".data";
constexpr const char *hint_file_suffix = ".hnt";
constexpr const char *log_file_suffix = ".log";

struct DatabaseEntry {
  explicit DatabaseEntry(file_id_t file_id, std::uint32_t pos,
                         std::uint16_t value_size)
      : file_id_(file_id), pos_(pos), value_size_(value_size) {}
  file_id_t file_id_;
  std::uint32_t pos_;
  std::uint16_t value_size_;
};

class DB {
 public:
  explicit DB(absl::string_view directory);
  DB &operator=(const DB &) = delete;
  DB(const DB &) = delete;

  absl::Status InitializeSSTables() noexcept;
  absl::Status InitializeHints() noexcept;
  absl::Status FlushMemoryTable() noexcept;
  absl::Status Insert(const std::string &key,
                      const std::string &value) noexcept;  // string_view?
  absl::StatusOr<std::string> Get(
      const std::string &key) noexcept;  // string_view?
  absl::Status Delete(const std::string &key) noexcept;
  std::vector<std::unique_ptr<sstable::SSTable>> sstable_list_;
  absl::Status ParseHintFiles() noexcept;

 private:
  // we hold memtables which we have not yet written to disk in the
  // memtable_list
  std::vector<std::unique_ptr<memtable::Memtable>> memtable_list_;
  std::unique_ptr<memtable::Memtable> current_memtable_ = nullptr;
  std::string database_directory_;
  phmap::parallel_flat_hash_map<std::string, DatabaseEntry> index_;

  absl::Mutex sstable_mutex_;
  absl::Mutex index_mutex_;
  absl::Mutex memtable_mutex_;
  absl::Mutex memtable_list_mutex_;
};
}  // namespace karu

#endif
