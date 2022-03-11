#ifndef _KARU_H
#define _KARU_H

#include <cstdint>
#include <string>
#include <unordered_map>

#include "../third_party/parallel_hashmap/phmap.h"
#include "absl/container/btree_map.h"
#include "absl/strings/string_view.h"
#include "sstable.h"
#include "types.h"

namespace karu {

constexpr const char *sstable_file_suffix = ".data";
constexpr const char *hint_file_suffix = ".hnt";
constexpr const char *log_file_suffix = ".log";

struct DBConfig {
  bool hint_files_;  // the option to write into hint files. This basically
                     // improves write performance a little bit, as we have to
                     // do one less file write. The downside is that to startup
                     // the application we need to parse the whole datafiles,
                     // which will take a lot more time compared to just parsing
                     // hint files.
  std::string database_directory_;
};

class DB {
 public:
  explicit DB(absl::string_view directory);
  explicit DB(const DBConfig &conf);
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
  absl::Status ParseHintFiles() noexcept;

 private:
  // we hold memtables which we have not yet written to disk in the
  // memtable_list
  std::string database_directory_;
  std::unique_ptr<sstable::SSTable> current_sstable_ = nullptr;

  phmap::parallel_flat_hash_map<std::string, DatabaseEntry> index_;
  phmap::node_hash_map<file_id_t, std::unique_ptr<sstable::SSTable>> datafiles_;

  absl::Mutex sstable_mutex_;
  absl::Mutex index_mutex_;
};
}  // namespace karu

#endif
