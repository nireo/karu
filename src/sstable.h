#ifndef _KARU_SSTABLE_H
#define _KARU_SSTABLE_H

#include <absl/strings/string_view.h>

#include <cstddef>
#include <map>
#include <string>

#include "../third_party/parallel_hashmap/phmap.h"
#include "absl/container/btree_map.h"
#include "bloom.h"
#include "file_io.h"
#include "memory_table.h"
#include "types.h"

namespace karu {
namespace sstable {

struct EntryPosition {
  std::uint32_t pos_;
  std::uint16_t value_size_;
};

class SSTable {
 public:
  explicit SSTable(const std::string& fname)
      : fname_(fname),
        id_(0),
        reader_(nullptr),
        write_(nullptr),
        bloom_(bloom::BloomFilter(30000, 13)){};
  explicit SSTable(const std::string& fname, std::int64_t id)
      : fname_(fname),
        id_(id),
        reader_(nullptr),
        write_(nullptr),
        bloom_(bloom::BloomFilter(30000, 13)){};
  explicit SSTable(std::map<std::string, EntryPosition>&& map,
                   const std::string& path);

  SSTable& operator=(const SSTable&) = delete;
  SSTable(const SSTable&) = delete;

  absl::StatusOr<std::uint32_t> Insert(const std::string& key,
                                       const std::string& value) noexcept;

  absl::Status PopulateFromFile() noexcept;
  absl::Status InitWriterAndReader() noexcept;
  absl::Status InitOnlyReader() noexcept;
  absl::StatusOr<std::string> Get(absl::string_view key) noexcept;
  absl::Status AddEntriesToIndex(
      phmap::parallel_flat_hash_map<std::string, karu::DatabaseEntry>&
          index) noexcept;

  absl::Status BuildFromBTree(
      const absl::btree_map<std::string, std::string>& btree) noexcept;
  absl::StatusOr<std::string> Find(const std::string& key) noexcept;
  absl::StatusOr<std::string> FindValueFromPos(
      const EntryPosition& pos) noexcept;
  std::map<std::string, EntryPosition> offset_map_;
  absl::StatusOr<std::string> Find(std::uint16_t value_size,
                                   std::uint32_t pos) noexcept;
  std::uint32_t Size() const noexcept { return size_; }
  std::int64_t ID() const noexcept { return id_; }

 private:
  std::string fname_;
  absl::Mutex mutex_;
  bloom::BloomFilter bloom_;
  std::int64_t id_;

  std::uint32_t size_ = 0;
  std::unique_ptr<io::FileReader> reader_ = nullptr;
  // this is only used when creating the sstable. When loading files after
  // reopening database we just initialize the reader_ field.
  std::unique_ptr<io::FileWriter> write_ = nullptr;
};

absl::StatusOr<std::unique_ptr<SSTable>> ParseSSTableFromFile(
    const std::string& key) noexcept;

absl::StatusOr<std::unique_ptr<SSTable>> CreateSSTableFromMemtable(
    const memtable::Memtable& memtable,
    const std::string& database_directory) noexcept;
}  // namespace sstable
}  // namespace karu

#endif
