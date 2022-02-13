#ifndef _KARU_SSTABLE_H
#define _KARU_SSTABLE_H

#include <absl/strings/string_view.h>

#include <cstddef>
#include <map>
#include <string>

#include "absl/container/btree_map.h"
#include "bloom.h"
#include "file_writer.h"

namespace karu {
namespace sstable {

struct EntryPosition {
  std::uint64_t pos_;
  std::uint16_t value_size_;
};

class SSTable {
 public:
  SSTable(const std::string& fname)
      : fname_(fname),
        reader_(nullptr),
        write_(nullptr),
        bloom_(bloom::BloomFilter(30000, 13)){};

  SSTable& operator=(const SSTable&) = delete;
  SSTable(const SSTable&) = delete;

  absl::Status PopulateFromFile() noexcept;
  absl::Status InitWriterAndReader() noexcept;
  absl::Status InitOnlyReader() noexcept;
  absl::StatusOr<std::string> Get(absl::string_view key) noexcept;
  absl::Status BuildFromBTree(
      const absl::btree_map<std::string, std::string>& btree) noexcept;
  absl::StatusOr<std::string> Find(const std::string& key) noexcept;
  absl::StatusOr<std::string> FindValueFromPos(
      const EntryPosition& pos) noexcept;
  std::map<std::string, EntryPosition> offset_map_;

 private:
  std::string fname_;
  absl::Mutex mutex_;
  bloom::BloomFilter bloom_;

  std::unique_ptr<io::FileReader> reader_ = nullptr;
  // this is only used when creating the sstable. When loading files after
  // reopening database we just initialize the reader_ field.
  std::unique_ptr<io::FileWriter> write_ = nullptr;
};

absl::StatusOr<std::unique_ptr<SSTable>> ParseSSTableFromFile(
    const std::string& key) noexcept;
}  // namespace sstable
}  // namespace karu

#endif
