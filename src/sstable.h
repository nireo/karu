#ifndef _KARU_SSTABLE_H
#define _KARU_SSTABLE_H

#include <cstddef>
#include <string>

#include "absl/container/btree_map.h"
#include "file_writer.h"

namespace karu {
namespace sstable {
class SSTable {
  SSTable(const std::string& fname)
      : fname_(fname), size_(0), reader_(nullptr), write_(nullptr){};
  SSTable& operator=(const SSTable&) = delete;
  SSTable(const SSTable&) = delete;

  absl::Status InitWriterAndReader() noexcept;
  absl::Status InitOnlyReader() noexcept;

 private:
  std::string fname_;
  std::uint64_t size_;
  absl::btree_map<std::string, std::uint64_t> offset_map_;
  absl::Mutex mutex_;

  std::unique_ptr<io::FileReader> reader_;
  // this is only used when creating the sstable. When loading files after
  // reopening database we just initialize the reader_ field.
  std::unique_ptr<io::FileWriter> write_;
};
}  // namespace sstable
}  // namespace karu

#endif
