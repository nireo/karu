#ifndef _KARU_HINT_H
#define _KARU_HINT_H

#include <absl/status/status.h>

#include <string>

#include "../third_party/parallel_hashmap/phmap.h"
#include "file_io.h"
#include "types.h"

namespace karu {
namespace hint {
class HintFile {
 public:
  explicit HintFile(std::string path);
  absl::Status WriteHint(const std::string &key, std::uint16_t value_size,
                         std::uint32_t pos) noexcept;
  HintFile &operator=(const HintFile &) = delete;
  HintFile(const HintFile &) = delete;

 private:
  std::string path_;
  std::unique_ptr<io::FileWriter> file_writer_ = nullptr;
  std::unique_ptr<io::FileReader> file_reader_ = nullptr;
};

absl::Status ParseHintFile(
    const std::string &path, file_id_t sstable_id,
    phmap::parallel_flat_hash_map<std::string, DatabaseEntry> &index) noexcept;
}  // namespace hint
}  // namespace karu

#endif
