#ifndef _KARU_HINT_H
#define _KARU_HINT_H

#include <absl/status/status.h>

#include "file_writer.h"
#include "karu.h"
#include "sstable.h"

namespace karu {
namespace hint {
class HintFile {
 public:
  explicit HintFile(const std::string &path);
  absl::Status WriteHint(const std::string &key, std::uint16_t value_size,
                         std::uint32_t pos) noexcept;
  absl::StatusOr<std::unique_ptr<sstable::SSTable>>
  BuildSSTableHintFile() noexcept;

  HintFile &operator=(const HintFile &) = delete;
  HintFile(const HintFile &) = delete;

 private:
  std::string path_;
  std::unique_ptr<io::FileWriter> file_writer_ = nullptr;
  std::unique_ptr<io::FileReader> file_reader_ = nullptr;
};

absl::StatusOr<std::unique_ptr<sstable::SSTable>> ParseHintFile(
    karu::file_id_t sstable_id, const std::string &database_directory) noexcept;
}  // namespace hint
}  // namespace karu

#endif
