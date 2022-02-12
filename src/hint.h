#ifndef _KARU_HINT_H
#define _KARU_HINT_H

#include <absl/status/status.h>

#include "file_writer.h"
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

 private:
  std::string path_;
  std::unique_ptr<io::FileWriter> file_writer_ = nullptr;
  std::unique_ptr<io::FileReader> file_reader_ = nullptr;
};
}  // namespace hint
}  // namespace karu

#endif
