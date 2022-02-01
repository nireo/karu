#ifndef _KARU_FILE_WRITER_H
#define _KARU_FILE_WRITER_H

#include <cstdint>
#include <fstream>

namespace karu {
namespace io {
class FileWriter {
public:
  ~FileWriter() { file_.close(); }
  [[nodiscard]] uint64_t append();

private:
  std::ofstream file_;
  uint64_t offset_;
  std::string filename_;
};
} // namespace io
} // namespace karu

#endif
