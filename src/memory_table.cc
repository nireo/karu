#include "memory_table.h"
#include <chrono>

MemoryTable::MemoryTable(const std::string &directory) {
  // get the unix timestamp
  std::time_t res = std::time(nullptr);
  int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

}
