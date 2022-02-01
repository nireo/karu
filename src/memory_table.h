#ifndef _KARU_MEMORY_TABLE_H
#define _KARU_MEMORY_TABLE_H

#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <fstream>

struct MemoryEntry {
  std::string key_, value_;
};

constexpr uint64_t MAX_MEM_SIZE = 32 * 1024 * 1024; // 32 mb
class MemoryTable {
public:
  MemoryTable();

  MemoryTable(const MemoryTable &) = delete;
  MemoryTable &operator=(const MemoryTable &) = delete;

private:
  std::string log_path_;
  std::ofstream log_file_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<char *, MemoryEntry> mp_;
};

#endif
