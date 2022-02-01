#ifndef _KARU_H
#define _KARU_H

#include <cstdint>
#include <string>
#include <unordered_map>

enum class Status {
  Ok,
  Error,
};

class MemoryTable {
private:
  uint64_t size_;
};

#endif
