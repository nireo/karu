#ifndef _KARU_SSTABLE_H
#define _KARU_SSTABLE_H

#include <cstddef>
#include <string>

constexpr size_t MAX_TABLE_SIZE = 1 << 20;
class SSTable {
private:
  std::string filename;
  // TODO: bloom filter
};

#endif
