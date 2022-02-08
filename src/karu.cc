#include "karu.h"

#include <absl/status/status.h>

#include <cstdio>
#include <filesystem>

#include "memory_table.h"
#include "sstable.h"

namespace karu {

absl::StatusOr<std::string> Get(const std::string &key) noexcept {
  return absl::OkStatus();
}

DB::DB(absl::string_view directory) {
  database_directory_ = directory;
  current_memtable_ = std::make_unique<memtable::Memtable>(database_directory_);
}

absl::Status DB::InitializeSSTables() noexcept {
  for (const auto &entry :
       std::filesystem::directory_iterator(database_directory_)) {
    // parse sstable id from filename
    std::string filename = entry.path().filename();
    file_id_t id = -1;
    int amount = std::sscanf(filename.c_str(), "%ld.data", &id);
    if (amount != 1 || amount == EOF) {
      return absl::InternalError("cannot parse file id from string");
    }

    if (id == -1) {
      return absl::InternalError("couldn't read file id properly");
    }

    std::unique_ptr<sstable::SSTable> sstable =
        std::make_unique<sstable::SSTable>(entry.path());

    auto status = sstable->InitOnlyReader();
    if (!status.ok()) {
      return status;
    }

    status = sstable->PopulateFromFile();
    sstable_map_[id] = std::move(sstable);
  }
  return absl::OkStatus();
}

absl::StatusOr<std::string> DB::Get(const std::string &key) noexcept {
  // check the memtable
  auto status = current_memtable_->Get(key);
  if (!status.ok()) {
    return status.status();
  } else {
    return *status;
  }
}

}  // namespace karu
