#include "karu.h"

#include <absl/status/status.h>

#include <cstdio>
#include <filesystem>
#include <thread>

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

  // look for value in sstables.
}

absl::Status DB::FlushMemoryTable() noexcept {
  memtable_mutex_.WriterLock();

  // copy the memory table such that operations can continue on the database.
  std::unique_ptr<memtable::Memtable> old_memtable =
      std::move(current_memtable_);
  current_memtable_ = std::make_unique<memtable::Memtable>(database_directory_);

  memtable_mutex_.WriterUnlock();

  int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

  std::thread t([&old_memtable, this, timestamp]() {
    std::string sstable_path =
        database_directory_ + '/' + std::to_string(timestamp) + ".data";
    std::unique_ptr<sstable::SSTable> ss =
        std::make_unique<sstable::SSTable>(sstable_path);

    // fill the sstable with data.
    auto status = ss->BuildFromBTree(old_memtable_->map_);
    if (!status.ok()) {
      std::cerr << "failed to build sstable from btree_map.\n";
      return;
    }

    sstable_map_[timestamp] = std::move(ss);  // store the sstable.
  });

  return absl::OkStatus();
}

}  // namespace karu
