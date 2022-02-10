#include "karu.h"

#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

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
  if (auto status = current_memtable_->Get(key); !status.ok()) {
    return status.status();
  } else {
    return *status;
  }

  // check the old memtable list
  memtable_list_mutex_.ReaderLock();
  for (const auto &mtable : memtable_list_) {
    if (auto status = mtable->Get(key); status.ok()) {
      return *status;  // we don't care about non valid values
    }
  }
  memtable_list_mutex_.ReaderUnlock();

  // look for value in sstables.
}

absl::Status DB::Insert(const std::string &key,
                        const std::string &value) noexcept {
  memtable_mutex_.WriterLock();
  if (auto status = current_memtable_->Insert(key, value); !status.ok()) {
    memtable_mutex_.WriterUnlock();
    return status;
  }
  memtable_mutex_.WriterUnlock();

  memtable_mutex_.ReaderLock();
  std::uint64_t size = current_memtable_->Size();
  memtable_mutex_.ReaderUnlock();
  if (size > memtable::kMaxMemSize) {
    if (auto status = FlushMemoryTable(); !status.ok()) {
      return status;
    }
  }

  return absl::OkStatus();
}

absl::Status DB::FlushMemoryTable() noexcept {
  memtable_mutex_.WriterLock();

  // copy the memory table such that operations can continue on the database.
  std::unique_ptr<memtable::Memtable> old_memtable =
      std::move(current_memtable_);
  current_memtable_ = std::make_unique<memtable::Memtable>(database_directory_);

  memtable_mutex_.WriterUnlock();

  memtable_list_mutex_.WriterLock();
  memtable_list_.push_back(std::move(old_memtable));
  size_t memtable_index = memtable_list_.size() - 1;
  memtable_list_mutex_.WriterUnlock();

  int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

  std::thread t([this, timestamp, memtable_index]() {
    std::string sstable_path =
        database_directory_ + '/' + std::to_string(timestamp) + ".data";
    std::unique_ptr<sstable::SSTable> ss =
        std::make_unique<sstable::SSTable>(sstable_path);

    // fill the sstable with data.
    auto status = ss->BuildFromBTree(memtable_list_[memtable_index]->map_);
    if (!status.ok()) {
      std::cerr << "failed to build sstable from btree_map.\n";
      return;
    }

    sstable_map_[timestamp] = std::move(ss);  // store the sstable.

    memtable_list_mutex_.WriterLock();
    std::unique_ptr<memtable::Memtable> useless_ =
        std::move(memtable_list_[memtable_index]);
    memtable_list_[memtable_index] = nullptr;
    // remove the log file since it is not needed anymore
    std::filesystem::remove(useless_->log_path_);
    memtable_list_.erase(memtable_list_.begin() + memtable_index);
    memtable_list_mutex_.WriterUnlock();
  });

  return absl::OkStatus();
}

// shutdown makes sure that all memtables are written to the disk. Log files do
// ensure that no data is lost, but the Shutdown() function can be considered a
// more graceful shutdown as it converts the data into its intended format.
absl::Status DB::Shutdown() noexcept {
  memtable_list_mutex_.WriterLock();

  for (size_t i = 0; i < memtable_list_.size(); ++i) {
    auto mtable = std::move(memtable_list_[i]);
    memtable_list_[i] = nullptr;
    std::string sstable_path =
        database_directory_ + '/' + std::to_string(mtable->ID()) + ".data";
    sstable::SSTable sstable(sstable_path);

    if (auto status = sstable.BuildFromBTree(mtable->map_); !status.ok()) {
      std::cerr << "error writing memtable(" << mtable->ID() << ") to disk.\n";
      continue;
    }

    // remove the log file as it's useless once data has been transferred into
    // the sstable.
    std::filesystem::remove(mtable->log_path_);
  }
  memtable_list_mutex_.WriterUnlock();

  return absl::OkStatus();
}

}  // namespace karu
