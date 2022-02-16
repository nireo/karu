#include "karu.h"

#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

#include <cstdio>
#include <filesystem>
#include <memory>
#include <thread>

#include "memory_table.h"
#include "sstable.h"
#include "utils.h"

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
    if (entry.path().extension() != ".data") {
      continue;
    }

    file_id_t id = -1;
    std::sscanf(filename.c_str(), "%ld.data", &id);
    auto sstable = std::make_unique<sstable::SSTable>(entry.path());

    auto status = sstable->InitOnlyReader();
    if (!status.ok()) {
      return status;
    }

    status = sstable->PopulateFromFile();
    sstable_list_.push_back(std::move(sstable));
  }

  return absl::OkStatus();
}

absl::StatusOr<std::string> DB::Get(const std::string &key) noexcept {
  // check the memtable
  std::cerr << "searching current memtable.\n";
  if (auto status = current_memtable_->Get(key); status.ok()) {
    return *status;
  }

  // check the old memtable list
  memtable_list_mutex_.ReaderLock();
  std::cerr << "searching older memtables.\n";
  for (const auto &mtable : memtable_list_) {
    if (auto status = mtable->Get(key); status.ok()) {
      return *status;  // we don't care about non valid values
    }
  }
  memtable_list_mutex_.ReaderUnlock();

  // look for value in sstables.
  sstable_mutex_.ReaderLock();
  std::cerr << "reading from sstables.\n";
  // since the newest sstable is always  the last, we loop starting from the
  // back, and the first one we find is the correct one.
  // TODO: Make this somewhat concurrent.
  for (auto it = sstable_list_.rbegin(); it != sstable_list_.rend(); ++it) {
    auto status = (*it)->Find(key);
    if (status.ok()) {
      return *status;
    }
  }

  sstable_mutex_.ReaderUnlock();
  return absl::NotFoundError("coult not find key in memtable for sstables.");
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

absl::Status DB::ParseHintFiles() noexcept {
  // find all hint files
  auto hint_files = utils::files_with_extension(".hnt", database_directory_);

  return absl::OkStatus();
}

absl::Status DB::FlushMemoryTable() noexcept {
  memtable_mutex_.WriterLock();

  std::unique_ptr<memtable::Memtable> old_memtable =
      std::move(current_memtable_);
  current_memtable_ = std::make_unique<memtable::Memtable>(database_directory_);
  memtable_mutex_.WriterUnlock();
  std::cerr << "created a new memtable.\n";

  memtable_list_mutex_.WriterLock();
  memtable_list_.push_back(std::move(old_memtable));
  size_t memtable_index = memtable_list_.size() - 1;
  memtable_list_mutex_.WriterUnlock();
  std::cerr << "added old memtable to queue.\n";

  int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

  std::string sstable_path =
      database_directory_ + '/' + std::to_string(timestamp) + ".data";
  std::unique_ptr<sstable::SSTable> ss =
      std::make_unique<sstable::SSTable>(sstable_path);
  if (auto status = ss->InitWriterAndReader(); !status.ok()) {
    std::cerr << "failed to initialize writer and reader.\n";
    return absl::InternalError("failed to initialize writer and reader.\n");
  }
  std::cerr << "created sstable\n";

  // fill the sstable with data.
  auto status = ss->BuildFromBTree(memtable_list_[memtable_index]->map_);
  if (auto status = ss->BuildFromBTree(memtable_list_[memtable_index]->map_);
      !status.ok()) {
    std::cerr << "failed to build sstable from btree_map.\n";
    return absl::InternalError("failed to build sstable from btree.\n");
  }

  sstable_mutex_.WriterLock();
  sstable_list_.push_back(std::move(ss));  // store the sstable.
  sstable_mutex_.WriterUnlock();

  memtable_list_mutex_.WriterLock();
  std::unique_ptr<memtable::Memtable> useless_ =
      std::move(memtable_list_[memtable_index]);

  memtable_list_[memtable_index] = nullptr;

  // remove the log file since it is not needed anymore
  std::filesystem::remove(useless_->log_path_);
  memtable_list_.erase(memtable_list_.begin() + memtable_index);
  std::cerr << "removed memtable";
  memtable_list_mutex_.WriterUnlock();

  return absl::OkStatus();
}
}  // namespace karu
