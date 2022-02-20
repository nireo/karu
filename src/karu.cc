#include "karu.h"

#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <thread>

#include "hint.h"
#include "memory_table.h"
#include "sstable.h"
#include "types.h"
#include "utils.h"

namespace karu {
DB::DB(absl::string_view directory) {
  database_directory_ = directory;

  auto status = InitializeSSTables();
  if (!status.ok()) {
    std::cerr << "error parsing sstables\n";
  }

  auto id = utils::generate_file_id();
  std::string sstable_string =
      database_directory_ + "/" + std::to_string(id) + sstable_file_suffix;
  current_sstable_ = std::make_unique<sstable::SSTable>(sstable_string, id);
  if (auto status = current_sstable_->InitWriterAndReader(); !status.ok()) {
    std::cerr << "could not initialize writer and reader\n";
  }
}

DB::DB(const DBConfig &conf) {
  database_directory_ = conf.database_directory_;

  if (conf.hint_files_) {
    if (auto status = ParseHintFiles(); !status.ok()) {
      std::cerr << "error parsing hint files: " << status.message() << '\n';
    }
  } else {
    if (auto status = InitializeSSTables(); !status.ok()) {
      std::cerr << "error parsing sstables: " << status.message() << '\n';
    }
  }

  auto id = utils::generate_file_id();
  std::string sstable_string =
      database_directory_ + "/" + std::to_string(id) + sstable_file_suffix;
  current_sstable_ = std::make_unique<sstable::SSTable>(sstable_string, id);
  if (auto status = current_sstable_->InitWriterAndReader(); !status.ok()) {
    std::cerr << "could not initialize writer and reader\n";
  }
}

absl::Status DB::InitializeSSTables() noexcept {
  for (const auto &entry :
       std::filesystem::directory_iterator(database_directory_)) {
    // parse sstable id from filename
    std::string filename = entry.path().filename();
    if (entry.path().extension() != ".data") {
      continue;
    }

    auto id = utils::parse_file_id(entry.path().string());
    if (!id.ok()) {
      std::cerr << "cannot parse id from filename\n";
      continue;
    }
    std::cerr << "parsed file id: " << *id << '\n';

    auto sstable = std::make_unique<sstable::SSTable>(entry.path(), *id);

    auto status = sstable->InitOnlyReader();
    if (!status.ok()) {
      return status;
    }

    status = sstable->AddEntriesToIndex(index_);
    datafiles_[*id] = std::move(sstable);
  }

  return absl::OkStatus();
}

absl::StatusOr<std::string> DB::Get(const std::string &key) noexcept {
#ifdef OLD
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
#endif
  if (!index_.contains(key)) {
    return absl::NotFoundError("coult not find key in index");
  }

  const auto &value = index_[key];
  sstable_mutex_.ReaderLock();
  if (value.file_id_ == current_sstable_->ID()) {
    auto status = current_sstable_->Find(value.value_size_, value.pos_);
    if (status.ok()) {
      sstable_mutex_.ReaderUnlock();
      return status;
    }
  }
  sstable_mutex_.ReaderUnlock();

  if (!datafiles_.contains(value.file_id_)) {
    std::cerr << "could not find datafile: " << value.file_id_ << '\n';
    for (const auto &entry : datafiles_) {
      std::cerr << "found datafile: " << entry.first << '\n';
    }
    return absl::InternalError("invalid file id.");
  }

  return datafiles_[value.file_id_]->Find(value.value_size_, value.pos_);
}

absl::Status DB::Insert(const std::string &key,
                        const std::string &value) noexcept {
  sstable_mutex_.WriterLock();
  auto status = current_sstable_->Insert(key, value);
  if (!status.ok()) {
    sstable_mutex_.WriterUnlock();
    return status.status();
  }

  std::uint32_t file_pos = *status;
  file_id_t id = current_sstable_->ID();
  sstable_mutex_.WriterUnlock();

  index_mutex_.WriterLock();
  index_[key] = {
      .file_id_ = current_sstable_->ID(),
      .pos_ = file_pos,
      .value_size_ = static_cast<std::uint16_t>(value.size()),
  };
  index_mutex_.WriterUnlock();

  return absl::OkStatus();
}

absl::Status DB::ParseHintFiles() noexcept {
  // find all hint files
  auto hint_files = utils::files_with_extension(".hnt", database_directory_);

  // we want to sort the file paths such that oldest tables first.
  std::sort(hint_files.begin(), hint_files.end());
  for (const auto &path : hint_files) {
    auto status = utils::parse_file_id(path);
    if (!status.ok()) {
      continue;
    }

    status = hint::ParseHintFile(path, *status, index_);
    if (!status.ok()) {
      continue;
    }
  }

  return absl::OkStatus();
}

absl::Status DB::FlushMemoryTable() noexcept {
#ifdef OLD
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
#endif
  absl::WriterMutexLock guard(&sstable_mutex_);
  file_id_t id = current_sstable_->ID();
  datafiles_[id] = std::move(current_sstable_);

  auto new_id = utils::generate_file_id();
  std::string sstable_string =
      database_directory_ + "/" + std::to_string(new_id) + sstable_file_suffix;
  current_sstable_ = std::make_unique<sstable::SSTable>(sstable_string, new_id);
  auto status = current_sstable_->InitWriterAndReader();
  if (!status.ok()) {
    return status;
  }

  return absl::OkStatus();
}
}  // namespace karu
