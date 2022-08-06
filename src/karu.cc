#include "karu.h"

#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <thread>

#include "hint.h"
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
  if (status = current_sstable_->InitWriterAndReader(); !status.ok()) {
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
absl::Status DB::InitializeHints() noexcept { return {}; }
}  // namespace karu
