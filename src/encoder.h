#ifndef _KARU_ENCODER_H
#define _KARU_ENCODER_H

#include <absl/status/status.h>

#include <cstdint>
#include <memory>

namespace karu {
namespace encoder {
constexpr std::uint16_t kTombstone = 0xFFFF;
constexpr std::uint32_t kKeyByteCount = 1;
constexpr std::uint32_t kValueByteCount = 2;
constexpr std::uint32_t kPosByteCount = 4;

constexpr std::uint32_t kFullHeader = 3;  // 1 = key length + 2 = value length
constexpr std::uint32_t kHintHeader =
    kKeyByteCount + kValueByteCount + kPosByteCount;

class HintHeader {
 public:
  HintHeader(std::uint8_t* const data) : data_(data){};
  std::uint8_t KeyLength() const noexcept;
  std::uint16_t ValueLength() const noexcept;
  std::uint32_t ValuePos() const noexcept;

  bool IsTombstoneValue() const noexcept;
  void MakeTombstone() noexcept;
  void SetKeyLength(std::uint8_t klen) noexcept;
  void SetValueLength(std::uint16_t vlen) noexcept;
  void SetPos(std::uint32_t pos) noexcept;

 private:
  std::uint8_t* const data_;
  std::uint16_t RawValueLength() const noexcept;
};

class EntryHeader {
 public:
  EntryHeader(std::uint8_t* const data) : data_(data){};
  std::uint8_t KeyLength() const noexcept;
  std::uint16_t ValueLength() const noexcept;
  bool IsTombstoneValue() const noexcept;
  void MakeTombstone() noexcept;
  void SetKeyLength(std::uint8_t klen) noexcept;
  void SetValueLength(std::uint16_t vlen) noexcept;

 private:
  std::uint8_t* const data_;
  std::uint16_t RawValueLength() const noexcept;
};

// Full encoding contains encoding for full entries, meaning that it contains
class FullEncoding {
  FullEncoding(std::uint32_t len)
      : main_data_(new uint8_t[len]), key_size_(0), value_size_(0) {}
  absl::Span<const std::uint8_t> Key() const;
  absl::Span<const std::uint8_t> Value() const;

 private:
  const std::uint8_t* raw_ptr() const { return main_data_.get(); }
  std::uint8_t* raw_ptr() { return main_data_.get(); }

  std::uint8_t key_size_;
  std::uint16_t value_size_;
  std::unique_ptr<std::uint8_t[]> main_data_;
  std::uint16_t RawValueLength() const noexcept;
};
}  // namespace encoder
}  // namespace karu

#endif
