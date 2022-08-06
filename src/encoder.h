#ifndef _KARU_ENCODER_H
#define _KARU_ENCODER_H

#include <absl/status/status.h>

#include <cstdint>
#include <memory>

namespace karu::encoder {
constexpr std::uint16_t kTombstone = 0xFFFF;
constexpr std::uint32_t kKeyByteCount = 2;
constexpr std::uint32_t kValueByteCount = 2;
constexpr std::uint32_t kPosByteCount = 4;

constexpr std::uint32_t kFullHeader =
    kKeyByteCount + kValueByteCount;  // 2 = key length + 2 = value length
constexpr std::uint32_t kHintHeader =
    kKeyByteCount + kValueByteCount + kPosByteCount;

class HintHeader {
 public:
  explicit HintHeader(std::uint8_t* const data) : data_(data){};
  [[nodiscard]] std::uint16_t KeyLength() const noexcept;
  [[nodiscard]] std::uint16_t ValueLength() const noexcept;
  [[nodiscard]] std::uint32_t ValuePos() const noexcept;

  [[nodiscard]] bool IsTombstoneValue() const noexcept;
  void MakeTombstone() noexcept;
  void SetKeyLength(std::uint16_t klen) noexcept;
  void SetValueLength(std::uint16_t vlen) noexcept;
  void SetPos(std::uint32_t pos) noexcept;

 private:
  std::uint8_t* const data_;
  [[nodiscard]] std::uint16_t RawValueLength() const noexcept;
};

class EntryHeader {
 public:
  explicit EntryHeader(std::uint8_t* const data) : data_(data){};
  [[nodiscard]] std::uint16_t KeyLength() const noexcept;
  [[nodiscard]] std::uint16_t ValueLength() const noexcept;
  [[nodiscard]] bool IsTombstoneValue() const noexcept;
  void MakeTombstone() noexcept;
  void SetKeyLength(std::uint16_t klen) noexcept;
  void SetValueLength(std::uint16_t vlen) noexcept;

 private:
  std::uint8_t* const data_;
  [[nodiscard]] std::uint16_t RawValueLength() const noexcept;
};

// Full encoding contains encoding for full entries, meaning that it contains
class FullEncoding {
  explicit FullEncoding(std::uint32_t len)
      : main_data_(new uint8_t[len]), key_size_(0), value_size_(0) {}
  static absl::Span<const std::uint8_t> Key() ;
  static absl::Span<const std::uint8_t> Value() ;

 private:
  [[nodiscard]] const std::uint8_t* raw_ptr() const { return main_data_.get(); }
  std::uint8_t* raw_ptr() { return main_data_.get(); }

  std::uint8_t key_size_;
  std::uint16_t value_size_;
  std::unique_ptr<std::uint8_t[]> main_data_;
  [[nodiscard]] static std::uint16_t RawValueLength() noexcept;
};
}  // namespace karu

#endif
