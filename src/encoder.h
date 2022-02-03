#ifndef _KARU_ENCODER_H
#define _KARU_ENCODER_H

#include <cstdint>
namespace karu {
namespace encoder {
constexpr std::uint16_t kTombstone = 0xFFFF;
constexpr std::uint8_t kKeyByteCount = 1;
constexpr std::uint8_t kValueByteCount = 2;
constexpr std::uint8_t kPosByteCount = 4;

class Encoding {
 public:
  Encoding(std::uint8_t* const data) : data_(data){};
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
}  // namespace encoder
}  // namespace karu

#endif
