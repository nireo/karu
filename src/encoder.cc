#include "encoder.h"

#include "absl/base/internal/endian.h"

namespace karu {
namespace encoder {
std::uint8_t HintHeader::KeyLength() const noexcept { return data_[0]; }

std::uint16_t HintHeader::ValueLength() const noexcept {
  if (IsTombstoneValue()) {
    return 0;
  }
  return RawValueLength();
}

std::uint16_t HintHeader::RawValueLength() const noexcept {
  return absl::little_endian::Load16(&data_[kKeyByteCount]);
}

bool HintHeader::IsTombstoneValue() const noexcept {
  return RawValueLength() == kTombstone;
}

void HintHeader::SetKeyLength(std::uint8_t klen) noexcept { data_[0] = klen; }
void HintHeader::SetValueLength(std::uint16_t vlen) noexcept {
  absl::little_endian::Store16(&data_[kKeyByteCount], vlen);
}

void HintHeader::MakeTombstone() noexcept { SetValueLength(kTombstone); }
}  // namespace encoder
}  // namespace karu
