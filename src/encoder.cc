#include "encoder.h"

#include <absl/base/internal/endian.h>

namespace karu {
namespace encoder {
std::uint8_t Encoding::KeyLength() const noexcept { return data_[0]; }

std::uint16_t Encoding::ValueLength() const noexcept {
  if (IsTombstoneValue()) {
    return 0;
  }
  return RawValueLength();
}

std::uint16_t Encoding::RawValueLength() const noexcept {
  return absl::little_endian::Load16(&data_[kKeyByteCount]);
}

bool Encoding::IsTombstoneValue() const noexcept {
  return RawValueLength() == kTombstone;
}

void Encoding::SetKeyLength(std::uint8_t klen) noexcept { data_[0] = klen; }
void Encoding::SetValueLength(std::uint16_t vlen) noexcept {
  absl::little_endian::Store16(&data_[kKeyByteCount], vlen);
}

void Encoding::MakeTombstone() noexcept { SetValueLength(kTombstone); }
}  // namespace encoder
}  // namespace karu
