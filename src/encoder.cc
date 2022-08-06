#include "encoder.h"

#include "absl/base/internal/endian.h"

namespace karu::encoder {
std::uint16_t HintHeader::KeyLength() const noexcept {
  return absl::little_endian::Load16(&data_[0]);
}

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

std::uint32_t HintHeader::ValuePos() const noexcept {
  return absl::little_endian::Load32(&data_[kKeyByteCount + kValueByteCount]);
}

void HintHeader::SetPos(std::uint32_t pos) noexcept {
  absl::little_endian::Store32(&data_[kKeyByteCount + kValueByteCount], pos);
}

void HintHeader::SetKeyLength(std::uint16_t klen) noexcept {
  absl::little_endian::Store16(&data_[0], klen);
}

void HintHeader::SetValueLength(std::uint16_t vlen) noexcept {
  absl::little_endian::Store16(&data_[kKeyByteCount], vlen);
}

void HintHeader::MakeTombstone() noexcept { SetValueLength(kTombstone); }

std::uint16_t EntryHeader::KeyLength() const noexcept {
  return absl::little_endian::Load16(&data_[0]);
}

std::uint16_t EntryHeader::ValueLength() const noexcept {
  if (IsTombstoneValue()) {
    return 0;
  }
  return RawValueLength();
}

std::uint16_t EntryHeader::RawValueLength() const noexcept {
  return absl::little_endian::Load16(&data_[kKeyByteCount]);
}

bool EntryHeader::IsTombstoneValue() const noexcept {
  return RawValueLength() == kTombstone;
}

void EntryHeader::SetKeyLength(std::uint16_t klen) noexcept {
  absl::little_endian::Store16(&data_[0], klen);
}

void EntryHeader::SetValueLength(std::uint16_t vlen) noexcept {
  absl::little_endian::Store16(&data_[kKeyByteCount], vlen);
}

void EntryHeader::MakeTombstone() noexcept { SetValueLength(kTombstone); }
absl::Span<const std::uint8_t> FullEncoding::Key() {
  return {};
}
absl::Span<const std::uint8_t> FullEncoding::Value() {
  return {};
}
std::uint16_t FullEncoding::RawValueLength() noexcept { return 0; }
}  // namespace karu
