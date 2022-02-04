#include "encoder.h"

#include "gtest/gtest.h"

namespace karu {
namespace encoder {}

TEST(EncoderTest, RawHeaderProperties) {
  std::uint8_t header_data[1 + 2 + 4];
  encoder::HintHeader header(header_data);

  header.SetKeyLength(10);
  header.SetValueLength(20);

  EXPECT_EQ(header.KeyLength(), 10);
  EXPECT_EQ(header.ValueLength(), 20);
}

}  // namespace karu
