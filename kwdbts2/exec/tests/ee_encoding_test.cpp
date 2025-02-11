// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
#include "ee_encoding.h"

#include "cm_assert.h"
#include "ee_string_info.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"

class TestEncoding : public ::testing::Test {  // inherit testing::Test
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
  void SetUp() override {}
  void TearDown() override {}

 public:
  TestEncoding() {}
};

// verify code，int，float，bool
TEST_F(TestEncoding, TestEncodingValue) {
  kwdbts::k_int64 input = 8787878787;
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenInt(0, input);
  kwdbts::CKSlice slice;

  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeIntValue(&slice, colID, input);
  free(slice.data);
  slice.len = 0;

  kwdbts::k_double64 val = 1000.253456;
  len = kwdbts::ValueEncoding::EncodeComputeLenFloat(colID);
  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeFloatValue(&slice, colID, val);
  free(slice.data);
  slice.len = 0;

  bool bol = true;
  len = kwdbts::ValueEncoding::EncodeComputeLenBool(colID, bol);
  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeBoolValue(&slice, colID, bol);
  free(slice.data);
  slice.len = 0;
}

// verify CKTime
TEST_F(TestEncoding, TestEncodeTimeStampValue) {
  kwdbts::k_int32 colID = 1;
  kwdbts::CKTime t;
  t.t_timespec.tv_sec = 123;
  t.t_timespec.tv_nsec = 234;
  t.t_abbv = 3600;

  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenTime(0, t);
  kwdbts::CKSlice slice;

  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeTimeValue(&slice, colID, t);
  free(slice.data);
  slice.len = 0;
}

// verify CKDecimal
TEST_F(TestEncoding, TestEncodingDecimal) {
  kwdbts::CKDecimal de;
  uint64_t abs = 314567;
  de.my_form = kwdbts::FormFinite;
  memcpy(de.my_coeff.abs, &abs, sizeof(uint64_t));
  de.my_coeff.abs_size = 1;
  de.Exponent = -5;
  de.my_coeff.neg = false;
  de.negative = true;
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len =
      kwdbts::ValueEncoding::EncodeComputeLenDecimal(colID, de);
  kwdbts::CKSlice slice;

  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeDecimalValue(&slice, colID, de);
  free(slice.data);
  slice.len = 0;
}

// verify string
TEST_F(TestEncoding, TestEncodingString) {
  std::string str = "hello world";
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len =
      kwdbts::ValueEncoding::EncodeComputeLenString(colID, str.size());
  kwdbts::CKSlice slice;

  slice.data = static_cast<char*>(malloc(len));
  kwdbts::ValueEncoding::EncodeBytesValue(&slice, colID, str);
  free(slice.data);
  slice.len = 0;
}

// write the encoding to a file
TEST_F(TestEncoding, TestEncodingValueToFile) {
  kwdbts::EE_StringInfo info = nullptr;
  info = kwdbts::ee_makeStringInfo();
  kwdbts::k_int64 input = 8787878787;
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenInt(0, input);
  kwdbts::CKSlice slice;
  kwdbts::ee_enlargeStringInfo(info, len);
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeIntValue(&slice, colID, input);
  info->len = info->len + len;

  kwdbts::k_double64 val = 1000.253456;
  len = kwdbts::ValueEncoding::EncodeComputeLenFloat(colID);
  kwdbts::ee_enlargeStringInfo(info, len);
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeFloatValue(&slice, colID, val);
  info->len = info->len + len;

  bool bol = true;
  len = kwdbts::ValueEncoding::EncodeComputeLenBool(colID, bol);
  kwdbts::ee_enlargeStringInfo(info, len);
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeBoolValue(&slice, colID, bol);
  info->len = info->len + len;

  FILE* fp;
  const char* file_path = "encode.txt";
  // Open the file in binary mode
  if ((fp = fopen(file_path, "wb")) == NULL) {
    std::cout << "Open file failed!" << std::endl;
    exit(0);
  }
  // get total size
  fseek(fp, 0, SEEK_END);
  // write data to buffer
  fwrite(info->data, info->len, 1, fp);
  fclose(fp);
  free(info->data);
  delete info;
}

// verify Duration
TEST_F(TestEncoding, TestEncodingDuration) {
  kwdbts::EE_StringInfo info = nullptr;
  info = kwdbts::ee_makeStringInfo();
  kwdbts::k_int64 input = 8787878787;
  struct kwdbts::KWDuration duration;
  duration.format(input, 1000);
  kwdbts::k_int32 colID = 1;
  kwdbts::k_int32 len =
      kwdbts::ValueEncoding::EncodeComputeLenDuration(colID, duration);
  ASSERT_EQ(kwdbts::ee_enlargeStringInfo(info, len), kwdbts::SUCCESS);
  kwdbts::CKSlice slice;
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeDurationValue(&slice, 0, duration);
  info->len = info->len + len;
  free(info->data);
  delete info;
  // delete slice.data;
  slice.len = 0;
}

// verify NullValue
TEST_F(TestEncoding, TestEncodingNullValue) {
  kwdbts::EE_StringInfo info = nullptr;
  info = kwdbts::ee_makeStringInfo();
  kwdbts::k_int32 len = kwdbts::ValueEncoding::EncodeComputeLenNull(0);
  ASSERT_EQ(kwdbts::ee_enlargeStringInfo(info, len), kwdbts::SUCCESS);

  kwdbts::CKSlice slice;
  slice.data = info->data + info->len;
  slice.len = len;
  kwdbts::ValueEncoding::EncodeNullValue(&slice, 0);
  info->len = info->len + len;
  free(info->data);
  delete info;
  // delete slice.data;
  slice.len = 0;
}
