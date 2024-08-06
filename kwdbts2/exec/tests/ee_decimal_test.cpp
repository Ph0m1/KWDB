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
#include "ee_decimal.h"

#include "gtest/gtest.h"

namespace kwdbts {
// class CKBigIntTest : public ::testing::Test {};
// class CKDecimalTest : public ::testing::Test {};
// // test CKBigInt::Sign
// TEST(CKBigIntTest, Sign) {
//   CKBigInt bigInt;

//   bigInt.abs_size = 0;
//   EXPECT_EQ(bigInt.Sign(), 0);

//   // test negtive
//   bigInt.abs_size = 1;
//   bigInt.neg = true;
//   EXPECT_EQ(bigInt.Sign(), -1);

//   // test positive
//   bigInt.neg = false;
//   EXPECT_EQ(bigInt.Sign(), 1);

//   // test very large positive
//   bigInt.abs_size = INT32_MAX;
//   bigInt.neg = false;
//   EXPECT_EQ(bigInt.Sign(), 1);

//   // test very large negtive
//   bigInt.neg = true;
//   EXPECT_EQ(bigInt.Sign(), -1);
// }

// // test CKDecimal::DecimalToDouble
// TEST(CKDecimalTest, DecimalToDouble) {
//   CKDecimal decimal;

//   // test 0
//   decimal.negative = false;
//   decimal.Exponent = 0;
//   decimal.my_coeff.abs_size = 0;
//   EXPECT_DOUBLE_EQ(decimal.DecimalToDouble(), 0.0);

//   // test positive value
//   k_uint64 posValue = 123456;
//   decimal.negative = false;
//   decimal.Exponent = -3;
//   decimal.my_coeff.abs_size = 1;
//   decimal.my_coeff.abs = &posValue;
//   EXPECT_DOUBLE_EQ(decimal.DecimalToDouble(), 123.456);

//   // test negValue
//   k_uint64 negValue = 123456;
//   decimal.negative = true;
//   decimal.Exponent = -3;
//   decimal.my_coeff.abs = &negValue;
//   EXPECT_DOUBLE_EQ(decimal.DecimalToDouble(), -123.456);

//   // Test very large values
//   k_uint64 largeValue = 987654321012345678;
//   decimal.negative = false;
//   decimal.Exponent = 9;  // 10^9
//   decimal.my_coeff.abs = &largeValue;
//   decimal.my_coeff.abs_size = 1;
//   EXPECT_NEAR(decimal.DecimalToDouble(), 9.87654321012345678e+26, 1e+19);
// }

// // test  CKDecimal::Sign
// TEST(CKDecimalTest, Sign) {
//   CKDecimal decimal;

//   // test 0
//   decimal.my_form = 0;
//   decimal.negative = false;
//   decimal.my_coeff.abs_size = 0;
//   EXPECT_EQ(decimal.Sign(), 0);

//   // test positive
//   decimal.negative = false;
//   decimal.my_coeff.abs_size = 1;
//   decimal.my_coeff.neg = false;
//   EXPECT_EQ(decimal.Sign(), 1);

//   // test negtive
//   decimal.negative = true;
//   EXPECT_EQ(decimal.Sign(), -1);
// }

// // test IntToDecimal
// TEST(CKDecimalTest, IntToDecimal) {
//   k_uint64 val = 123456;
//   bool unsigned_flag = true;

//   CKDecimal decimal = IntToDecimal(&val, unsigned_flag);
//   EXPECT_EQ(decimal.Sign(), 1);
//   EXPECT_EQ(decimal.Exponent, 0);
//   EXPECT_EQ(*decimal.my_coeff.abs, val);

//   unsigned_flag = false;
//   decimal = IntToDecimal(&val, unsigned_flag);
//   EXPECT_EQ(decimal.Sign(), -1);

//   // Test maximum integer values
//   k_uint64 maxInt = INT64_MAX;
//   CKDecimal maxDecimal = IntToDecimal(&maxInt, true);
//   EXPECT_EQ(maxDecimal.Sign(), 1);
//   EXPECT_EQ(*maxDecimal.my_coeff.abs, maxInt);
// }

// // test DoubleToDecimal
// TEST(CKDecimalTest, DoubleToDecimal) {
//   k_double64 dval = 123.456;
//   k_uint64 uval;
//   bool unsigned_flag = true;

//   CKDecimal decimal = DoubleToDecimal(&dval, &uval, unsigned_flag);
//   EXPECT_EQ(decimal.Sign(), 1);
//   EXPECT_EQ(decimal.Exponent <= 0, true);
//   EXPECT_EQ(decimal.my_coeff.abs != nullptr, true);
//   EXPECT_EQ(decimal.my_coeff.abs_size, 1);

//   unsigned_flag = false;
//   decimal = DoubleToDecimal(&dval, &uval, unsigned_flag);
//   EXPECT_EQ(decimal.Sign(), -1);

//   // Test maximum floating-point values
//   k_double64 hugeVal = DBL_MAX;
//   k_uint64 hugeInt;
//   CKDecimal hugeDecimal = DoubleToDecimal(&hugeVal, &hugeInt, true);
//   EXPECT_EQ(hugeDecimal.Sign(), 1);

//   // Test for very small floating-point values (not 0)
//   k_double64 tinyVal = DBL_MIN;
//   k_uint64 tinyInt;
//   CKDecimal tinyDecimal = DoubleToDecimal(&tinyVal, &tinyInt, true);
//   EXPECT_EQ(tinyDecimal.Sign(), 1);
// }

}  // namespace kwdbts
