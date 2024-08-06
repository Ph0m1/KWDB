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

#include "ee_field.h"

#include "ee_field_func_math.h"
#include "ee_field_func_string.h"
#include "ee_processors.h"
#include "gtest/gtest.h"

namespace kwdbts {

typedef struct FieldMathTest {
  KString func_name;
  k_int64 expect_int;
  k_double64 expect_real;
} FieldMathTest;

typedef struct FieldStringTest {
  KString func_name;
  KString expect_val;
} FieldStringTest;

const FieldMathTest math_tests[] = {
    {.func_name = "sin", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "cos", .expect_int = 1, .expect_real = 1.0},
    {.func_name = "tan", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "asin", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "acos", .expect_int = 1, .expect_real = 1.5707963267948966},
    {.func_name = "atan", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "sqrt", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "round", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "abs", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "ceil", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "floor", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "isnan", .expect_int = 0, .expect_real = 0.0},
    // {.func_name = "ln", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "radians", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "sign", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "trunc", .expect_int = 0, .expect_real = 0.0},
    // {.func_name = "cot", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "sign", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "cbrt", .expect_int = 0, .expect_real = 0.0},
    {.func_name = "exp", .expect_int = 1, .expect_real = 1.0},
    {.func_name = "degrees", .expect_int = 0, .expect_real = 0.0},
};

const FieldStringTest string_tests[] = {
  {.func_name = "concat", .expect_val = "a12a12"},
  {.func_name = "substr", .expect_val = "a12"},
  {.func_name = "lpad", .expect_val = ""},
  {.func_name = "rpad", .expect_val = ""},
  {.func_name = "ltrim", .expect_val = ""},
  {.func_name = "rtrim", .expect_val = ""},
  {.func_name = "left", .expect_val = ""},
  {.func_name = "right", .expect_val = ""},
  // {.func_name = "upper", .expect_val = "A12"},
  // {.func_name = "lower", .expect_val = "a12"},

};
class BaseField : public FieldNum {
 public:
  using FieldNum::FieldNum;
  k_int64 ValInt() { return 0; }
  k_int64 ValInt(char *ptr) { return 0; }

  k_double64 ValReal() { return 0.0; }
  k_double64 ValReal(char *ptr) { return 0.0; }
  String ValStr() {
    String s(3);
    snprintf(s.ptr_, 3 + 1, "%s", "a12");
    s.length_ = strlen(s.ptr_);
    return s;
  }
  String ValStr(char *ptr) {
    String s(3);
    snprintf(s.ptr_, 3 + 1, "%s", "a12");
    s.length_ = strlen(s.ptr_);
    return s;
  }
  Field *field_to_copy() { return new BaseField(*this); }

  k_int64 ValInt(k_int64 *val, k_bool negative) { return 0; }
  k_bool fill_template_field(char *ptr) { return 0; }
  char *get_ptr() { return nullptr; }
};

class TestFieldIterator : public ::testing::Test {
 protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
  kwdbContext_t g_pool_context;
  kwdbContext_p ctx_ = &g_pool_context;
};

TEST_F(TestFieldIterator, TestMathFunc) {
  std::list<Field *> args;
  args.push_back(KNEW BaseField());
  Field *field;
  size_t len = sizeof(math_tests) / sizeof(math_tests[0]);
  for (k_int32 j = 0; j < len; j++) {
    for (k_int32 i = 0; i < mathFuncBuiltinsNum1; i++) {
      if (mathFuncBuiltins1[i].name == math_tests[j].func_name) {
        field = KNEW FieldFuncMath(args, mathFuncBuiltins1[i]);
        break;
      }
    }
    EXPECT_EQ(field->ValInt(), math_tests[j].expect_int)
        << math_tests[j].func_name;
    EXPECT_DOUBLE_EQ(field->ValReal(), math_tests[j].expect_real)
        << math_tests[j].func_name;
    SafeDeletePointer(field);
  }
  // auto *field_sum = KNEW FieldFuncMath(args, mathFuncBuiltins1[0]);

  // BaseOperator *noop_iter = NewIterator<NoopIterator>(ok_iter_);
  for (auto a : args) {
    SafeDeletePointer(a);
  }
}

TEST_F(TestFieldIterator, TestStringFunc) {
  std::list<Field *> args;
  args.push_back(KNEW BaseField());
  args.push_back(KNEW BaseField());

  Field *field;
  size_t len = sizeof(string_tests) / sizeof(string_tests[0]);
  for (k_int32 j = 0; j < len; j++) {
    field = KNEW FieldFuncString(string_tests[j].func_name, args);

    EXPECT_EQ(field->ValStr().ptr_, string_tests[j].expect_val)
        << string_tests[j].func_name;
    SafeDeletePointer(field);
  }
  // auto *field_sum = KNEW FieldFuncMath(args, mathFuncBuiltins1[0]);

  // BaseOperator *noop_iter = NewIterator<NoopIterator>(ok_iter_);
  for (auto a : args) {
    SafeDeletePointer(a);
  }
}
}  // namespace kwdbts
