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
#include "gtest/gtest.h"
#include "ee_count_timer.h"
#include "kwdb_type.h"
class CountTimerTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};
TEST(CountTimerTest, TestElapsedTime) {
  kwdbts::CountTimer timer;
  using std::cout;
  using std::endl;
  timer.start();
  cout << "sleep 1s" << endl;
  timer.stop();

  auto elapsed_time = timer.elapsed_time();
  EXPECT_GT(elapsed_time, 0);
}

TEST(CountTimerTest, TestReset) {
  kwdbts::CountTimer timer;
  using std::cout;
  using std::endl;
  timer.start();
  cout << "sleep 1s" << endl;
  timer.reset();

  auto elapsed_time = timer.elapsed_time();
  EXPECT_GT(elapsed_time, 0);
}
