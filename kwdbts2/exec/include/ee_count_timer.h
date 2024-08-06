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
// Created by liuxiupeng .
#pragma once

#include <sys/resource.h>
#include <sys/time.h>

#include <ctime>

#include "kwdb_type.h"
namespace kwdbts {
class CountTimer {
 public:
  CountTimer() {
    _start = {};
    _total_time = 0;
    _running = false;
  }

  void start() {
    if (!_running) {
      clock_gettime(CLOCK_MONOTONIC, &_start);
      _running = true;
    }
  }

  k_uint64 stop() {
    // if (_running) {
    _total_time = elapsed_time();
    //  _running = false;
    //}
    return _total_time;
  }

  // Restarts the timer. Returns the elapsed time until this point.
  k_uint64 reset() {
    k_uint64 ret = elapsed_time();

    if (_running) {
      clock_gettime(CLOCK_MONOTONIC, &_start);
    }

    return ret;
  }

  // Returns time in nanosecond.
  k_uint64 elapsed_time() const {
    if (!_running) {
      return _total_time;
    }

    timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    return (end.tv_sec - _start.tv_sec) * 1000L * 1000L * 1000L +
           (end.tv_nsec - _start.tv_nsec);
  }

 private:
  timespec _start;
  k_uint64 _total_time;  // in nanosec
  bool _running;
};

};  // namespace kwdbts

