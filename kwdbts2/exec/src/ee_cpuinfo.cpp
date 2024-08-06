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
// Created by liuxiupeng on 2024/01/04.
//

#include "ee_cpuinfo.h"

#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>

#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif

namespace kwdbts {
k_int32 CpuInfo::num_cores_ = 1;
k_bool CpuInfo::initialized_ = false;

std::string& trim(std::string& s) {
  if (s.empty()) {
    return s;
  }
  s.erase(0, s.find_first_not_of(" "));
  s.erase(s.find_last_not_of(" ") + 1);
  return s;
}

void CpuInfo::Init() {
  if (initialized_) return;
  std::string line;
  std::string name;
  // std::string value;

  // float max_mhz = 0;
  int num_cores = 0;

  // Read from /proc/cpuinfo
  std::ifstream cpuinfo("/proc/cpuinfo");
  while (cpuinfo) {
    getline(cpuinfo, line);
    size_t colon = line.find(':');
    if (colon != std::string::npos) {
      name = line.substr(0, colon - 1);
      trim(name);
      if (name.compare("processor") == 0) {
        ++num_cores;
      }
    }
  }

  if (num_cores > 0) {
    num_cores_ = num_cores;
  }
  initialized_ = true;
}

}  // namespace kwdbts
