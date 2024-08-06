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

#pragma once

#include <string>
#include <vector>
#include "libkwdbts2.h"
#include "ts_common.h"

namespace kwdbts {

enum WALMode : uint8_t {
  OFF = 0,
  ON = 1,
  SYNC = 2
};

struct EngineOptions {
  EngineOptions() {
  }

  std::string db_path;
  // WAL work level: 0:off, 1:on, 2:sync, default is sync.
  uint8_t wal_level = WALMode::SYNC;
  // WAL file size, default is 64Mb
  uint16_t wal_file_size = 64;
  uint16_t wal_file_in_group = 3;
  uint16_t wal_buffer_size = 4;
  uint16_t thread_pool_size = 10;
  uint16_t task_queue_size = 1024;
  // 1MiB / 4KiB(BLOCK_SIZE) = 256
  [[nodiscard]] uint32_t GetBlockNumPerFile() const {
    return wal_file_size * 256 - 1;
  }
  bool wal_archive = false;
  char* wal_archive_command = nullptr;
  TsEngineLogOption lg_opts = {"", 0, 0, DEBUG, ""};
  std::vector<RangeGroup> ranges;  // record engine hash value range
};

}  // namespace kwdbts
