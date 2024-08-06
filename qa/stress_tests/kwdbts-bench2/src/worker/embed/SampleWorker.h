// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
/*
 * @author jiadx
 * @date 2023/1/31
 * @version 1.0
 */

#include <vector>
#include "../worker.h"

#pragma once

namespace kwdbts {
using namespace kwdbts;

class SampleWorker : public Worker {
 public:
  SampleWorker(const std::string& pname, const BenchParams& params,
               const std::vector<uint32_t>& dev_ids) : Worker(pname, params, dev_ids) {
  }

  KBStatus Init() override {
    fprintf(stdout, "*** SampleWorker->Init()*** \n");
    return KBStatus::OK();
  }

  KBStatus Destroy() override {
    fprintf(stdout, "*** SampleWorker->Destroy()*** \n");
    return KBStatus::OK();
  }

  std::string show_extra() override {
    return "SampleWorker extra info";
  }

 protected:
  k_uint64 count = 0;

  KBStatus do_work(KTimestamp new_ts) override {
    // fprintf(stdout, "*** SampleWorker->do_work(), time=%ld*** \n", new_ts);
    count++;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return KBStatus::OK();
  }
};
}
