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
 * @date 2023/3/29
 * @version 1.0
 */

#pragma once
#include "st_worker.h"

namespace kwdbts {
class StOrderWorker : public StWriteWorker{
 public:
  StOrderWorker(const std::string& pname, const BenchParams& params,
                const std::vector<uint32_t>& tbl_ids) :
      StWriteWorker(pname, params, tbl_ids) {
  }

 protected:
  KBStatus do_work(KTimestamp new_ts) override;
};

class StOrderRange : public StScanWorker{
 public:
  StOrderRange(const std::string& pname, const BenchParams& params,
               const std::vector<uint32_t>& tbl_ids) :
      StScanWorker(pname, params, tbl_ids) {
  }

  std::string FullTableNameOf(uint32_t tbl_id) {
    return "d" + std::to_string(tbl_id) + "_d" + std::to_string(tbl_id);
  }

 protected:
  KBStatus do_work(KTimestamp new_ts) override;
};
}
