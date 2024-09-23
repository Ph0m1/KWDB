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
 * @date 2022/10/31
 * @version 1.0
 */
#pragma once

#include <cm_kwdb_context.h>
#include <engine.h>
#include <settings.h>
#include <cm_trace_plugin.h>
#include <utility>
#include <vector>
#include "../util.h"
#include "../worker.h"
#include "st_instance.h"

namespace kwdbts {

class TSEntityGroupWorker : public Worker {
 public:
  TSEntityGroupWorker(const std::string& pname, const BenchParams& params,
           const std::vector<uint32_t>& tbl_ids) : Worker(pname, params, tbl_ids) {
    st_inst_ = StEngityGroupInstance::Get();
    st_inst_->Init(params);
    ctx = &context;
    kwdbts::InitKWDBContext(ctx);
    tag_schema_ = st_inst_->GetEntityGroup()->GetSchema();
    KStatus s = st_inst_->GetTable()->GetDataSchemaExcludeDropped(ctx, &data_schema_);
    assert(s == KStatus::SUCCESS);
    entity_group_ = st_inst_->GetEntityGroup();
  }

  // complete initialization and other tasks
  KBStatus Init() override {return KBStatus::OK();}
  // data initialization
  KBStatus InitData(KTimestamp& new_ts) override {
    return KBStatus::OK();
  }
  // clean data
  KBStatus Destroy() override  {return KBStatus::OK();}

 protected:
  kwdbts::kwdbContext_t context;
  kwdbts::kwdbContext_p ctx;
  StEngityGroupInstance* st_inst_;
  std::vector<TagColumn*> tag_schema_;
  std::vector<AttributeInfo> data_schema_;
  std::shared_ptr<TsEntityGroup> entity_group_{nullptr};
};

class TSEntityGroupWriteWorker : public TSEntityGroupWorker {
 public:
  TSEntityGroupWriteWorker(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids) :
      TSEntityGroupWorker(pname, params, tbl_ids) {
  }

  virtual std::string show_extra() override;

  virtual KBStatus do_work(KTimestamp new_ts) override;

 protected:
  kwdbts::AvgStat _row_prepare_time;  // row data prepare time
  kwdbts::AvgStat _row_put_time;      // row data put time
  int64_t _row_sum = 0;
};

class TSEntityGroupWriteWorkerWithScan : public TSEntityGroupWriteWorker {
 public:
  TSEntityGroupWriteWorkerWithScan(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids) :
      TSEntityGroupWriteWorker(pname, params, tbl_ids) {
  }

  std::string show_extra() override;

  KBStatus do_work(KTimestamp new_ts) override;

 protected:
  kwdbts::AvgStat _row_scan_time;
};

class TSEntityGroupScanWorker : public TSEntityGroupWorker {
 public:
  TSEntityGroupScanWorker(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids)
      : TSEntityGroupWorker(pname, params, tbl_ids), start_ts_(-1) {
    rand_seed_ = time(NULL);
  }

  std::string show_extra() override;

  KBStatus do_work(KTimestamp new_ts) override;

 private:
  int64_t start_ts_;  // start time of query
  kwdbts::AvgStat _scan_rows;  // the number of rows returned per query
  kwdbts::AvgStat _scan_time;  // each query time(including traversal)
  kwdbts::AvgStat _agg_time;  // each full table aggregation query time
  kwdbts::KTimestamp ts_range_min_{0};
  kwdbts::KTimestamp  ts_range_max_;
  unsigned int rand_seed_;
};

}
