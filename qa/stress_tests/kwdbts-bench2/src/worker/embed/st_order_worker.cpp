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

#include "st_order_worker.h"

namespace kwdbts {
// the time of partition interval::
const KTimestamp PARTION_TIME = 60 * 1000 * 10;

KBStatus StOrderWorker::do_work(KTimestamp new_ts) {
//  auto ks = StWriteWorker::do_work(new_ts);
//  if (ks.isNotOK()) {
//    return ks;
//  }
//  if (_row_sum % (100 * table_ids_.size()) == 0) {
//    // Write 100 rows per table and insert a disorder data to the second partition
//    KTimestamp old_ts = new_ts - rand() % (PARTION_TIME * 2);
//    ks = StWriteWorker::do_work(old_ts);
//  }
  return KBStatus::OK();
}

KBStatus StOrderRange::do_work(KTimestamp new_ts) {
//  if (table_ids_.empty()) {
//    can_run_ = false;
//    return KBStatus::Invalid("no table to run");
//  }
//  if (start_ts_ < 0) {
//    start_ts_ = new_ts - 1;
//  }
//// select the table in order and execute the last read
//  uint32_t r_table = getCurrentTable();
//  if (!IsTableCreated(r_table)) {
//    log_INFO("Table[%d] not created!", r_table);
//    return KBStatus::OK();
//  }
//
//  // obtain minimum and maximum timestamps
//  KReadOptions opt;
//  opt.min_ts = 0;
//  opt.max_ts = 0;
//  opt.iterator_mode = IteratorType::ITERATOR_SINGLE;
//  std::unique_ptr<KObjectIterator> iter;
//  CalcResultMeta resultMeta;
//  std::string table_name = FullTableNameOf(r_table);
//  std::string sql_stmt = "select min(k_timestamp),max(k_timestamp) from " + table_name;
//  KBStatus ks = dump_zstatus("select query error:", ctx,
//                             st_inst_->GetKSchema()->GetCalcResultData(ctx, sql_stmt, opt, &iter, &resultMeta));
//  if (ks.isNotOK()) {
//    if (ks.message().find("doesn't exist") != std::string::npos) {
//      log_INFO("select query :%s", ks.message().c_str());
//      return KBStatus::OK();
//    } else {
//      return ks;
//    }
//  }
//  char* data;
//  ks = dump_zstatus("Next error:", ctx, iter->Next(ctx, &data));
//  if (ks.isNotOK()) {
//    return ks;
//  }
//  KWDB_START();
//  opt.min_ts = KTimestamp(data);
//  opt.max_ts = KTimestamp(data + 8);
//  KTimestamp max_range = opt.max_ts - opt.min_ts;
//  if (max_range == 0){
//    max_range = 1;
//  }
//  // calculate a random start time
//  opt.min_ts += rand() % max_range;
//  opt.max_ts = opt.min_ts + params_.scan_range;
//
//  opt.object_id = r_table;
//  opt.snapshot_id = 0;
//  opt.cols = query_cols_;
//
//  ks = dump_zstatus("NewObjectIterator error:", ctx, st_inst_->GetKSchema()->NewObjectIterator(ctx, opt, &iter));
//  if (ks.isNotOK()) {
//    return ks;
//  }
//
//  int total_rows = 0;
//  char* buffer;
//  if (kwdbts::KStatus::SUCCESS != iter->SeekToFirst(ctx)) {
//    return KBStatus::OK();
//  }
//  KTimestamp k_ts = 0;
//  while (iter->Next(ctx, &buffer) == kwdbts::KStatus::SUCCESS) {
//    if (buffer == 0) {
//      break;
//    }
//    KTimestamp data_ts = KTimestamp(buffer);
//    if (k_ts > data_ts) {
//      return KBStatus::ERROR(StatusCode::Invalid,
//                             "Scan[%s,%ld,%ld], invalid k_timestamp : %ld > %ld",
//                             table_name.c_str(), opt.min_ts, opt.max_ts,
//                             k_ts , data_ts);
//    }
//    k_ts = data_ts;
//    // EXPECT_EQ(check_ts, data1);
//    total_rows++;
//  }
//  _scan_rows.add(total_rows);
//
//  KWDB_DURATION(_scan_time);

  return KBStatus::OK();
}
}
