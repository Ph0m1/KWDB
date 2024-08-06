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

#include "cm_assert.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"

namespace kwdbts {
class Messagemq;
class Processors;
class TSFlowSpec;
class KWThd;
class DmlExec {
  enum TsScanRetState { TS_RT_NORMAL, TS_RT_LASTRECORD, TS_RT_EOF, TS_RT_ERROR };
  struct TsScan {
    k_int32 id;
    k_int32 unique_id;
    TSFlowSpec *fspecs;
    Processors *processors;
    k_bool is_init_pr;
    TsScanRetState ret_state;
    TsScan *next;
    TsScan() {
      id = -1;
      unique_id = 0;
      fspecs = nullptr;
      processors = nullptr;
      is_init_pr = KFALSE;
      ret_state = TsScanRetState::TS_RT_NORMAL;
      next = nullptr;
    }
  };

 public:
  DmlExec():tsscan_head_(nullptr), tsscan_end_{nullptr} {}
  ~DmlExec();

  // dml exec query func
  static KStatus ExecQuery(kwdbContext_p ctx, QueryInfo *req, RespInfo *resp);

 private:
  // create ts scan for execute
  KStatus CreateTsScan(kwdbContext_p ctx, TsScan **tsScan);
  // destroy ts scan
  void DestroyTsScan(TsScan *tsScan);
  void ClearTsScans(kwdbContext_p ctx);
  // setup
  KStatus Setup(kwdbContext_p ctx, k_char *message, k_uint32 len,
                       k_int32 id, k_int32 uniqueID, RespInfo *resp);
  // execute include init ,exec and encode
  KStatus InnerNext(kwdbContext_p ctx, TsScan *tsScan, bool isPG, RespInfo *resp);
  KStatus Next(kwdbContext_p ctx, k_int32 id, bool isPG, RespInfo *resp);
  KStatus VirtualNext(kwdbContext_p ctx, k_int32 id, bool isPG, RespInfo *resp);
  // new thd
  KStatus Init();
  inline void DisposeError(kwdbContext_p ctx, QueryInfo *return_info);

 private:
  TsScan *tsscan_head_;
  TsScan *tsscan_end_;
  bool   first_next_{true};
  KWThd  *thd_{nullptr};
};
};  // namespace kwdbts

