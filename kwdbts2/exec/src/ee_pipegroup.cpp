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

#include "ee_pipegroup.h"

#include "ee_handler.h"
#include "ee_kwthd.h"
#include "ee_synchronizer_op.h"
#include "ee_scan_row_batch.h"
#include "ee_cancel_checker.h"

namespace kwdbts {

PipeGroup::~PipeGroup() {}

KStatus PipeGroup::Init(kwdbContext_p ctx) {
  EnterFunc();
  ts_engine_ = ctx->ts_engine;
  fetcher_ = ctx->fetcher;
  is_parallel_pg_ = false;
  relation_ctx_ = ctx->relation_ctx;
  Return(SUCCESS);
}

void PipeGroup::Run(kwdbContext_p ctx) {
  EEPgErrorInfo::ResetPgErrorInfo();
  ctx->ts_engine = ts_engine_;
  ctx->relation_ctx = relation_ctx_;
  ctx->fetcher = fetcher_;
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThd *thd = KNEW KWThd();
  if (!thd) {
    sparent_->FinishPipeGroup(code, EEPgErrorInfo::GetPgErrorInfo());
    return;
  }
  current_thd = thd;
  thd->SetPipeGroup(this);
  code = iterator_->Init(ctx);
  if (code != EE_OK || EEPgErrorInfo::IsError() || CheckCancel(ctx) != SUCCESS) {
    iterator_->Reset(ctx);
    sparent_->FinishPipeGroup(code, EEPgErrorInfo::GetPgErrorInfo());
    thd->Reset();
    SafeDeletePointer(thd);
    return;
  }
  while (true) {
    if (is_stop_ || CheckCancel(ctx) != SUCCESS) {
      iterator_->Reset(ctx);
      sparent_->FinishPipeGroup(code, EEPgErrorInfo::GetPgErrorInfo());
      break;
    }

    DataChunkPtr ptr = nullptr;
    code = iterator_->Next(ctx, ptr);
    if (EEIteratorErrCode::EE_OK != code || EEPgErrorInfo::IsError() || is_stop_ || CheckCancel(ctx) != SUCCESS) {
      iterator_->Reset(ctx);
      sparent_->FinishPipeGroup(code, EEPgErrorInfo::GetPgErrorInfo());
      break;
    }
    ptr->ResetLine();
    sparent_->PushData(ptr);
  }
  thd->Reset();
  SafeDeletePointer(thd);
}

}  // namespace kwdbts
