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

#include "ee_cancel_checker.h"

#include <libkwdbts2.h>

#include "ee_global.h"
#include "pgcode.h"

namespace kwdbts {

KStatus CheckCancel(kwdbContext_p ctx) {
  if (isCanceledCtx(ctx->relation_ctx)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_QUERY_CANCELED,
                                  "query execution canceled");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
