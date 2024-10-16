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

#include <memory>
#include <string>
#include <vector>

#include "engine.h"
#include "libkwdbts2.h"

namespace kwdbts {

RangeGroup test_range{default_entitygroup_id_in_dist_v2, 0};

TSEngine* CreateTestTsEngine(kwdbContext_p ctx, const string& db_path) {
  EngineOptions opts;
  opts.wal_level = 0;
  opts.db_path = db_path;
  auto* ts_engine = static_cast<TSEngine*>(ctx->ts_engine);
  TSEngineImpl::OpenTSEngine(ctx, db_path, opts, &ts_engine);
  ctx->ts_engine = ts_engine;
  return ts_engine;
}

void CloseTestTsEngine(kwdbContext_p ctx) {
  auto* ts_engine = static_cast<TSEngine*>(ctx->ts_engine);
  TSEngineImpl::CloseTSEngine(ctx, ts_engine);
}
}  // namespace kwdbts
