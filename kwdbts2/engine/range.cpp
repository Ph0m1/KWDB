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

#include "engine.h"

namespace kwdbts {

KStatus TSEngineImpl::CreateRangeGroup(kwdbContext_p ctx, const KTableKey& table_id,
                                       roachpb::CreateTsTable* meta, const RangeGroup& range) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  std::shared_ptr<TsEntityGroup> table_range;
  std::vector<TagInfo> tag_schema;
  std::vector<AttributeInfo> metric_schema;
  s = parseMetaSchema(ctx, meta, metric_schema, tag_schema);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = table->CreateEntityGroup(ctx, range, tag_schema, &table_range);
  if (s == KStatus::SUCCESS) {
    MUTEX_LOCK(tables_lock_);
    Defer defer([&]() {
      MUTEX_UNLOCK(tables_lock_);
    });
    auto it = tables_range_groups_.find(table_id);
    if (it != tables_range_groups_.end()) {
      it->second.insert({range.range_group_id, range.typ});
    }
  }
  return s;
}

KStatus TSEngineImpl::DeleteRangeGroup(kwdbContext_p ctx, const KTableKey& table_id, const RangeGroup& range) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  s = table->DropEntityGroup(ctx, range.range_group_id, true);
  if (s == SUCCESS) {
    MUTEX_LOCK(tables_lock_);
    Defer defer([&]() {
      MUTEX_UNLOCK(tables_lock_);
    });
    auto table_it = tables_range_groups_.find(table_id);
    if (table_it != tables_range_groups_.end()) {
      auto range_it = table_it->second.find(range.range_group_id);
      if (range_it != table_it->second.end()) {
        table_it->second.erase(range_it);
      }
    }
  }
  return s;
}

KStatus TSEngineImpl::GetRangeGroups(kwdbContext_p ctx, const KTableKey& table_id, RangeGroups *groups) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  return table->GetEntityGroups(ctx, groups);
}

KStatus TSEngineImpl::UpdateRangeGroup(kwdbContext_p ctx, const KTableKey& table_id, const RangeGroup& range) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  s = table->UpdateEntityGroup(ctx, range);
  if (s == KStatus::SUCCESS) {
    MUTEX_LOCK(tables_lock_);
    Defer defer([&]() {
      MUTEX_UNLOCK(tables_lock_);
    });
    auto table_it = tables_range_groups_.find(table_id);
    if (table_it != tables_range_groups_.end()) {
      auto range_it = table_it->second.find(range.range_group_id);
      if (range_it != table_it->second.end()) {
        range_it->second = range.typ;
      }
    }
  }
  return s;
}

KStatus TSEngineImpl::CreateSnapshot(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                     uint64_t begin_hash, uint64_t end_hash, uint64_t* snapshot_id) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  s = table->CreateSnapshot(ctx, range_group_id, begin_hash, end_hash, snapshot_id);
  return s;
}


KStatus TSEngineImpl::DropSnapshot(kwdbContext_p ctx, const KTableKey& table_id,
                                   uint64_t range_group_id, uint64_t snapshot_id) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  return table->DropSnapshot(ctx, range_group_id, snapshot_id);
}

KStatus TSEngineImpl::GetSnapshotData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                      uint64_t snapshot_id, size_t offset, size_t limit, TSSlice* data, size_t* total) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  return table->GetSnapshotData(ctx, range_group_id, snapshot_id, offset, limit, data, total);
}

KStatus TSEngineImpl::InitSnapshotForWrite(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t range_group_id,
                                           uint64_t snapshot_id, size_t snapshot_size) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  return s;
}

KStatus TSEngineImpl::WriteSnapshotData(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t range_group_id,
                                        uint64_t snapshot_id, size_t offset, TSSlice data, bool finished) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  return table->WriteSnapshotData(ctx, range_group_id, snapshot_id, offset, data, finished);
}

KStatus TSEngineImpl::EnableSnapshot(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                    uint64_t snapshot_id) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return table->EnableSnapshot(ctx, range_group_id, snapshot_id);
}

KStatus TSEngineImpl::DeleteRangeEntities(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t& range_group_id,
                                          const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  return table->DeleteRangeEntities(ctx, range_group_id, hash_span, count, mtr_id);
}

}  //  namespace kwdbts
