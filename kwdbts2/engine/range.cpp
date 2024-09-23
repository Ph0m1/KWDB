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

uint64_t TSEngineImpl::insertToSnapshots(TsTableEntitiesSnapshot* snapshot) {
  auto now = std::chrono::system_clock::now();
  uint64_t snapshot_id = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  {
    MUTEX_LOCK(tables_lock_);
    // snapshot_id must unique. if generated snaphsot_id already exists, we need change one.
    while (snapshots_.find(snapshot_id) != snapshots_.end()) {
      snapshot_id += 1;
    }
    snapshots_[snapshot_id] = snapshot;
    MUTEX_UNLOCK(tables_lock_);
  }
  return snapshot_id;
}

TsTableEntitiesSnapshot* TSEngineImpl::getSnapshot(uint64_t snapshot_id) {
  MUTEX_LOCK(tables_lock_);
  Defer defer{[&]() {
    MUTEX_UNLOCK(tables_lock_);
  }};
  if (snapshots_.find(snapshot_id) == snapshots_.end()) {
    LOG_FATAL("snapshot[%lu] not found.", snapshot_id);
    return nullptr;
  }
  return snapshots_[snapshot_id];
}

KStatus TSEngineImpl::CreateSnapshotForRead(kwdbContext_p ctx, const KTableKey& table_id,
                                     uint64_t begin_hash, uint64_t end_hash,
                                     const KwTsSpan& ts_span, uint64_t* snapshot_id) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) {
    LOG_ERROR("cannot find table [%lu]", table_id);
    return s;
  }
  TsSnapshotInfo ts_snapshot_info;
  ts_snapshot_info.begin_hash = begin_hash;
  ts_snapshot_info.end_hash = end_hash;
  ts_snapshot_info.ts_span = ts_span;
  ts_snapshot_info.type = 0;
  ts_snapshot_info.table_id = table_id;
  ts_snapshot_info.table = table;
  TsTableEntitiesSnapshot* snapshot = SnapshotFactory::Get()->NewProductor();
  *snapshot_id = insertToSnapshots(snapshot);
  ts_snapshot_info.id = *snapshot_id;
  s = snapshot->Init(ctx, ts_snapshot_info);
  if (s != KStatus::SUCCESS) {
    MUTEX_LOCK(tables_lock_);
    snapshots_.erase(ts_snapshot_info.id);
    MUTEX_UNLOCK(tables_lock_);
    delete snapshot;
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::DeleteSnapshot(kwdbContext_p ctx, uint64_t snapshot_id) {
  TsTableEntitiesSnapshot* cur = getSnapshot(snapshot_id);
  if (cur == nullptr) {
    LOG_ERROR("cannot found snapshot[%lu] while deleting.", snapshot_id);
    return KStatus::FAIL;
  }
  {
    MUTEX_LOCK(tables_lock_);
    snapshots_.erase(snapshot_id);
    MUTEX_UNLOCK(tables_lock_);
    delete cur;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::GetSnapshotNextBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice* data) {
  TsTableEntitiesSnapshot* cur_snapshot = getSnapshot(snapshot_id);
  if (cur_snapshot == nullptr) {
    LOG_ERROR("cannot found snapshot [%lu].", snapshot_id);
    return KStatus::FAIL;
  }
  auto productor = dynamic_cast<TsSnapshotProductor*>(cur_snapshot);
  if (productor == nullptr) {
    LOG_ERROR("cannot parse to Snapshot productor. id [%lu]", snapshot_id);
    return KStatus::FAIL;
  }
  return productor->NextData(ctx, data);
}

KStatus TSEngineImpl::CreateSnapshotForWrite(kwdbContext_p ctx, const KTableKey& table_id,
                                   uint64_t begin_hash, uint64_t end_hash,
                                   const KwTsSpan& ts_span, uint64_t* snapshot_id) {
  std::shared_ptr<TsTable> table{nullptr};
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) {
    LOG_DEBUG("CreateSnapshotForWrite cannot find table [%lu], need create one while writing.", table_id);
  }
  TsSnapshotInfo ts_snapshot_info;
  ts_snapshot_info.type = 1;
  ts_snapshot_info.begin_hash = begin_hash;
  ts_snapshot_info.end_hash = end_hash;
  ts_snapshot_info.ts_span = ts_span;
  ts_snapshot_info.table_id = table_id;
  ts_snapshot_info.table = table;
  TsTableEntitiesSnapshot* snapshot = SnapshotFactory::Get()->NewConsumer();
  *snapshot_id = insertToSnapshots(snapshot);
  ts_snapshot_info.id = *snapshot_id;
  s = snapshot->Init(ctx, ts_snapshot_info);
  if (s != KStatus::SUCCESS) {
    delete snapshot;
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::WriteSnapshotBatchData(kwdbContext_p ctx, uint64_t snapshot_id, TSSlice data) {
  TsTableEntitiesSnapshot* cur_snapshot = getSnapshot(snapshot_id);
  if (cur_snapshot == nullptr) {
    LOG_ERROR("cannot found snapshot [%lu].", snapshot_id);
    return KStatus::FAIL;
  }
  auto consumer = dynamic_cast<TsSnapshotConsumer*>(cur_snapshot);
  if (consumer == nullptr) {
    LOG_ERROR("cannot parse to snapshot consumer. [%lu].", snapshot_id);
    return KStatus::FAIL;
  }
  KStatus s = KStatus::SUCCESS;
  if (UNLIKELY(!consumer->IsTableExist())) {
    // in case storage table is no exist in current node. we should create it.
    SnapshotPayloadData cur_data = SnapshotPayloadData::ParseData(data);
    if (cur_data.type != TsSnapshotDataType::STORAGE_SCHEMA) {
      LOG_ERROR("snapshot[%s] first write is not schema info.", consumer->Print().c_str());
      return KStatus::FAIL;
    }
    roachpb::CreateTsTable meta;
    if (!meta.ParseFromString({cur_data.data_part.data, cur_data.data_part.len})) {
      LOG_ERROR("snapshot[%s] Parse schema From String failed.", consumer->Print().c_str());
      return KStatus::FAIL;
    }
    s = CreateTsTable(ctx, consumer->GetTableId(), &meta, {{default_entitygroup_id_in_dist_v2, 1}});
    // may be create failed, if other thread just created. we need get table at below to make sure table exists.
    if (s == KStatus::SUCCESS) {
      // using current schema version create table. no need to check schema version in consumer writedata.
    }
    std::shared_ptr<TsTable> table;
    s = GetTsTable(ctx, consumer->GetTableId(), table);
    if (s == FAIL) {
      LOG_ERROR("snapshot[%s] cannot Get Table", consumer->Print().c_str());
      return KStatus::FAIL;
    }
    consumer->SetTable(table);
  }
  return consumer->WriteData(ctx, data);
}

KStatus TSEngineImpl::WriteSnapshotSuccess(kwdbContext_p ctx, uint64_t snapshot_id) {
  TsTableEntitiesSnapshot* cur_snapshot = getSnapshot(snapshot_id);
  if (cur_snapshot == nullptr) {
    LOG_ERROR("cannot found snapshot [%lu].", snapshot_id);
    return KStatus::FAIL;
  }
  auto consumer = dynamic_cast<TsSnapshotConsumer*>(cur_snapshot);
  if (consumer == nullptr) {
    LOG_ERROR("cannot parse to snapshot consumer. [%lu].", snapshot_id);
    return KStatus::FAIL;
  }
  return consumer->WriteCommit(ctx);
}

KStatus TSEngineImpl::WriteSnapshotRollback(kwdbContext_p ctx, uint64_t snapshot_id) {
  TsTableEntitiesSnapshot* cur_snapshot = getSnapshot(snapshot_id);
  if (cur_snapshot == nullptr) {
    LOG_ERROR("cannot found snapshot [%lu].", snapshot_id);
    return KStatus::FAIL;
  }
  auto consumer = dynamic_cast<TsSnapshotConsumer*>(cur_snapshot);
  if (consumer == nullptr) {
    LOG_ERROR("cannot parse to snapshot consumer. [%lu].", snapshot_id);
    return KStatus::FAIL;
  }
  return consumer->WriteRollback(ctx);
}

KStatus TSEngineImpl::DeleteRangeEntities(kwdbContext_p ctx, const KTableKey& table_id, const uint64_t& range_group_id,
                                          const HashIdSpan& hash_span, uint64_t* count, uint64_t& mtr_id) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) return s;
  return table->DeleteRangeEntities(ctx, range_group_id, hash_span, count, mtr_id);
}

}  //  namespace kwdbts
