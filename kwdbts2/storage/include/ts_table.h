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

#include <map>
#include <memory>
#include <utility>
#include <list>
#include <set>
#include <unordered_map>
#include <string>
#include <vector>
#include "ts_common.h"
#include "libkwdbts2.h"
#include "cm_kwdb_context.h"
#include "cm_func.h"
#include "lg_api.h"
#include "iterator.h"
#include "tag_iterator.h"
#include "payload.h"
#include "mmap/mmap_root_table_manager.h"
#include "mmap/mmap_tag_column_table.h"
#include "st_group_manager.h"
#include "st_wal_internal_log_structure.h"
#include "lt_rw_latch.h"
#include "ts_snapshot.h"
#include "ts_table_snapshot.h"

namespace kwdbts {

class TsEntityGroup;
class TsTableSnapshot;
class TsIterator;

// in distributed Verison2, every ts table has just one entitygroup
const uint64_t default_entitygroup_id_in_dist_v2 = 1;


class TsTable {
 public:
  TsTable() = delete;

  TsTable(kwdbContext_p ctx, const string& db_path, const KTableKey& table_id);

  virtual ~TsTable();

  virtual KStatus Init(kwdbContext_p ctx, std::unordered_map<uint64_t, int8_t>& range_groups,
                       ErrorInfo& err_info = getDummyErrorInfo());

  /**
   * @brief Is the current table created and does it really exist
   *
   * @return bool
   */
  virtual bool IsExist() {
    return this->entity_bt_manager_ != nullptr;
  }

  /**
   * @brief Query Table Column Definition
   *
   * @return std::vector<AttributeInfo>
   */
  KStatus GetDataSchemaIncludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema);

  /**
   * @brief Query Table Column Definition
   *
   * @return std::vector<AttributeInfo>
   */
  KStatus GetDataSchemaExcludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema);

  /**
   * @brief Query Table tags Definition
   *
   * @return std::vector<AttributeInfo>
   */
  KStatus GetTagSchema(kwdbContext_p ctx, RangeGroup range, std::vector<TagColumn*>* tag_schema);

  // convert schema info to protobuf
  KStatus GenerateMetaSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta, std::vector<AttributeInfo>& metric_schema,
                             std::vector<TagInfo>& tag_schema);
  /**
   * @brief get table id
   *
   * @return KTableKey
   */
  virtual KTableKey GetTableId() {
    return table_id_;
  }

  virtual uint32_t GetCurrentTableVersion();

  /**
   * @brief create ts table
   * @param[in] metric_schema schema
   *
   * @return KStatus
   */
  virtual KStatus Create(kwdbContext_p ctx, vector<AttributeInfo>& metric_schema, uint32_t ts_version = 1,
                         uint64_t partition_interval = kwdbts::EngineOptions::iot_interval);

  /**
   * @brief Create an EntityGroup corresponding to Range
   * @param[in] range
   * @param[in] tag_schema
   * @param[out] entity_group
   *
   * @return KStatus
   */
  virtual KStatus CreateEntityGroup(kwdbContext_p ctx, RangeGroup range, vector<TagInfo>& tag_schema,
                                    std::shared_ptr<TsEntityGroup>* entity_group);

  /**
   * @brief get all Entity Group
   * @param[out] groups
   *
   * @return KStatus
   */
  KStatus GetEntityGroups(kwdbContext_p ctx, RangeGroups *groups);

  /**
   * @brief Update local range group types
   * @param[in] range
   *
   * @return KStatus
   */
  virtual KStatus UpdateEntityGroup(kwdbContext_p ctx, const RangeGroup& range);

  /**
   * @brief get entitygroup
   * @param[in] range
   * @param[out] entity_group
   *
   * @return KStatus
   */
  virtual KStatus
  GetEntityGroup(kwdbContext_p ctx, uint64_t range_group_id, std::shared_ptr<TsEntityGroup>* entity_group);
  virtual KStatus GetEntityGroupByHash(kwdbContext_p ctx, uint16_t hashpoint, uint64_t *range_group_id,
                       std::shared_ptr<TsEntityGroup>* entity_group);
  /**
   * @brief get entitygroup by primary key of entity ,which is parsed from payload
   * @param[in]   primary_key  parsed from payload.
   * @param[out]  entity_group
   *
   * @return KStatus
   */
  KStatus GetEntityGroupByPrimaryKey(kwdbContext_p ctx, const TSSlice& primary_key,
                                    std::shared_ptr<TsEntityGroup>* entity_group);

  /**
   * @brief put data to ts table
   * @param[in] range_group_id
   * @param[in] payload
   * @param[in] payload_num
   * @param[in] dedup_rule deduplicate policy
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus PutData(kwdbContext_p ctx, uint64_t range_group_id, TSSlice* payload, int payload_num,
                          uint64_t mtr_id, DedupResult* dedup_result, const DedupRule& dedup_rule);

  KStatus PutDataWithoutWAL(kwdbContext_p ctx, uint64_t range_group_id, TSSlice* payload, int payload_num,
                            uint64_t mtr_id, DedupResult* dedup_result, const DedupRule& dedup_rule);
  /**
  * @brief Flush caches the WAL of all EntityGroups in the current timeline to a disk file
  *
  * @return KStatus
  */
  virtual KStatus FlushBuffer(kwdbContext_p ctx);

  /**
  * @brief Start the checkpoint operation for all EntityGroups in the current timeline.
  *
  * @return KStatus
  */
  virtual KStatus CreateCheckpoint(kwdbContext_p ctx);

  /**
  * @brief Start the log recovery operation for all EntityGroups in the current timeline.
  *
  * @return KStatus
  */
  virtual KStatus Recover(kwdbContext_p ctx, const std::map<uint64_t, uint64_t>& applied_indexes);

  /**
   * @brief get all leader entity group
   * @param[out] leader_entity_groups
   *
   * @return KStatus
   */
  virtual KStatus GetAllLeaderEntityGroup(kwdbContext_p ctx,
                                          std::vector<std::shared_ptr<TsEntityGroup>>* leader_entity_groups);

  /**
   * @brief delete certain range group.
   * @param[in] range_group_id RangeGroupID
   * @param[in] sync  wait for success
   *
   * @return KStatus
   */
  virtual KStatus DropEntityGroup(kwdbContext_p ctx, uint64_t range_group_id, bool sync);

  /**
  * @brief Delete the entire table
 * @param[in] is_force Do you want to force deletion: do not wait for threads that are reading or writing to end
  *
  * @return KStatus
  */
  virtual KStatus DropAll(kwdbContext_p ctx, bool is_force = false);

  /**
   * @brief Compress the segment whose maximum timestamp in the time series entity group is less than ts
   * @param[in] ctx Database Context
   * @param[in] ts A timestamp that needs to be compressed. If ts=INT64_MAX, 
   *            all segments will be compressed, including the current one.
   *
   * @return KStatus
   */
  virtual KStatus Compress(kwdbContext_p ctx, const KTimestamp& ts, ErrorInfo &err_info);


  std::string GetStoreDirectory() {
    return db_path_ + tbl_sub_path_;
  }

  /**
   * @brief get entitygroup by primary key of entity.
   *        in function, we will scan all entitygroups, if not found ,we will return default entitygroup.
   * @param[in]  primary_key   Entity primary key
   * @param[out] entity_grp_id which entitygroup the entity belong to.
   *
   * @return KStatus
   */
  KStatus GetEntityGrpByPriKey(kwdbContext_p ctx, const TSSlice& primary_key, uint64_t* entity_grp_id);

  /**
   * @brief get all entity info of hash span.
   *        in function, we will scan all entitygroups, find matched entities.
   * @param[in] begin_hash,end_hash Entity primary tag hashID
   * @param[out] entity_store
   *
   * @return KStatus
   */
  KStatus GetEntityIndex(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                        std::vector<EntityResultIndex> &entity_store);

  KStatus GetEntityIndexWithRowNum(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                  std::vector<std::pair<int, EntityResultIndex>> &entity_tag);

  KStatus GetAvgTableRowSize(kwdbContext_p ctx, uint64_t* row_size);

  virtual KStatus GetDataVolume(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                const KwTsSpan& ts_span, uint64_t* volume);

  virtual KStatus GetDataVolumeHalfTS(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                const KwTsSpan& ts_span, timestamp64* half_ts);

  /**
   * @brief drop all data in range. if table is empty,we will drop table directory at same time.
   * @param[in] ts_span   timestamp span
   * @param[in] begin_hash,end_hash Entity primary tag hashID
   *
   * @return KStatus
   */
  KStatus DeleteTotalRange(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash, KwTsSpan ts_span, uint64_t mtr_id);

  /**
   * @brief row-based payload convert to col-based payload
   * @param[in] payload_row  row-based payload struct.
   * @param[out] payload    col-based payload
   *
   * @return KStatus
   */
  virtual KStatus ConvertRowTypePayload(kwdbContext_p ctx,  TSSlice payload_row, TSSlice* payload);

  /**
   * @brief Get range row count.
   * @param[in] begin_hash,end_hash Entity primary tag hashID
   * @param[in] ts_span   timestamp span
   *
   * @return KStatus
   */
  KStatus GetRangeRowCount(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                            KwTsSpan ts_span, uint64_t* count);

  /**
   * @brief Create a temporary snapshot of range_group in the local temporary directory, usually used for data migration.
   * @param[in] range_group_id RangeGroupID
   * @param[in] begin_hash,end_hash Entity primary tag hashID
   * @param[out] snapshot_id
   *
   * @return KStatus
   */
  KStatus CreateSnapshot(kwdbContext_p ctx, uint64_t range_group_id, uint64_t begin_hash, uint64_t end_hash,
                                 uint64_t* snapshot_id);

  /**
   * @brief Drop the temporary snapshot of range_group in the local temporary directory after data migration finished.
   *        This function is called to drop snapshot at source node usually, because destination node's snapshot data
   *        which get from source node will delete automatically after snapshot applied successfully.
   * @param[in] range_group_id RangeGroupID
   * @param[in] snapshot_id Temporary snapshot id
   * @return KStatus
   */
  KStatus DropSnapshot(kwdbContext_p ctx, uint64_t range_group_id, uint64_t snapshot_id);

  /**
   * @brief Get snapshot data from source node and send to destination node, snapshot will be built and compressed
   *        when the function called at first time. And since the snapshot may be relatively large,
   *        the size of the snapshot data block taken at a time is limited,
   *        therefore, getting a full snapshot may call this function multiple times.
   * @param[in] range_group_id The range group ID of snapshot
   * @param[in] snapshot_id ID of snapshot
   * @param[in] offset The offset of the snapshot data taken by this call
   * @param[in] limit The size limit of the data block to be taken by this call
   * @param[out] data The data block taken by this call
   * @param[in] total total size of compressed file
   * @return KStatus
   */
  KStatus GetSnapshotData(kwdbContext_p ctx, uint64_t range_group_id, uint64_t snapshot_id,
                                  size_t offset, size_t limit, TSSlice* data, size_t* total);

  /**
   * @brief Since `GetSnapshotData` take a limited size data block at a time, each time `WriteSnapshotData` get
   *        data block appended to the snapshot file according to the offset, when the transfer is completed,
   *        the data is decompressed and the snapshot data is written to the destination node.
   * @param[in] range_group_id The range group ID of snapshot
   * @param[in] snapshot_id ID of snapshot
   * @param[in] offset The offset of the snapshot data obtained by this call
   * @param[in] data The data block obtained by this call
   * @param[in] finished The flag of transfer completed
   * @return KStatus
   */
  KStatus WriteSnapshotData(kwdbContext_p ctx, const uint64_t range_group_id, uint64_t snapshot_id,
                            size_t offset, TSSlice data, bool finished);

  /**
   * @brief After the data is received, the snapshot data is written by `ApplySnapshot`.
   * @param[in] range_group_id The range group ID of snapshot
   * @param[in] snapshot_id ID of snapshot
   * @param[in] delete_after_apply Whether to delete the received compressed snapshot data.
   * @return KStatus
   */
  KStatus ApplySnapshot(kwdbContext_p ctx,  uint64_t range_group_id, uint64_t snapshot_id, bool delete_after_apply  = true);

  /**
   * @brief  `EnableSnapshot` takes effect on the written snapshot.
   * @param[in] range_group_id The range group ID of snapshot
   * @param[in] snapshot_id ID of snapshot
   * @return
   */
  KStatus EnableSnapshot(kwdbContext_p ctx,  uint64_t range_group_id, uint64_t snapshot_id);

  /**
    * @brief Perform data reorganization on partitioned data within a specified time range.
    * @param[in] range_group_id RangeGroupID
     * @param[in] ts_span  metrics time span
               Explanation: The data reorganization logic is executed on a time partition basis,
               and time data that cannot cover the complete time partition will not undergo reorganization logic.
               For example, if the time partition unit is 1 day and the [start, end] condition passed in is
               [8pm on the 1st, 5pm on the 4th], the data in the [2,3] day partition will be reorganized,
               while the data in the [1] day and [4] day partitions will not be reorganized.
    *
    * @return KStatus
    */
  KStatus CompactData(kwdbContext_p ctx, uint64_t range_group_id, const KwTsSpan& ts_span);

  /**
   * @brief Delete data within a hash range, usually used for data migration.
   * @param[in] range_group_id RangeGroupID
   * @param[in] hash_span The range of hash IDs to be deleted from the data
   * @param[out] count delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteRangeEntities(kwdbContext_p ctx, const uint64_t& range_group_id, const HashIdSpan& hash_span,
                                      uint64_t* count, uint64_t mtr_id);

  /**
   * @brief Delete data based on the hash id range and timestamp range.
   * @param[in] range_group_id RangeGroupID
   * @param[in] hash_span The range of hash IDs to be deleted from the data
   * @param[in] ts_spans The range of timestamps to be deleted from the data
   * @param[out] count The number of rows of data that have been deleted
   * @param[in] mtr_id Mini-transaction id for TS table.
   * @return
   */
  virtual KStatus DeleteRangeData(kwdbContext_p ctx, uint64_t range_group_id, HashIdSpan& hash_span,
                                  const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id);

  /**
   * @brief Delete data based on the primary tag and timestamp range.
   * @param[in] range_group_id RangeGroupID
   * @param[in] primary_tag The primary tag of the deleted data
   * @param[in] ts_spans The range of timestamps to be deleted from the data
   * @param[out] count The number of rows of data that have been deleted
   * @param[in] mtr_id Mini-transaction id for TS table.
   * @return KStatus
   */
  virtual KStatus DeleteData(kwdbContext_p ctx, uint64_t range_group_id, std::string& primary_tag,
                             const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id);

  /**
   * @brief Delete expired data whose timestamp is older than the end_ts in all entity group,
   * and data deletion is based on time partition as the smallest unit, partition will be deleted
   * until the latest data in this partition is expired.
   * @param[in] end_ts end timestamp of expired data
   * @return KStatus
   */
  virtual KStatus DeleteExpiredData(kwdbContext_p ctx, int64_t end_ts);

  /**
    * @brief Create the iterator TsIterator for the timeline and query the data of all entities within the Leader EntityGroup
    * @param[in] ts_span
    * @param[in] scan_cols  column to read
    * @param[in] scan_agg_types Read column agg type array for filtering block statistics information
    * @param[in] table_version The maximum table version that needs to be queried
    * @param[out] TsIterator*
    */
  virtual KStatus GetIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                              std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                              std::vector<Sumfunctype> scan_agg_types, k_uint32 table_version,
                              TsTableIterator** iter, std::vector<timestamp64> ts_points,
                              bool reverse, bool sorted);

  /**
    * @brief Create the iterator TsIterator in the order of entity_ids for the timeline and query the data of all entities within the Leader EntityGroup for multiple model processing
    * @param[in] ts_span
    * @param[in] scan_cols  column to read
    * @param[in] scan_agg_types Read column agg type array for filtering block statistics information
    * @param[in] table_version The maximum table version that needs to be queried
    * @param[out] TsIterator*
    */
  virtual KStatus GetIteratorInOrder(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                              std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                              std::vector<Sumfunctype> scan_agg_types, k_uint32 table_version,
                              TsTableIterator** iter, std::vector<timestamp64> ts_points,
                              bool reverse, bool sorted);

  /**
   * @brief get entityId List
   * @param[in] primary_tags primaryTag
   * @param[in] scan_tags    scan tag
   * @param[out] entityId List
   * @param[out] res
   * @param[out] count
   *
   * @return KStatus
   */
  virtual KStatus
  GetEntityIdList(kwdbContext_p ctx, const std::vector<void*>& primary_tags, const std::vector<uint32_t>& scan_tags,
                  std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count);


  /**
   * @brief Create an iterator TsIterator for Tag tables
   * @param[in] scan_tags tag index
   * @param[out] TagIterator**
   */
  virtual KStatus GetTagIterator(kwdbContext_p ctx,
                                 std::vector<uint32_t> scan_tags,
                                 const vector<uint32_t> hps,
                                 TagIterator** iter, k_uint32 table_version);

  KStatus GetTagIterator(kwdbContext_p ctx, std::vector<uint32_t> scan_tags,
                                TagIterator** iter, k_uint32 table_version) {
    return GetTagIterator(ctx, scan_tags, {}, iter, table_version);
  }
  /**
   * @brief create MetaIterator
   * @param[out] MetaIterator**
   */
  virtual KStatus GetMetaIterator(kwdbContext_p ctx, MetaIterator** iter, k_uint32 table_version);

  virtual KStatus AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                             uint32_t cur_version, uint32_t new_version, string& msg);

  virtual KStatus AlterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                                uint32_t cur_version, uint32_t new_version, string& msg);

  virtual KStatus AlterTableCol(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                                uint32_t cur_version, uint32_t new_version, string& msg);

  KStatus AddSchemaVersion(kwdbContext_p ctx, roachpb::CreateTsTable* meta, MMapMetricsTable ** version_schema);

  virtual KStatus UndoAlterTable(kwdbContext_p ctx, LogEntry* log);

  virtual KStatus undoAlterTable(kwdbContext_p ctx, AlterType alter_type,
                                 roachpb::KWDBKTSColumn* column, uint32_t cur_version, uint32_t new_version);

  virtual KStatus undoAlterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                                    uint32_t cur_version, uint32_t new_version);

  virtual KStatus undoAlterTableCol(kwdbContext_p ctx, uint32_t cur_version, uint32_t new_version);

  virtual KStatus AlterPartitionInterval(kwdbContext_p ctx, uint64_t partition_interval);

  virtual uint64_t GetPartitionInterval();

  void SetDropped();

  bool IsDropped();

  uint64_t partitionInterval() {
    return entity_bt_manager_->GetPartitionInterval();
  }

  MMapRootTableManager* GetRootTableManager() {
    return entity_bt_manager_;
  }

  KStatus GetEntityNum(kwdbContext_p ctx, uint64_t* entity_num);

  KStatus GetDataRowNum(kwdbContext_p ctx, const KwTsSpan& ts_span, uint64_t* row_num);

  /**
    * @brief clean ts table
    *
    * @return KStatus
    */
  virtual KStatus TSxClean(kwdbContext_p ctx);

  inline MMapRootTableManager* GetMetricsTableMgr() {
    return entity_bt_manager_;
  }

 protected:
  string db_path_;
  KTableKey table_id_;
  string tbl_sub_path_;

//  MMapTagColumnTable* tag_bt_;
  MMapRootTableManager* entity_bt_manager_;

  std::unordered_map<uint64_t, std::shared_ptr<TsEntityGroup>> entity_groups_{};

  std::atomic_bool is_dropped_;

  // Create an internal method for an EntityGroup instance, subclasses can overload the structEntityGroup method,
  // and create subclasses of TsEntityGroup
  KStatus newEntityGroup(kwdbContext_p ctx, RangeGroup hash_range, const string& range_tbl_sub_path,
                         std::shared_ptr<TsEntityGroup>* ent_group);

  virtual void constructEntityGroup(kwdbContext_p ctx,
                                    const RangeGroup& hash_range,
                                    const string& range_tbl_sub_path,
                                    std::shared_ptr<TsEntityGroup>* entity_group) {
    auto t_range = std::make_shared<TsEntityGroup>(ctx, entity_bt_manager_, db_path_, table_id_,
                                                   hash_range, range_tbl_sub_path);
    *entity_group = std::move(t_range);
  }

  void UpdateTagTsVersion(uint32_t new_version);

 public:
  // Save the correspondence between snapshot ID and snapshot table under this table
  std::unordered_map<uint64_t, std::shared_ptr<TsTableSnapshot>>  snapshot_manage_pool_;
  std::unordered_map<uint64_t, size_t>  snapshot_get_size_pool_;

  // TODO(lfl): 此hash算法和GO层一致，后续修改为此算法
  static uint32_t GetConsistentHashId(const char* data, size_t length);

  static MMapRootTableManager* CreateMMapRootTableManager(string& db_path, string& tbl_sub_path, KTableKey table_id,
                                                          vector<AttributeInfo>& schema, uint32_t table_version,
                                                          uint64_t partition_interval, ErrorInfo& err_info);

  static MMapRootTableManager* OpenMMapRootTableManager(string& db_path, string& tbl_sub_path, KTableKey table_id,
                                                        ErrorInfo& err_info);

 protected:
  using TsTableEntityGrpsRwLatch = KRWLatch;
  TsTableEntityGrpsRwLatch* entity_groups_mtx_;

 private:
  using TsTableSnapshotLatch = KLatch;
  TsTableSnapshotLatch* snapshot_manage_mtx_;

  void latchLock() {
    MUTEX_LOCK(snapshot_manage_mtx_);
  }

  void latchUnlock() {
    MUTEX_UNLOCK(snapshot_manage_mtx_);
  }
};

// PutAfterProcessInfo records the information that needs to be processed after writing.
struct PutAfterProcessInfo {
  std::vector<BlockSpan> spans;  // Record the requested space when writing, and roll back when writing fails
  // When writing a record for deduplication, the MetricRowID of the deleted record needs to be deduplicated.
  // Mark deletion after successful writing
  std::vector<MetricRowID> del_real_rows;
};

struct PartitionPayload {
  int32_t start_row;
  int32_t end_row;
};

class TsEntityGroup {
 public:
  TsEntityGroup() = delete;

  explicit TsEntityGroup(kwdbContext_p ctx, MMapRootTableManager*& root_table_manager, const string& db_path,
                         const KTableKey& table_id, const RangeGroup& range, const string& tbl_sub_path);

  virtual ~TsEntityGroup();

  /**
   * @brief create TsTableRange
   * @param[in] tag_schema   tags schema
   * @param[in] metrics_tb   entity object
   *
   * @return KStatus
   */
  virtual KStatus Create(kwdbContext_p ctx, vector<TagInfo>& tag_schema);

  /**
   * @brief Open and initialize TsTableRange
   * @param[in] entity_bt   entity object
   *
   * @return KStatus
   */
  virtual KStatus OpenInit(kwdbContext_p ctx);

  virtual KStatus Drop(kwdbContext_p ctx, bool is_force = false);

  /**
   * @brief Compress the segment whose maximum timestamp in the time series entity group is less than ts
   * @param[in] ctx Database Context
   * @param[in] ts A timestamp that needs to be compressed. If ts=INT64_MAX, 
   *            all segments will be compressed, including the current one.
   *
   * @return KStatus
   */
  virtual KStatus Compress(kwdbContext_p ctx, const KTimestamp& ts, ErrorInfo &err_info);

  /**
   * @brief Write entity tags values and support tag value modification.
   *            If the primary tag does not exist, write the tag data.
   *            If the primary tag already exists and there are other tag values in the payload, update the tag value.
   *            If there is temporal data in the payload, write it to the data table.
   * @param[in] payload   The PayLoad object with tag value contains primary tag information.
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus PutEntity(kwdbContext_p ctx, TSSlice& payload_data, uint64_t mtr_id);

  /**
   * @brief  PutData writes the Tag value and time series data to the entity
   *
   * @param[in] payload  Comprises tag values and time-series data
   *
   * @return Return the status code of the operation, indicating its success or failure.
   */
  virtual KStatus PutData(kwdbContext_p ctx, TSSlice payload_data);

  /**
   * PutData writes the Tag value and time series data to the entity
   *
   * @param ctx Database Context
   * @param payload_data  Comprises tag values and time-series data
   * @param mini_trans_id A unique transaction ID is recorded to ensure data consistency.
   * @param dedup_result Stores the deduplication results of this operation, exclusively for Reject and Discard modes.
   * @param dedup_rule The deduplication rule defaults to OVERRIDE.
   * @return Return the status code of the operation, indicating its success or failure.
   */
  virtual KStatus PutData(kwdbContext_p ctx, TSSlice payload_data, TS_LSN mini_trans_id, DedupResult* dedup_result,
                          DedupRule dedup_rule = DedupRule::OVERRIDE);

  /**
   * PutData writes the Tag value and time series data to the entity
   *
   * @param[in] payloads Comprises tag values and time-series data
   * @param[in] length  The length of the payloads array
   * @param[in] mtr_id Mini-transaction id for TS table.
   * @param dedup_result Stores the deduplication results of this operation, exclusively for Reject and Discard modes.
   * @param dedup_rule The deduplication rule defaults to OVERRIDE.
   * @return Return the status code of the operation, indicating its success or failure.
   */
  virtual KStatus PutData(kwdbContext_p ctx, TSSlice* payloads, int length, uint64_t mtr_id, DedupResult* dedup_result,
                          DedupRule dedup_rule = DedupRule::OVERRIDE);

  KStatus PutDataWithoutWAL(kwdbContext_p ctx, TSSlice payload, TS_LSN mini_trans_id,
                            DedupResult* dedup_result, DedupRule dedup_rule);

  /**
   * get all partition times. not partition object.
   *
   * @param[out] subgrp_partitions
   * @return Return the status code of the operation, indicating its success or failure.
   */
  virtual KStatus GetAllPartitions(kwdbContext_p ctx, std::unordered_map<SubGroupID,
                                  std::vector<timestamp64>>* subgrp_partitions);

  /**
   * @brief Mark the deletion of temporal data within the specified time range for range entities.
   * @param[in] table_id   ID
   * @param[in] hash_span entity
   * @param[in] ts_spans time range to delete
   * @param[out] count  delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteRangeData(kwdbContext_p ctx, const HashIdSpan& hash_span, TS_LSN lsn,
                                  const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpans>* del_rows,
                                  uint64_t* count, uint64_t mtr_id, bool evaluate_del);

  /**
   * @brief Mark the deletion of temporal data within a specified time range for a certain entity.
   * @param[in] table_id   ID
   * @param[in] primary_tag entity
   * @param[in] ts_spans time range to delete
   * @param[out] count  delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteData(kwdbContext_p ctx, const string& primary_tag, TS_LSN lsn,
                             const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpan>* rows,
                             uint64_t* count, uint64_t mtr_id, bool evaluate_del);

  /**
   * DeleteExpiredData deletes expired partition data whose timestamp is older than the end_ts
   * @param ctx database context
   * @param[in] end_ts end timestamp of expired data
   * @return KStatus code
   */
  virtual KStatus DeleteExpiredData(kwdbContext_p ctx, int64_t end_ts);

  /**
   * @brief Delete Entity and sequential data.
   * @param[in] table_id   ID
   * @param[in] primary_tag entity
   * @param[out] count  delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteEntity(kwdbContext_p ctx, const string& primary_tag, uint64_t* count, uint64_t mtr_id);

  /**
   * @brief Batch deletion of Entity and sequential data, generally used for Range migration.
   * @param[in] table_id   ID
   * @param[in] primary_tag entities
   * @param[out] count  delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteEntities(kwdbContext_p ctx, const std::vector<std::string>& primary_tags,
                                 uint64_t* count, uint64_t mtr_id);

  /**
   * @brief Delete an Entity and data within a hash range, usually used for data migration.
   * @param[in] hash_span Entity
   * @param[in] count  delete row num
   * @param[in] mtr_id Mini-transaction id for TS table.
   *
   * @return KStatus
   */
  virtual KStatus DeleteRangeEntities(kwdbContext_p ctx, const HashIdSpan& hash_span, uint64_t* count, uint64_t mtr_id);

  /**
   * @brief Obtain entityId List based on conditions
   * @param[in] primary_tags primaryTag
   * @param[in] scan_tags    scan tag
   * @param[out] entityId List
   * @param[out] res
   * @param[out] count
   *
   * @return KStatus
   */
  virtual KStatus
  GetEntityIdList(kwdbContext_p ctx, const std::vector<void*>& primary_tags, const std::vector<uint32_t>& scan_tags,
                  std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count);

  /**
   * @brief Creating an Iterator for Timetable
   * @param[in] entity_id entity id
   * @param[in] ts_span
   * @param[in] scan_cols column index
   * @param[in] scan_agg_types Read column agg type array for filtering block statistics information
   * @param[in] table_version The maximum table version that needs to be queried
   * @param[out] TsIterator*
   */
  virtual KStatus GetIterator(kwdbContext_p ctx, SubGroupID sub_group_id,
                              vector<uint32_t> entity_ids,
                              std::vector<KwTsSpan> ts_spans,
                              std::vector<k_uint32> scan_cols,
                              std::vector<k_uint32> ts_scan_cols,
                              std::vector<Sumfunctype> scan_agg_types,
                              uint32_t table_version, TsIterator** iter,
                              std::shared_ptr<TsEntityGroup> entity_group,
                              std::vector<timestamp64> ts_points,
                              bool reverse, bool sorted, bool compaction);

  /**
   * @brief Create an iterator TsIterator for Tag tables
   * @param[in] scan_tags tag index
   * @param[out] TagIterator**
   */
  virtual KStatus GetTagIterator(kwdbContext_p ctx, std::vector<uint32_t>& scan_tags, EntityGroupTagIterator** iter);
  virtual KStatus GetTagIterator(kwdbContext_p ctx, std::vector<k_uint32>& scan_tags, EntityGroupTagIterator** iter,
                          const std::vector<uint32_t>& hps);
  /**
   * @brief create EntityGroupMetaIterator
   * @param[out] EntityGroupMetaIterator**
   */
  virtual KStatus GetMetaIterator(kwdbContext_p ctx, EntityGroupMetaIterator** iter);

  /**
  * @brief Flush cache the current EntityGroup's WAL to a disk file.
  *
  * @return KStatus
  */
  virtual KStatus FlushBuffer(kwdbContext_p ctx);

  /**
  * @brief Start the checkpoint operation for the current EntityGroup.
  *
  * @return KStatus
  */
  virtual KStatus CreateCheckpoint(kwdbContext_p ctx);

  inline RangeGroup& HashRange() {
    return range_;
  }

  /**
   * @brief Obtain metadata information for tags
   */
  const std::vector<TagColumn*>& GetSchema() const {
    return tag_bt_->getSchemaInfo();
  }

  /**
    * @brief Clean the timeline of the current entity group.
    *
    * @return KStatus
    */
  KStatus TSxClean(kwdbContext_p ctx);

  virtual KStatus AlterTagInfo(kwdbContext_p ctx, TagInfo& new_tag_schema,
                               ErrorInfo& err_info, uint32_t new_table_version = 1);

  virtual KStatus AddTagInfo(kwdbContext_p ctx, TagInfo& new_tag_schema,
                             ErrorInfo& err_info, uint32_t new_table_version = 1);

  virtual KStatus DropTagInfo(kwdbContext_p ctx, TagInfo& tag_schema,
                              ErrorInfo& err_info, uint32_t new_table_version = 1);

  virtual KStatus UndoAddTagInfo(kwdbContext_p ctx, TagInfo& tag_schema, uint32_t new_table_version = 1);

  virtual KStatus UndoDropTagInfo(kwdbContext_p ctx, TagInfo& tag_schema, uint32_t new_table_version = 1);

  virtual KStatus UndoAlterTagInfo(kwdbContext_p ctx, TagInfo& origin_tag_schema, uint32_t new_table_version = 1);

  /**
   * @brief Convert roachpb::KWDBKTSColumn to attribute info.
   * @param col[in] roachpb::KWDBKTSColumn column
   * @param attr_info[out] attribute info
   * @param first_col[in]  Whether it's the first column or not
   * @return KStatus
   */
  static KStatus GetColAttributeInfo(kwdbContext_p ctx, const roachpb::KWDBKTSColumn& col,
                                     struct AttributeInfo& attr_info, bool first_col);

  /**
   * @brief Convert attribute info to roachpb::KWDBKTSColumn.
   * @param attr_info[in] attribute info
   * @param col[out]  roachpb::KWDBKTSColumn column
   * @return KStatus
   */
  static KStatus GetMetricColumnInfo(kwdbContext_p ctx, struct AttributeInfo& attr_info, roachpb::KWDBKTSColumn& col);

  /**
   * @brief Convert tag info to roachpb::KWDBKTSColumn.
   * @param tag_info[in] tag info
   * @param col[out] roachpb::KWDBKTSColumn column
   * @return KStatus
   */
  static KStatus GetTagColumnInfo(kwdbContext_p ctx, struct TagInfo& tag_info, roachpb::KWDBKTSColumn& col);

  virtual void SetAllSubgroupAvailable() {
    ebt_manager_->SetSubgroupAvailable();
  }

  // for test
  inline SubEntityGroupManager* GetSubEntityGroupManager() {
    return ebt_manager_;
  }

  inline MMapTagColumnTable* GetSubEntityGroupTagbt() {
    return tag_bt_;
  }

  [[nodiscard]] uint64_t GetOptimisticReadLsn() const {
    return optimistic_read_lsn_.load();
  }

  void SetOptimisticReadLsn(uint64_t optimistic_read_lsn) {
    optimistic_read_lsn_.store(optimistic_read_lsn);
  }

  MMapRootTableManager* GetRootTableManager() {
    return root_bt_manager_;
  }

  MMapRootTableManager*& root_bt_manager_;

  void RdDropLock() {
    RW_LATCH_S_LOCK(drop_mutex_);
  }

  void DropUnlock() {
    RW_LATCH_UNLOCK(drop_mutex_);
  }

  void UpdateTagVersion(uint32_t table_version) {
    tag_bt_->mutexLock();
    tag_bt_->SetTableVersion(table_version);
    tag_bt_->mutexUnlock();
  }

  size_t GetTagCount() const  {
    return tag_bt_->actual_size();
  }

 protected:
  string db_path_;
  KTableKey table_id_;
  RangeGroup range_;
  string tbl_sub_path_;
  SubEntityGroupManager* ebt_manager_ = nullptr;
  MMapTagColumnTable* tag_bt_ = nullptr;
  uint32_t cur_subgroup_id_ = 0;

  std::atomic_uint64_t optimistic_read_lsn_{0};

  /**
   * PutDataColumn writes data by column to the specified entity.
   * The function iterates through the data by partition and writes the data for each partition to the corresponding partition table.
   * During this process, deduplication and aggregation are also carried out.
   * If it is an imported scene, space will be applied for Bitmap to record the rows that need to be discarded.
   * After the write is completed, recover the memory that failed to write and delete the data that needs to be deleted
   * after deduplication.
   *
   *
   * @param ctx           Database context.
   * @param group_id       entity group ID.
   * @param entity_id     entity ID.
   * @param payload       The data to be written.
   * @param dedup_result  Pointer to the deduplication result, optional.
   * @return Operation status, success returns KStatus::SUCCESS, failure returns KStatus::FAIL.
   */
  virtual KStatus putDataColumnar(kwdbContext_p ctx, int32_t group_id, int32_t entity_id,
                                  Payload& payload, DedupResult* dedup_result);

  /**
   * payloadNextSlice attempts to retrieve the payload for the next partition from within the payload.
   *
   * @param sub_group   Pointer to TsSubEntityGroup.
   * @param payload     Data to be written
   * @param last_p_time The timestamp of the previous partition.
   * @param start_row   The starting line number of the payload.
   * @param end_row     The end line number of the payload
   * @param cur_p_time  The timestamp of the current partition
   * @return If the next partition is found, return true; If not found or start_row exceeds the valid range, return false.
   */
  bool payloadNextSlice(TsSubEntityGroup* sub_group, Payload& payload, timestamp64 last_p_time, int start_row,
                        int32_t* end_row, timestamp64* cur_p_time);

  bool findPartitionPayload(TsSubEntityGroup* sub_group, Payload& payload,
                            std::multimap<timestamp64, PartitionPayload>* partition_map);

  /**
   * RecordPutAfterProInfo processes data for each partition and logs post-write processing information
   *
   * @param after_process_info It is used to store the processing information for each partition table after data placement.
   * @param p_bt Pointer to the MMapPartitionTable currently being processed.
   * @param cur_alloc_spans The currently allocated BlockSpan set.
   * @param to_deleted_real_rows The set of actual row IDs to be deleted indicates the rows that need to be removed from the table.
   */
  void recordPutAfterProInfo(unordered_map<TsTimePartition*, PutAfterProcessInfo*>& after_process_info,
                             TsTimePartition* p_bt, std::vector<BlockSpan>& cur_alloc_spans,
                             std::vector<MetricRowID>& to_deleted_real_rows);

  /**
   * @brief PutAfterProcess processes the pending data for all partitions. If pushback is not successful, it marks all requested spaces as deleted.
   * If there is data that needs to be removed (duplicate data needs to be removed in deduplication mode), it will be deleted.
   *
   * @param after_process_info A unordered_map that includes the partition table to be processed and its corresponding processing information.
   * @param entity_id  entity id.
   * @param all_success Boolean value indicating whether all processing was successful.
   */
  void putAfterProcess(unordered_map<TsTimePartition*, PutAfterProcessInfo*>& after_process_info,
                       uint32_t entity_id, bool all_success);

  virtual KStatus putTagData(kwdbContext_p ctx, int32_t groupid, int32_t entity_id, Payload& payload);
  virtual KStatus putTagDataHashed(kwdbContext_p ctx, int32_t groupid, int32_t entity_id,
                                  uint32_t hashpoint, Payload& payload);

  /**
   * AllocateEntityGroupID assigns an entity group ID to an entity group
   * Query or assign EntityGroupID and EntityID based on the provided payload. Firstly, attempt to directly obtain
   * the ID from the tag table. If it does not exist, allocate it and insert it into the tag table.
   *
   * @param ctx Database context
   * @param payload It contains the data necessary for querying or assigning IDs.
   * @param entity_id Pointer to the assigned EntityID, which will be returned here upon successful execution of the function.
   * @param group_id Pointer to the assigned EntityGroupID, which will be returned here upon successful execution of the function.
   * @return The status of function execution, successful return is KStatus::SUCCESS, and failure return is KStatus::FAIL.
   */
  KStatus allocateEntityGroupId(kwdbContext_p ctx, Payload& payload, uint32_t* entity_id,
                                uint32_t* group_id, bool* new_tag);

  KStatus getTagTable(ErrorInfo& err_info);

  void releaseTagTable();

 private:
  using TsEntityGroupLatch = KLatch;
  TsEntityGroupLatch* mutex_;

  using TsEntityGroupsRWLatch = KRWLatch;
  TsEntityGroupsRWLatch* drop_mutex_;
};

}  // namespace kwdbts
