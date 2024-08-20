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

#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <map>
#include "libkwdbts2.h"
#include "kwdb_type.h"
#include "mmap/mmap_tag_column_table.h"
#include "ts_common.h"
#include "ts_table.h"

namespace kwdbts {

struct SnapshotInfo {
  uint64_t id;    // snapshot id
  uint8_t type;   // type: 0-build snapshot(build at source node), 1-apply snapshot(apply at target node)
  uint8_t level;  // level: 0-EntityGroup, 1-SubEntityGroup
  bool reorder;   // snapshot need to be reordered
  // snapshot need to include the range of primary tag hash ids
  uint64_t begin_hash;
  uint64_t end_hash;
  // parameters are used for SubEntityGroup reconstruction
  uint32_t subgroup_id;  // reconstructed subgroup_id
  KwTsSpan partition_ts;  // partition timestamp
};

class TsEntityGroup;
class TsTableSnapshot {
 public:
  /**
   * @brief Constructor
   * @param[in] db_path database root path
   * @param[in] table_id ID of the table associated with the snapshot
   * @param[in] tbl_sub_path tbl_sub_path of snapshot
   * @param[in] entity_group TsEntityGroup from source/target
   * @param[in] entity_bt root table
   * @param[in] snapshot_info SnapshotInfo
   */
  TsTableSnapshot(const string& db_path, const KTableKey& table_id, const string& tbl_sub_path,
                  std::shared_ptr<TsEntityGroup> entity_group, MMapRootTableManager* entity_bt_manager,
                  SnapshotInfo& snapshot_info)
      : snapshot_info_(snapshot_info), db_path_(db_path), table_id_(table_id), tbl_sub_path_(tbl_sub_path),
        entity_bt_manager_(entity_bt_manager), entity_group_(entity_group) {}

  ~TsTableSnapshot();

  /**
   * @brief Initialize the snapshot object
   * The function creates a snapshot for a given table.
   * Depending on the type of snapshot_info, it either creates a new snapshot
   * or opens and initializes an existing snapshot.
   * @return KStatus
   */
  KStatus Init(kwdbContext_p ctx);

  /**
   * @brief Build snapshot based on the lsn
   * First, get primary tag rows to be migrated, and then use PayloadBuilder to build snapshot with the tag rows.
   * @param[in] lsn  data is read from the source TsEntityGroup based on the LSN and written to the snapshot.
   *
   * @return KStatus
   */
  KStatus BuildSnapshot(kwdbContext_p ctx, TS_LSN lsn);

  /**
   * @brief Compress snapshot use mksqushfs which generates .sqfs compressed file.
   * @param[in] get_data_handle
   * @param[out] total total total size of compressed file
   * @return KStatus
   */
  KStatus CompressSnapshot(kwdbContext_p ctx, size_t* total);

  /**
   * @brief GetSnapshotData gets compressed snapshot from the source node,
   * and since the snapshot may be relatively large,
   * the size of the snapshot data block taken at a time is limited,
   * therefore, getting a full snapshot may call this function multiple times.
   * @param range_group_id The range group ID of snapshot
   * @param offset The offset of the snapshot data taken by this call
   * @param limit The size limit of the data block to be taken by this call
   * @param data The data block taken by this call
   * @param total total size of compressed file
   * @return KStatus
   */
  KStatus GetSnapshotData(kwdbContext_p ctx, uint64_t range_group_id,  size_t offset, size_t limit,
                          TSSlice* data, size_t* total);

  /**
   * @brief Apply snapshot to the TsEntityGroup, atomicity needs to be guaranteed.
   * The caller is responsible for concurrent write control:
   * 1. The TsEntityGroup level snapshot needs to be locked to prevent it from being written
   * 2. The data of the subgroup table is reorganized and locked in the TsSubEntityGroup
   *
   * Apply snapshot process:
   * 1. Allocate all subgroups needed in the target TsEntityGroup to avoid possible problems
   *    caused by concurrent writes during snapshot application.
   * 2. Get the number of entities in each subgroup in the snapshot.
   * 3. Traverse all tags in the snapshot and apply them to the target subgroup (created in step 1).
   * 4. If the tag in the destination subgroup is full or all snapshot tag has been migrated,
   *    migrate the metric data to the subgroup at one time,
   * 5. Change to the next subgroup if the migration is not yet complete, and continue from step 3
   *
   * Because when we generate the snapshot data, we create a new entity group (of course, its subgroup is also all new),
   * and write all the metric data corresponding to the tag into the group in order, and the subgroup is also created
   * in the process of snapshot application, so the subgroup of the snapshot and the new subgroup in the target group
   * are one-to-one, and the tag and metric data are also completely corresponding. After appending the tags of a
   * subgroup to the destination group in a snapshot, we can copy the data of the subgroup in the snapshot to the
   * destination subgroup directly.
   *
   * @return KStatus
  */
  KStatus Apply();

  /**
    * @brief Before compaction, sort out the subgroup each entity belong to, and change the segment's status.
    * @param[out] obsolete_max_block: map[subgroup_id][partition_ts][entity_id]block_id,
    *      record latest block_id of each entity when compaction starts
    * @param[out] obsolete_segment_ids: map[subgroup_id][partition_ts]{segment_id...},
    *      record segment_ids under partition when compaction starts, these segment will be deleted.
    * @param[out] subgroup_row: map[subgroup_id]{tag_row...}, compacting tags and their subgroup, for BuildCompactData
    * @param[out] subgroup_ts_span: map[subgroup_id]{KwTsSpan...},
    *      record compacting partitions of each subgroup, all convert to KwTsSpan, for BuildCompactData
   */
  KStatus PrepareCompactData(kwdbContext_p ctx,
                             std::map<SubGroupID, std::map<timestamp64, std::map<uint32_t, BLOCK_ID>>> &obsolete_max_block,
                             std::map<SubGroupID, std::map<timestamp64, std::vector<BLOCK_ID>>> &obsolete_segment_ids,
                             std::map<SubGroupID, std::vector<uint32_t>> &subgroup_row,
                             std::map<SubGroupID, std::vector<KwTsSpan>> &subgroup_ts_span);

  KStatus BuildCompactData(kwdbContext_p ctx, std::vector<uint32_t> &tag_rows, std::vector<KwTsSpan> &partition_ts);

  /**
   * @brief Apply the compation result to EntityGroup. The ongoing subgroup cannot read/write.
   * @param[in] subgroup_id: the subgroup applying the compaction result
   * @param[in] obsolete_max_block: map[partition_ts][entity_id]block_id,
   *      record the latest block_id of the entityies when compaction starts, for meta linking.
   * @param[in] obsolete_segment_ids: map[partition_ts]{segment_id...},
   *      record segment_ids under partition when compaction starts, these segment will be deleted.
  */
  KStatus ApplyCompactData(kwdbContext_p ctx, SubGroupID subgroup_id,
                           std::map<timestamp64, std::map<uint32_t, BLOCK_ID>> &obsolete_max_block,
                           std::map<timestamp64, std::vector<BLOCK_ID>> &obsolete_segment_ids);
  /**
   * @brief Drop all snapshot data includes compressed file
   * @return
   */
  KStatus DropAll();

  TsEntityGroup*  GetSnapshotEntityGroup() {
    return snapshot_group_;
  }

  uint64_t GetSnapshotId() {
    return snapshot_info_.id;
  }

 private:
  /**
   * @brief Get the tag rows to be migrated from the source node, which tag is to be migrated
   * is determined by the consistent hash ID calculated by the primary tag.
   * @return KStatus
   */
  KStatus getMigratedTagRows();

  /**
   * @brief applyTagData construct the tag into payload and write to the target group through the data writing interface
   * @param row_id tag row id
   * @param entity_id entity id that corresponds to the tag row
   * @param subgroup_id subgroup id that corresponds to the tag row
   * @return KStatus
   */
  KStatus applyTagData(int row_id, const EntityID& entity_id, const SubGroupID& subgroup_id);

  /**
   * @brief Copy the snapshot subgroup data to the destination group and refresh the memory records of the destination group
   * @param subgroup_id destination subgroup id
   * @param snapshot_sub_group_id snapshot subgroup id
   * @return KStatus
   */
  KStatus applyEntityData(uint32_t& subgroup_id, SubGroupID& snapshot_sub_group_id);

  /**
   * @brief Generate the payload data for the snapshot to be migrated by PayloadBuilder
   * First, create iterator to read data in batches from source entity group, the read data batch is then assembled
   * into a payload through the builder and the snapshot group calls PutData to write the data.
   * @param[in] row_id The data to be migrated corresponds to the row number of the primary tag in the tag table
   * @param[in] ts_span timestamp range of this time to generate paload
   * @return KStatus
   */
  KStatus genMigratePayloadByBuilder(kwdbContext_p ctx, uint32_t row_id, KwTsSpan ts_span);

  string db_path_;
  KTableKey table_id_;
  string tbl_sub_path_;
  MMapRootTableManager* entity_bt_manager_;
  SnapshotInfo snapshot_info_;
  // TsEntityGroup instance of a snapshot
  TsEntityGroup* snapshot_group_;
  // TsEntityGroup instance of the original ts table
  std::shared_ptr<TsEntityGroup> entity_group_;
  vector<AttributeInfo> schema_;
  std::vector<uint32_t> tag_row_nums_;
  std::mutex snapshot_mutex_;
};

}  //  namespace kwdbts
