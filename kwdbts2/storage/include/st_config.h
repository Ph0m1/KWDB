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

#include <cstdint>

#ifndef KWBASE_OSS
#include "ts_config_autonomy.h"
#endif

extern uint16_t CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP;  // SUBGROUP_ENTITIES from cluster setting
extern uint32_t CLUSTER_SETTING_MAX_BLOCKS_PER_SEGMENT;  // PARTITION_BLOCKS from cluster setting
extern uint16_t CLUSTER_SETTING_MAX_ROWS_PER_BLOCK;  // PARTITION_ROWS from cluster setting

namespace kwdbts {

/**
 * @brief Get max entities per subgroup
 * @param max_entities_of_prev_subgroup max entities of previous subgroup
 * @return uint16_t
 */
uint16_t GetMaxEntitiesPerSubgroup(uint16_t max_entities_of_prev_subgroup = 0);

/**
 * @brief Get segment config
 * @param max_blocks_per_segment max blocks per segment
 * @param max_rows_per_block max rows per block
 * @param table_id table id
 * @param max_entities_of_subgroup max entities of subgroup
 * @param partition_interval partition interval of the table
 * @return void
 */
void GetSegmentConfig(uint32_t& max_blocks_per_segment, uint16_t& max_rows_per_block,
                      uint64_t table_id = 0, uint32_t max_entities_of_subgroup = 0,
                      uint32_t partition_interval = 0);

}  // namespace kwdbts
