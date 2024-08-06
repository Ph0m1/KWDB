// Copyright 2020 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package gcjob

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

func updateDescriptorGCMutations(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	table *sqlbase.TableDescriptor,
	garbageCollectedIndexID sqlbase.IndexID,
) error {
	log.Infof(ctx, "updating GCMutations for table %d after removing index %d", table.ID, garbageCollectedIndexID)
	// Remove the mutation from the table descriptor.
	updateTableMutations := func(tbl *sqlbase.MutableTableDescriptor) error {
		for i := 0; i < len(tbl.GCMutations); i++ {
			other := tbl.GCMutations[i]
			if other.IndexID == garbageCollectedIndexID {
				tbl.GCMutations = append(tbl.GCMutations[:i], tbl.GCMutations[i+1:]...)
				break
			}
		}

		return nil
	}

	_, err := execCfg.LeaseManager.Publish(
		ctx,
		table.ID,
		updateTableMutations,
		nil, /* logEvent */
	)
	if err != nil {
		return err
	}
	return nil
}

// dropTableDesc removes a descriptor from the KV database.
func dropTableDesc(ctx context.Context, db *kv.DB, tableDesc *sqlbase.TableDescriptor) error {
	log.Infof(ctx, "removing table descriptor for table %d", tableDesc.ID)
	descKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)
	zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(tableDesc.ID))

	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		b := &kv.Batch{}
		// Delete the descriptor.
		b.Del(descKey)
		// Delete the zone config entry for this table.
		b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)

		return txn.Run(ctx, b)
	})
}

// deleteDatabaseZoneConfig removes the zone config for a given database ID.
func deleteDatabaseZoneConfig(ctx context.Context, db *kv.DB, databaseID sqlbase.ID) error {
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := &kv.Batch{}
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}

		// Delete the zone config entry for the dropped database associated with the
		// job, if it exists.
		if databaseID == sqlbase.InvalidID {
			return nil
		}
		dbZoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(databaseID))
		b.DelRange(dbZoneKeyPrefix, dbZoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		return txn.Run(ctx, b)
	})
}
