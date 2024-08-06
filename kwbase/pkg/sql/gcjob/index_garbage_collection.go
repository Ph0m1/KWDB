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

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// gcIndexes find the indexes that need to be GC'd, GC's them, and then updates
// the cleans up the table descriptor, zone configs and job payload to indicate
// the work that it did.
func gcIndexes(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	parentID sqlbase.ID,
	progress *jobspb.SchemaChangeGCProgress,
) (bool, error) {
	didGC := false
	droppedIndexes := progress.Indexes
	if log.V(2) {
		log.Infof(ctx, "GC is being considered on table %d for indexes indexes: %+v", parentID, droppedIndexes)
	}

	var parentTable *sqlbase.TableDescriptor
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		parentTable, err = sqlbase.GetTableDescFromID(ctx, txn, parentID)
		return err
	}); err != nil {
		return false, errors.Wrapf(err, "fetching parent table %d", parentID)
	}

	for _, index := range droppedIndexes {
		if index.Status != jobspb.SchemaChangeGCProgress_DELETING {
			continue
		}

		indexDesc := sqlbase.IndexDescriptor{ID: index.IndexID}
		if err := clearIndex(ctx, execCfg.DB, parentTable, indexDesc); err != nil {
			return false, errors.Wrapf(err, "clearing index %d", indexDesc.ID)
		}

		// All the data chunks have been removed. Now also removed the
		// zone configs for the dropped indexes, if any.
		if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return sql.RemoveIndexZoneConfigs(ctx, txn, execCfg, parentTable.GetID(), []sqlbase.IndexDescriptor{indexDesc})
		}); err != nil {
			return false, errors.Wrapf(err, "removing index %d zone configs", indexDesc.ID)
		}

		if err := completeDroppedIndex(ctx, execCfg, parentTable, index.IndexID, progress); err != nil {
			return false, err
		}

		didGC = true
	}

	return didGC, nil
}

// clearIndexes issues Clear Range requests over all specified indexes.
func clearIndex(
	ctx context.Context, db *kv.DB, tableDesc *sqlbase.TableDescriptor, index sqlbase.IndexDescriptor,
) error {
	log.Infof(ctx, "clearing index %d from table %d", index.ID, tableDesc.ID)
	if index.IsInterleaved() {
		return errors.Errorf("unexpected interleaved index %d", index.ID)
	}

	sp := tableDesc.IndexSpan(index.ID)

	// ClearRange cannot be run in a transaction, so create a
	// non-transactional batch to send the request.
	b := &kv.Batch{}
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    sp.Key,
			EndKey: sp.EndKey,
		},
	})
	return db.Run(ctx, b)
}

// completeDroppedIndexes updates the mutations of the table descriptor to
// indicate that the index was dropped, as well as the job detail payload.
func completeDroppedIndex(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	table *sqlbase.TableDescriptor,
	indexID sqlbase.IndexID,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	if err := updateDescriptorGCMutations(ctx, execCfg, table, indexID); err != nil {
		return errors.Wrapf(err, "updating GC mutations")
	}

	markIndexGCed(ctx, indexID, progress)

	return nil
}
