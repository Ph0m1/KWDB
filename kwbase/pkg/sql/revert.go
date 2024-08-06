// Copyright 2019 The Cockroach Authors.
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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RevertTableDefaultBatchSize is the default batch size for reverting tables.
// This only needs to be small enough to keep raft/rocks happy -- there is no
// reply size to worry about.
// TODO(dt): tune this via experimentation.
const RevertTableDefaultBatchSize = 500000

// RevertTables reverts the passed table to the target time.
func RevertTables(
	ctx context.Context,
	db *kv.DB,
	tables []*sqlbase.TableDescriptor,
	targetTime hlc.Timestamp,
	batchSize int64,
) error {
	reverting := make(map[sqlbase.ID]bool, len(tables))
	for i := range tables {
		reverting[tables[i].ID] = true
	}

	spans := make([]roachpb.Span, 0, len(tables))

	// Check that all the tables are revertable -- i.e. offline and that their
	// full interleave hierarchy is being reverted.
	for i := range tables {
		if tables[i].State != sqlbase.TableDescriptor_OFFLINE {
			return errors.New("only offline tables can be reverted")
		}

		if !tables[i].IsPhysicalTable() {
			return errors.Errorf("cannot revert virtual table %s", tables[i].Name)
		}
		for _, idx := range tables[i].AllNonDropIndexes() {
			for _, parent := range idx.Interleave.Ancestors {
				if !reverting[parent.TableID] {
					return errors.New("cannot revert table without reverting all interleaved tables and indexes")
				}
			}
			for _, child := range idx.InterleavedBy {
				if !reverting[child.Table] {
					return errors.New("cannot revert table without reverting all interleaved tables and indexes")
				}
			}
		}
		spans = append(spans, tables[i].TableSpan())
	}

	for i := range tables {
		// This is a) rare and b) probably relevant if we are looking at logs so it
		// probably makes sense to log it without a verbosity filter.
		log.Infof(ctx, "reverting table %s (%d) to time %v", tables[i].Name, tables[i].ID, targetTime)
	}

	// TODO(dt): pre-split requests up using a rangedesc cache and run batches in
	// parallel (since we're passing a key limit, distsender won't do its usual
	// splitting/parallel sending to separate ranges).
	for len(spans) != 0 {
		var b kv.Batch
		for _, span := range spans {
			b.AddRawRequest(&roachpb.RevertRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    span.Key,
					EndKey: span.EndKey,
				},
				TargetTime: targetTime,
			})
		}
		b.Header.MaxSpanRequestKeys = batchSize

		if err := db.Run(ctx, &b); err != nil {
			return err
		}

		spans = spans[:0]
		for _, raw := range b.RawResponse().Responses {
			r := raw.GetRevertRange()
			if r.ResumeSpan != nil {
				if !r.ResumeSpan.Valid() {
					return errors.Errorf("invalid resume span: %s", r.ResumeSpan)
				}
				spans = append(spans, *r.ResumeSpan)
			}
		}
	}

	return nil
}
