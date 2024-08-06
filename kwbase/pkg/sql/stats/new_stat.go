// Copyright 2018 The Cockroach Authors.
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

package stats

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

// InsertNewStats inserts a slice of statistics at the current time into the
// system table.
func InsertNewStats(
	ctx context.Context,
	executor sqlutil.InternalExecutor,
	txn *kv.Txn,
	tableStats []*TableStatisticProto,
) error {
	var err error
	for _, statistic := range tableStats {
		err = InsertNewStat(
			ctx,
			executor,
			txn,
			statistic.TableID,
			statistic.Name,
			statistic.ColumnIDs,
			int64(statistic.RowCount),
			int64(statistic.DistinctCount),
			int64(statistic.NullCount),
			statistic.HistogramData,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertNewStat inserts a new statistic in the system table.
// The caller is responsible for calling GossipTableStatAdded to notify the stat
// caches.
func InsertNewStat(
	ctx context.Context,
	executor sqlutil.InternalExecutor,
	txn *kv.Txn,
	tableID sqlbase.ID,
	name string,
	columnIDs []sqlbase.ColumnID,
	rowCount, distinctCount, nullCount int64,
	h *HistogramData,
) error {
	// We must pass a nil interface{} if we want to insert a NULL.
	var nameVal, histogramVal interface{}
	if name != "" {
		nameVal = name
	}
	if h != nil {
		var err error
		histogramVal, err = protoutil.Marshal(h)
		if err != nil {
			return err
		}
	}

	columnIDsVal := tree.NewDArray(types.Int)
	for _, c := range columnIDs {
		if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(int(c)))); err != nil {
			return err
		}
	}

	_, err := executor.Exec(
		ctx, "insert-statistic", txn,
		`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"rowCount",
					"distinctCount",
					"nullCount",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tableID,
		nameVal,
		columnIDsVal,
		rowCount,
		distinctCount,
		nullCount,
		histogramVal,
	)
	return err
}

// GossipTableStatAdded causes the statistic caches for this table to be
// invalidated.
func GossipTableStatAdded(g *gossip.Gossip, tableID sqlbase.ID) error {
	// TODO(radu): perhaps use a TTL here to avoid having a key per table floating
	// around forever (we would need the stat cache to evict old entries
	// automatically though).
	return g.AddInfo(
		gossip.MakeTableStatAddedKey(uint32(tableID)),
		nil, /* value */
		0,   /* ttl */
	)
}
