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

package rowexec

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// rowFetcher is an interface used to abstract a row fetcher so that a stat
// collector wrapper can be plugged in.
type rowFetcher interface {
	StartScan(
		_ context.Context, _ *kv.Txn, _ roachpb.Spans, limitBatches bool, limitHint int64, traceKV bool,
	) error
	StartInconsistentScan(
		_ context.Context,
		_ *kv.DB,
		initialTimestamp hlc.Timestamp,
		maxTimestampAge time.Duration,
		spans roachpb.Spans,
		limitBatches bool,
		limitHint int64,
		traceKV bool,
	) error

	NextRow(ctx context.Context) (
		sqlbase.EncDatumRow, *sqlbase.TableDescriptor, *sqlbase.IndexDescriptor, error)
	// PartialKey is not stat-related but needs to be supported.
	PartialKey(int) (roachpb.Key, error)
	Reset()
	GetBytesRead() int64
	GetRangesInfo() []roachpb.RangeInfo
	NextRowWithErrors(context.Context) (sqlbase.EncDatumRow, error)
}

// initRowFetcher initializes the fetcher.
func initRowFetcher(
	fetcher *row.Fetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	colIdxMap map[sqlbase.ColumnID]int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	alloc *sqlbase.DatumAlloc,
	scanVisibility execinfrapb.ScanVisibility,
	lockStr sqlbase.ScanLockingStrength,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*desc)
	index, isSecondaryIndex, err = immutDesc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	cols := immutDesc.Columns
	if scanVisibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC {
		cols = immutDesc.ReadableColumns
	}
	tableArgs := row.FetcherTableArgs{
		Desc:             immutDesc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		reverseScan, lockStr, true /* returnRangeInfo */, isCheck, alloc, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}
