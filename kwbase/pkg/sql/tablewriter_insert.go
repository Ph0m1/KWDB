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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// tableInserter handles writing kvs and forming table rows for inserts.
type tableInserter struct {
	tableWriterBase
	ri row.Inserter
}

var _ tableWriter = &tableInserter{}

// desc is part of the tableWriter interface.
func (*tableInserter) desc() string { return "inserter" }

// init is part of the tableWriter interface.
func (ti *tableInserter) init(_ context.Context, txn *kv.Txn, _ *tree.EvalContext) error {
	ti.tableWriterBase.init(txn)
	return nil
}

// row is part of the tableWriter interface.
func (ti *tableInserter) row(ctx context.Context, values tree.Datums, traceKV bool) error {
	ti.batchSize++
	return ti.ri.InsertRow(ctx, ti.b, values, false /* overwrite */, row.CheckFKs, traceKV, ti.txn)
}

// atBatchEnd is part of the tableWriter interface.
func (ti *tableInserter) atBatchEnd(_ context.Context, _ bool) error { return nil }

// flushAndStartNewBatch is part of the tableWriter interface.
func (ti *tableInserter) flushAndStartNewBatch(ctx context.Context) error {
	return ti.tableWriterBase.flushAndStartNewBatch(ctx, ti.tableDesc())
}

// finalize is part of the tableWriter interface.
func (ti *tableInserter) finalize(ctx context.Context, _ bool) (*rowcontainer.RowContainer, error) {
	return nil, ti.tableWriterBase.finalize(ctx, ti.tableDesc())
}

// tableDesc is part of the tableWriter interface.
func (ti *tableInserter) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return ti.ri.Helper.TableDesc
}

// close is part of the tableWriter interface.
func (ti *tableInserter) close(_ context.Context) {}

// walkExprs is part of the tableWriter interface.
func (ti *tableInserter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}
