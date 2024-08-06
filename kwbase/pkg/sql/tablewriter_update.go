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

// tableUpdater handles writing kvs and forming table rows for updates.
type tableUpdater struct {
	tableWriterBase
	ru row.Updater
}

var _ tableWriter = &tableUpdater{}

// desc is part of the tableWriter interface.
func (*tableUpdater) desc() string { return "updater" }

// init is part of the tableWriter interface.
func (tu *tableUpdater) init(_ context.Context, txn *kv.Txn, _ *tree.EvalContext) error {
	tu.tableWriterBase.init(txn)
	return nil
}

// row is part of the tableWriter interface.
// We don't implement this because tu.ru.UpdateRow wants two slices
// and it would be a shame to split the incoming slice on every call.
// Instead provide a separate rowForUpdate() below.
func (tu *tableUpdater) row(context.Context, tree.Datums, bool) error {
	panic("unimplemented")
}

// rowForUpdate extends row() from the tableWriter interface.
func (tu *tableUpdater) rowForUpdate(
	ctx context.Context, oldValues, updateValues tree.Datums, traceKV bool,
) (tree.Datums, error) {
	tu.batchSize++
	return tu.ru.UpdateRow(ctx, tu.b, oldValues, updateValues, row.CheckFKs, traceKV, tu.txn)
}

// atBatchEnd is part of the tableWriter interface.
func (tu *tableUpdater) atBatchEnd(_ context.Context, _ bool) error { return nil }

// flushAndStartNewBatch is part of the tableWriter interface.
func (tu *tableUpdater) flushAndStartNewBatch(ctx context.Context) error {
	return tu.tableWriterBase.flushAndStartNewBatch(ctx, tu.tableDesc())
}

// finalize is part of the tableWriter interface.
func (tu *tableUpdater) finalize(ctx context.Context, _ bool) (*rowcontainer.RowContainer, error) {
	return nil, tu.tableWriterBase.finalize(ctx, tu.tableDesc())
}

// tableDesc is part of the tableWriter interface.
func (tu *tableUpdater) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return tu.ru.Helper.TableDesc
}

// close is part of the tableWriter interface.
func (tu *tableUpdater) close(_ context.Context) {}

// walkExprs is part of the tableWriter interface.
func (tu *tableUpdater) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}
