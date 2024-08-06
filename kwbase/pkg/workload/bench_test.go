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

package workload_test

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/skip"
	"gitee.com/kwbasedb/kwbase/pkg/util/bufalloc"
	"gitee.com/kwbasedb/kwbase/pkg/workload"
	"gitee.com/kwbasedb/kwbase/pkg/workload/bank"
	"gitee.com/kwbasedb/kwbase/pkg/workload/tpcc"
	"gitee.com/kwbasedb/kwbase/pkg/workload/tpch"
)

func columnByteSize(col coldata.Vec) int64 {
	switch col.Type() {
	case coltypes.Int64:
		return int64(len(col.Int64()) * 8)
	case coltypes.Int16:
		return int64(len(col.Int16()) * 2)
	case coltypes.Float64:
		return int64(len(col.Float64()) * 8)
	case coltypes.Bytes:
		// We subtract the overhead to be in line with Int64 and Float64 cases.
		return int64(col.Bytes().Size() - coldata.FlatBytesOverhead)
	default:
		panic(fmt.Sprintf(`unhandled type %s`, col.Type().GoTypeName()))
	}
}

func benchmarkInitialData(b *testing.B, gen workload.Generator) {
	tables := gen.Tables()

	var bytes int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Share the Batch and ByteAllocator across tables but not across benchmark
		// iterations.
		cb := coldata.NewMemBatch(nil /* types */)
		var a bufalloc.ByteAllocator
		for _, table := range tables {
			for rowIdx := 0; rowIdx < table.InitialRows.NumBatches; rowIdx++ {
				a = a[:0]
				table.InitialRows.FillBatch(rowIdx, cb, &a)
				for _, col := range cb.ColVecs() {
					bytes += columnByteSize(col)
				}
			}
		}
	}
	b.StopTimer()
	b.SetBytes(bytes / int64(b.N))
}

func BenchmarkInitialData(b *testing.B) {
	b.Run(`tpcc/warehouses=1`, func(b *testing.B) {
		benchmarkInitialData(b, tpcc.FromWarehouses(1))
	})
	b.Run(`bank/rows=1000`, func(b *testing.B) {
		benchmarkInitialData(b, bank.FromRows(1000))
	})
	b.Run(`tpch/scaleFactor=1`, func(b *testing.B) {
		skip.UnderShort(b, "tpch loads a lot of data")
		benchmarkInitialData(b, tpch.FromScaleFactor(1))
	})
}
