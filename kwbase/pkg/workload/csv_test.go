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

package workload_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/util/bufalloc"
	"gitee.com/kwbasedb/kwbase/pkg/util/httputil"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/workload"
	"gitee.com/kwbasedb/kwbase/pkg/workload/bank"
	"gitee.com/kwbasedb/kwbase/pkg/workload/tpcc"
	"github.com/stretchr/testify/require"
)

func TestHandleCSV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		params, expected string
	}{
		{
			`?rows=1`, `
0,0,initial-dTqnRurXztAPkykhZWvsCmeJkMwRNcJAvTlNbgUEYfagEQJaHmfPsquKZUBOGwpAjPtATpGXFJkrtQCEJODSlmQctvyh`,
		},
		{
			`?rows=5&row-start=1&row-end=3&batch-size=1`, `
1,0,initial-vOpikzTTWxvMqnkpfEIVXgGyhZNDqvpVqpNnHawruAcIVltgbnIEIGmCDJcnkVkfVmAcutkMvRACFuUBPsZTemTDSfZT
2,0,initial-qMvoPeRiOBXvdVQxhZUfdmehETKPXyBaVWxzMqwiStIkxfoDFygYxIDyXiaVEarcwMboFhBlCAapvKijKAyjEAhRBNZz`,
		},
	}

	meta := bank.FromRows(0).Meta()
	for _, test := range tests {
		t.Run(test.params, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := workload.HandleCSV(w, r, `/bank/`, meta); err != nil {
					panic(err)
				}
			}))
			defer ts.Close()

			res, err := httputil.Get(context.Background(), ts.URL+`/bank/bank`+test.params)
			if err != nil {
				t.Fatal(err)
			}
			data, err := ioutil.ReadAll(res.Body)
			res.Body.Close()
			if err != nil {
				t.Fatal(err)
			}

			if d, e := strings.TrimSpace(string(data)), strings.TrimSpace(test.expected); d != e {
				t.Errorf("got [\n%s\n] expected [\n%s\n]", d, e)
			}
		})
	}
}

func BenchmarkWriteCSVRows(b *testing.B) {
	ctx := context.Background()

	var batches []coldata.Batch
	for _, table := range tpcc.FromWarehouses(1).Tables() {
		cb := coldata.NewMemBatch(nil /* types */)
		var a bufalloc.ByteAllocator
		table.InitialRows.FillBatch(0, cb, &a)
		batches = append(batches, cb)
	}
	table := workload.Table{
		InitialRows: workload.BatchedTuples{
			FillBatch: func(batchIdx int, cb coldata.Batch, _ *bufalloc.ByteAllocator) {
				*cb.(*coldata.MemBatch) = *batches[batchIdx].(*coldata.MemBatch)
			},
		},
	}

	var buf bytes.Buffer
	fn := func() {
		const limit = -1
		if _, err := workload.WriteCSVRows(ctx, &buf, table, 0, len(batches), limit); err != nil {
			b.Fatalf(`%+v`, err)
		}
	}

	// Run fn once to pre-size buf.
	fn()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		fn()
	}
	b.StopTimer()
	b.SetBytes(int64(buf.Len()))
}

func TestCSVRowsReader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	table := bank.FromRows(10).Tables()[0]
	r := workload.NewCSVRowsReader(table, 1, 3)
	b, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	expected := `
1,0,initial-vOpikzTTWxvMqnkpfEIVXgGyhZNDqvpVqpNnHawruAcIVltgbnIEIGmCDJcnkVkfVmAcutkMvRACFuUBPsZTemTDSfZT
2,0,initial-qMvoPeRiOBXvdVQxhZUfdmehETKPXyBaVWxzMqwiStIkxfoDFygYxIDyXiaVEarcwMboFhBlCAapvKijKAyjEAhRBNZz
`
	require.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(string(b)))
}

func BenchmarkCSVRowsReader(b *testing.B) {
	var batches []coldata.Batch
	for _, table := range tpcc.FromWarehouses(1).Tables() {
		cb := coldata.NewMemBatch(nil /* types */)
		var a bufalloc.ByteAllocator
		table.InitialRows.FillBatch(0, cb, &a)
		batches = append(batches, cb)
	}
	table := workload.Table{
		InitialRows: workload.BatchedTuples{
			NumBatches: len(batches),
			FillBatch: func(batchIdx int, cb coldata.Batch, _ *bufalloc.ByteAllocator) {
				*cb.(*coldata.MemBatch) = *batches[batchIdx].(*coldata.MemBatch)
			},
		},
	}

	var buf bytes.Buffer
	fn := func() {
		r := workload.NewCSVRowsReader(table, 0, 0)
		_, err := io.Copy(&buf, r)
		require.NoError(b, err)
	}

	// Run fn once to pre-size buf.
	fn()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		fn()
	}
	b.StopTimer()
	b.SetBytes(int64(buf.Len()))
}
