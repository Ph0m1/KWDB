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

package colserde_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/colserde"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFileRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	typs, b := randomBatch(testAllocator)

	t.Run(`mem`, func(t *testing.T) {
		// Make a copy of the original batch because the converter modifies and
		// casts data without copying for performance reasons.
		original := colexec.CopyBatch(testAllocator, b)

		var buf bytes.Buffer
		s, err := colserde.NewFileSerializer(&buf, typs)
		require.NoError(t, err)
		require.NoError(t, s.AppendBatch(b))
		require.NoError(t, s.Finish())

		// Parts of the deserialization modify things (null bitmaps) in place, so
		// run it twice to make sure those modifications don't leak back to the
		// buffer.
		for i := 0; i < 2; i++ {
			func() {
				roundtrip := coldata.NewMemBatchWithSize(nil, 0)
				d, err := colserde.NewFileDeserializerFromBytes(buf.Bytes())
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()
				require.Equal(t, typs, d.Typs())
				require.Equal(t, 1, d.NumBatches())
				require.NoError(t, d.GetBatch(0, roundtrip))

				coldata.AssertEquivalentBatches(t, original, roundtrip)
			}()
		}
	})

	t.Run(`disk`, func(t *testing.T) {
		dir, cleanup := testutils.TempDir(t)
		defer cleanup()
		path := filepath.Join(dir, `rng.arrow`)

		// Make a copy of the original batch because the converter modifies and
		// casts data without copying for performance reasons.
		original := colexec.CopyBatch(testAllocator, b)

		f, err := os.Create(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		s, err := colserde.NewFileSerializer(f, typs)
		require.NoError(t, err)
		require.NoError(t, s.AppendBatch(b))
		require.NoError(t, s.Finish())
		require.NoError(t, f.Sync())

		// Parts of the deserialization modify things (null bitmaps) in place, so
		// run it twice to make sure those modifications don't leak back to the
		// file.
		for i := 0; i < 2; i++ {
			func() {
				roundtrip := coldata.NewMemBatchWithSize(nil, 0)
				d, err := colserde.NewFileDeserializerFromPath(path)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()
				require.Equal(t, typs, d.Typs())
				require.Equal(t, 1, d.NumBatches())
				require.NoError(t, d.GetBatch(0, roundtrip))

				coldata.AssertEquivalentBatches(t, original, roundtrip)
			}()
		}
	})
}

func TestFileIndexing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numInts = 10
	typs := []coltypes.T{coltypes.Int64}

	var buf bytes.Buffer
	s, err := colserde.NewFileSerializer(&buf, typs)
	require.NoError(t, err)

	for i := 0; i < numInts; i++ {
		b := coldata.NewMemBatchWithSize(typs, 1)
		b.SetLength(1)
		b.ColVec(0).Int64()[0] = int64(i)
		require.NoError(t, s.AppendBatch(b))
	}
	require.NoError(t, s.Finish())

	d, err := colserde.NewFileDeserializerFromBytes(buf.Bytes())
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	require.Equal(t, typs, d.Typs())
	require.Equal(t, numInts, d.NumBatches())
	for batchIdx := numInts - 1; batchIdx >= 0; batchIdx-- {
		b := coldata.NewMemBatchWithSize(nil, 0)
		require.NoError(t, d.GetBatch(batchIdx, b))
		require.Equal(t, 1, b.Length())
		require.Equal(t, 1, b.Width())
		require.Equal(t, coltypes.Int64, b.ColVec(0).Type())
		require.Equal(t, int64(batchIdx), b.ColVec(0).Int64()[0])
	}
}
