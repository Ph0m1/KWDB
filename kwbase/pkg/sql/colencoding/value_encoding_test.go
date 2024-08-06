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

package colencoding

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func TestDecodeTableValueToCol(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	var buf []byte
	var scratch []byte
	nCols := 1000
	datums := make([]tree.Datum, nCols)
	colTyps := make([]*types.T, nCols)
	typs := make([]coltypes.T, nCols)
	for i := 0; i < nCols; i++ {
		ct := sqlbase.RandType(rng)
		et := typeconv.FromColumnType(ct)
		if et == coltypes.Unhandled {
			i--
			continue
		}
		datum := sqlbase.RandDatum(rng, ct, false /* nullOk */)
		colTyps[i] = ct
		typs[i] = et
		datums[i] = datum
		var err error
		fmt.Println(datum)
		buf, err = sqlbase.EncodeTableValue(buf, sqlbase.ColumnID(encoding.NoColumnID), datum, scratch)
		if err != nil {
			t.Fatal(err)
		}
	}
	batch := coldata.NewMemBatchWithSize(typs, 1)
	for i := 0; i < nCols; i++ {
		typeOffset, dataOffset, _, typ, err := encoding.DecodeValueTag(buf)
		fmt.Println(typ)
		if err != nil {
			t.Fatal(err)
		}
		buf, err = DecodeTableValueToCol(batch.ColVec(i), 0 /* rowIdx */, typ,
			dataOffset, colTyps[i], buf[typeOffset:])
		if err != nil {
			t.Fatal(err)
		}

		// TODO(jordan): should actually compare the outputs as well, but this is a
		// decent enough smoke test.
	}

	if len(buf) != 0 {
		t.Fatalf("leftover bytes %s", buf)
	}
}
