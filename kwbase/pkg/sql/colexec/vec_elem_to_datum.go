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

package colexec

import (
	"fmt"
	"math/big"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil/pgdate"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/lib/pq/oid"
)

// PhysicalTypeColElemToDatum converts an element in a colvec to a datum of
// semtype ct. The returned Datum is a deep copy of the colvec element. Note
// that this function handles nulls as well, so there is no need for a separate
// null check.
func PhysicalTypeColElemToDatum(
	col coldata.Vec, rowIdx int, da *sqlbase.DatumAlloc, ct *types.T,
) tree.Datum {
	if col.MaybeHasNulls() {
		if col.Nulls().NullAt(rowIdx) {
			return tree.DNull
		}
	}
	switch ct.Family() {
	case types.BoolFamily:
		if col.Bool()[rowIdx] {
			return tree.DBoolTrue
		}
		return tree.DBoolFalse
	case types.IntFamily:
		switch ct.Width() {
		case 8:
			return da.NewDInt(tree.DInt(col.Int16()[rowIdx]))
		case 16:
			return da.NewDInt(tree.DInt(col.Int16()[rowIdx]))
		case 32:
			return da.NewDInt(tree.DInt(col.Int32()[rowIdx]))
		default:
			return da.NewDInt(tree.DInt(col.Int64()[rowIdx]))
		}
	case types.FloatFamily:
		if ct.Oid() == oid.T_float4 {
			// Reduce output accuracy.
			data := col.Float64()[rowIdx]
			str := strconv.FormatFloat(data, 'f', 6, 32)
			e1, err := tree.ParseDFloat(str)
			if err != nil {
				return nil
			}
			return da.NewDFloat(*e1)
		}
		return da.NewDFloat(tree.DFloat(col.Float64()[rowIdx]))
	case types.DecimalFamily:
		d := da.NewDDecimal(tree.DDecimal{Decimal: col.Decimal()[rowIdx]})
		// Clear the Coeff so that the Set below allocates a new slice for the
		// Coeff.abs field.
		d.Coeff = big.Int{}
		d.Coeff.Set(&col.Decimal()[rowIdx].Coeff)
		return d
	case types.DateFamily:
		return tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(col.Int64()[rowIdx]))
	case types.StringFamily:
		// Note that there is no need for a copy since casting to a string will do
		// that.
		b := col.Bytes().Get(rowIdx)
		if ct.Oid() == oid.T_name {
			return da.NewDName(tree.DString(string(b)))
		}
		return da.NewDString(tree.DString(string(b)))
	case types.BytesFamily:
		// Note that there is no need for a copy since DBytes uses a string as
		// underlying storage, which will perform the copy for us.
		return da.NewDBytes(tree.DBytes(col.Bytes().Get(rowIdx)))
	case types.OidFamily:
		return da.NewDOid(tree.MakeDOid(tree.DInt(col.Int64()[rowIdx])))
	case types.UuidFamily:
		// Note that there is no need for a copy because uuid.FromBytes will perform
		// a copy.
		id, err := uuid.FromBytes(col.Bytes().Get(rowIdx))
		if err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return da.NewDUuid(tree.DUuid{UUID: id})
	case types.TimestampFamily:
		return da.NewDTimestamp(tree.DTimestamp{Time: col.Timestamp()[rowIdx]})
	case types.TimestampTZFamily:
		return da.NewDTimestampTZ(tree.DTimestampTZ{Time: col.Timestamp()[rowIdx]})
	case types.IntervalFamily:
		return da.NewDInterval(tree.DInterval{Duration: col.Interval()[rowIdx]})
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("Unsupported column type %s", ct.String()))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}
