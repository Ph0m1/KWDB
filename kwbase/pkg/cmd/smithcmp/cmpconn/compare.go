// Copyright 2020 The Cockroach Authors.
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

package cmpconn

import (
	"math/big"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"github.com/cockroachdb/apd"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
)

// CompareVals returns an error if a and b differ, specifying what the
// difference is. It is designed to compare SQL results from a query
// executed on two different servers or configurations (i.e., kwbase and
// postgres). Postgres and Cockroach have subtle differences in their result
// types and OIDs. This function is aware of those and is able to correctly
// compare those values.
func CompareVals(a, b []interface{}) error {
	if len(a) != len(b) {
		return errors.Errorf("size difference: %d != %d", len(a), len(b))
	}
	if len(a) == 0 {
		return nil
	}
	if diff := cmp.Diff(a, b, cmpOptions...); diff != "" {
		return errors.New(diff)
	}
	return nil
}

var (
	cmpOptions = []cmp.Option{
		cmp.Transformer("", func(x []interface{}) []interface{} {
			out := make([]interface{}, len(x))
			for i, v := range x {
				switch t := v.(type) {
				case *pgtype.TextArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = ""
					}
				case *pgtype.BPCharArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = ""
					}
				case *pgtype.VarcharArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = ""
					}
				case *pgtype.Int8Array:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.Int8Array{}
					}
				case *pgtype.Float8Array:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.Float8Array{}
					}
				case *pgtype.UUIDArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.UUIDArray{}
					}
				case *pgtype.ByteaArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.ByteaArray{}
					}
				case *pgtype.InetArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.InetArray{}
					}
				case *pgtype.TimestampArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.TimestampArray{}
					}
				case *pgtype.BoolArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.BoolArray{}
					}
				case *pgtype.DateArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.BoolArray{}
					}
				case *pgtype.Varbit:
					if t.Status == pgtype.Present {
						s, _ := t.EncodeText(nil, nil)
						v = string(s)
					}
				case *pgtype.Bit:
					vb := pgtype.Varbit(*t)
					v = &vb
				case *pgtype.Interval:
					if t.Status == pgtype.Present {
						v = duration.DecodeDuration(int64(t.Months), int64(t.Days), t.Microseconds*1000)
					}
				case string:
					// Postgres sometimes adds spaces to the end of a string.
					t = strings.TrimSpace(t)
					v = strings.Replace(t, "T00:00:00+00:00", "T00:00:00Z", 1)
					v = strings.Replace(t, ":00+00:00", ":00", 1)
				case *pgtype.Numeric:
					if t.Status == pgtype.Present {
						v = apd.NewWithBigInt(t.Int, t.Exp)
					}
				case int64:
					v = apd.New(t, 0)
				}
				out[i] = v
			}
			return out
		}),

		cmpopts.EquateEmpty(),
		cmpopts.EquateNaNs(),
		cmpopts.EquateApprox(0.00001, 0),
		cmp.Comparer(func(x, y *big.Int) bool {
			return x.Cmp(y) == 0
		}),
		cmp.Comparer(func(x, y *apd.Decimal) bool {
			var a, b, min, sub apd.Decimal
			a.Abs(x)
			b.Abs(y)
			if a.Cmp(&b) > 1 {
				min.Set(&b)
			} else {
				min.Set(&a)
			}
			ctx := tree.DecimalCtx
			_, _ = ctx.Mul(&min, &min, decimalCloseness)
			_, _ = ctx.Sub(&sub, x, y)
			sub.Abs(&sub)
			return sub.Cmp(&min) <= 0
		}),
		cmp.Comparer(func(x, y duration.Duration) bool {
			return x.Compare(y) == 0
		}),
	}
	decimalCloseness = apd.New(1, -6)
)
