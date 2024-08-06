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

package coldata

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"github.com/cockroachdb/apd"
)

// unknown is a Vec that represents an unhandled type. Used when a batch needs a placeholder Vec.
type unknown struct{}

var _ Vec = &unknown{}

func (u unknown) Type() coltypes.T {
	return coltypes.Unhandled
}

func (u unknown) Bool() []bool {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Int16() []int16 {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Int32() []int32 {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Int64() []int64 {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Float64() []float64 {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Bytes() *Bytes {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Decimal() []apd.Decimal {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Timestamp() []time.Time {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Interval() []duration.Duration {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Col() interface{} {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) SetCol(interface{}) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) _TemplateType() []interface{} {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Append(SliceArgs) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Copy(CopySliceArgs) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Window(colType coltypes.T, start int, end int) Vec {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) MaybeHasNulls() bool {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Nulls() *Nulls {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) SetNulls(*Nulls) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Length() int {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) SetLength(int) {
	panic("Vec is of unknown type and should not be accessed")
}

func (u unknown) Capacity() int {
	return 0
}
