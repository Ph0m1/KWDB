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

package coldata

import (
	"fmt"
	"math/rand"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// maxVarLen specifies a length limit for variable length types (e.g. byte slices).
const maxVarLen = 64

var locations []*time.Location

func init() {
	// Load some random time zones.
	for _, locationName := range []string{
		"Africa/Addis_Ababa",
		"America/Anchorage",
		"Antarctica/Davis",
		"Asia/Ashkhabad",
		"Australia/Sydney",
		"Europe/Minsk",
		"Pacific/Palau",
	} {
		loc, err := timeutil.LoadLocation(locationName)
		if err == nil {
			locations = append(locations, loc)
		}
	}
}

// RandomVec populates vec with n random values of typ, setting each value to
// null with a probability of nullProbability. It is assumed that n is in bounds
// of the given vec.
// bytesFixedLength (when greater than zero) specifies the fixed length of the
// bytes slice to be generated. It is used only if typ == coltypes.Bytes.
func RandomVec(
	rng *rand.Rand, typ coltypes.T, bytesFixedLength int, vec Vec, n int, nullProbability float64,
) {
	switch typ {
	case coltypes.Bool:
		bools := vec.Bool()
		for i := 0; i < n; i++ {
			if rng.Float64() < 0.5 {
				bools[i] = true
			} else {
				bools[i] = false
			}
		}
	case coltypes.Bytes:
		bytes := vec.Bytes()
		for i := 0; i < n; i++ {
			bytesLen := bytesFixedLength
			if bytesLen <= 0 {
				bytesLen = rng.Intn(maxVarLen)
			}
			randBytes := make([]byte, bytesLen)
			// Read always returns len(bytes[i]) and nil.
			_, _ = rand.Read(randBytes)
			bytes.Set(i, randBytes)
		}
	case coltypes.Decimal:
		decs := vec.Decimal()
		for i := 0; i < n; i++ {
			// int64(rng.Uint64()) to get negative numbers, too
			decs[i].SetFinite(int64(rng.Uint64()), int32(rng.Intn(40)-20))
		}
	case coltypes.Int16:
		ints := vec.Int16()
		for i := 0; i < n; i++ {
			ints[i] = int16(rng.Uint64())
		}
	case coltypes.Int32:
		ints := vec.Int32()
		for i := 0; i < n; i++ {
			ints[i] = int32(rng.Uint64())
		}
	case coltypes.Int64:
		ints := vec.Int64()
		for i := 0; i < n; i++ {
			ints[i] = int64(rng.Uint64())
		}
	case coltypes.Float64:
		floats := vec.Float64()
		for i := 0; i < n; i++ {
			floats[i] = rng.Float64()
		}
	case coltypes.Timestamp:
		timestamps := vec.Timestamp()
		for i := 0; i < n; i++ {
			timestamps[i] = timeutil.Unix(rng.Int63n(1000000), rng.Int63n(1000000))
			loc := locations[rng.Intn(len(locations))]
			timestamps[i] = timestamps[i].In(loc)
		}
	case coltypes.Interval:
		intervals := vec.Interval()
		for i := 0; i < n; i++ {
			intervals[i] = duration.FromFloat64(rng.Float64())
		}
	default:
		panic(fmt.Sprintf("unhandled type %s", typ))
	}
	vec.Nulls().UnsetNulls()
	if nullProbability == 0 {
		return
	}

	for i := 0; i < n; i++ {
		if rng.Float64() < nullProbability {
			vec.Nulls().SetNull(i)
		}
	}
}
