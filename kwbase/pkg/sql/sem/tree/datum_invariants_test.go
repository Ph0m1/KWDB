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

package tree

import (
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeofday"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllTypesCastableToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, typ := range types.Scalar {
		if ok, _ := isCastDeepValid(typ, types.String); !ok {
			t.Errorf("%s is not castable to STRING, all types should be", typ)
		}
	}
}

func TestAllTypesCastableFromString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, typ := range types.Scalar {
		if ok, _ := isCastDeepValid(types.String, typ); !ok {
			t.Errorf("%s is not castable from STRING, all types should be", typ)
		}
	}
}

func TestCompareTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	pacificTimeZone := int32(7 * 60 * 60)
	sydneyTimeZone := int32(-10 * 60 * 60)

	sydneyFixedZone := time.FixedZone("otan@sydney", -int(sydneyTimeZone))
	// kiwiFixedZone is 2 hours ahead of Sydney.
	kiwiFixedZone := time.FixedZone("otan@auckland", -int(sydneyTimeZone)+2*60*60)

	ddate, err := NewDDateFromTime(time.Date(2019, time.November, 22, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	testCases := []struct {
		desc     string
		left     Datum
		right    Datum
		location *time.Location
		expected int
	}{
		{
			desc:     "same DTime are equal",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    MakeDTime(timeofday.New(12, 0, 0, 0)),
			expected: 0,
		},
		{
			desc:     "same DTimeTZ are equal",
			left:     NewDTimeTZFromOffset(timeofday.New(22, 0, 0, 0), sydneyTimeZone),
			right:    NewDTimeTZFromOffset(timeofday.New(22, 0, 0, 0), sydneyTimeZone),
			expected: 0,
		},
		{
			desc:     "DTime and DTimeTZ both UTC, and so are equal",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    NewDTimeTZFromOffset(timeofday.New(12, 0, 0, 0), 0),
			expected: 0,
		},
		{
			desc:     "DTime and DTimeTZ both Sydney time, and so are equal",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    NewDTimeTZFromOffset(timeofday.New(12, 0, 0, 0), sydneyTimeZone),
			location: sydneyFixedZone,
			expected: 0,
		},
		{
			desc:     "DTimestamp and DTimestampTZ (Sydney) equal in Sydney zone",
			left:     MakeDTimestamp(time.Date(2019, time.November, 22, 10, 0, 0, 0, time.UTC), time.Microsecond),
			right:    MakeDTimestampTZ(time.Date(2019, time.November, 22, 10, 0, 0, 0, sydneyFixedZone), time.Microsecond),
			location: sydneyFixedZone,
			expected: 0,
		},
		{
			desc:     "DTimestamp and DTimestampTZ (Sydney) equal in Sydney+2 zone",
			left:     MakeDTimestamp(time.Date(2019, time.November, 22, 12, 0, 0, 0, time.UTC), time.Microsecond),
			right:    MakeDTimestampTZ(time.Date(2019, time.November, 22, 10, 0, 0, 0, sydneyFixedZone), time.Microsecond),
			location: kiwiFixedZone,
			expected: 0,
		},
		{
			desc:     "Date and DTimestampTZ (Sydney) equal in Sydney zone",
			left:     ddate,
			right:    MakeDTimestampTZ(time.Date(2019, time.November, 22, 0, 0, 0, 0, sydneyFixedZone), time.Microsecond),
			location: sydneyFixedZone,
			expected: 0,
		},
		{
			desc:     "Date and DTimestampTZ (Sydney) equal in Sydney+2 zone",
			left:     ddate,
			right:    MakeDTimestampTZ(time.Date(2019, time.November, 21, 22, 0, 0, 0, sydneyFixedZone), time.Microsecond),
			location: kiwiFixedZone,
			expected: 0,
		},
		{
			desc:     "equal wall clock time for DTime and DTimeTZ, with TimeTZ ahead",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    NewDTimeTZFromOffset(timeofday.New(22, 0, 0, 0), sydneyTimeZone),
			expected: 1,
		},
		{
			desc:     "equal wall clock time for DTime and DTimeTZ, with TimeTZ behind",
			left:     MakeDTime(timeofday.New(12, 0, 0, 0)),
			right:    NewDTimeTZFromOffset(timeofday.New(5, 0, 0, 0), pacificTimeZone),
			expected: -1,
		},
		{
			desc:     "equal wall clock time for DTime and DTimeTZ, with TimeTZ ahead",
			left:     NewDTimeTZFromOffset(timeofday.New(22, 0, 0, 0), sydneyTimeZone),
			right:    NewDTimeTZFromOffset(timeofday.New(5, 0, 0, 0), pacificTimeZone),
			expected: -1,
		},
		{
			desc:     "wall clock time different for DTimeTZ and DTimeTZ",
			left:     NewDTimeTZFromOffset(timeofday.New(23, 0, 0, 0), sydneyTimeZone),
			right:    NewDTimeTZFromOffset(timeofday.New(5, 0, 0, 0), pacificTimeZone),
			expected: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.desc,
			func(t *testing.T) {
				ctx := &EvalContext{
					SessionData: &sessiondata.SessionData{
						DataConversion: sessiondata.DataConversionConfig{
							Location: tc.location,
						},
					},
				}
				assert.Equal(t, tc.expected, compareTimestamps(ctx, tc.left, tc.right))
				assert.Equal(t, -tc.expected, compareTimestamps(ctx, tc.right, tc.left))
			},
		)
	}
}
