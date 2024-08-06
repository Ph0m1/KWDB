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

package ptstorage

import (
	"strconv"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptpb"
	roachpb "gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestValidateRecordForProtect(t *testing.T) {
	spans := []roachpb.Span{
		{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("b"),
		},
	}
	for i, tc := range []struct {
		r   *ptpb.Record
		err error
	}{
		{
			r: &ptpb.Record{
				ID:        uuid.MakeV4(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
				Spans:     spans,
			},
			err: nil,
		},
		{
			r: &ptpb.Record{
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
				Spans:     spans,
			},
			err: errZeroID,
		},
		{
			r: &ptpb.Record{
				ID:       uuid.MakeV4(),
				MetaType: "job",
				Meta:     []byte("junk"),
				Spans:    spans,
			},
			err: errZeroTimestamp,
		},
		{
			r: &ptpb.Record{
				ID:        uuid.MakeV4(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				Meta:      []byte("junk"),
				Spans:     spans,
			},
			err: errInvalidMeta,
		},
		{
			r: &ptpb.Record{
				ID:        uuid.MakeV4(),
				Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1},
				MetaType:  "job",
				Meta:      []byte("junk"),
			},
			err: errEmptySpans,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, validateRecordForProtect(tc.r), tc.err)
		})
	}
}
