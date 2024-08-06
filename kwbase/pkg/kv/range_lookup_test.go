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

package kv

import (
	"context"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

// TestRangeLookupRace tests that a RangeLookup will retry its scanning process
// if it sees inconsistent results. This is possible because RangeLookup scans
// are not required to be consistent or within a transaction, meaning that they
// may race with splits.
func TestRangeLookupRaceSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc1BeforeSplit := roachpb.RangeDescriptor{
		RangeID:              1,
		StartKey:             roachpb.RKey("j"),
		EndKey:               roachpb.RKey("p"),
		Generation:           proto.Int64(0),
		GenerationComparable: proto.Bool(true),
	}
	desc1AfterSplit := roachpb.RangeDescriptor{
		RangeID:              1,
		StartKey:             roachpb.RKey("j"),
		EndKey:               roachpb.RKey("m"),
		Generation:           proto.Int64(1),
		GenerationComparable: proto.Bool(true),
	}
	desc2AfterSplit := roachpb.RangeDescriptor{
		RangeID:              2,
		StartKey:             roachpb.RKey("m"),
		EndKey:               roachpb.RKey("p"),
		Generation:           proto.Int64(1),
		GenerationComparable: proto.Bool(true),
	}

	lookupKey := roachpb.Key("k")
	assertRangeLookupScan := func(ba roachpb.BatchRequest) {
		if len(ba.Requests) != 1 {
			t.Fatalf("expected single request, found %v", ba)
		}
		scan, ok := ba.Requests[0].GetInner().(*roachpb.ScanRequest)
		if !ok {
			t.Fatalf("expected single scan request, found %v", ba)
		}
		expStartKey := keys.RangeMetaKey(roachpb.RKey(lookupKey)).Next()
		if !scan.Key.Equal(expStartKey.AsRawKey()) {
			t.Fatalf("expected scan start key %v, found %v", expStartKey, scan.Key)
		}
	}

	// The test simulates a RangeLookup that experiences inconsistent results
	// twice before eventually seeing consistent results. It is expected that it
	// will continue to retry until the results are consistent and a matching
	// RangeDescriptor is found.
	//
	// The scenario is modeled after:
	// https://gitee.com/kwbasedb/kwbase/issues/19147#issuecomment-336741791
	// See that comment for a description of why a non-transactional scan starting
	// at /meta2/k may only see desc2AfterSplit when racing with a split, assuming
	// there is a range boundary at /meta2/n.
	t.Run("MissingDescriptor", func(t *testing.T) {
		badRes := newScanRespFromRangeDescriptors(&desc2AfterSplit)
		goodRes := newScanRespFromRangeDescriptors(&desc1AfterSplit)

		attempt := 0
		sender := SenderFunc(func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			// Increment the attempt counter after each attempt.
			defer func() {
				attempt++
			}()

			assertRangeLookupScan(ba)
			switch attempt {
			case 0:
				// Return an inconsistent result.
				return badRes, nil
			case 1:
				// For the sake of testing, return the same inconsistent result. In
				// reality, it shouldn't be possible to see the same race twice.
				return badRes, nil
			case 2:
				return goodRes, nil
			default:
				t.Fatalf("unexpected range lookup attempt #%d, %v", attempt, ba)
				return nil, nil
			}
		})

		rs, preRs, err := RangeLookup(
			context.Background(),
			sender,
			lookupKey,
			roachpb.READ_UNCOMMITTED,
			0,     /* prefetchNum */
			false, /* prefetchReverse */
		)
		if err != nil {
			t.Fatalf("RangeLookup returned error: %v", err)
		}
		if len(preRs) != 0 {
			t.Fatalf("RangeLookup returned unexpected prefetched descriptors: %v", preRs)
		}
		expRs := []roachpb.RangeDescriptor{desc1AfterSplit}
		if !reflect.DeepEqual(expRs, rs) {
			t.Fatalf("expected RangeLookup to return %v, found %v", expRs, rs)
		}
	})

	// This scenario is similar to the previous one, but assumes that a scan
	// sees two descriptors that match its desired key, one from before a split
	// and one from after. The can occur both with inconsistent scans and with
	// non-transactional scans that span ranges. This situation is easier to
	// handle because we can figure out which descriptor is stale without
	// needing to perform another scan.
	t.Run("ExtraDescriptor", func(t *testing.T) {
		// It doesn't matter which descriptor comes first in the response.
		testutils.RunTrueAndFalse(t, "staleFirst", func(t *testing.T, staleFirst bool) {
			goodRes := newScanRespFromRangeDescriptors(&desc1AfterSplit, &desc1BeforeSplit)
			if staleFirst {
				goodRes = newScanRespFromRangeDescriptors(&desc1BeforeSplit, &desc1AfterSplit)
			}

			attempt := 0
			sender := SenderFunc(func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				// Increment the attempt counter after each attempt.
				defer func() {
					attempt++
				}()

				assertRangeLookupScan(ba)
				switch attempt {
				case 0:
					return goodRes, nil
				default:
					t.Fatalf("unexpected range lookup attempt #%d, %v", attempt, ba)
					return nil, nil
				}
			})

			rs, preRs, err := RangeLookup(
				context.Background(),
				sender,
				lookupKey,
				roachpb.READ_UNCOMMITTED,
				0,     /* prefetchNum */
				false, /* prefetchReverse */
			)
			if err != nil {
				t.Fatalf("RangeLookup returned error: %v", err)
			}
			if len(preRs) != 0 {
				t.Fatalf("RangeLookup returned unexpected prefetched descriptors: %v", preRs)
			}
			expRs := []roachpb.RangeDescriptor{desc1AfterSplit}
			if !reflect.DeepEqual(expRs, rs) {
				t.Fatalf("expected RangeLookup to return %v, found %v", expRs, rs)
			}
		})
	})
}

func newScanRespFromRangeDescriptors(descs ...*roachpb.RangeDescriptor) *roachpb.BatchResponse {
	br := &roachpb.BatchResponse{}
	r := &roachpb.ScanResponse{}
	for _, desc := range descs {
		var kv roachpb.KeyValue
		if err := kv.Value.SetProto(desc); err != nil {
			panic(err)
		}
		r.Rows = append(r.Rows, kv)
	}
	br.Add(r)
	return br
}
