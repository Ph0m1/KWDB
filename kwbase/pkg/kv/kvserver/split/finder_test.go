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

package split

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// TestSplitFinderKey verifies the Key() method correctly
// finds an appropriate split point for the range.
func TestSplitFinderKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	const ReservoirKeyOffset = 1000

	// Test an empty reservoir (reservoir without load).
	basicReservoir := [splitKeySampleSize]sample{}

	// Test reservoir with no load should have no splits.
	noLoadReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      0,
			right:     0,
			contained: 0,
		}
		noLoadReservoir[i] = tempSample
	}

	// Test a uniform reservoir.
	uniformReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      splitKeyMinCounter,
			right:     splitKeyMinCounter,
			contained: 0,
		}
		uniformReservoir[i] = tempSample
	}

	// Testing a non-uniform reservoir.
	nonUniformReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      splitKeyMinCounter * i,
			right:     splitKeyMinCounter * (splitKeySampleSize - i),
			contained: 0,
		}
		nonUniformReservoir[i] = tempSample
	}

	// Test a load heavy reservoir on a single hot key (the last key).
	singleHotKeyReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      0,
			right:     splitKeyMinCounter,
			contained: 0,
		}
		singleHotKeyReservoir[i] = tempSample
	}

	// Test a load heavy reservoir on multiple hot keys (first and last key).
	multipleHotKeysReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      splitKeyMinCounter,
			right:     splitKeyMinCounter,
			contained: 0,
		}
		multipleHotKeysReservoir[i] = tempSample
	}
	multipleHotKeysReservoir[0].left = 0

	// Test a spanning reservoir where splits shouldn't occur.
	spanningReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      0,
			right:     0,
			contained: splitKeyMinCounter,
		}
		spanningReservoir[i] = tempSample
	}

	// Test that splits happen between two heavy spans.
	multipleSpanReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      splitKeyMinCounter,
			right:     splitKeyMinCounter,
			contained: splitKeyMinCounter,
		}
		multipleSpanReservoir[i] = tempSample
	}
	midSample := sample{
		key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + splitKeySampleSize/2)),
		left:      splitKeyMinCounter,
		right:     splitKeyMinCounter,
		contained: 0,
	}
	multipleSpanReservoir[splitKeySampleSize/2] = midSample

	testCases := []struct {
		reservoir      [splitKeySampleSize]sample
		splitByLoadKey roachpb.Key
	}{
		// Test an empty reservoir.
		{basicReservoir, nil},
		// Test reservoir with no load should have no splits.
		{noLoadReservoir, nil},
		// Test a uniform reservoir (Splits at the first key)
		{uniformReservoir, keys.MakeTablePrefix(ReservoirKeyOffset)},
		// Testing a non-uniform reservoir.
		{nonUniformReservoir, keys.MakeTablePrefix(ReservoirKeyOffset + splitKeySampleSize/2)},
		// Test a load heavy reservoir on a single hot key. Splitting can't help here.
		{singleHotKeyReservoir, nil},
		// Test a load heavy reservoir on multiple hot keys. Splits between the hot keys.
		{multipleHotKeysReservoir, keys.MakeTablePrefix(ReservoirKeyOffset + 1)},
		// Test a spanning reservoir. Splitting will be bad here. Should avoid it.
		{spanningReservoir, nil},
		// Test that splits happen between two heavy spans.
		{multipleSpanReservoir, keys.MakeTablePrefix(ReservoirKeyOffset + splitKeySampleSize/2)},
	}

	for i, test := range testCases {
		finder := NewFinder(timeutil.Now())
		finder.samples = test.reservoir
		if splitByLoadKey := finder.Key(); !bytes.Equal(splitByLoadKey, test.splitByLoadKey) {
			t.Errorf(
				"%d: expected splitByLoadKey: %v, but got splitByLoadKey: %v",
				i, test.splitByLoadKey, splitByLoadKey)
		}
	}
}

// TestSplitFinderRecorder verifies the Record() method correctly
// records a span.
func TestSplitFinderRecorder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	const ReservoirKeyOffset = 1000

	// getLargest is an IntN function that returns the largest number possible in [0, n)
	getLargest := func(n int) int {
		var result int
		if n > 0 {
			result = n - 1
		}
		return result
	}

	// getZero is an IntN function that returns 0
	getZero := func(n int) int { return 0 }

	// Test recording a key query before the reservoir is full.
	basicReservoir := [splitKeySampleSize]sample{}
	basicSpan := roachpb.Span{
		Key:    keys.MakeTablePrefix(ReservoirKeyOffset),
		EndKey: keys.MakeTablePrefix(ReservoirKeyOffset + 1),
	}
	expectedBasicReservoir := [splitKeySampleSize]sample{}
	expectedBasicReservoir[0] = sample{
		key: basicSpan.Key,
	}

	// Test recording a key query after the reservoir is full with replacement.
	replacementReservoir := [splitKeySampleSize]sample{}
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      0,
			right:     0,
			contained: 0,
		}
		replacementReservoir[i] = tempSample
	}
	replacementSpan := roachpb.Span{
		Key:    keys.MakeTablePrefix(ReservoirKeyOffset + splitKeySampleSize),
		EndKey: keys.MakeTablePrefix(ReservoirKeyOffset + splitKeySampleSize + 1),
	}
	expectedReplacementReservoir := replacementReservoir
	expectedReplacementReservoir[0] = sample{
		key: replacementSpan.Key,
	}

	// Test recording a key query after the reservoir is full without replacement.
	fullReservoir := replacementReservoir
	fullSpan := roachpb.Span{
		Key:    keys.MakeTablePrefix(ReservoirKeyOffset),
		EndKey: keys.MakeTablePrefix(ReservoirKeyOffset + 1),
	}
	expectedFullReservoir := fullReservoir
	for i := 0; i < splitKeySampleSize; i++ {
		tempSample := sample{
			key:       keys.MakeTablePrefix(uint32(ReservoirKeyOffset + i)),
			left:      1,
			right:     0,
			contained: 0,
		}
		expectedFullReservoir[i] = tempSample
	}
	expectedFullReservoir[0].left = 0
	expectedFullReservoir[0].right = 1

	// Test recording a spanning query.
	spanningReservoir := replacementReservoir
	spanningSpan := roachpb.Span{
		Key:    keys.MakeTablePrefix(ReservoirKeyOffset - 1),
		EndKey: keys.MakeTablePrefix(ReservoirKeyOffset + splitKeySampleSize + 1),
	}
	expectedSpanningReservoir := spanningReservoir
	for i := 0; i < splitKeySampleSize; i++ {
		expectedSpanningReservoir[i].contained++
	}

	testCases := []struct {
		recordSpan        roachpb.Span
		intNFn            func(int) int
		currCount         int
		currReservoir     [splitKeySampleSize]sample
		expectedReservoir [splitKeySampleSize]sample
	}{
		// Test recording a key query before the reservoir is full.
		{basicSpan, getLargest, 0, basicReservoir, expectedBasicReservoir},
		// Test recording a key query after the reservoir is full with replacement.
		{replacementSpan, getZero, splitKeySampleSize + 1, replacementReservoir, expectedReplacementReservoir},
		// Test recording a key query after the reservoir is full without replacement.
		{fullSpan, getLargest, splitKeySampleSize + 1, fullReservoir, expectedFullReservoir},
		// Test recording a spanning query.
		{spanningSpan, getLargest, splitKeySampleSize + 1, spanningReservoir, expectedSpanningReservoir},
	}

	for i, test := range testCases {
		finder := NewFinder(timeutil.Now())
		finder.samples = test.currReservoir
		finder.count = test.currCount
		finder.Record(test.recordSpan, test.intNFn)
		if !reflect.DeepEqual(finder.samples, test.expectedReservoir) {
			t.Errorf(
				"%d: expected reservoir: %v, but got reservoir: %v",
				i, test.expectedReservoir, finder.samples)
		}
	}
}
