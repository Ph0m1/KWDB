// Copyright 2017 The Cockroach Authors.
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

package tscache

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

const (
	// defaultSklPageSize is the default size of each page in the sklImpl's
	// read and write intervalSkl.
	defaultSklPageSize = 32 << 20 // 32 MB
	// TestSklPageSize is passed to tests as the size of each page in the
	// sklImpl to limit its memory footprint. Reducing this size can hurt
	// performance but it decreases the risk of OOM failures when many tests
	// are running concurrently.
	TestSklPageSize = 128 << 10 // 128 KB
)

// sklImpl implements the Cache interface. It maintains a pair of skiplists
// containing keys or key ranges and the timestamps at which they were most
// recently read or written. If a timestamp was read or written by a
// transaction, the txn ID is stored with the timestamp to avoid advancing
// timestamps on successive requests from the same transaction.
type sklImpl struct {
	cache    *intervalSkl
	clock    *hlc.Clock
	pageSize uint32
	metrics  Metrics
}

var _ Cache = &sklImpl{}

// newSklImpl returns a new treeImpl with the supplied hybrid clock.
func newSklImpl(clock *hlc.Clock, pageSize uint32) *sklImpl {
	if pageSize == 0 {
		pageSize = defaultSklPageSize
	}
	tc := sklImpl{clock: clock, pageSize: pageSize, metrics: makeMetrics()}
	tc.clear(clock.Now())
	return &tc
}

// clear clears the cache and resets the low-water mark.
func (tc *sklImpl) clear(lowWater hlc.Timestamp) {
	tc.cache = newIntervalSkl(tc.clock, MinRetentionWindow, tc.pageSize, tc.metrics.Skl)
	tc.cache.floorTS = lowWater
}

// Add implements the Cache interface.
func (tc *sklImpl) Add(start, end roachpb.Key, ts hlc.Timestamp, txnID uuid.UUID) {
	start, end = tc.boundKeyLengths(start, end)

	val := cacheValue{ts: ts, txnID: txnID}
	if len(end) == 0 {
		tc.cache.Add(nonNil(start), val)
	} else {
		tc.cache.AddRange(nonNil(start), end, excludeTo, val)
	}
}

// SetLowWater implements the Cache interface.
func (tc *sklImpl) SetLowWater(start, end roachpb.Key, ts hlc.Timestamp) {
	tc.Add(start, end, ts, noTxnID)
}

// getLowWater implements the Cache interface.
func (tc *sklImpl) getLowWater() hlc.Timestamp {
	return tc.cache.FloorTS()
}

// GetMax implements the Cache interface.
func (tc *sklImpl) GetMax(start, end roachpb.Key) (hlc.Timestamp, uuid.UUID) {
	var val cacheValue
	if len(end) == 0 {
		val = tc.cache.LookupTimestamp(nonNil(start))
	} else {
		val = tc.cache.LookupTimestampRange(nonNil(start), end, excludeTo)
	}
	return val.ts, val.txnID
}

// boundKeyLengths makes sure that the key lengths provided are well below the
// size of each sklPage, otherwise we'll never be successful in adding it to
// an intervalSkl.
func (tc *sklImpl) boundKeyLengths(start, end roachpb.Key) (roachpb.Key, roachpb.Key) {
	// We bound keys to 1/32 of the page size. These could be slightly larger
	// and still not trigger the "key range too large" panic in intervalSkl,
	// but anything larger could require multiple page rotations before it's
	// able to fit in if other ranges are being added concurrently.
	maxKeySize := int(tc.pageSize / 32)

	// If either key is too long, truncate its length, making sure to always
	// grow the [start,end) range instead of shrinking it. This will reduce the
	// precision of the entry in the cache, which could allow independent
	// requests to interfere, but will never permit consistency anomalies.
	if l := len(start); l > maxKeySize {
		start = start[:maxKeySize]
		log.Warningf(context.TODO(), "start key with length %d exceeds maximum key length of %d; "+
			"losing precision in timestamp cache", l, maxKeySize)
	}
	if l := len(end); l > maxKeySize {
		end = end[:maxKeySize].PrefixEnd() // PrefixEnd to grow range
		log.Warningf(context.TODO(), "end key with length %d exceeds maximum key length of %d; "+
			"losing precision in timestamp cache", l, maxKeySize)
	}
	return start, end
}

// Metrics implements the Cache interface.
func (tc *sklImpl) Metrics() Metrics {
	return tc.metrics
}

// intervalSkl doesn't handle nil keys the same way as empty keys. Cockroach's
// KV API layer doesn't make a distinction.
var emptyStartKey = []byte("")

func nonNil(b []byte) []byte {
	if b == nil {
		return emptyStartKey
	}
	return b
}
