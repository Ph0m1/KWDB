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

package props

import (
	"sort"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/constraint"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// SortedHistogram captures the distribution of values for a timestamp column for the entities.
type SortedHistogram struct {
	evalCtx *tree.EvalContext
	col     opt.ColumnID
	buckets []cat.SortedHistogramBucket
}

// Init initializes the histogram with data from the catalog.
func (h *SortedHistogram) Init(
	evalCtx *tree.EvalContext, col opt.ColumnID, buckets []cat.SortedHistogramBucket,
) {
	h.evalCtx = evalCtx
	h.col = col
	h.buckets = buckets
}

// BucketCount returns the number of buckets in the histogram.
func (h *SortedHistogram) BucketCount() int {
	return len(h.buckets)
}

// Bucket returns a pointer to the ith bucket in the histogram.
// i must be greater than or equal to 0 and less than BucketCount.
func (h *SortedHistogram) Bucket(i int) *cat.SortedHistogramBucket {
	return &h.buckets[i]
}

// makeSpanFromSortBucket is used to construct a span by sort bucket.
func makeSpanFromSortBucket(
	b *cat.SortedHistogramBucket, lowerBound tree.Datum,
) (span constraint.Span) {
	span.Init(
		constraint.MakeKey(lowerBound),
		constraint.IncludeBoundary,
		constraint.MakeKey(b.UpperBound),
		constraint.IncludeBoundary,
	)
	return span
}

// addEmptySortBucket is used to add a empty bucket for sort histogram.
func (h *SortedHistogram) addEmptySortBucket(upperBound tree.Datum) {
	h.addBucket(&cat.SortedHistogramBucket{UpperBound: upperBound})
}

// addBucket is used to add a new bucket for sort histogram.
func (h *SortedHistogram) addBucket(bucket *cat.SortedHistogramBucket) {
	// Check whether we can combine this bucket with the previous bucket.
	if len(h.buckets) != 0 {
		lastBucket := &h.buckets[len(h.buckets)-1]
		if lastBucket.RowCount == 0 && lastBucket.UnorderedRowCount == 0 &&
			bucket.RowCount == 0 && bucket.UnorderedRowCount == 0 {
			lastBucket.UpperBound = bucket.UpperBound
			return
		}
		if lastBucket.UpperBound.Compare(h.evalCtx, bucket.UpperBound) == 0 {
			lastBucket.RowCount += bucket.RowCount
			lastBucket.UnorderedEntities += bucket.UnorderedEntities
			lastBucket.OrderedEntities = (lastBucket.OrderedEntities + bucket.OrderedEntities) / 2
			lastBucket.UnorderedEntities = (lastBucket.UnorderedEntities + bucket.UnorderedEntities) / 2
			return
		}
	}
	h.buckets = append(h.buckets, *bucket)
}

// getNextLowerBound is used to get next value for current UpperBound.
func (h *SortedHistogram) getNextLowerBound(currentUpperBound tree.Datum) tree.Datum {
	nextLowerBound, ok := currentUpperBound.Next(h.evalCtx)
	if !ok {
		nextLowerBound = currentUpperBound
	}
	return nextLowerBound
}

// getFilteredBucket filters the histogram bucket according to the given span,
// and returns a new bucket with the results.
func getFilteredSortBucket(
	b *cat.SortedHistogramBucket,
	keyCtx *constraint.KeyContext,
	filteredSpan *constraint.Span,
	bucketLowerBound tree.Datum,
) *cat.SortedHistogramBucket {
	spanLowerBound := filteredSpan.StartKey().Value(0)
	spanUpperBound := filteredSpan.EndKey().Value(0)

	// Check that the given span is contained in the bucket.
	cmpSpanStartBucketStart := spanLowerBound.Compare(keyCtx.EvalCtx, bucketLowerBound)
	cmpSpanEndBucketEnd := spanUpperBound.Compare(keyCtx.EvalCtx, b.UpperBound)
	if cmpSpanStartBucketStart < 0 || cmpSpanEndBucketEnd > 0 {
		panic(errors.AssertionFailedf("span must be fully contained in the bucket"))
	}

	// Extract the range sizes before and after filtering. Only numeric and
	// date-time types will have ok=true, since these are the only types for
	// which we can accurately calculate the range size of a non-equality span.
	rangeBefore, rangeAfter, ok := getRangesBeforeAndAfter(
		bucketLowerBound, b.UpperBound, spanLowerBound, spanUpperBound,
	)

	// Determine whether this span represents an equality condition.
	isEqualityCondition := spanLowerBound.Compare(keyCtx.EvalCtx, spanUpperBound) == 0

	// Calculate the new value for row count.
	var rowCount, unorderedRowCount, unorderedEntities, orderedEntities float64
	if isEqualityCondition {
		rowCount = 1
		unorderedRowCount = 0
	} else if ok && rangeBefore > 0 {
		// If we were successful in finding the ranges before and after filtering,
		// calculate the fraction of values that should be assigned to the new
		// bucket.
		rowCount = (b.RowCount) * rangeAfter / rangeBefore
		unorderedRowCount = (b.UnorderedRowCount) * rangeAfter / rangeBefore
		orderedEntities = (b.OrderedEntities) * rangeAfter / rangeBefore
		unorderedEntities = (b.UnorderedEntities) * rangeAfter / rangeBefore
	} else {
		// In the absence of any information, assume we reduced the size of the
		// bucket by half.
		rowCount = 0.5 * (b.RowCount)
		unorderedRowCount = 0.5 * (b.UnorderedRowCount)
		orderedEntities = 0.5 * (b.OrderedEntities)
		unorderedEntities = 0.5 * (b.UnorderedEntities)
	}

	return &cat.SortedHistogramBucket{
		RowCount:          rowCount,
		UnorderedRowCount: unorderedRowCount,
		OrderedEntities:   orderedEntities,
		UnorderedEntities: unorderedEntities,
		UpperBound:        spanUpperBound,
	}
}

// ValuesCount returns the total number of values in the sort histogram.
func (h *SortedHistogram) ValuesCount() float64 {
	var count float64
	for i := range h.buckets {
		count += float64(h.buckets[i].RowCount)
	}
	return count
}

// UnorderedValuesCount returns the total number of unordered values in the sort histogram.
func (h *SortedHistogram) UnorderedValuesCount() float64 {
	var count float64
	for i := range h.buckets {
		count += float64(h.buckets[i].UnorderedRowCount)
	}
	return count
}

// EntitiesCount returns the total number of entities count in the sort histogram.
func (h *SortedHistogram) EntitiesCount() float64 {
	var count float64
	for i := range h.buckets {
		count += float64(h.buckets[i].OrderedEntities)
		count += float64(h.buckets[i].UnorderedEntities)
	}
	return count
}

// UnorderedEntitiesCount returns the total number of unordered entities count in the sort histogram.
func (h *SortedHistogram) UnorderedEntitiesCount() float64 {
	var count float64
	for i := range h.buckets {
		count += float64(h.buckets[i].UnorderedEntities)
	}
	return count
}

// CanFilter returns true if the given constraint can filter the histogram.
// This is the case if there is only one constrained column in c, it is
// ascending, and it matches the column of the histogram.
func (h *SortedHistogram) CanFilter(c *constraint.Constraint, md *opt.Metadata) bool {
	if h.BucketCount() <= 0 {
		return false
	}
	if c.ConstrainedColumns(h.evalCtx) != 1 || c.Columns.Get(0).ID() != h.col {
		return false
	}
	col := c.Columns.Get(0).ID()
	if md.ColumnMeta(col).Table.ColumnOrdinal(col) != 0 {
		return false
	}
	if c.Columns.Get(0).Descending() {
		return false
	}
	return true
}

// Filter filters the histogram according to the given constraint, and returns
// a new histogram with the results. CanFilter should be called first to
// validate that c can filter the histogram.
func (h *SortedHistogram) Filter(c *constraint.Constraint) *SortedHistogram {
	bucketCount := h.BucketCount()
	filtered := &SortedHistogram{
		buckets: make([]cat.SortedHistogramBucket, 0, bucketCount),
	}
	if bucketCount == 0 {
		return filtered
	}

	// The lower bound of the histogram is the upper bound of the first bucket.
	lowerBound := h.Bucket(0).UpperBound

	bucIndex := 0
	spanIndex := 0
	keyCtx := constraint.KeyContext{EvalCtx: h.evalCtx}
	keyCtx.Columns.InitSingle(opt.MakeOrderingColumn(h.col, false /* descending */))

	// Find the first span that may overlap with the histogram.
	firstBucket := makeSpanFromSortBucket(h.Bucket(bucIndex), lowerBound)
	spanCount := c.Spans.Count()
	for spanIndex < spanCount {
		span := c.Spans.Get(spanIndex)
		if firstBucket.StartsAfter(&keyCtx, span) {
			spanIndex++
			continue
		}
		break
	}
	if spanIndex == spanCount {
		return filtered
	}

	// Use binary search to find the first bucket that overlaps with the span.
	span := c.Spans.Get(spanIndex)
	bucIndex = sort.Search(bucketCount, func(i int) bool {
		// The lower bound of the bucket doesn't matter here since we're just
		// checking whether the span starts after the *upper bound* of the bucket.
		bucket := makeSpanFromSortBucket(h.Bucket(i), lowerBound)
		return !span.StartsAfter(&keyCtx, &bucket)
	})
	if bucIndex == bucketCount {
		return filtered
	}
	if bucIndex > 0 {
		prevUpperBound := h.Bucket(bucIndex - 1).UpperBound
		filtered.addEmptySortBucket(prevUpperBound)
		lowerBound = h.getNextLowerBound(prevUpperBound)
	}

	// For the remaining buckets and spans, use a variation on merge sort.
	for bucIndex < bucketCount && spanIndex < spanCount {
		bucket := h.Bucket(bucIndex)
		// Convert the bucket to a span in order to take advantage of the
		// constraint library.
		left := makeSpanFromSortBucket(bucket, lowerBound)
		right := c.Spans.Get(spanIndex)

		if left.StartsAfter(&keyCtx, right) {
			spanIndex++
			continue
		}

		filteredSpan := left
		if !filteredSpan.TryIntersectWith(&keyCtx, right) {
			filtered.addEmptySortBucket(bucket.UpperBound)
			lowerBound = h.getNextLowerBound(bucket.UpperBound)
			bucIndex++
			continue
		}

		filteredBucket := bucket
		if filteredSpan.Compare(&keyCtx, &left) != 0 {
			// The bucket was cut off in the middle. Get the resulting filtered
			// bucket.
			filteredBucket = getFilteredSortBucket(bucket, &keyCtx, &filteredSpan, lowerBound)
			if filteredSpan.CompareStarts(&keyCtx, &left) != 0 {
				// We need to add an empty bucket before the new bucket.
				emptyBucketUpperBound := filteredSpan.StartKey().Value(0)
				if filteredSpan.StartBoundary() == constraint.IncludeBoundary {
					if prev, ok := emptyBucketUpperBound.Prev(h.evalCtx); ok {
						emptyBucketUpperBound = prev
					}
				}
				filtered.addEmptySortBucket(emptyBucketUpperBound)
			}
		}
		filtered.addBucket(filteredBucket)

		// Skip past whichever span ends first, or skip past both if they have
		// the same endpoint.
		cmp := left.CompareEnds(&keyCtx, right)
		if cmp <= 0 {
			lowerBound = h.getNextLowerBound(bucket.UpperBound)
			bucIndex++
		}
		if cmp >= 0 {
			spanIndex++
		}
	}

	return filtered
}
