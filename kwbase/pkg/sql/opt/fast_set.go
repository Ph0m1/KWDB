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

package opt

import (
	"bytes"
	"fmt"
	"math/bits"

	"github.com/cockroachdb/errors"
	"golang.org/x/tools/container/intsets"
)

// FastSet flag col id
type FastSet struct {
	small uint64  // quick operator
	large *IntSet // need malloc buffer to store
}

// We store bits for values smaller than this cutoff.
// Note: this can be set to a smaller value, e.g. for testing.
const smallCutoff = 64

func (s *FastSet) toLarge() *IntSet {
	if s.large != nil {
		return s.large
	}
	large := new(IntSet)
	for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
		large.Add(i)
	}
	return large
}

func (s *FastSet) largeToSmall() (small uint64, otherValues bool) {
	if s.large == nil {
		panic(errors.AssertionFailedf("set not large"))
	}
	return s.small, s.large.HasOtherValue()
}

// Add adds value to the set
func (s *FastSet) Add(i int) {
	withinSmallBounds := i >= 0 && i < smallCutoff
	if withinSmallBounds {
		s.small |= (1 << uint64(i))
	}
	if !withinSmallBounds && s.large == nil {
		s.large = s.toLarge()
	}
	if s.large != nil {
		s.large.Add(i)
	}
}

// Remove removes a value from the set. No-op if the value is not in the set.
func (s *FastSet) Remove(i int) {
	if i >= 0 && i < smallCutoff {
		s.small &= ^(1 << uint64(i))
	}
	if s.large != nil {
		s.large.Remove(i)
	}
}

// Contains returns true if the set contains the value.
func (s FastSet) Contains(i int) bool {
	if s.large != nil {
		return s.large.Contains(i)
	}

	if i >= 0 && i < smallCutoff {
		return (s.small & (1 << uint64(i))) != 0
	}

	return false
}

// Empty returns true if the set is empty.
func (s FastSet) Empty() bool {
	return s.small == 0 && (s.large == nil || s.large.Empty())
}

// Len returns the number of the elements in the set.
func (s FastSet) Len() int {
	if s.large != nil {
		return s.large.Len()
	}
	return bits.OnesCount64(s.small)
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s FastSet) Next(startVal int) (int, bool) {
	if startVal < smallCutoff {
		if startVal < 0 {
			startVal = 0
		}

		if ntz := bits.TrailingZeros64(s.small >> uint64(startVal)); ntz < 64 {
			return startVal + ntz, true
		}
	}
	if s.large != nil {
		return s.large.Next(startVal)
	}
	return intsets.MaxInt, false
}

// ForEach calls a function for each value in the set (in increasing order).
func (s FastSet) ForEach(f func(i int)) {
	if s.large != nil {
		s.large.ForEach(f)
		return
	}

	for v := s.small; v != 0; {
		i := bits.TrailingZeros64(v)
		f(i)
		v &^= 1 << uint(i)
	}
}

// Ordered returns a slice with all the integers in the set, in increasing order.
func (s FastSet) Ordered() []int {
	if s.Empty() {
		return nil
	}
	if s.large != nil {
		return s.large.Ordered()
	}
	result := make([]int, 0, s.Len())
	s.ForEach(func(i int) {
		result = append(result, i)
	})
	return result
}

// Copy returns a copy of s which can be modified independently.
func (s FastSet) Copy() FastSet {
	var c FastSet
	c.small = s.small
	if s.large != nil {
		c.large = new(IntSet)
		c.large.CopyFrom(s.large)
	}
	return c
}

// CopyFrom sets the receiver to a copy of other, which can then be modified
// independently.
func (s *FastSet) CopyFrom(other FastSet) {
	s.small = other.small
	if other.large != nil {
		if s.large == nil {
			s.large = new(IntSet)
		}
		s.large.CopyFrom(other.large)
	} else {
		if s.large != nil {
			s.large = nil
		}
	}
}

// UnionWith adds all the elements from rhs to this set.
func (s *FastSet) UnionWith(rhs FastSet) {
	s.small |= rhs.small
	if s.large == nil && rhs.large == nil {
		// Fast path.
		return
	}

	if s.large == nil {
		s.large = s.toLarge()
	}
	if rhs.large == nil {
		for i, ok := rhs.Next(0); ok; i, ok = rhs.Next(i + 1) {
			s.large.Add(i)
		}
	} else {
		s.large.UnionWith(rhs.large)
	}
}

// Union returns the union of s and rhs as a new set.
func (s FastSet) Union(rhs FastSet) FastSet {
	r := s.Copy()
	r.UnionWith(rhs)
	return r
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *FastSet) IntersectionWith(rhs FastSet) {
	s.small &= rhs.small
	if rhs.large == nil {
		s.large = nil
	}
	if s.large == nil {
		// Fast path.
		return
	}

	s.large.IntersectionWith(rhs.toLarge())
}

// Intersection returns the intersection of s and rhs as a new set.
func (s FastSet) Intersection(rhs FastSet) FastSet {
	r := s.Copy()
	r.IntersectionWith(rhs)
	return r
}

// Intersects returns true if s has any elements in common with rhs.
func (s FastSet) Intersects(rhs FastSet) bool {
	if (s.small & rhs.small) != 0 {
		return true
	}
	if s.large == nil || rhs.large == nil {
		return false
	}
	return s.large.Intersects(rhs.toLarge())
}

// DifferenceWith removes any elements in rhs from this set.
func (s *FastSet) DifferenceWith(rhs FastSet) {
	s.small &^= rhs.small
	if s.large != nil {
		s.large.DifferenceWith(rhs.toLarge())
	}
}

// Difference returns the elements of s that are not in rhs as a new set.
func (s FastSet) Difference(rhs FastSet) FastSet {
	r := s.Copy()
	r.DifferenceWith(rhs)
	return r
}

// Equals returns true if the two sets are identical.
func (s FastSet) Equals(rhs FastSet) bool {
	if s.large == nil && rhs.large == nil {
		return s.small == rhs.small
	}
	if s.large != nil && rhs.large != nil {
		return s.large.Equals(rhs.large)
	}
	// One set is "large" and one is "small". They might still be equal (the large
	// set could have had a large element added and then removed).
	var extraVals bool
	s1 := s.small
	s2 := rhs.small
	if s.large != nil {
		s1, extraVals = s.largeToSmall()
	} else {
		s2, extraVals = rhs.largeToSmall()
	}
	return !extraVals && s1 == s2
}

// SubsetOf returns true if rhs contains all the elements in s.
func (s FastSet) SubsetOf(rhs FastSet) bool {
	if s.large == nil {
		return (s.small & rhs.small) == s.small
	}
	if s.large != nil && rhs.large != nil {
		return s.large.SubsetOf(rhs.large)
	}
	// s is "large" and rhs is "small".
	_, hasExtra := s.largeToSmall()
	if hasExtra {
		return false
	}
	// s is "large", but has no elements outside of the range [0, 63]. This can
	// happen if elements outside of the range are removed.
	return (s.small & rhs.small) == s.small
}

// String returns a list representation of elements. Sequential runs of positive
// numbers are shown as ranges. For example, for the set {0, 1, 2, 5, 6, 10},
// the output is "(0-2,5,6,10)".
func (s FastSet) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	appendRange := func(start, end int) {
		if buf.Len() > 1 {
			buf.WriteByte(',')
		}
		if start == end {
			fmt.Fprintf(&buf, "%d", start)
		} else if start+1 == end {
			fmt.Fprintf(&buf, "%d,%d", start, end)
		} else {
			fmt.Fprintf(&buf, "%d-%d", start, end)
		}
	}
	rangeStart, rangeEnd := -1, -1
	s.ForEach(func(i int) {
		if i < 0 {
			appendRange(i, i)
			return
		}
		if rangeStart != -1 && rangeEnd == i-1 {
			rangeEnd = i
		} else {
			if rangeStart != -1 {
				appendRange(rangeStart, rangeEnd)
			}
			rangeStart, rangeEnd = i, i
		}
	})
	if rangeStart != -1 {
		appendRange(rangeStart, rangeEnd)
	}
	buf.WriteByte(')')
	return buf.String()
}
