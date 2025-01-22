// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"math/bits"

	"golang.org/x/tools/container/intsets"
)

const (
	// uint64 is 64 bit, per bit flag a column id, start from 1
	// column id is 1, block is 2(1 << 1), max column id is 63
	blockBit = 64

	// blockExpandRate expand block
	blockExpandRate = 2
)

// IntSet flag col id
type IntSet struct {
	blockSize uint64   // block count
	curCutoff uint64   // block cut off
	block     []uint64 // quick operator
}

func (s *IntSet) init(size uint64) {
	s.curCutoff = size * blockBit
	s.blockSize = size
	s.block = make([]uint64, size)
}

func (s *IntSet) expand(size uint64, auto bool) {
	if auto {
		if s.curCutoff == 0 && size == 0 {
			s.init(1)
		} else {
			old := s.block
			oldSize := s.blockSize
			newSize := s.blockSize * blockExpandRate
			if newSize < size {
				newSize = size + 2
			}
			s.init(newSize)
			copy(s.block[:oldSize], old[:oldSize])
		}
	} else if size > s.blockSize {
		old := s.block
		oldSize := s.blockSize
		s.init(size)
		copy(s.block[:oldSize], old[:oldSize])
	} else if size < s.blockSize {
		old := s.block
		s.init(size)
		copy(s.block[:size], old[:size])
	}
}

func index(i int) (int, int) {
	return i / blockBit, i % blockBit
}

// Add adds value to the set
func (s *IntSet) Add(i int) {
	// expand block for store
	if i >= int(s.curCutoff) {
		s.expand(uint64(i/blockBit)+1, true)
	}
	idx, val := index(i)
	s.block[idx] |= (1 << uint64(val))
}

// HasOtherValue check large have Values other than small
func (s *IntSet) HasOtherValue() bool {
	for i := len(s.block) - 1; i > 0; i-- {
		if s.block[i] > 0 {
			return true
		}
	}
	return false
}

// Remove removes a value from the set. No-op if the value is not in the set.
func (s *IntSet) Remove(i int) {
	idx, val := index(i)
	if int(s.blockSize) <= idx {
		return
	}
	s.block[idx] &= ^(1 << uint64(val))
}

// Contains returns true if the set contains the value.
func (s *IntSet) Contains(i int) bool {
	idx, val := index(i)
	if int(s.blockSize) <= idx {
		return false
	}
	return (s.block[idx] & (1 << uint64(val))) != 0
}

// Empty returns true if the set is empty.
func (s *IntSet) Empty() bool {
	for i := range s.block {
		if s.block[i] != 0 {
			return false
		}
	}

	return true
}

// Len returns the number of the elements in the set.
func (s *IntSet) Len() int {
	count := 0
	for _, v := range s.block {
		if v > 0 {
			count += bits.OnesCount64(v)
		}
	}

	return count
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s *IntSet) Next(startVal int) (int, bool) {
	if startVal < 0 {
		startVal = 0
	}
	idx, val := index(startVal)
	var tmp uint64
	for {
		if idx >= int(s.blockSize) {
			break
		}
		if s.block[idx] > 0 {
			tmp = s.block[idx] >> uint64(val)
			if ntz := bits.TrailingZeros64(tmp); ntz < 64 {
				return val + ntz + idx*blockBit, true
			}
		}

		// overflow for uint64
		// over one block , start val need start 0
		// 54 not find next, so idx++ -> 1 and start need become 0
		idx++
		val = 0
	}

	return intsets.MaxInt, false
}

// ForEach calls a function for each value in the set (in increasing order).
func (s *IntSet) ForEach(f func(i int)) {
	for j := range s.block {
		for v := s.block[j]; v != 0; {
			i := bits.TrailingZeros64(v)
			f(i + j*blockBit)
			v &^= 1 << uint(i)
		}
	}
}

// Ordered returns a slice with all the integers in the set, in increasing order.
func (s *IntSet) Ordered() []int {
	if s.Empty() {
		return nil
	}
	result := make([]int, 0, s.Len())
	s.ForEach(func(i int) {
		result = append(result, i)
	})
	return result
}

// Copy returns a copy of s which can be modified independently.
func (s *IntSet) Copy() *IntSet {
	var c IntSet
	c.init(s.blockSize)
	copy(c.block[:s.blockSize], s.block[:s.blockSize])
	return &c
}

// CopyFrom sets the receiver to a copy of other, which can then be modified
// independently.
func (s *IntSet) CopyFrom(other *IntSet) {
	s.init(other.blockSize)
	copy(s.block[:other.blockSize], other.block[:other.blockSize])
}

// UnionWith adds all the elements from rhs to this set.
func (s *IntSet) UnionWith(rhs *IntSet) {
	if s.blockSize < rhs.blockSize {
		s.expand(rhs.blockSize, false)
	}

	for j := range rhs.block {
		s.block[j] |= rhs.block[j]
	}
}

// Union returns the union of s and rhs as a new set.
func (s *IntSet) Union(rhs *IntSet) *IntSet {
	r := s.Copy()
	r.UnionWith(rhs)
	return r
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *IntSet) IntersectionWith(rhs *IntSet) {
	if s.blockSize > rhs.blockSize {
		s.expand(rhs.blockSize, false)
	}

	for i := range s.block {
		s.block[i] &= rhs.block[i]
	}
}

// Intersection returns the intersection of s and rhs as a new set.
func (s *IntSet) Intersection(rhs *IntSet) *IntSet {
	r := s.Copy()
	r.IntersectionWith(rhs)
	return r
}

// Intersects returns true if s has any elements in common with rhs.
func (s *IntSet) Intersects(rhs *IntSet) bool {
	size := 0
	if s.blockSize < rhs.blockSize {
		size = int(s.blockSize)
	} else {
		size = int(rhs.blockSize)
	}
	for i := 0; i < size; i++ {
		if (s.block[i] & rhs.block[i]) != 0 {
			return true
		}
	}
	return false
}

// DifferenceWith removes any elements in rhs from this set.
func (s *IntSet) DifferenceWith(rhs *IntSet) {
	size := 0
	if s.blockSize < rhs.blockSize {
		size = int(s.blockSize)
	} else {
		size = int(rhs.blockSize)
	}
	for i := 0; i < size; i++ {
		s.block[i] &^= rhs.block[i]
	}
}

// Difference returns the elements of s that are not in rhs as a new set.
func (s *IntSet) Difference(rhs *IntSet) *IntSet {
	r := s.Copy()
	r.DifferenceWith(rhs)
	return r
}

// Equals returns true if the two sets are identical.
func (s *IntSet) Equals(rhs *IntSet) bool {
	var checkSize int
	var check *[]uint64
	var leaveCheck *[]uint64
	if s.blockSize == rhs.blockSize {
		checkSize = int(s.blockSize)
		check = &s.block
	} else if s.blockSize > rhs.blockSize {
		checkSize = int(s.blockSize)
		leaveCheck = &s.block
		check = &rhs.block
	} else {
		checkSize = int(rhs.blockSize)
		leaveCheck = &rhs.block
		check = &s.block
	}

	var j int
	for j = range *check {
		if s.block[j] != rhs.block[j] {
			return false
		}
	}
	if leaveCheck != nil {
		for ; j < checkSize; j++ {
			if (*leaveCheck)[j] != 0 {
				return false
			}
		}
	}

	return true
}

// SubsetOf returns true if rhs contains all the elements in s.
func (s *IntSet) SubsetOf(rhs *IntSet) bool {
	// left less than right, right has value, return false
	if s.blockSize < rhs.blockSize {
		for j := range s.block {
			if (s.block[j] & rhs.block[j]) != s.block[j] {
				return false
			}
		}

		return true
	} else if s.blockSize > rhs.blockSize {
		for j := range rhs.block {
			if (s.block[j] & rhs.block[j]) != s.block[j] {
				return false
			}
		}

		for i := rhs.blockSize; i < s.blockSize; i++ {
			if s.block[i] > 0 {
				return false
			}
		}

		return true
	}

	for j := range rhs.block {
		if (s.block[j] & rhs.block[j]) != s.block[j] {
			return false
		}
	}

	return true
}
