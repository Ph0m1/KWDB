// Copyright 2020 The Cockroach Authors.
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

package unique

import (
	"bytes"
	"sort"
)

// UniquifyByteSlices takes as input a slice of slices of bytes, and
// deduplicates them using a sort and unique. The output will not contain any
// duplicates but it will be sorted.
func UniquifyByteSlices(slices [][]byte) [][]byte {
	if len(slices) == 0 {
		return slices
	}
	// First sort:
	sort.Slice(slices, func(i int, j int) bool {
		return bytes.Compare(slices[i], slices[j]) < 0
	})
	// Then distinct: (wouldn't it be nice if Go had generics?)
	lastUniqueIdx := 0
	for i := 1; i < len(slices); i++ {
		if !bytes.Equal(slices[i], slices[lastUniqueIdx]) {
			// We found a unique entry, at index i. The last unique entry in the array
			// was at lastUniqueIdx, so set the entry after that one to our new unique
			// entry, and bump lastUniqueIdx for the next loop iteration.
			lastUniqueIdx++
			slices[lastUniqueIdx] = slices[i]
		}
	}
	slices = slices[:lastUniqueIdx+1]
	return slices
}
