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

package storage

import (
	"bytes"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestMultiIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rocksDB := newRocksDBInMem(roachpb.Attributes{}, 1<<20)
	defer rocksDB.Close()

	// Each `input` is turned into an iterator and these are passed to a new
	// MultiIterator, which is fully iterated (using either NextKey or Next) and
	// turned back into a string in the same format as `input`. This is compared
	// to expectedNextKey or expectedNext.
	//
	// Input is a string containing key, timestamp, value tuples: first a single
	// character key, then a single character timestamp walltime. If the
	// character after the timestamp is an M, this entry is a "metadata" key
	// (timestamp=0, sorts before any non-0 timestamp, and no value). If the
	// character after the timestamp is an X, this entry is a deletion
	// tombstone. Otherwise the value is the same as the timestamp.
	tests := []struct {
		inputs          []string
		expectedNextKey string
		expectedNext    string
	}{
		{[]string{}, "", ""},

		{[]string{"a1"}, "a1", "a1"},
		{[]string{"a1b1"}, "a1b1", "a1b1"},
		{[]string{"a2a1"}, "a2", "a2a1"},
		{[]string{"a2a1b1"}, "a2b1", "a2a1b1"},

		{[]string{"a1", "a2"}, "a2", "a2a1"},
		{[]string{"a2", "a1"}, "a2", "a2a1"},
		{[]string{"a1", "b2"}, "a1b2", "a1b2"},
		{[]string{"b2", "a1"}, "a1b2", "a1b2"},
		{[]string{"a1b2", "b3"}, "a1b3", "a1b3b2"},
		{[]string{"a1c2", "b3"}, "a1b3c2", "a1b3c2"},

		{[]string{"aM", "a1"}, "aM", "aMa1"},
		{[]string{"a1", "aM"}, "aM", "aMa1"},
		{[]string{"aMa2", "a1"}, "aM", "aMa2a1"},
		{[]string{"aMa1", "a2"}, "aM", "aMa2a1"},

		{[]string{"a1", "a2X"}, "a2X", "a2Xa1"},
		{[]string{"a1", "a2X", "a3"}, "a3", "a3a2Xa1"},
		{[]string{"a1", "a2Xb2"}, "a2Xb2", "a2Xa1b2"},
		{[]string{"a1b2", "a2X"}, "a2Xb2", "a2Xa1b2"},

		{[]string{"a1", "a1"}, "a1", "a1"},
		{[]string{"a4a2a1", "a4a3a1"}, "a4", "a4a3a2a1"},
		{[]string{"a1b1", "a1b2"}, "a1b2", "a1b2b1"},
		{[]string{"a1b2", "a1b1"}, "a1b2", "a1b2b1"},
		{[]string{"a1b1", "a1b1"}, "a1b1", "a1b1"},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%q", test.inputs)
		t.Run(name, func(t *testing.T) {
			var iters []SimpleIterator
			for _, input := range test.inputs {
				batch := rocksDB.NewBatch()
				defer batch.Close()
				for i := 0; ; {
					if i == len(input) {
						break
					}
					k := []byte{input[i]}
					ts := hlc.Timestamp{WallTime: int64(input[i+1])}
					var v []byte
					if i+1 < len(input) && input[i+1] == 'M' {
						ts = hlc.Timestamp{}
						v = nil
					} else if i+2 < len(input) && input[i+2] == 'X' {
						v = nil
						i++
					} else {
						v = []byte{input[i+1]}
					}
					i += 2
					if err := batch.Put(MVCCKey{Key: k, Timestamp: ts}, v); err != nil {
						t.Fatalf("%+v", err)
					}
				}
				iter := batch.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
				defer iter.Close()
				iters = append(iters, iter)
			}

			subtests := []struct {
				name     string
				expected string
				fn       func(SimpleIterator)
			}{
				{"NextKey", test.expectedNextKey, (SimpleIterator).NextKey},
				{"Next", test.expectedNext, (SimpleIterator).Next},
			}
			for _, subtest := range subtests {
				t.Run(subtest.name, func(t *testing.T) {
					var output bytes.Buffer
					it := MakeMultiIterator(iters)
					for it.SeekGE(MVCCKey{Key: keys.MinKey}); ; subtest.fn(it) {
						ok, err := it.Valid()
						if err != nil {
							t.Fatalf("unexpected error: %+v", err)
						}
						if !ok {
							break
						}
						output.Write(it.UnsafeKey().Key)
						if it.UnsafeKey().Timestamp == (hlc.Timestamp{}) {
							output.WriteRune('M')
						} else {
							output.WriteByte(byte(it.UnsafeKey().Timestamp.WallTime))
							if len(it.UnsafeValue()) == 0 {
								output.WriteRune('X')
							}
						}
					}
					if actual := output.String(); actual != subtest.expected {
						t.Errorf("got %q expected %q", actual, subtest.expected)
					}
				})
			}
		})
	}
}
