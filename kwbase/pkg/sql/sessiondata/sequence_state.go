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

package sessiondata

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// SequenceState stores session-scoped state used by sequence builtins.
//
// All public methods of SequenceState are thread-safe, as the structure is
// meant to be shared by statements executing in parallel on a session.
type SequenceState struct {
	mu struct {
		syncutil.Mutex
		// latestValues stores the last value obtained by nextval() in this session
		// by descriptor id.
		latestValues map[uint32]int64

		// lastSequenceIncremented records the descriptor id of the last sequence
		// nextval() was called on in this session.
		lastSequenceIncremented uint32
	}
}

// NewSequenceState creates a SequenceState.
func NewSequenceState() *SequenceState {
	ss := SequenceState{}
	ss.mu.latestValues = make(map[uint32]int64)
	return &ss
}

// NextVal ever called returns true if a sequence has ever been incremented on
// this session.
func (ss *SequenceState) nextValEverCalledLocked() bool {
	return len(ss.mu.latestValues) > 0
}

// RecordValue records the latest manipulation of a sequence done by a session.
func (ss *SequenceState) RecordValue(seqID uint32, val int64) {
	ss.mu.Lock()
	ss.mu.lastSequenceIncremented = seqID
	ss.mu.latestValues[seqID] = val
	ss.mu.Unlock()
}

// SetLastSequenceIncremented sets the id of the last incremented sequence.
// Usually this id is set through RecordValue().
func (ss *SequenceState) SetLastSequenceIncremented(seqID uint32) {
	ss.mu.Lock()
	ss.mu.lastSequenceIncremented = seqID
	ss.mu.Unlock()
}

// GetLastValue returns the value most recently obtained by
// nextval() for the last sequence for which RecordLatestVal() was called.
func (ss *SequenceState) GetLastValue() (int64, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if v, ok := ss.mu.latestValues[ss.mu.lastSequenceIncremented]; ok {
		return v, nil
	}

	// if lastValue does not exist, return error
	return 0, pgerror.New(
		pgcode.ObjectNotInPrerequisiteState, "lastval is not yet defined in this session")
}

// GetLastValueByID returns the value most recently obtained by nextval() for
// the given sequence in this session.
// The bool retval is false if RecordLatestVal() was never called on the
// requested sequence.
func (ss *SequenceState) GetLastValueByID(seqID uint32) (int64, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	val, ok := ss.mu.latestValues[seqID]
	return val, ok
}

// Export returns a copy of the SequenceState's state - the latestValues and
// lastSequenceIncremented.
// lastSequenceIncremented is only defined if latestValues is non-empty.
func (ss *SequenceState) Export() (map[uint32]int64, uint32) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	res := make(map[uint32]int64, len(ss.mu.latestValues))
	for k, v := range ss.mu.latestValues {
		res[k] = v
	}
	return res, ss.mu.lastSequenceIncremented
}

// DeleteLastValue delete lastValue of sequence by id from sessionData
func (ss *SequenceState) DeleteLastValue(seqID uint32) {
	ss.mu.Lock()
	delete(ss.mu.latestValues, seqID)
	ss.mu.Unlock()
}
