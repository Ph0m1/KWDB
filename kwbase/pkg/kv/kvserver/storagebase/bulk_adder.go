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

package storagebase

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// BulkAdderOptions is used to configure the behavior of a BulkAdder.
type BulkAdderOptions struct {
	// Name is used in logging messages to identify this adder or the process on
	// behalf of which it is adding data.
	Name string

	// SSTSize is the size at which an SST will be flushed and a new one started.
	// SSTs are also split during a buffer flush to avoid spanning range bounds so
	// they may be smaller than this limit.
	SSTSize func() int64

	// SplitAndScatterAfter is the number of bytes which if added without hitting
	// an existing split will cause the adder to split and scatter the next span.
	SplitAndScatterAfter func() int64

	// MinBufferSize is the initial size of the BulkAdder buffer. It indicates the
	// amount of memory we require to be able to buffer data before flushing for
	// SST creation.
	MinBufferSize int64

	// BufferSize is the maximum size we can grow the BulkAdder buffer to.
	MaxBufferSize func() int64

	// StepBufferSize is the increment in which we will attempt to grow the
	// BulkAdder buffer if the memory monitor permits.
	StepBufferSize int64

	// SkipLocalDuplicates configures handling of duplicate keys within a local
	// sorted batch. When true if the same key/value pair is added more than once
	// subsequent additions will be ignored instead of producing an error. If an
	// attempt to add the same key has a differnet value, it is always an error.
	// Once a batch is flushed – explicitly or automatically – local duplicate
	// detection does not apply.
	SkipDuplicates bool

	// DisallowShadowing controls whether shadowing of existing keys is permitted
	// when the SSTables produced by this adder are ingested.
	DisallowShadowing bool

	ShouldLogLogicalOp bool
}

// BulkAdderFactory describes a factory function for BulkAdders.
type BulkAdderFactory func(
	ctx context.Context, db *kv.DB, timestamp hlc.Timestamp, opts BulkAdderOptions,
) (BulkAdder, error)

// BulkAdder describes a bulk-adding helper that can be used to add lots of KVs.
type BulkAdder interface {
	// Add adds a KV pair to the adder's buffer, potentially flushing if needed.
	Add(ctx context.Context, key roachpb.Key, value []byte) error
	// Flush explicitly flushes anything remaining in the adder's buffer.
	Flush(ctx context.Context) error
	// IsEmpty returns whether or not this BulkAdder has data buffered.
	IsEmpty() bool
	// CurrentBufferFill returns how full the configured buffer is.
	CurrentBufferFill() float32
	// GetSummary returns a summary of rows/bytes/etc written by this batcher.
	GetSummary() roachpb.BulkOpSummary
	// Close closes the underlying buffers/writers.
	Close(ctx context.Context)
	// SetOnFlush sets a callback function called after flushing the buffer.
	SetOnFlush(func())
}

// DuplicateKeyError represents a failed attempt to ingest the same key twice
// using a BulkAdder within the same batch.
type DuplicateKeyError struct {
	Key   roachpb.Key
	Value []byte
}

func (d DuplicateKeyError) Error() string {
	return fmt.Sprintf("duplicate key: %s", d.Key)
}
