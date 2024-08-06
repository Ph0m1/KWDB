// Copyright 2020 The Cockroach Authors.
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

package kvnemesis

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/bufalloc"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// Engine is a simplified version of storage.ReadWriter. It is a multi-version
// key-value map, meaning that each read or write has an associated timestamp
// and a read returns the write for the key with the highest timestamp (which is
// not necessarily the most recently ingested write). Engine is not threadsafe.
type Engine struct {
	kvs *pebble.DB
	b   bufalloc.ByteAllocator
}

// MakeEngine returns a new Engine.
func MakeEngine() (*Engine, error) {
	opts := storage.DefaultPebbleOptions()
	opts.FS = vfs.NewMem()
	kvs, err := pebble.Open(`kvnemesis`, opts)
	if err != nil {
		return nil, err
	}
	return &Engine{kvs: kvs}, nil
}

// Close closes the Engine, freeing associated resources.
func (e *Engine) Close() {
	if err := e.kvs.Close(); err != nil {
		panic(err)
	}
}

// Get returns the value for this key with the highest timestamp <= ts. If no
// such value exists, the returned value's RawBytes is nil.
func (e *Engine) Get(key roachpb.Key, ts hlc.Timestamp) roachpb.Value {
	iter := e.kvs.NewIter(nil)
	defer func() { _ = iter.Close() }()
	iter.SeekGE(storage.EncodeKey(storage.MVCCKey{Key: key, Timestamp: ts}))
	if !iter.Valid() {
		return roachpb.Value{}
	}
	// This use of iter.Key() is safe because it comes entirely before the
	// deferred iter.Close.
	mvccKey, err := storage.DecodeMVCCKey(iter.Key())
	if err != nil {
		panic(err)
	}
	if !mvccKey.Key.Equal(key) {
		return roachpb.Value{}
	}
	var valCopy []byte
	valCopy, e.b = e.b.Copy(iter.Value(), 0 /* extraCap */)
	return roachpb.Value{RawBytes: valCopy, Timestamp: mvccKey.Timestamp}
}

// Put inserts a key/value/timestamp tuple. If an exact key/timestamp pair is
// Put again, it overwrites the previous value.
func (e *Engine) Put(key storage.MVCCKey, value []byte) {
	if err := e.kvs.Set(storage.EncodeKey(key), value, nil); err != nil {
		panic(err)
	}
}

// Iterate calls the given closure with every KV in the Engine, in ascending
// order.
func (e *Engine) Iterate(fn func(key storage.MVCCKey, value []byte, err error)) {
	iter := e.kvs.NewIter(nil)
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := iter.Error(); err != nil {
			fn(storage.MVCCKey{}, nil, err)
			continue
		}
		var keyCopy, valCopy []byte
		keyCopy, e.b = e.b.Copy(iter.Key(), 0 /* extraCap */)
		valCopy, e.b = e.b.Copy(iter.Value(), 0 /* extraCap */)
		key, err := storage.DecodeMVCCKey(keyCopy)
		if err != nil {
			fn(storage.MVCCKey{}, nil, err)
			continue
		}
		fn(key, valCopy, nil)
	}
}

// DebugPrint returns the entire contents of this Engine as a string for use in
// debugging.
func (e *Engine) DebugPrint(indent string) string {
	var buf strings.Builder
	e.Iterate(func(key storage.MVCCKey, value []byte, err error) {
		if buf.Len() > 0 {
			buf.WriteString("\n")
		}
		if err != nil {
			fmt.Fprintf(&buf, "(err:%s)", err)
		} else {
			fmt.Fprintf(&buf, "%s%s %s -> %s",
				indent, key.Key, key.Timestamp, roachpb.Value{RawBytes: value}.PrettyPrint())
		}
	})
	return buf.String()
}
