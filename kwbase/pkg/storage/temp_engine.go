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
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/diskmap"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/fs"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// NewTempEngine creates a new engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewTempEngine(
	ctx context.Context,
	engine enginepb.EngineType,
	tempStorage base.TempStorageConfig,
	storeSpec base.StoreSpec,
) (diskmap.Factory, fs.FS, error) {
	switch engine {
	case enginepb.EngineTypeTeePebbleRocksDB:
		fallthrough
	case enginepb.EngineTypePebble:
		return NewPebbleTempEngine(ctx, tempStorage, storeSpec)
	case enginepb.EngineTypeDefault, enginepb.EngineTypeRocksDB:
		return NewRocksDBTempEngine(tempStorage, storeSpec)
	}
	panic(fmt.Sprintf("unknown engine type: %d", engine))
}

type rocksDBTempEngine struct {
	db *RocksDB
}

// Close implements the diskmap.Factory interface.
func (r *rocksDBTempEngine) Close() {
	r.db.Close()
}

// NewSortedDiskMap implements the diskmap.Factory interface.
func (r *rocksDBTempEngine) NewSortedDiskMap() diskmap.SortedDiskMap {
	return newRocksDBMap(r.db, false /* allowDuplications */)
}

// NewSortedDiskMultiMap implements the diskmap.Factory interface.
func (r *rocksDBTempEngine) NewSortedDiskMultiMap() diskmap.SortedDiskMap {
	return newRocksDBMap(r.db, true /* allowDuplicates */)
}

// storageConfigFromTempStorageConfigAndStoreSpec creates a base.StorageConfig
// used by both the RocksDB and Pebble temp engines from the given arguments.
func storageConfigFromTempStorageConfigAndStoreSpec(
	config base.TempStorageConfig, spec base.StoreSpec,
) base.StorageConfig {
	return base.StorageConfig{
		Attrs: roachpb.Attributes{},
		Dir:   config.Path,
		// MaxSize doesn't matter for temp storage - it's not enforced in any way.
		MaxSize:         0,
		UseFileRegistry: spec.UseFileRegistry,
		ExtraOptions:    spec.ExtraOptions,
	}
}

// NewRocksDBTempEngine creates a new RocksDB engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewRocksDBTempEngine(
	tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, fs.FS, error) {
	if tempStorage.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to temporary store.
		db := newRocksDBInMem(roachpb.Attributes{} /* attrs */, 0 /* cacheSize */)
		return &rocksDBTempEngine{db: db}, db, nil
	}

	cfg := RocksDBConfig{
		StorageConfig: storageConfigFromTempStorageConfigAndStoreSpec(tempStorage, storeSpec),
		MaxOpenFiles:  128, // TODO(arjun): Revisit this.
	}
	rocksDBCache := NewRocksDBCache(0)
	defer rocksDBCache.Release()
	db, err := NewRocksDB(cfg, rocksDBCache)
	if err != nil {
		return nil, nil, err
	}

	return &rocksDBTempEngine{db: db}, db, nil
}

type pebbleTempEngine struct {
	db *pebble.DB
}

// Close implements the diskmap.Factory interface.
func (r *pebbleTempEngine) Close() {
	err := r.db.Close()
	if err != nil {
		log.Fatal(context.TODO(), err)
	}
}

// NewSortedDiskMap implements the diskmap.Factory interface.
func (r *pebbleTempEngine) NewSortedDiskMap() diskmap.SortedDiskMap {
	return newPebbleMap(r.db, false /* allowDuplications */)
}

// NewSortedDiskMultiMap implements the diskmap.Factory interface.
func (r *pebbleTempEngine) NewSortedDiskMultiMap() diskmap.SortedDiskMap {
	return newPebbleMap(r.db, true /* allowDuplicates */)
}

// NewPebbleTempEngine creates a new Pebble engine for DistSQL processors to use
// when the working set is larger than can be stored in memory.
func NewPebbleTempEngine(
	ctx context.Context, tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, fs.FS, error) {
	return newPebbleTempEngine(ctx, tempStorage, storeSpec)
}

var pebbleTempEngineTablePropertyCollectors = []func() pebble.TablePropertyCollector{
	func() pebble.TablePropertyCollector { return &pebbleDeleteRangeCollector{} },
}

func newPebbleTempEngine(
	ctx context.Context, tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (*pebbleTempEngine, fs.FS, error) {
	// Default options as copied over from pebble/cmd/pebble/db.go
	opts := DefaultPebbleOptions()
	// Pebble doesn't currently support 0-size caches, so use a 128MB cache for
	// now.
	opts.Cache = pebble.NewCache(128 << 20)
	defer opts.Cache.Unref()

	// The Pebble temp engine does not use MVCC Encoding. Instead, the
	// caller-provided key is used as-is (with the prefix prepended). See
	// pebbleMap.makeKey and pebbleMap.makeKeyWithSequence on how this works.
	// Use the default bytes.Compare-like comparer.
	opts.Comparer = pebble.DefaultComparer
	opts.DisableWAL = true
	opts.TablePropertyCollectors = pebbleTempEngineTablePropertyCollectors

	storageConfig := storageConfigFromTempStorageConfigAndStoreSpec(tempStorage, storeSpec)
	if tempStorage.InMemory {
		opts.FS = vfs.NewMem()
		storageConfig.Dir = ""
	}

	p, err := NewPebble(
		ctx,
		PebbleConfig{
			StorageConfig: storageConfig,
			Opts:          opts,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return &pebbleTempEngine{db: p.db}, p, nil
}
