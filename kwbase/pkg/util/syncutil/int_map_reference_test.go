// Copyright 2019 The Cockroach Authors.
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

// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// This code originated in Go's sync package.

package syncutil

import (
	"sync/atomic"
	"unsafe"
)

// This file contains reference map implementations for unit-tests.

// mapInterface is the interface Map implements.
type mapInterface interface {
	Load(int64) (unsafe.Pointer, bool)
	Store(key int64, value unsafe.Pointer)
	LoadOrStore(key int64, value unsafe.Pointer) (actual unsafe.Pointer, loaded bool)
	Delete(int64)
	Range(func(key int64, value unsafe.Pointer) (shouldContinue bool))
}

// RWMutexMap is an implementation of mapInterface using a RWMutex.
type RWMutexMap struct {
	mu    RWMutex
	dirty map[int64]unsafe.Pointer
}

func (m *RWMutexMap) Load(key int64) (value unsafe.Pointer, ok bool) {
	m.mu.RLock()
	value, ok = m.dirty[key]
	m.mu.RUnlock()
	return
}

func (m *RWMutexMap) Store(key int64, value unsafe.Pointer) {
	m.mu.Lock()
	if m.dirty == nil {
		m.dirty = make(map[int64]unsafe.Pointer)
	}
	m.dirty[key] = value
	m.mu.Unlock()
}

func (m *RWMutexMap) LoadOrStore(
	key int64, value unsafe.Pointer,
) (actual unsafe.Pointer, loaded bool) {
	m.mu.Lock()
	actual, loaded = m.dirty[key]
	if !loaded {
		actual = value
		if m.dirty == nil {
			m.dirty = make(map[int64]unsafe.Pointer)
		}
		m.dirty[key] = value
	}
	m.mu.Unlock()
	return actual, loaded
}

func (m *RWMutexMap) Delete(key int64) {
	m.mu.Lock()
	delete(m.dirty, key)
	m.mu.Unlock()
}

func (m *RWMutexMap) Range(f func(key int64, value unsafe.Pointer) (shouldContinue bool)) {
	m.mu.RLock()
	keys := make([]int64, 0, len(m.dirty))
	for k := range m.dirty {
		keys = append(keys, k)
	}
	m.mu.RUnlock()

	for _, k := range keys {
		v, ok := m.Load(k)
		if !ok {
			continue
		}
		if !f(k, v) {
			break
		}
	}
}

// DeepCopyMap is an implementation of mapInterface using a Mutex and
// atomic.Value.  It makes deep copies of the map on every write to avoid
// acquiring the Mutex in Load.
type DeepCopyMap struct {
	mu    Mutex
	clean atomic.Value
}

func (m *DeepCopyMap) Load(key int64) (value unsafe.Pointer, ok bool) {
	clean, _ := m.clean.Load().(map[int64]unsafe.Pointer)
	value, ok = clean[key]
	return value, ok
}

func (m *DeepCopyMap) Store(key int64, value unsafe.Pointer) {
	m.mu.Lock()
	dirty := m.dirty()
	dirty[key] = value
	m.clean.Store(dirty)
	m.mu.Unlock()
}

func (m *DeepCopyMap) LoadOrStore(
	key int64, value unsafe.Pointer,
) (actual unsafe.Pointer, loaded bool) {
	clean, _ := m.clean.Load().(map[int64]unsafe.Pointer)
	actual, loaded = clean[key]
	if loaded {
		return actual, loaded
	}

	m.mu.Lock()
	// Reload clean in case it changed while we were waiting on m.mu.
	clean, _ = m.clean.Load().(map[int64]unsafe.Pointer)
	actual, loaded = clean[key]
	if !loaded {
		dirty := m.dirty()
		dirty[key] = value
		actual = value
		m.clean.Store(dirty)
	}
	m.mu.Unlock()
	return actual, loaded
}

func (m *DeepCopyMap) Delete(key int64) {
	m.mu.Lock()
	dirty := m.dirty()
	delete(dirty, key)
	m.clean.Store(dirty)
	m.mu.Unlock()
}

func (m *DeepCopyMap) Range(f func(key int64, value unsafe.Pointer) (shouldContinue bool)) {
	clean, _ := m.clean.Load().(map[int64]unsafe.Pointer)
	for k, v := range clean {
		if !f(k, v) {
			break
		}
	}
}

func (m *DeepCopyMap) dirty() map[int64]unsafe.Pointer {
	clean, _ := m.clean.Load().(map[int64]unsafe.Pointer)
	dirty := make(map[int64]unsafe.Pointer, len(clean)+1)
	for k, v := range clean {
		dirty[k] = v
	}
	return dirty
}
