// Copyright 2018 The Cockroach Authors.
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

package spanlatch

// latchList is a double-linked circular list of *latch elements.
type latchList struct {
	root latch
	len  int
}

func (ll *latchList) front() *latch {
	if ll.len == 0 {
		return nil
	}
	return ll.root.next
}

func (ll *latchList) lazyInit() {
	if ll.root.next == nil {
		ll.root.next = &ll.root
		ll.root.prev = &ll.root
	}
}

func (ll *latchList) pushBack(la *latch) {
	ll.lazyInit()
	at := ll.root.prev
	n := at.next
	at.next = la
	la.prev = at
	la.next = n
	n.prev = la
	ll.len++
}

func (ll *latchList) remove(la *latch) {
	la.prev.next = la.next
	la.next.prev = la.prev
	la.next = nil // avoid memory leaks
	la.prev = nil // avoid memory leaks
	ll.len--
}
