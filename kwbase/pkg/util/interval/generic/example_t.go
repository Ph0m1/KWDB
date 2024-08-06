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

package generic

import "gitee.com/kwbasedb/kwbase/pkg/roachpb"

//go:generate ./gen.sh *example generic

type example struct {
	id   uint64
	span roachpb.Span
}

// Methods required by generic contract.
func (ex *example) ID() uint64         { return ex.id }
func (ex *example) Key() []byte        { return ex.span.Key }
func (ex *example) EndKey() []byte     { return ex.span.EndKey }
func (ex *example) String() string     { return ex.span.String() }
func (ex *example) New() *example      { return new(example) }
func (ex *example) SetID(v uint64)     { ex.id = v }
func (ex *example) SetKey(v []byte)    { ex.span.Key = v }
func (ex *example) SetEndKey(v []byte) { ex.span.EndKey = v }
