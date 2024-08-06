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

package kvcoord

import "gitee.com/kwbasedb/kwbase/pkg/base"

// ClientTestingKnobs contains testing options that dictate the behavior
// of the key-value client.
type ClientTestingKnobs struct {
	// The RPC dispatcher. Defaults to grpc but can be changed here for
	// testing purposes.
	TransportFactory TransportFactory

	// The maximum number of times a txn will attempt to refresh its
	// spans for a single transactional batch.
	// 0 means use a default. -1 means disable refresh.
	MaxTxnRefreshAttempts int

	// CondenseRefreshSpansFilter, if set, is called when the span refresher is
	// considering condensing the refresh spans. If it returns false, condensing
	// will not be attempted and the span refresher will behave as if condensing
	// failed to save enough memory.
	CondenseRefreshSpansFilter func() bool
}

var _ base.ModuleTestingKnobs = &ClientTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*ClientTestingKnobs) ModuleTestingKnobs() {}
