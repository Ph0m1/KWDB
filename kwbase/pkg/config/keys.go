// Copyright 2015 The Cockroach Authors.
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

package config

import (
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
)

// MakeZoneKeyPrefix returns the key prefix for id's row in the system.zones
// table.
func MakeZoneKeyPrefix(id uint32) roachpb.Key {
	return keys.ZoneKeyPrefix(id)
}

// MakeZoneKey returns the key for id's entry in the system.zones table.
func MakeZoneKey(id uint32) roachpb.Key {
	return keys.ZoneKey(id)
}

// DecodeObjectID decodes the object ID from the front of key. It returns the
// decoded object ID, the remainder of the key, and whether the result is valid
// (i.e., whether the key was within the structured key space).
func DecodeObjectID(key roachpb.RKey) (uint32, []byte, bool) {
	if key.Equal(roachpb.RKeyMax) {
		return 0, nil, false
	}
	if encoding.PeekType(key) != encoding.Int {
		// TODO(marc): this should eventually return SystemDatabaseID.
		return 0, nil, false
	}
	// Consume first encoded int.
	rem, id64, err := encoding.DecodeUvarintAscending(key)
	return uint32(id64), rem, err == nil
}
