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

package api

// SplitType is split by hashPoint or timestamp
type SplitType int32

const (
	// SplitWithOneHashPoint {78/1 78/2} {78/2 78/3}
	SplitWithOneHashPoint SplitType = 2
	// SplitWithHashPointAndPositiveTimeStamp {78/1 78/1/1681111110000} {78/1/1681111110000 78/2}
	SplitWithHashPointAndPositiveTimeStamp SplitType = 3
	// SplitWithHashPointAndNegativeTimeStamp {78/1 78/1/-1681111110000} {78/1/-1681111110000 78/2}
	SplitWithHashPointAndNegativeTimeStamp SplitType = 4
)

// EntityRangeGroupID is the id of EntityRangeGroup
type EntityRangeGroupID uint32

// HashParam is the const of 65535
const HashParam = 65535

// HashParamV2 is the const of 10
const HashParamV2 = 20

// HashPoint is the hashID
type HashPoint uint16

// MppMode MppMode
var MppMode bool

// SingleNode SingleNode
var SingleNode bool
