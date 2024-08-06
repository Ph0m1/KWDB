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

package sqlbase

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// ToScanLockingStrength converts a tree.LockingStrength to its corresponding
// ScanLockingStrength.
func ToScanLockingStrength(s tree.LockingStrength) ScanLockingStrength {
	switch s {
	case tree.ForNone:
		return ScanLockingStrength_FOR_NONE
	case tree.ForKeyShare:
		return ScanLockingStrength_FOR_KEY_SHARE
	case tree.ForShare:
		return ScanLockingStrength_FOR_SHARE
	case tree.ForNoKeyUpdate:
		return ScanLockingStrength_FOR_NO_KEY_UPDATE
	case tree.ForUpdate:
		return ScanLockingStrength_FOR_UPDATE
	default:
		panic(fmt.Sprintf("unknown locking strength %s", s))
	}
}

// ToScanLockingWaitPolicy converts a tree.LockingWaitPolicy to its
// corresponding ScanLockingWaitPolicy.
func ToScanLockingWaitPolicy(wp tree.LockingWaitPolicy) ScanLockingWaitPolicy {
	switch wp {
	case tree.LockWaitBlock:
		return ScanLockingWaitPolicy_BLOCK
	case tree.LockWaitSkip:
		return ScanLockingWaitPolicy_SKIP
	case tree.LockWaitError:
		return ScanLockingWaitPolicy_ERROR
	default:
		panic(fmt.Sprintf("unknown locking wait policy %s", wp))
	}
}
