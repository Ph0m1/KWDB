// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package timeutil

import "time"

// Timeout wraps a Timer, and allows us to call Stop() directly.
type Timeout struct {
	timeout *time.Timer
}

// Stop stops timer within Timeout
func (t Timeout) Stop() {
	if t.timeout != nil {
		t.timeout.Stop()
	}
}

// Set setts timer within Timeout
func (t Timeout) Set(timer *time.Timer) {
	t.timeout = timer
}
