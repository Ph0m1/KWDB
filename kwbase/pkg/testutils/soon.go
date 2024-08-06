// Copyright 2016 The Cockroach Authors.
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

package testutils

import (
	"context"
	"runtime/debug"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// DefaultSucceedsSoonDuration is the maximum amount of time unittests
// will wait for a condition to become true. See SucceedsSoon().
const DefaultSucceedsSoonDuration = 45 * time.Second

// SucceedsSoon fails the test (with t.Fatal) unless the supplied
// function runs without error within a preset maximum duration. The
// function is invoked immediately at first and then successively with
// an exponential backoff starting at 1ns and ending at around 1s.
func SucceedsSoon(t testing.TB, fn func() error) {
	t.Helper()
	if err := SucceedsSoonError(fn); err != nil {
		t.Fatalf("condition failed to evaluate within %s: %s\n%s",
			DefaultSucceedsSoonDuration, err, string(debug.Stack()))
	}
}

// SucceedsSoonError returns an error unless the supplied function runs without
// error within a preset maximum duration. The function is invoked immediately
// at first and then successively with an exponential backoff starting at 1ns
// and ending at around 1s.
func SucceedsSoonError(fn func() error) error {
	tBegin := timeutil.Now()
	wrappedFn := func() error {
		err := fn()
		if timeutil.Since(tBegin) > 3*time.Second && err != nil {
			log.InfoDepth(context.Background(), 4, errors.Wrap(err, "SucceedsSoon"))
		}
		return err
	}
	return retry.ForDuration(DefaultSucceedsSoonDuration, wrappedFn)
}
