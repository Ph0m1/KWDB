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

package log

import "context"

// Intercept diverts log traffic to the given function `f`. When `f` is not nil,
// the logging package begins operating at full verbosity (i.e. `V(n) == true`
// for all `n`) but nothing will be printed to the logs. Instead, `f` is invoked
// for each log entry.
//
// To end log interception, invoke `Intercept()` with `f == nil`. Note that
// interception does not terminate atomically, that is, the originally supplied
// callback may still be invoked after a call to `Intercept` with `f == nil`.
func Intercept(ctx context.Context, f InterceptorFn) {
	// TODO(tschottdorf): restore sanity so that all methods have a *loggingT
	// receiver.
	if f != nil {
		logDepth(ctx, 0, Severity_WARNING, "log traffic is now intercepted; log files will be incomplete", nil)
	}
	logging.interceptor.Store(f) // intentionally also when f == nil
	if f == nil {
		logDepth(ctx, 0, Severity_INFO, "log interception is now stopped; normal logging resumes", nil)
	}
}

// InterceptorFn is the type of function accepted by Intercept().
type InterceptorFn func(entry Entry)
