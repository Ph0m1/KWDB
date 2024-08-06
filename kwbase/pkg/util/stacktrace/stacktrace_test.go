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

package stacktrace_test

import (
	"reflect"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/stacktrace"
	"github.com/kr/pretty"
)

func genStack2() *stacktrace.StackTrace {
	return stacktrace.NewStackTrace(0)
}

func genStack1() *stacktrace.StackTrace {
	return genStack2()
}

func TestStackTrace(t *testing.T) {
	st := genStack1()

	t.Logf("Stack trace:\n%s", stacktrace.PrintStackTrace(st))

	encoded, err := stacktrace.EncodeStackTrace(st)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("encoded:\n%s", encoded)
	if !strings.Contains(encoded, `"function":"genStack1"`) ||
		!strings.Contains(encoded, `"function":"genStack2"`) {
		t.Fatalf("function genStack not in call stack:\n%s", encoded)
	}

	st2, err := stacktrace.DecodeStackTrace(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(st, st2) {
		t.Fatalf("stack traces not identical: %v", pretty.Diff(st, st2))
	}
}
