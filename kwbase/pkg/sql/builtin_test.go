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

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestFuncNull execs all builtin funcs with various kinds of NULLs,
// attempting to induce a panic.
func TestFuncNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	run := func(t *testing.T, q string) {
		rows, err := db.QueryContext(ctx, q)
		if err == nil {
			rows.Close()
		}
	}

	for _, name := range builtins.AllBuiltinNames {
		switch strings.ToLower(name) {
		case "kwdb_internal.force_panic", "kwdb_internal.force_log_fatal", "pg_sleep":
			continue
		}
		_, variations := builtins.GetBuiltinProperties(name)
		for _, builtin := range variations {
			// Untyped NULL.
			{
				var sb strings.Builder
				fmt.Fprintf(&sb, "SELECT %s(", name)
				for i := range builtin.Types.Types() {
					if i > 0 {
						sb.WriteString(", ")
					}
					sb.WriteString("NULL")
				}
				sb.WriteString(")")
				run(t, sb.String())
			}
			// Typed NULL.
			{
				var sb strings.Builder
				fmt.Fprintf(&sb, "SELECT %s(", name)
				for i, typ := range builtin.Types.Types() {
					if i > 0 {
						sb.WriteString(", ")
					}
					fmt.Fprintf(&sb, "NULL::%s", typ)
				}
				sb.WriteString(")")
				run(t, sb.String())
			}
			// NULL that the type system can't (at least not yet?) know is NULL.
			{
				var sb strings.Builder
				fmt.Fprintf(&sb, "SELECT %s(", name)
				for i, typ := range builtin.Types.Types() {
					if i > 0 {
						sb.WriteString(", ")
					}
					fmt.Fprintf(&sb, "(SELECT NULL)::%s", typ)
				}
				sb.WriteString(")")
				run(t, sb.String())
			}
			// For array types, make an array with a NULL.
			{
				var sb strings.Builder
				fmt.Fprintf(&sb, "SELECT %s(", name)
				hasArray := false
				for i, typ := range builtin.Types.Types() {
					if i > 0 {
						sb.WriteString(", ")
					}
					if typ.Family() == types.ArrayFamily {
						hasArray = true
						if typ.ArrayContents().Family() == types.AnyFamily {
							fmt.Fprintf(&sb, "ARRAY[NULL]::STRING[]")
						} else {
							fmt.Fprintf(&sb, "ARRAY[NULL]::%s", typ)
						}
					} else {
						fmt.Fprintf(&sb, "NULL::%s", typ)
					}
				}
				if hasArray {
					sb.WriteString(")")
					run(t, sb.String())
				}
			}
		}
	}
}
