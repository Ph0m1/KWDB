// Copyright 2019 The Cockroach Authors.
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

package delegate

import (
	"bytes"
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// delegateShowSyntax implements SHOW SYNTAX. This statement is usually handled
// as a special case in Executor, but for FROM [SHOW SYNTAX ...] we will arrive
// here too.
func (d *delegator) delegateShowSyntax(n *tree.ShowSyntax) (tree.Statement, error) {
	// Construct an equivalent SELECT query that produces the results:
	//
	// SELECT @1 AS field, @2 AS message
	//   FROM (VALUES
	//           ('file',     'foo.go'),
	//           ('line',     '123'),
	//           ('function', 'blix()'),
	//           ('detail',   'some details'),
	//           ('hint',     'some hints'))
	//
	var query bytes.Buffer
	fmt.Fprintf(
		&query, "SELECT @1 AS %s, @2 AS %s FROM (VALUES ",
		sqlbase.ShowSyntaxColumns[0].Name, sqlbase.ShowSyntaxColumns[1].Name,
	)

	comma := ""
	// TODO(knz): in the call below, reportErr is nil although we might
	// want to be able to capture (and report) these errors as well.
	//
	// However, this code path is only used when SHOW SYNTAX is used as
	// a data source, i.e. a client actively uses a query of the form
	// SELECT ... FROM [SHOW SYNTAX ' ... '] WHERE ....  This is not
	// what `kwbase sql` does: the SQL shell issues a straight `SHOW
	// SYNTAX` that goes through the "statement observer" code
	// path. Since we care mainly about what users do in the SQL shell,
	// it's OK if we only deal with that case well for now and, for the
	// time being, forget/ignore errors when SHOW SYNTAX is used as data
	// source. This can be added later if deemed useful or necessary.
	parser.RunShowSyntax(
		d.ctx, n.Statement,
		func(ctx context.Context, field, msg string) {
			fmt.Fprintf(&query, "%s('%s', ", comma, field)
			lex.EncodeSQLString(&query, msg)
			query.WriteByte(')')
			comma = ", "
		},
		nil, /* reportErr */
	)
	query.WriteByte(')')
	return parse(query.String())
}
