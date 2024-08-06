// Copyright 2017 The Cockroach Authors.
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
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// Statement contains a statement with optional expected result columns and metadata.
type Statement struct {
	parser.Statement

	ExpectedTypes sqlbase.ResultColumns
	AnonymizedStr string
	queryID       ClusterWideID

	// Prepared is non-nil during the PREPARE phase, as well as during EXECUTE of
	// a previously prepared statement. The Prepared statement can be modified
	// during either phase; the PREPARE phase sets its initial state, and the
	// EXECUTE phase can re-prepare it. This happens when the original plan has
	// been invalidated by schema changes, session data changes, permission
	// changes, or other changes to the context in which the original plan was
	// prepared.
	//
	// Given that the PreparedStatement can be modified during planning, it is
	// not safe for use on multiple threads.
	Prepared *PreparedStatement
}

func (s Statement) String() string {
	// We have the original SQL, but we still use String() because it obfuscates
	// passwords.
	return s.AST.String()
}
