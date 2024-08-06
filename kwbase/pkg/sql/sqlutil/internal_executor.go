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

package sqlutil

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// InternalExecutor is meant to be used by layers below SQL in the system that
// nevertheless want to execute SQL queries (presumably against system tables).
// It is extracted in this "sql/util" package to avoid circular references and
// is implemented by *sql.InternalExecutor.
type InternalExecutor interface {
	// Exec executes the supplied SQL statement and returns the number of rows
	// affected (not like the results; see Query()). If no user has been previously
	// set through SetSessionData, the statement is executed as the root user.
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	//
	// Exec is deprecated because it may transparently execute a query as root. Use
	// ExecEx instead.
	Exec(
		ctx context.Context, opName string, txn *kv.Txn, statement string, params ...interface{},
	) (int, error)

	// ExecEx is like Exec, but allows the caller to override some session data
	// fields.
	//
	// The fields set in session that are set override the respective fields if they
	// have previously been set through SetSessionData().
	ExecEx(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		o sqlbase.InternalExecutorSessionDataOverride,
		stmt string,
		qargs ...interface{},
	) (int, error)

	// Query executes the supplied SQL statement and returns the resulting rows.
	// If no user has been previously set through SetSessionData, the statement is
	// executed as the root user.
	//
	// If txn is not nil, the statement will be executed in the respective txn.
	//
	// Query is deprecated because it may transparently execute a query as root. Use
	// QueryEx instead.
	Query(
		ctx context.Context, opName string, txn *kv.Txn, statement string, qargs ...interface{},
	) ([]tree.Datums, error)

	// QueryEx is like Query, but allows the caller to override some session data
	// fields.
	//
	// The fields set in session that are set override the respective fields if
	// they have previously been set through SetSessionData().
	QueryEx(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		session sqlbase.InternalExecutorSessionDataOverride,
		stmt string,
		qargs ...interface{},
	) ([]tree.Datums, error)

	// QueryWithCols is like QueryEx, but it also returns the computed ResultColumns
	// of the input query.
	QueryWithCols(
		ctx context.Context, opName string, txn *kv.Txn,
		o sqlbase.InternalExecutorSessionDataOverride, statement string, qargs ...interface{},
	) ([]tree.Datums, sqlbase.ResultColumns, error)

	// QueryRow is like Query, except it returns a single row, or nil if not row is
	// found, or an error if more that one row is returned.
	//
	// QueryRow is deprecated (like Query). Use QueryRowEx() instead.
	QueryRow(
		ctx context.Context, opName string, txn *kv.Txn, statement string, qargs ...interface{},
	) (tree.Datums, error)

	// QueryRowEx is like QueryRow, but allows the caller to override some session data
	// fields.
	//
	// The fields set in session that are set override the respective fields if they
	// have previously been set through SetSessionData().
	QueryRowEx(
		ctx context.Context,
		opName string,
		txn *kv.Txn,
		session sqlbase.InternalExecutorSessionDataOverride,
		stmt string,
		qargs ...interface{},
	) (tree.Datums, error)
}

// SessionBoundInternalExecutorFactory is a function that produces a "session
// bound" internal executor.
type SessionBoundInternalExecutorFactory func(
	context.Context, *sessiondata.SessionData,
) InternalExecutor
