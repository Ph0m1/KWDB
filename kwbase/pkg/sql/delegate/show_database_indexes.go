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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// delegateShowDatabaseIndexes rewrites ShowDatabaseIndexes statement to select statement
// which returns table_name, index_name... from information_schema.statistics and pg_class
func (d *delegator) delegateShowDatabaseIndexes(
	n *tree.ShowDatabaseIndexes,
) (tree.Statement, error) {
	if err1 := CheckDBSupportShow(d, n.Database, n); err1 != nil {
		return nil, err1
	}
	getAllIndexesQuery := `
SELECT
	table_name,
	index_name,
	non_unique::BOOL,
	seq_in_index,
	column_name,
	direction,
	storing::BOOL,
	implicit::BOOL`

	if n.WithComment {
		getAllIndexesQuery += `,
	obj_description(pg_class.oid) AS comment`
	}

	getAllIndexesQuery += `
FROM
	%s.information_schema.statistics`

	if n.WithComment {
		getAllIndexesQuery += `
	LEFT JOIN pg_class ON
		statistics.index_name = pg_class.relname`
	}

	return parse(fmt.Sprintf(getAllIndexesQuery, n.Database.String()))
}
