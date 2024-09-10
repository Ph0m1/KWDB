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
	"encoding/hex"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// delegateShowTsPartitions implements the SHOW TS PARTITIONS statement:
//
//	SHOW TS PARTITIONS FROM TABLE t
//	SHOW TS PARTITIONS FROM INDEX t@idx
//	SHOW TS PARTITIONS FROM DATABASE db
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.
func (d *delegator) delegateShowTsPartitions(n *tree.ShowTsPartitions) (tree.Statement, error) {
	if n.DatabaseName != "" {
		if err1 := CheckTsDBSupportShow(d, n.DatabaseName, n); err1 != nil {
			return nil, err1
		}
		const dbQuery = `
SELECT group_id,
     	 partition_id,
       start_pretty,
       end_pretty,
       database_name,
       table_name,
       lease_holder,
       replicas,
       size,
       CASE status
           WHEN 0 THEN 'running'
           WHEN 1 THEN 'transferring'
           WHEN 2 THEN 'relocating'
           WHEN 3 THEN 'adding'
           WHEN 4 THEN 'lacking'
           END AS status
FROM %[1]s.kwdb_internal.kwdb_ts_partitions
WHERE database_name=%[2]s
ORDER BY  group_id, table_name, partition_id
		`
		// Note: n.DatabaseName.String() != string(n.DatabaseName)
		return parse(fmt.Sprintf(dbQuery, n.DatabaseName.String(), lex.EscapeSQLString(string(n.DatabaseName))))
	}

	if n.TableOrIndex.Table.TableName == "" {
		return parse(fmt.Sprintf(`
SELECT group_id,
    	 partition_id,
       start_pretty,
       end_pretty,
       database_name,
       table_name,
       lease_holder,
       replicas,
       size,
       CASE status
           WHEN 0 THEN 'running'
           WHEN 1 THEN 'transferring'
           WHEN 2 THEN 'relocating'
           WHEN 3 THEN 'adding'
           WHEN 4 THEN 'lacking'
           END AS status
FROM kwdb_internal.kwdb_ts_partitions
ORDER BY group_id, database_name, table_name, partition_id;
`))
	}

	idx, resName, err := cat.ResolveTableIndex(
		d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, &n.TableOrIndex,
	)
	if err != nil {
		return nil, err
	}

	if err := d.catalog.CheckPrivilege(d.ctx, idx.Table(), privilege.SELECT); err != nil {
		return nil, err
	}

	if idx.Table().GetTableType() == tree.RelationalTable {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported, "%s is not supported in relation table", "show ts partitions")
	}

	id := sqlbase.ID(idx.Table().ID())
	startKey := hex.EncodeToString(sqlbase.MakeTsHashPointKey(id, 0))
	endKey := hex.EncodeToString(sqlbase.MakeTsHashPointKey(id, api.HashParam))
	return parse(fmt.Sprintf(`
SELECT group_id,
    	 partition_id,
       start_pretty,
       end_pretty,
       database_name,
       table_name,
       lease_holder,
       replicas,
       size,
       CASE status
           WHEN 0 THEN 'running'
           WHEN 1 THEN 'transferring'
           WHEN 2 THEN 'relocating'
           WHEN 3 THEN 'adding'
           WHEN 4 THEN 'lacking'
           END AS status
FROM %[3]s.kwdb_internal.kwdb_ts_partitions
WHERE start_key < x'%[2]s'
  AND end_key > x'%[1]s'
ORDER BY group_id, partition_id;
`,
		startKey, endKey, resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
	))
}
