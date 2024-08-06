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

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

// delegateShowRanges implements the SHOW RANGES statement:
//
//	SHOW RANGES FROM TABLE t
//	SHOW RANGES FROM INDEX t@idx
//	SHOW RANGES FROM DATABASE db
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.
func (d *delegator) delegateShowRanges(n *tree.ShowRanges) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Ranges)
	if n.DatabaseName != "" {
		if err1 := CheckDBSupportShow(d, n.DatabaseName, n); err1 != nil {
			return nil, err1
		}
		const dbQuery = `
		SELECT
			table_name,
			CASE
				WHEN kwdb_internal.pretty_key(r.start_key, 2) = '' THEN NULL
				ELSE kwdb_internal.pretty_key(r.start_key, 2)
			END AS start_key,
			CASE
				WHEN kwdb_internal.pretty_key(r.end_key, 2) = '' THEN NULL
				ELSE kwdb_internal.pretty_key(r.end_key, 2)
			END AS end_key,
			range_id,
			range_size / 1000000 as range_size_mb,
			lease_holder,
    	gossip_nodes.locality as lease_holder_locality,
			replicas,
			replica_localities
		FROM %[1]s.kwdb_internal.ranges AS r
	  LEFT JOIN kwdb_internal.gossip_nodes ON lease_holder = node_id
		WHERE database_name=%[2]s
		ORDER BY table_name, r.start_key
		`
		// Note: n.DatabaseName.String() != string(n.DatabaseName)
		return parse(fmt.Sprintf(dbQuery, n.DatabaseName.String(), lex.EscapeSQLString(string(n.DatabaseName))))
	}

	idx, resName, err := cat.ResolveTableIndex(
		d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, &n.TableOrIndex,
	)
	if err != nil {
		return nil, err
	}

	if idx.Table().GetTableType() != tree.RelationalTable {
		return nil, sqlbase.TSUnsupportedError("show ranges")
	}

	if err := d.catalog.CheckPrivilege(d.ctx, idx.Table(), privilege.SELECT); err != nil {
		return nil, err
	}

	span := idx.Span()
	startKey := hex.EncodeToString([]byte(span.Key))
	endKey := hex.EncodeToString([]byte(span.EndKey))
	return parse(fmt.Sprintf(`
SELECT 
  CASE WHEN r.start_key <= x'%[1]s' THEN NULL ELSE kwdb_internal.pretty_key(r.start_key, 2) END AS start_key,
  CASE WHEN r.end_key >= x'%[2]s' THEN NULL ELSE kwdb_internal.pretty_key(r.end_key, 2) END AS end_key,
  range_id,
  range_size / 1000000 as range_size_mb,
  lease_holder,
  gossip_nodes.locality as lease_holder_locality,
  replicas,
  replica_localities
FROM %[3]s.kwdb_internal.ranges AS r
LEFT JOIN %[3]s.kwdb_internal.gossip_nodes ON lease_holder = node_id
WHERE (r.start_key < x'%[2]s')
  AND (r.end_key   > x'%[1]s') ORDER BY r.start_key
`,
		startKey, endKey, resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
	))
}
