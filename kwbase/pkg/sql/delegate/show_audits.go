// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// delegateShowAudits implements SHOW AUDITS which returns audit policies for the
// specified events, objects or users.
func (d *delegator) delegateShowAudits(n *tree.ShowAudits) (tree.Statement, error) {
	const selectQuery = `
SELECT * FROM kwdb_internal.audit_policies`
	var query bytes.Buffer
	var subQueryCount int
	query.WriteString(selectQuery)

	if n.Target.Type != "" {
		fmt.Fprintf(&query, ` WHERE target_type = '%s'`, n.Target.Type)
		subQueryCount++
	}

	if n.Target.Name.TableName != "" {
		var targetName string
		if !n.Target.Name.ExplicitSchema {
			targetName += d.evalCtx.SessionData.Database + "."
		}
		targetName += n.Target.Name.String()
		fmt.Fprintf(&query, ` AND target_name = '%s'`, targetName)
	}

	if n.Operations != nil {
		if n.Operations[0] != "ALL" {
			for _, op := range n.Operations {
				fmt.Fprintf(&query, ` AND operations LIKE '%%%s%%'`, strings.ToUpper(string(op)))
			}
			fmt.Fprintf(&query, ` OR operations = 'ALL'`)
		} else {
			fmt.Fprintf(&query, ` AND operations = 'ALL'`)
		}
	}

	if n.Operators != nil {
		if subQueryCount != 0 {
			fmt.Fprintf(&query, ` AND`)
		} else {
			fmt.Fprintf(&query, ` WHERE`)
		}
		if n.Operators[0] != "ALL" {
			for i, op := range n.Operators {
				if i > 0 {
					fmt.Fprintf(&query, ` AND`)
				}
				fmt.Fprintf(&query, ` operators LIKE '%%%s%%'`, op)
			}
			fmt.Fprintf(&query, ` OR operators = 'ALL'`)
		} else {
			fmt.Fprintf(&query, ` operators = 'ALL'`)
		}
	}

	return parse(query.String())
}
