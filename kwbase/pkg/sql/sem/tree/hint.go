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

package tree

import (
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
)

// Hint represents a hint
type Hint interface {
	GetStmtHint() *StmtHint
	GetHintType() keys.HintType
}

// StmtHint encapsulate a HintForest
type StmtHint struct {
	HintForest HintForest
}

// GetStmtHint *StmtHint
func (i *StmtHint) GetStmtHint() *StmtHint {
	return i
}

// GetHintType return order hint type
func (i *StmtHint) GetHintType() keys.HintType {
	return keys.StmtLevelHint
}

// HintSet represent a slice hint
type HintSet []Hint

// GetStmtHint Get StmtHint from HintSet
func (h HintSet) GetStmtHint() *StmtHint {
	for _, hint := range h {
		if hint.GetHintType() == keys.StmtLevelHint {
			return hint.GetStmtHint()
		}
	}
	return nil
}

// GetHintNodeType return HintNode Type string
func GetHintNodeType(htnode HintTree) string {
	var NodeType string
	switch htnode.(type) {
	case *HintTreeJoinNode:
		NodeType = "JoinNode"
	case *HintTreeScanNode:
		NodeType = "ScanNode"
	}
	return NodeType
}

// HasLeadingTable determines whether a TableExpr contains a leading table
func HasLeadingTable(leftchild TableExpr) bool {
	switch left := leftchild.(type) {
	case *JoinTableExpr:
		return left.JoinNodeHint.LeadingTable
	case *AliasedTableExpr:
		return left.IndexFlags.LeadingTable
	default:
		return false
	}
}

// GetHintInfoFromHintForest retrieves HintInfo information from
// HintForest and corresponds it to a table in the 'from Tables' section
func (h HintForest) GetHintInfoFromHintForest(
	fromTables TableExprs, currentDatabase, currentSchema string,
) TableExprs {
	// ToDo:It is necessary to consider whether there is a SubQuery
	// or JoinExpr in the from tables, and to handle the query block

	if len(h) == 0 {
		return nil
	}

	var err error
	var tblexprs TableExprs
	var tblexpr TableExpr
	for _, htree := range h {
		tblexpr, fromTables, err = GetInfoFromHintTree(htree, fromTables, currentDatabase, currentSchema)
		if err != nil {
			panic(err)
		}
		if tblexpr != nil {
			tblexprs = append(tblexprs, tblexpr)
		}
	}
	// Place TableExpr containing leading information at the beginning of the freetable
	var sortedtblexprs TableExprs
	var hasleading bool
	for l := 0; l < len(tblexprs); l++ {
		switch tble := (tblexprs[l]).(type) {
		case *JoinTableExpr:
			if tble.JoinNodeHint.LeadingTable {
				tblexprs = append(tblexprs[:l], tblexprs[l+1:]...)
				sortedtblexprs = append(sortedtblexprs, tble)
				sortedtblexprs = append(sortedtblexprs, tblexprs...)
				hasleading = true
				break
			}
		case *AliasedTableExpr:
			if tble.IndexFlags.LeadingTable {
				tblexprs = append(tblexprs[:l], tblexprs[l+1:]...)
				sortedtblexprs = append(sortedtblexprs, tble)
				sortedtblexprs = append(sortedtblexprs, tblexprs...)
				hasleading = true
				break
			}
		}
	}
	if hasleading {
		sortedtblexprs = append(sortedtblexprs, fromTables...)
		return sortedtblexprs
	}
	tblexprs = append(tblexprs, fromTables...)
	return tblexprs
}

// isMatchName returns true if the tn table is consistent with the hint, otherwise returns false
func isMatchName(tn *TableName, currentDatabase, currentSchema, targetName string) bool {
	result := string(tn.TableNamePrefix.CatalogName) + "." + string(tn.TableNamePrefix.SchemaName) + "." + string(tn.TableName)
	//tn.CatalogName, tn.SchemaName both are empty, use the default db and schema for the current SQL execution
	if tn.CatalogName == "" && tn.SchemaName == "" {
		result = currentDatabase + "." + currentSchema + "." + string(tn.TableName)
	}
	if tn.CatalogName == "" && tn.SchemaName != "" {
		// Is this the database name or the schema name
		// example：
		// tpcc.t1 => Input is tn.schemaName = tpcc, currentDatabase = defaultdb(sql execution database),  currentSchema = public
		// kwbase_ais.t1 => Input is tn.schemaName = public, currentDatabase=defaultdb(sql execution database)，currentSchema = kwbase_ais
		// todo:How to determine if schemaName is db or schema, and temporarily treat it as dbName
		result = string(tn.SchemaName) + "." + currentSchema + "." + string(tn.TableName)
	}
	if result == targetName {
		return true
	}
	return false
}

// GetInfoFromHintTree obtain individual Hint information
func GetInfoFromHintTree(
	htree HintTree, fromtables TableExprs, currentDatabase, currentSchema string,
) (TableExpr, TableExprs, error) {

	switch htnode := htree.(type) {
	case *HintTreeJoinNode:
		var err error
		var ordertable *JoinTableExpr
		if ordertable == nil {
			ordertable = new(JoinTableExpr)
		}

		// JoinMethod must appear simultaneously with RealOrder, otherwise JoinMethod will become invalid
		if htnode.HintType != keys.NoJoinHint && !htnode.RealJoinOrder {
			return nil, nil, pgerror.Newf(pgcode.ExternalBindRealOrder, "Stmt Hint Err: JoinMethod must specify RealJoinOrder")
		}

		// The right side of the Lookup join Hint must be Scan
		if htnode.HintType == keys.UseLookup || htnode.HintType == keys.ForceLookup {
			if GetHintNodeType(htnode.Right) != "ScanNode" {
				return nil, nil, pgerror.Newf(pgcode.ExternalBindLookupRight, "Stmt Hint Err: Right of Lookup Hint must be ScanNode")
			}
		}

		ordertable.Left, fromtables, err = GetInfoFromHintTree(htnode.Left, fromtables, currentDatabase, currentSchema)
		if err != nil {
			return nil, nil, err
		}
		// Get leading information from the left child node
		leading := HasLeadingTable(ordertable.Left)
		ordertable.Right, fromtables, err = GetInfoFromHintTree(htnode.Right, fromtables, currentDatabase, currentSchema)
		if err != nil {
			return nil, nil, err
		}
		ordertable.JoinNodeHint.JoinMethod, ordertable.JoinNodeHint.IndexName, ordertable.JoinNodeHint.RealOrder, ordertable.JoinNodeHint.TotalCardinality, ordertable.JoinNodeHint.EstimatedCardinality, ordertable.JoinNodeHint.LeadingTable =
			htnode.HintType, htnode.IndexName, htnode.RealJoinOrder, htnode.TotalCardinality, htnode.EstimatedCardinality, htnode.LeadingTable
		if leading {
			ordertable.JoinNodeHint.LeadingTable = true
		}
		return ordertable, fromtables, nil

	case *HintTreeScanNode:
		for i, table := range fromtables {
			switch source := table.(type) {
			case *AliasedTableExpr:
				// Embed hint matching
				asName := source.As.Alias
				if tn, ok := source.Expr.(*TableName); ok && (isMatchName(tn, currentDatabase, currentSchema, string(htnode.TableName)) || (tn.TableName == htnode.TableName || asName == htnode.TableName)) {
					fromtables = append(fromtables[:i], fromtables[i+1:]...)
					if source.IndexFlags == nil {
						source.IndexFlags = new(IndexFlags)
					}
					if htnode.HintType == keys.UseIndexScan || htnode.HintType == keys.IgnoreIndexScan || htnode.HintType == keys.ForceIndexScan ||
						htnode.HintType == keys.UseIndexOnly || htnode.HintType == keys.IgnoreIndexOnly || htnode.HintType == keys.ForceIndexOnly {
						if len(htnode.IndexName) == 0 {
							return nil, nil, pgerror.Newf(pgcode.ExternalBindScanIndex, "stmt hint err: no index for hint: %v", htnode.HintType)
						}
					}

					source.IndexFlags.HintType, source.IndexFlags.ForestIndex, source.IndexFlags.TotalCardinality, source.IndexFlags.EstimatedCardinality, source.IndexFlags.LeadingTable =
						htnode.HintType, htnode.IndexName, htnode.TotalCardinality, htnode.EstimatedCardinality, htnode.LeadingTable
					source.IndexFlags.FromHintTree = true
					return source, fromtables, nil
				}
			}
		}
		err := pgerror.Newf(pgcode.ExternalBindNoTable, "stmt hint err: unknown table name of hint scan node: %s", htnode.TableName)
		return nil, nil, err

	case *HintTreeGroupNode:
		return nil, nil, nil
	default:
		err := pgerror.Newf(pgcode.ExternalBindUnknowType, "stmt hint err: unknown hint node type: %T", htnode)
		return nil, nil, err
	}
}
