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

	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// CompressData compresses data manually.
func (p *planner) CompressData(ctx context.Context, n *tree.Compress) (planNode, error) {

	if err := p.RequireAdminRole(ctx, "COMPRESS"); err != nil {
		return nil, err
	}
	opNode := &operateDataNode{}
	var tableDescs []sqlbase.TableDescriptor
	nodeList, err := api.GetHealthyNodeIDs(ctx)
	if err != nil {
		return nil, err
	}
	opNode.nodeID = nodeList

	switch n.Typ {
	case tree.CompressTypeAll, tree.CompressTypeDB:
		var dbDesc *UncachedDatabaseDescriptor
		if n.Typ == tree.CompressTypeAll {
			opNode.operateType = compressAll
		} else {
			opNode.operateType = compressDB
			dbDesc, err = p.ResolveUncachedDatabaseByName(ctx, string(n.DBName), true)
			if err != nil {
				return nil, err
			}
			if dbDesc.EngineType == tree.EngineTypeRelational {
				return nil, pgerror.Newf(pgcode.FeatureNotSupported, "COMPRESS is not supported in relational mode")
			}
		}

		allDesc, err := GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return nil, err
		}

		for i := range allDesc {
			tab, ok := allDesc[i].(*sqlbase.TableDescriptor)
			if ok && (n.Typ == tree.CompressTypeAll || (n.Typ == tree.CompressTypeDB && dbDesc.ID == tab.ParentID)) {
				if tab.IsTSTable() {
					tableDescs = append(tableDescs, *tab)
				}
			}
		}

	case tree.CompressTypeTable:
		opNode.operateType = compressTable
		tableDesc, err := p.ResolveMutableTableDescriptor(
			ctx, &n.TblName, true /*required*/, ResolveRequireTableDesc,
		)
		if err != nil {
			return nil, err
		}
		if tableDesc.IsTSTable() {
			tableDescs = append(tableDescs, tableDesc.TableDescriptor)
		} else {
			return nil, pgerror.Newf(pgcode.FeatureNotSupported, "COMPRESS is not supported in relational mode")
		}
	}

	if len(tableDescs) == 0 {
		return newZeroNode(nil /* columns */), nil
	}
	opNode.desc = tableDescs

	return opNode, nil
}
