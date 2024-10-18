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

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

func createTSPartitioning(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.MutableTableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
) (sqlbase.PartitioningDescriptor, error) {
	var partDesc sqlbase.PartitioningDescriptor
	if partBy.HashPoint == nil {
		return partDesc, nil
	}
	for _, hashPoint := range partBy.HashPoint {
		if len(hashPoint.HashPoints) != 0 {
			partDesc.HashPoint = append(partDesc.HashPoint, sqlbase.PartitioningDescriptor_HashPoint{
				Name:       hashPoint.Name.String(),
				HashPoints: hashPoint.HashPoints,
			})
		} else {
			if hashPoint.From > hashPoint.To {
				return partDesc, errors.Errorf("from must less than to;")
			}
			var points []int32
			for i := hashPoint.From; i < hashPoint.To; i++ {
				points = append(points, i)
			}
			partDesc.HashPoint = append(partDesc.HashPoint, sqlbase.PartitioningDescriptor_HashPoint{
				Name:       hashPoint.Name.String(),
				HashPoints: points,
			})
		}

	}
	return partDesc, nil
}

func init() {
	CreatePartitioningCCL = createTSPartitioning
}
