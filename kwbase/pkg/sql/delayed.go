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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// delayedNode wraps a planNode in cases where the planNode
// constructor must be delayed during query execution (as opposed to
// SQL prepare) for resource tracking purposes.
type delayedNode struct {
	name        string
	columns     sqlbase.ResultColumns
	constructor nodeConstructor
	plan        planNode
}

type nodeConstructor func(context.Context, *planner) (planNode, error)

func (d *delayedNode) Next(params runParams) (bool, error) { return d.plan.Next(params) }
func (d *delayedNode) Values() tree.Datums                 { return d.plan.Values() }

func (d *delayedNode) Close(ctx context.Context) {
	if d.plan != nil {
		d.plan.Close(ctx)
		d.plan = nil
	}
}

// startExec constructs the wrapped planNode now that execution is underway.
func (d *delayedNode) startExec(params runParams) error {
	if d.plan != nil {
		panic("wrapped plan should not yet exist")
	}

	plan, err := d.constructor(params.ctx, params.p)
	if err != nil {
		return err
	}
	d.plan = plan

	// Recursively invoke startExec on new plan. Normally, startExec doesn't
	// recurse - calling children is handled by the planNode walker. The reason
	// this won't suffice here is that the the child of this node doesn't exist
	// until after startExec is invoked.
	return startExec(params, plan)
}
