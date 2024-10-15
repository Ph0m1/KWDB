// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// planDependencyInfo collects the dependencies related to a single
// table -- which index and columns are being depended upon.
type planDependencyInfo struct {
	// desc is a reference to the descriptor for the table being
	// depended on.
	desc *sqlbase.ImmutableTableDescriptor
	// deps is the list of ways in which the current plan depends on
	// that table. There can be more than one entries when the same
	// table is used in different places. The entries can also be
	// different because some may reference an index and others may
	// reference only a subset of the table's columns.
	// Note: the "ID" field of TableDescriptor_Reference is not
	// (and cannot be) filled during plan construction / dependency
	// analysis because the descriptor that is using this dependency
	// has not been constructed yet.
	deps []sqlbase.TableDescriptor_Reference
}

// planDependencies maps the ID of a table depended upon to a list of
// detailed dependencies on that table.
type planDependencies map[sqlbase.ID]planDependencyInfo

// String implements the fmt.Stringer interface.
func (d planDependencies) String() string {
	var buf bytes.Buffer
	for id, deps := range d {
		fmt.Fprintf(&buf, "%d (%q):", id, tree.ErrNameStringP(&deps.desc.Name))
		for _, dep := range deps.deps {
			buf.WriteString(" [")
			if dep.IndexID != 0 {
				fmt.Fprintf(&buf, "idx: %d ", dep.IndexID)
			}
			fmt.Fprintf(&buf, "cols: %v]", dep.ColumnIDs)
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// checkViewMatchesMaterialized ensures that if a view is required, then the view
// is materialized or not as desired.
func checkViewMatchesMaterialized(
	desc sqlbase.MutableTableDescriptor, requireView, wantMaterialized bool,
) error {
	if !requireView {
		return nil
	}
	if !desc.IsView() {
		return nil
	}
	isMaterialized := desc.MaterializedView()
	if isMaterialized && !wantMaterialized {
		err := pgerror.Newf(pgcode.WrongObjectType, "%q is a materialized view", desc.GetName())
		return errors.WithHint(err, "use the corresponding MATERIALIZED VIEW command")
	}
	if !isMaterialized && wantMaterialized {
		return pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", desc.GetName())
	}
	return nil
}
