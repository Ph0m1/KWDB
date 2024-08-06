// Copyright 2018 The Cockroach Authors.
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

package optbuilder

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
)

var _ sqlutils.ColumnItemResolverTester = &scope{}

// GetColumnItemResolver is part of the sqlutils.ColumnItemResolverTester
// interface.
func (s *scope) GetColumnItemResolver() tree.ColumnItemResolver {
	return s
}

// AddTable is part of the sqlutils.ColumnItemResolverTester interface.
func (s *scope) AddTable(tabName tree.TableName, colNames []tree.Name) {
	for _, col := range colNames {
		s.cols = append(s.cols, scopeColumn{name: col, table: tabName})
	}
}

// ResolveQualifiedStarTestResults is part of the
// sqlutils.ColumnItemResolverTester interface.
func (s *scope) ResolveQualifiedStarTestResults(
	srcName *tree.TableName, srcMeta tree.ColumnSourceMeta,
) (string, string, error) {
	s, ok := srcMeta.(*scope)
	if !ok {
		return "", "", fmt.Errorf("resolver did not return *scope, found %T instead", srcMeta)
	}
	nl := make(tree.NameList, 0, len(s.cols))
	for i := range s.cols {
		col := s.cols[i]
		if col.table == *srcName && !col.hidden {
			nl = append(nl, col.name)
		}
	}
	return srcName.String(), nl.String(), nil
}

// ResolveColumnItemTestResults is part of the
// sqlutils.ColumnItemResolverTester interface.
func (s *scope) ResolveColumnItemTestResults(colRes tree.ColumnResolutionResult) (string, error) {
	col, ok := colRes.(*scopeColumn)
	if !ok {
		return "", fmt.Errorf("resolver did not return *scopeColumn, found %T instead", colRes)
	}
	return fmt.Sprintf("%s.%s", col.table.String(), col.name), nil
}

func TestResolveQualifiedStar(t *testing.T) {
	s := &scope{}
	sqlutils.RunResolveQualifiedStarTest(t, s)
}

func TestResolveColumnItem(t *testing.T) {
	s := &scope{}
	sqlutils.RunResolveColumnItemTest(t, s)
}
