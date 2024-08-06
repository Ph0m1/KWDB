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

package exprgen

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ParseType parses a string describing a type.
// It supports tuples using the syntax "tuple{<type>, <type>, ...}" but does not
// support tuples of tuples.
func ParseType(typeStr string) (*types.T, error) {
	// Special case for tuples for which there is no SQL syntax.
	if strings.HasPrefix(typeStr, "tuple{") && strings.HasSuffix(typeStr, "}") {
		s := strings.TrimPrefix(typeStr, "tuple{")
		s = strings.TrimSuffix(s, "}")
		// Hijack the PREPARE syntax which takes a list of types.
		// TODO(radu): this won't work for tuples of tuples; we would need to add
		// some special syntax.
		parsed, err := parser.ParseOne(fmt.Sprintf("PREPARE x ( %s ) AS SELECT 1", s))
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse %s as a type", typeStr)
		}
		colTypes := parsed.AST.(*tree.Prepare).Types
		contents := make([]types.T, len(colTypes))
		for i := range colTypes {
			contents[i] = *colTypes[i]
		}
		return types.MakeTuple(contents), nil
	}
	return parser.ParseType(typeStr)
}

// ParseTypes parses a list of types.
func ParseTypes(colStrs []string) ([]*types.T, error) {
	res := make([]*types.T, len(colStrs))
	for i, s := range colStrs {
		var err error
		res[i], err = ParseType(s)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}
