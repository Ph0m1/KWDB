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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// delegateShowSchemas implements SHOW SCHEMAS which returns all the schemas in
// the given or current database.
// Privileges: None.
func (d *delegator) delegateShowSchemas(n *tree.ShowSchemas) (tree.Statement, error) {
	name, err := d.getSpecifiedOrCurrentDatabase(n.Database)
	if err != nil {
		return nil, err
	}
	getSchemasQuery := fmt.Sprintf(`
			SELECT schema_name
			FROM %[1]s.information_schema.schemata
			WHERE catalog_name = %[2]s
			ORDER BY schema_name`,
		name.String(), // note: (tree.Name).String() != string(name)
		lex.EscapeSQLString(string(name)),
	)

	return parse(getSchemasQuery)
}

// getSpecifiedOrCurrentDatabase returns the name of the specified database, or
// of the current database if the specified name is empty.
//
// Returns an error if there is no current database, or if the specified
// database doesn't exist.
func (d *delegator) getSpecifiedOrCurrentDatabase(specifiedDB tree.Name) (tree.Name, error) {
	var name cat.SchemaName
	if specifiedDB != "" {
		// Note: the schema name may be interpreted as database name,
		// see name_resolution.go.
		name.SchemaName = specifiedDB
		name.ExplicitSchema = true
	} else {
		return tree.Name(d.catalog.GetCurrentDatabase(d.ctx)), nil
	}

	flags := cat.Flags{AvoidDescriptorCaches: true}
	_, resName, err := d.catalog.ResolveSchema(d.ctx, flags, &name)
	if err != nil {
		return "", err
	}
	return resName.CatalogName, nil
}
