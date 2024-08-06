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

package testcat

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gopkg.in/yaml.v2"
)

// SetZoneConfig is a partial implementation of the ALTER TABLE ... CONFIGURE
// ZONE USING statement.
func (tc *Catalog) SetZoneConfig(stmt *tree.SetZoneConfig) *zonepb.ZoneConfig {
	// Update the table name to include catalog and schema if not provided.
	tabName := stmt.TableOrIndex.Table
	tc.qualifyTableName(&tabName)
	tab := tc.Table(&tabName)

	// Handle special case of primary index.
	if stmt.TableOrIndex.Index == "" {
		tab.Indexes[0].IdxZone = makeZoneConfig(stmt.Options)
		return tab.Indexes[0].IdxZone
	}

	for _, idx := range tab.Indexes {
		if idx.IdxName == string(stmt.TableOrIndex.Index) {
			idx.IdxZone = makeZoneConfig(stmt.Options)
			return idx.IdxZone
		}
	}
	panic(fmt.Errorf("\"%q\" is not an index", stmt.TableOrIndex.Index))
}

// makeZoneConfig constructs a ZoneConfig from options provided to the CONFIGURE
// ZONE USING statement.
func makeZoneConfig(options tree.KVOptions) *zonepb.ZoneConfig {
	zone := &zonepb.ZoneConfig{}
	for i := range options {
		switch options[i].Key {
		case "constraints":
			constraintsList := &zonepb.ConstraintsList{}
			value := options[i].Value.(*tree.StrVal).RawString()
			if err := yaml.UnmarshalStrict([]byte(value), constraintsList); err != nil {
				panic(err)
			}
			zone.Constraints = constraintsList.Constraints

		case "lease_preferences":
			value := options[i].Value.(*tree.StrVal).RawString()
			if err := yaml.UnmarshalStrict([]byte(value), &zone.LeasePreferences); err != nil {
				panic(err)
			}
		}
	}
	return zone
}
