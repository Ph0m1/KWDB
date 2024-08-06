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

import "gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"

// delegateShowClusterSettingList rewrites ShowClusterSettingList statement to select statement
// which returns variable, value, setting_type... from kwdb_internal.cluster_settings
func (d *delegator) delegateShowClusterSettingList(
	stmt *tree.ShowClusterSettingList,
) (tree.Statement, error) {
	if err := d.catalog.RequireAdminRole(d.ctx, "SHOW CLUSTER SETTINGS"); err != nil {
		return nil, err
	}
	if stmt.All {
		return parse(
			`SELECT variable, value, type AS setting_type, public, description
       FROM   kwdb_internal.cluster_settings`,
		)
	}
	return parse(
		`SELECT variable, value, type AS setting_type, description
     FROM   kwdb_internal.cluster_settings
     WHERE  public IS TRUE`,
	)
}
