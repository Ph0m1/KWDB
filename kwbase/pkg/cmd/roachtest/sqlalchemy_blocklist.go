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

package main

var sqlAlchemyBlocklists = blocklistsForVersion{
	{"v2.1", "sqlAlchemyBlocklist", sqlAlchemyBlocklist, "sqlAlchemyIgnoreList", sqlAlchemyIgnoreList},
	{"v19.1", "sqlAlchemyBlocklist", sqlAlchemyBlocklist, "sqlAlchemyIgnoreList", sqlAlchemyIgnoreList},
	{"v19.2", "sqlAlchemyBlocklist", sqlAlchemyBlocklist, "sqlAlchemyIgnoreList", sqlAlchemyIgnoreList},
	{"v20.1", "sqlAlchemyBlocklist20_1", sqlAlchemyBlocklist20_1, "sqlAlchemyIgnoreList20_1", sqlAlchemyIgnoreList20_1},
	{"v20.2", "sqlAlchemyBlocklist20_2", sqlAlchemyBlocklist20_2, "sqlAlchemyIgnoreList20_2", sqlAlchemyIgnoreList20_2},
}

var sqlAlchemyBlocklist20_2 = blocklist{}

var sqlAlchemyBlocklist20_1 = blocklist{
	"test/dialect/test_suite.py::ExpandingBoundInTest_kwbasedb+psycopg2_9_5_0::test_null_in_empty_set_is_false": "41596",
}

var sqlAlchemyBlocklist = blocklist{
	"test/dialect/test_suite.py::ExpandingBoundInTest_kwbasedb+psycopg2_9_5_0::test_null_in_empty_set_is_false": "41596",
}

var sqlAlchemyIgnoreList20_2 = sqlAlchemyIgnoreList

var sqlAlchemyIgnoreList20_1 = sqlAlchemyIgnoreList

var sqlAlchemyIgnoreList = blocklist{
	"test/dialect/test_suite.py::ComponentReflectionTest_kwbasedb+psycopg2_9_5_0::test_deprecated_get_primary_keys": "test has a bug and is getting removed",
}
