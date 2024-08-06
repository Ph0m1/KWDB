// Copyright 2020 The Cockroach Authors.
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

var activeRecordBlocklists = blocklistsForVersion{
	{"v20.1", "activeRecordBlockList20_1", activeRecordBlockList20_1, "activeRecordIgnoreList20_1", activeRecordIgnoreList20_1},
	{"v20.2", "activeRecordBlockList20_2", activeRecordBlockList20_2, "activeRecordIgnoreList20_2", activeRecordIgnoreList20_2},
}

// These are lists of known activerecord test errors and failures.
// When the activerecord test suite is run, the results are compared to this list.
// Any passed test that is not on this list is reported as PASS - expected
// Any passed test that is on this list is reported as PASS - unexpected
// Any failed test that is on this list is reported as FAIL - expected
// Any failed test that is not on this list is reported as FAIL - unexpected
// Any test on this list that is not run is reported as FAIL - not run
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var activeRecordBlockList20_2 = blocklist{}

var activeRecordBlockList20_1 = blocklist{
	"ActiveRecord::ConnectionAdapters::PostgreSQLAdapterTest#test_partial_index":          "9683",
	"ActiveRecord::Migration::CompatibilityTest#test_migration_does_remove_unnamed_index": "9683",
	"PostgresqlActiveSchemaTest#test_add_index":                                           "9683",
	"PostgresqlEnumTest#test_assigning_enum_to_nil":                                       "24873",
	"PostgresqlEnumTest#test_column":                                                      "24873",
	"PostgresqlEnumTest#test_enum_defaults":                                               "24873",
	"PostgresqlEnumTest#test_enum_mapping":                                                "24873",
	"PostgresqlEnumTest#test_enum_type_cast":                                              "24873",
	"PostgresqlEnumTest#test_invalid_enum_update":                                         "24873",
	"PostgresqlEnumTest#test_no_oid_warning":                                              "24873",
	"PostgresqlUUIDTest#test_add_column_with_default_array":                               "55320",
}

var activeRecordIgnoreList20_2 = blocklist{
	"FixturesTest#test_create_fixtures": "flaky - FK constraint violated sometimes when loading all fixture data",
}

var activeRecordIgnoreList20_1 = blocklist{
	"FixturesTest#test_create_fixtures": "flaky - FK constraint violated sometimes when loading all fixture data",
}
