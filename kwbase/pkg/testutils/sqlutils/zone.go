// Copyright 2017 The Cockroach Authors.
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

package sqlutils

import (
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

// ZoneRow represents a row returned by SHOW ZONE CONFIGURATION.
type ZoneRow struct {
	ID     uint32
	Config zonepb.ZoneConfig
}

func (row ZoneRow) sqlRowString() ([]string, error) {
	configProto, err := protoutil.Marshal(&row.Config)
	if err != nil {
		return nil, err
	}
	return []string{
		fmt.Sprintf("%d", row.ID),
		string(configProto),
	}, nil
}

// RemoveAllZoneConfigs removes all installed zone configs.
func RemoveAllZoneConfigs(t testing.TB, sqlDB *SQLRunner) {
	t.Helper()
	for _, row := range sqlDB.QueryStr(t, "SHOW ALL ZONE CONFIGURATIONS") {
		target := row[0]
		if target == fmt.Sprintf("RANGE %s", zonepb.DefaultZoneName) {
			// The default zone cannot be removed.
			continue
		}
		DeleteZoneConfig(t, sqlDB, target)
	}
}

// DeleteZoneConfig deletes the specified zone config through the SQL interface.
func DeleteZoneConfig(t testing.TB, sqlDB *SQLRunner, target string) {
	t.Helper()
	sqlDB.Exec(t, fmt.Sprintf("ALTER %s CONFIGURE ZONE DISCARD", target))
}

// SetZoneConfig updates the specified zone config through the SQL interface.
func SetZoneConfig(t testing.TB, sqlDB *SQLRunner, target string, config string) {
	t.Helper()
	sqlDB.Exec(t, fmt.Sprintf("ALTER %s CONFIGURE ZONE = %s",
		target, lex.EscapeSQLString(config)))
}

// TxnSetZoneConfig updates the specified zone config through the SQL interface
// using the provided transaction.
func TxnSetZoneConfig(t testing.TB, sqlDB *SQLRunner, txn *gosql.Tx, target string, config string) {
	t.Helper()
	_, err := txn.Exec(fmt.Sprintf("ALTER %s CONFIGURE ZONE = %s",
		target, lex.EscapeSQLString(config)))
	if err != nil {
		t.Fatal(err)
	}
}

// VerifyZoneConfigForTarget verifies that the specified zone matches the specified
// ZoneRow.
func VerifyZoneConfigForTarget(t testing.TB, sqlDB *SQLRunner, target string, row ZoneRow) {
	t.Helper()
	sqlRow, err := row.sqlRowString()
	if err != nil {
		t.Fatal(err)
	}
	sqlRow[1] = encodeExpectedRows(sqlRow[1])
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`
SELECT zone_id, raw_config_protobuf
  FROM [SHOW ZONE CONFIGURATION FOR %s]`, target),
		[][]string{sqlRow})
}

// VerifyAllZoneConfigs verifies that the specified ZoneRows exactly match the
// list of active zone configs.
func VerifyAllZoneConfigs(t testing.TB, sqlDB *SQLRunner, rows ...ZoneRow) {
	t.Helper()
	expected := make([][]string, len(rows))
	for i, row := range rows {
		var err error
		expected[i], err = row.sqlRowString()
		if err != nil {
			t.Fatal(err)
		}
		expected[i][1] = encodeExpectedRows(expected[i][1])
	}
	sqlDB.CheckQueryResults(t, `SELECT zone_id, raw_config_protobuf FROM kwdb_internal.zones`, expected)
}

// ZoneConfigExists returns whether a zone config with the provided name exists.
func ZoneConfigExists(t testing.TB, sqlDB *SQLRunner, name string) bool {
	t.Helper()
	var exists bool
	sqlDB.QueryRow(
		t, "SELECT EXISTS (SELECT 1 FROM kwdb_internal.zones WHERE target = $1)", name,
	).Scan(&exists)
	return exists
}

// encodeExceptRows encodes the expected result, only used for zone_test.
func encodeExpectedRows(s string) string {
	result := make([]byte, 2+hex.EncodedLen(len(s)))
	result[0] = '\\'
	result[1] = 'x'
	hex.Encode(result[2:], []byte(s))
	return string(result)
}
