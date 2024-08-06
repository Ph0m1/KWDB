// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

var impExpNullMap = map[string]bool{
	"null": true,
	"NULL": true,
	"Null": true,
	"\\N":  true,
	"":     true,
}

var escapedMap = map[rune]bool{'"': true, '\\': true}
var enclosedMap = map[rune]bool{'"': true, '\'': true}
var delimiterMap = map[rune]bool{'"': false, '\n': false}

// ImpExpCheckNullOpt used in export_details.go import_stmt.go to check nullas nullif is valid
func ImpExpCheckNullOpt(nullas string) error {
	if _, ok := impExpNullMap[nullas]; !ok {
		return errors.Newf("options nullas/nullif cannot be %s", nullas)
	}
	return nil
}

// CheckEscapeValue used in export_details.go import_stmt.go to check escape is valid
func CheckEscapeValue(escaped rune) error {
	if _, ok := escapedMap[escaped]; ok {
		return nil
	}
	return pgerror.New(pgcode.InvalidParameterValue, fmt.Sprintf("escaped can't be %c", escaped))
}

// CheckEnclosedValue used in export_details.go import_stmt.go to check enclosed is valid
func CheckEnclosedValue(enclosed rune) error {
	if _, ok := enclosedMap[enclosed]; ok {
		return nil
	}
	return pgerror.New(pgcode.InvalidParameterValue, fmt.Sprintf("enclosed can't be %c", enclosed))
}

// CheckImpExpInfoConflict used in export_details.go import_stmt.go to check delimiter && enclosed is puzzled
func CheckImpExpInfoConflict(opts roachpb.CSVOptions) error {
	if (opts.Enclosed != 0 && opts.Comma != 0) && (opts.Enclosed == opts.Comma) {
		return pgerror.New(pgcode.InvalidParameterValue, fmt.Sprintf("delimiter can't be same with enclosed: %c", opts.Enclosed))
	}
	if (opts.Escaped != 0 && opts.Comma != 0) && (opts.Escaped == opts.Comma) {
		return pgerror.New(pgcode.InvalidParameterValue, fmt.Sprintf("delimiter can't be same with enclosed: %c", opts.Escaped))
	}
	return nil
}

// CheckDelimiterValue used in export_details.go import_stmt.go to check delimiter is valid
func CheckDelimiterValue(delimiter rune) error {
	if delimiter < 0 || delimiter > 127 {
		return pgerror.New(pgcode.InvalidParameterValue, "delimiter exceeds the limit of the char type")
	}
	if canbe, ok := delimiterMap[delimiter]; ok {
		if !canbe {
			return pgerror.New(pgcode.InvalidParameterValue, fmt.Sprintf("delimiter can't be %c", delimiter))
		}
	}
	return nil
}
