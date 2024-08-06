// Copyright 2016 The Cockroach Authors.
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

package yacc

import (
	"io/ioutil"
	"testing"

	// Needed for the -verbosity flag on circleci tests.
	_ "gitee.com/kwbasedb/kwbase/pkg/util/log"
)

const sqlYPath = "../../../sql/parser/sql.y"

func TestLex(t *testing.T) {
	b, err := ioutil.ReadFile(sqlYPath)
	if err != nil {
		t.Fatal(err)
	}
	l := lex(sqlYPath, string(b))
Loop:
	for {
		item := l.nextItem()
		switch item.typ {
		case itemEOF:
			break Loop
		case itemError:
			t.Fatalf("%s:%d: %s", sqlYPath, l.lineNumber(), item)
		}
	}
}

func TestParse(t *testing.T) {
	b, err := ioutil.ReadFile(sqlYPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Parse(sqlYPath, string(b))
	if err != nil {
		t.Fatal(err)
	}
}
