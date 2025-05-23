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

package sessiondata

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Tests the implied search path when no temporary schema has been created
// by the session.
func TestImpliedSearchPath(t *testing.T) {
	testTempSchemaName := `test_temp_schema`

	testCases := []struct {
		explicitSearchPath                                             []string
		expectedSearchPath                                             []string
		expectedSearchPathWithoutImplicitPgSchemas                     []string
		expectedSearchPathWhenTemporarySchemaExists                    []string
		expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists []string
	}{
		{
			explicitSearchPath:                                             []string{},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{},
		},
		{
			explicitSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`pg_catalog`, `pg_temp`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{`pg_catalog`, testTempSchemaName},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`pg_catalog`, testTempSchemaName},
		},
		{
			explicitSearchPath:                                             []string{`pg_temp`, `pg_catalog`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{testTempSchemaName, `pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`foobar`, `pg_catalog`},
			expectedSearchPath:                                             []string{`foobar`, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`, `pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `foobar`, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`, `pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`foobar`, `pg_temp`},
			expectedSearchPath:                                             []string{`pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{`pg_catalog`, `foobar`, testTempSchemaName},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`, testTempSchemaName},
		},
		{
			explicitSearchPath:                                             []string{`foobar`},
			expectedSearchPath:                                             []string{`pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`},
		},
	}

	for tcNum, tc := range testCases {
		t.Run(strings.Join(tc.explicitSearchPath, ","), func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath)
			actualSearchPath := make([]string, 0)
			iter := searchPath.Iter()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPath, actualSearchPath) {
				t.Errorf(
					`#%d: Expected search path to be %#v, but was %#v.`,
					tcNum,
					tc.expectedSearchPath,
					actualSearchPath,
				)
			}
		})

		t.Run(strings.Join(tc.explicitSearchPath, ",")+"/no-pg-schemas", func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath)
			actualSearchPath := make([]string, 0)
			iter := searchPath.IterWithoutImplicitPGSchemas()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPathWithoutImplicitPgSchemas, actualSearchPath) {
				t.Errorf(
					`#%d: Expected search path to be %#v, but was %#v.`,
					tcNum,
					tc.expectedSearchPathWithoutImplicitPgSchemas,
					actualSearchPath,
				)
			}
		})

		t.Run(strings.Join(tc.explicitSearchPath, ",")+"/temp-schema-exists", func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath).WithTemporarySchemaName(testTempSchemaName)
			actualSearchPath := make([]string, 0)
			iter := searchPath.Iter()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPathWhenTemporarySchemaExists, actualSearchPath) {
				t.Errorf(
					`#%d: Expected search path to be %#v, but was %#v.`,
					tcNum,
					tc.expectedSearchPathWhenTemporarySchemaExists,
					actualSearchPath,
				)
			}
		})

		t.Run(strings.Join(tc.explicitSearchPath, ",")+"/no-pg-schemas/temp-schema-exists", func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath).WithTemporarySchemaName(testTempSchemaName)
			actualSearchPath := make([]string, 0)
			iter := searchPath.IterWithoutImplicitPGSchemas()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists, actualSearchPath) {
				t.Errorf(
					`#%d: Expected search path to be %#v, but was %#v.`,
					tcNum,
					tc.expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists,
					actualSearchPath,
				)
			}
		})
	}
}

func TestSearchPathEquals(t *testing.T) {
	testTempSchemaName := `test_temp_schema`

	a1 := MakeSearchPath([]string{"x", "y", "z"})
	a2 := MakeSearchPath([]string{"x", "y", "z"})
	assert.True(t, a1.Equals(&a1))
	assert.True(t, a2.Equals(&a2))

	assert.True(t, a1.Equals(&a2))
	assert.True(t, a2.Equals(&a1))

	b := MakeSearchPath([]string{"x", "z", "y"})
	assert.False(t, a1.Equals(&b))

	c1 := MakeSearchPath([]string{"x", "y", "z", "pg_catalog"})
	c2 := MakeSearchPath([]string{"x", "y", "z", "pg_catalog"})
	assert.True(t, c1.Equals(&c2))
	assert.False(t, a1.Equals(&c1))

	d := MakeSearchPath([]string{"x"})
	assert.False(t, a1.Equals(&d))

	e1 := MakeSearchPath([]string{"x", "y", "z"}).WithTemporarySchemaName(testTempSchemaName)
	e2 := MakeSearchPath([]string{"x", "y", "z"}).WithTemporarySchemaName(testTempSchemaName)
	assert.True(t, e1.Equals(&e1))
	assert.True(t, e1.Equals(&e2))
	assert.False(t, e1.Equals(&a1))

	f := MakeSearchPath([]string{"x", "z", "y"}).WithTemporarySchemaName(testTempSchemaName)
	assert.False(t, e1.Equals(&f))

	g := MakeSearchPath([]string{"x", "y", "z", "pg_temp"})
	assert.False(t, e1.Equals(&g))
	assert.False(t, g.Equals(&c1))

	h := MakeSearchPath([]string{"x", "y", "z", "pg_temp"}).WithTemporarySchemaName(testTempSchemaName)
	assert.False(t, g.Equals(&h))

	i := MakeSearchPath([]string{"x", "y", "z", "pg_temp", "pg_catalog"}).WithTemporarySchemaName(testTempSchemaName)
	assert.False(t, i.Equals(&h))
	assert.False(t, i.Equals(&c1))
}

func TestWithTemporarySchema(t *testing.T) {
	testTempSchemaName := `test_temp_schema`

	sp := MakeSearchPath([]string{"x", "y", "z"})
	sp = sp.UpdatePaths([]string{"x", "pg_catalog"})
	assert.True(t, sp.GetTemporarySchemaName() == "")

	sp = sp.WithTemporarySchemaName(testTempSchemaName)
	sp = sp.UpdatePaths([]string{"pg_catalog"})
	assert.True(t, sp.GetTemporarySchemaName() == testTempSchemaName)

	sp = sp.UpdatePaths([]string{"x", "pg_temp"})
	assert.True(t, sp.GetTemporarySchemaName() == testTempSchemaName)
}
