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

package a

import "gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"

func F() {
	var d sqlbase.Descriptor
	d.GetTable() // want `Illegal call to Descriptor.GetTable\(\), see sqlbase.TableFromDescriptor\(\)`

	//nolint:descriptormarshal
	d.GetTable()

	// nolint:descriptormarshal
	d.GetTable()

	// nolint:descriptormarshal
	if t := d.GetTable(); t != nil {
		panic("foo")
	}

	if t := d.
		// nolint:descriptormarshal
		GetTable(); t != nil {
		panic("foo")
	}

	if t :=
		// nolint:descriptormarshal
		d.GetTable(); t != nil {
		panic("foo")
	}

	if t := d.GetTable(); t != // want `Illegal call to Descriptor.GetTable\(\), see sqlbase.TableFromDescriptor\(\)`
		// nolint:descriptormarshal
		nil {
		panic("foo")
	}

	// It does not work to put the comment as an inline with the preamble to an
	// if statement.
	if t := d.GetTable(); t != nil { // nolint:descriptormarshal // want `Illegal call to Descriptor.GetTable\(\), see sqlbase.TableFromDescriptor\(\)`
		panic("foo")
	}

	if t := d.GetTable(); t != nil { // want `Illegal call to Descriptor.GetTable\(\), see sqlbase.TableFromDescriptor\(\)`
		panic("foo")
	}
}
