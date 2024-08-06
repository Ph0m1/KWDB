// Copyright 2015 The Cockroach Authors.
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

package resolver

import (
	"net"

	"gitee.com/kwbasedb/kwbase/pkg/util"
	"github.com/pkg/errors"
)

// socketResolver represents the different types of socket-based
// address resolvers.
type socketResolver struct {
	typ  string
	addr string
}

// Type returns the resolver type.
func (sr *socketResolver) Type() string { return sr.typ }

// Addr returns the resolver address.
func (sr *socketResolver) Addr() string { return sr.addr }

// GetAddress returns a net.Addr or error.
func (sr *socketResolver) GetAddress() (net.Addr, error) {
	switch sr.typ {
	case "tcp":
		_, err := net.ResolveTCPAddr("tcp", sr.addr)
		if err != nil {
			return nil, err
		}
		return util.NewUnresolvedAddr("tcp", sr.addr), nil
	}
	return nil, errors.Errorf("unknown address type: %q", sr.typ)
}
