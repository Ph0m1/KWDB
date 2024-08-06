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

package rpc

import (
	"bytes"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
)

// ConnectionClass is the identifier of a group of RPC client sessions that are
// allowed to share an underlying TCP connections; RPC sessions with different
// connection classes are guaranteed to use separate gRPC client connections.
//
// RPC sessions that share a connection class are arbitrated using the gRPC flow
// control logic, see google.golang.org/grpc/internal/transport. The lack of
// support of prioritization in the current gRPC implementation is the reason
// why we are separating different priority flows across separate TCP
// connections. Future gRPC improvements may enable further simplification
// here. See https://github.com/grpc/grpc-go/issues/1448 for progress on gRPC's
// adoption of HTTP2 priorities.
type ConnectionClass int8

const (
	// DefaultClass is the default ConnectionClass and should be used for most
	// client traffic.
	DefaultClass ConnectionClass = iota
	// SystemClass is the ConnectionClass used for system traffic.
	SystemClass

	// NumConnectionClasses is the number of valid ConnectionClass values.
	NumConnectionClasses int = iota
)

var systemClassKeyPrefixes = []roachpb.RKey{
	roachpb.RKey(keys.Meta1Prefix),
	roachpb.RKey(keys.NodeLivenessPrefix),
}

// ConnectionClassForKey determines the ConnectionClass which should be used
// for traffic addressed to the RKey.
func ConnectionClassForKey(key roachpb.RKey) ConnectionClass {
	// An empty RKey addresses range 1 and warrants SystemClass.
	if len(key) == 0 {
		return SystemClass
	}
	for _, prefix := range systemClassKeyPrefixes {
		if bytes.HasPrefix(key, prefix) {
			return SystemClass
		}
	}
	return DefaultClass
}
