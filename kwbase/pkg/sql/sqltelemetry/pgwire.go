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

package sqltelemetry

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
)

// CancelRequestCounter is to be incremented every time a pgwire-level
// cancel request is received from a client.
var CancelRequestCounter = telemetry.GetCounterOnce("pgwire.unimplemented.cancel_request")

// UnimplementedClientStatusParameterCounter is to be incremented
// every time a client attempts to configure a status parameter
// that's not supported upon session initialization.
func UnimplementedClientStatusParameterCounter(key string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("unimplemented.pgwire.parameter.%s", key))
}

// BinaryDecimalInfinityCounter is to be incremented every time a
// client requests the binary encoding for a decimal infinity, which
// is not well defined in the pg protocol (#32489).
var BinaryDecimalInfinityCounter = telemetry.GetCounterOnce("pgwire.#32489.binary_decimal_infinity")

// UncategorizedErrorCounter is to be incremented every time an error
// flows to the client without having been decorated with a pg error.
var UncategorizedErrorCounter = telemetry.GetCounterOnce("othererror." + pgcode.Uncategorized)

// InterleavedPortalRequestCounter is to be incremented every time an open
// portal attempts to interleave work with another portal.
var InterleavedPortalRequestCounter = telemetry.GetCounterOnce("pgwire.#40195.interleaved_portal")

// PortalWithLimitRequestCounter is to be incremented every time a portal request is
// made.
var PortalWithLimitRequestCounter = telemetry.GetCounterOnce("pgwire.portal_with_limit_request")
