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

package settings

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"github.com/cockroachdb/errors"
)

// DefaultPartitionCoefficient is the balance of partition_coefficient_num
var DefaultPartitionCoefficient = settings.RegisterIntSetting(
	"sql.hashrouter.partition_coefficient_num",
	"the ts range count in the cluster",
	1,
)

// DefaultEntityRangeReplicaNum is the default replica number
var DefaultEntityRangeReplicaNum = settings.RegisterIntSetting(
	"sql.hashrouter.tsrangereplicanum",
	"the ts range replica num in the cluster",
	3,
)

// HaLivenessCheckInterval is the ha liveness check interval.
var HaLivenessCheckInterval = settings.RegisterValidatedDurationSetting(
	"server.distribute.ha_check_interval",
	"node liveness ha check interval",
	4*time.Second,
	func(d time.Duration) error {
		if d < 4*time.Second || d > 30*time.Second {
			return errors.New("ha check interval must be >= 4 seconds and <= 30 seconds")
		}
		return nil
	},
)

// GroupChangesProcesses is the number of concurrent group changes processes
var GroupChangesProcesses = settings.RegisterIntSetting(
	"sql.hashrouter.group_changes_processes",
	"the number of concurrent group changes processes",
	3,
)

// AllowAdvanceDistributeSettingName is the name of cluster setting server.allow_advanced_distributed_operations
const AllowAdvanceDistributeSettingName = "server.advanced_distributed_operations.enabled"

// AllowAdvanceDistributeSetting if set true, distributed function operations are allowed,
// including joining new node, node decommission, node death, and manual balancing
var AllowAdvanceDistributeSetting = settings.RegisterBoolSetting(
	AllowAdvanceDistributeSettingName,
	"if set true, distributed function operations are allowed, including joining new node, node decommission, node death, and manual balancing",
	false,
)
