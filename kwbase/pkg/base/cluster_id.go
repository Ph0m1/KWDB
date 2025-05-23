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

package base

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

// ClusterIDContainer is used to share a single Cluster ID instance between
// multiple layers. It allows setting and getting the value. Once a value is
// set, the value cannot change.
//
// The cluster ID is determined on startup as follows:
// - If there are existing stores, their cluster ID is used.
// - If the node is bootstrapping, a new UUID is generated.
// - Otherwise, it is determined via gossip with other nodes.
type ClusterIDContainer struct {
	syncutil.Mutex

	clusterID uuid.UUID
}

// String returns the cluster ID, or "?" if it is unset.
func (c *ClusterIDContainer) String() string {
	val := c.Get()
	if val == uuid.Nil {
		return "?"
	}
	return val.String()
}

// Get returns the current cluster ID; uuid.Nil if it is unset.
func (c *ClusterIDContainer) Get() uuid.UUID {
	c.Lock()
	defer c.Unlock()
	return c.clusterID
}

// Set sets the current cluster ID. If it is already set, the value must match.
func (c *ClusterIDContainer) Set(ctx context.Context, val uuid.UUID) {
	c.Lock()
	defer c.Unlock()
	if c.clusterID == uuid.Nil {
		c.clusterID = val
		if log.V(2) {
			log.Infof(ctx, "ClusterID set to %s", val)
		}
	} else if c.clusterID != val {
		log.Fatalf(ctx, "different ClusterIDs set: %s, then %s", c.clusterID, val)
	}
}

// Reset changes the ClusterID regardless of the old value.
//
// Should only be used in testing code.
func (c *ClusterIDContainer) Reset(val uuid.UUID) {
	c.Lock()
	defer c.Unlock()
	c.clusterID = val
}
