// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

func runEventLog(ctx context.Context, t *test, c *cluster) {
	type nodeEventInfo struct {
		Descriptor roachpb.NodeDescriptor
		ClusterID  uuid.UUID
	}

	c.Put(ctx, kwbase, "./kwbase")
	c.Start(ctx, t)

	// Verify that "node joined" and "node restart" events are recorded whenever
	// a node starts and contacts the cluster.
	db := c.Conn(ctx, 1)
	defer db.Close()
	waitForFullReplication(t, db)
	var clusterID uuid.UUID

	err := retry.ForDuration(10*time.Second, func() error {
		rows, err := db.Query(
			`SELECT "targetID", info FROM system.eventlog WHERE "eventType" = 'node_join'`,
		)
		if err != nil {
			t.Fatal(err)
		}
		clusterID = uuid.UUID{}
		seenIds := make(map[int64]struct{})
		for rows.Next() {
			var targetID int64
			var infoStr gosql.NullString
			if err := rows.Scan(&targetID, &infoStr); err != nil {
				t.Fatal(err)
			}

			// Verify the stored node descriptor.
			if !infoStr.Valid {
				t.Fatalf("info not recorded for node join, target node %d", targetID)
			}
			var info nodeEventInfo
			if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
				t.Fatal(err)
			}
			if a, e := int64(info.Descriptor.NodeID), targetID; a != e {
				t.Fatalf("Node join with targetID %d had descriptor for wrong node %d", e, a)
			}

			// Verify cluster ID is recorded, and is the same for all nodes.
			if (info.ClusterID == uuid.UUID{}) {
				t.Fatalf("Node join recorded nil cluster id, info: %v", info)
			}
			if (clusterID == uuid.UUID{}) {
				clusterID = info.ClusterID
			} else if clusterID != info.ClusterID {
				t.Fatalf(
					"Node join recorded different cluster ID than earlier node. Expected %s, got %s. Info: %v",
					clusterID, info.ClusterID, info)
			}

			// Verify that all NodeIDs are different.
			if _, ok := seenIds[targetID]; ok {
				t.Fatalf("Node ID %d seen in two different node join messages", targetID)
			}
			seenIds[targetID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if c.spec.NodeCount != len(seenIds) {
			return fmt.Errorf("expected %d node join messages, found %d: %v",
				c.spec.NodeCount, len(seenIds), seenIds)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Stop and Start Node 3, and verify the node restart message.
	c.Stop(ctx, c.Node(3))
	c.Start(ctx, t, c.Node(3))

	err = retry.ForDuration(10*time.Second, func() error {
		// Query all node restart events. There should only be one.
		rows, err := db.Query(
			`SELECT "targetID", info FROM system.eventlog WHERE "eventType" = 'node_restart'`,
		)
		if err != nil {
			return err
		}

		seenCount := 0
		for rows.Next() {
			var targetID int64
			var infoStr gosql.NullString
			if err := rows.Scan(&targetID, &infoStr); err != nil {
				return err
			}

			// Verify the stored node descriptor.
			if !infoStr.Valid {
				t.Fatalf("info not recorded for node join, target node %d", targetID)
			}
			var info nodeEventInfo
			if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
				t.Fatal(err)
			}
			if a, e := int64(info.Descriptor.NodeID), targetID; a != e {
				t.Fatalf("node join with targetID %d had descriptor for wrong node %d", e, a)
			}

			// Verify cluster ID is recorded, and is the same for all nodes.
			if clusterID != info.ClusterID {
				t.Fatalf("expected cluser ID %s, got %s\n%v", clusterID, info.ClusterID, info)
			}

			seenCount++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if seenCount != 1 {
			return fmt.Errorf("expected one node restart event, found %d", seenCount)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
