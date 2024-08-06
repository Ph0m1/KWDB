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
	"fmt"
	"time"
)

// isAlive returns whether the node queried by db is alive.
func isAlive(db *gosql.DB, l *logger) bool {
	// The cluster might have just restarted, in which case the first call to db
	// might return an error. In fact, the first db.Ping() reliably returns an
	// error (but a db.Exec() only seldom returns an error). So, we're gonna
	// Ping() twice to allow connections to be re-established.
	_ = db.Ping()
	if err := db.Ping(); err != nil {
		l.Printf("isAlive returned err=%v (%T)", err, err)
	} else {
		return true
	}
	return false
}

// dbUnixEpoch returns the current time in db.
func dbUnixEpoch(db *gosql.DB) (float64, error) {
	var epoch float64
	if err := db.QueryRow("SELECT now()::DECIMAL").Scan(&epoch); err != nil {
		return 0, err
	}
	return epoch, nil
}

// offsetInjector is used to inject clock offsets in roachtests.
type offsetInjector struct {
	c        *cluster
	deployed bool
}

// deploy installs ntp and downloads / compiles bumptime used to create a clock offset.
func (oi *offsetInjector) deploy(ctx context.Context) error {
	if err := oi.c.RunE(ctx, oi.c.All(), "test -x ./bumptime"); err == nil {
		oi.deployed = true
		return nil
	}

	if err := oi.c.Install(ctx, oi.c.l, oi.c.All(), "ntp"); err != nil {
		return err
	}
	if err := oi.c.Install(ctx, oi.c.l, oi.c.All(), "gcc"); err != nil {
		return err
	}
	if err := oi.c.RunL(ctx, oi.c.l, oi.c.All(), "sudo", "service", "ntp", "stop"); err != nil {
		return err
	}
	if err := oi.c.RunL(ctx, oi.c.l,
		oi.c.All(),
		"curl",
		"--retry", "3",
		"--fail",
		"--show-error",
		"-kO",
		"https://raw.githubusercontent.com/kwbasedb/jepsen/master/kwbasedb/resources/bumptime.c",
	); err != nil {
		return err
	}
	if err := oi.c.RunL(ctx, oi.c.l,
		oi.c.All(), "gcc", "bumptime.c", "-o", "bumptime", "&&", "rm bumptime.c",
	); err != nil {
		return err
	}
	oi.deployed = true
	return nil
}

// offset injects a offset of s into the node with the given nodeID.
func (oi *offsetInjector) offset(ctx context.Context, nodeID int, s time.Duration) {
	if !oi.deployed {
		oi.c.t.Fatal("Offset injector must be deployed before injecting a clock offset")
	}

	oi.c.Run(
		ctx,
		oi.c.Node(nodeID),
		fmt.Sprintf("sudo ./bumptime %f", float64(s)/float64(time.Millisecond)),
	)
}

// recover force syncs time on the node with the given nodeID to recover
// from any offsets.
func (oi *offsetInjector) recover(ctx context.Context, nodeID int) {
	if !oi.deployed {
		oi.c.t.Fatal("Offset injector must be deployed before recovering from clock offsets")
	}

	syncCmds := [][]string{
		{"sudo", "service", "ntp", "stop"},
		{"sudo", "ntpdate", "-u", "time.google.com"},
		{"sudo", "service", "ntp", "start"},
	}
	for _, cmd := range syncCmds {
		oi.c.Run(
			ctx,
			oi.c.Node(nodeID),
			cmd...,
		)
	}
}

// newOffsetInjector creates a offsetInjector which can be used to inject
// and recover from clock offsets.
func newOffsetInjector(c *cluster) *offsetInjector {
	return &offsetInjector{c: c}
}
