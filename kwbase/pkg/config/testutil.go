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

package config

import (
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

type zoneConfigMap map[uint32]zonepb.ZoneConfig

var (
	testingZoneConfig   zoneConfigMap
	testingHasHook      bool
	testingPreviousHook zoneConfigHook
	testingLock         syncutil.Mutex
)

// TestingSetupZoneConfigHook initializes the zone config hook
// to 'testingZoneConfigHook' which uses 'testingZoneConfig'.
// Settings go back to their previous values when the stopper runs our closer.
func TestingSetupZoneConfigHook(stopper *stop.Stopper) {
	stopper.AddCloser(stop.CloserFn(testingResetZoneConfigHook))

	testingLock.Lock()
	defer testingLock.Unlock()
	if testingHasHook {
		panic("TestingSetupZoneConfigHook called without restoring state")
	}
	testingHasHook = true
	testingZoneConfig = make(zoneConfigMap)
	testingPreviousHook = ZoneConfigHook
	ZoneConfigHook = testingZoneConfigHook
	testingLargestIDHook = func(maxID uint32) (max uint32) {
		testingLock.Lock()
		defer testingLock.Unlock()
		for id := range testingZoneConfig {
			if maxID > 0 && id > maxID {
				continue
			}
			if id > max {
				max = id
			}
		}
		return
	}
}

// testingResetZoneConfigHook resets the zone config hook back to what it was
// before TestingSetupZoneConfigHook was called.
func testingResetZoneConfigHook() {
	testingLock.Lock()
	defer testingLock.Unlock()
	if !testingHasHook {
		panic("TestingResetZoneConfigHook called on uninitialized testing hook")
	}
	testingHasHook = false
	ZoneConfigHook = testingPreviousHook
	testingLargestIDHook = nil
}

// TestingSetZoneConfig sets the zone config entry for object 'id'
// in the testing map.
func TestingSetZoneConfig(id uint32, zone zonepb.ZoneConfig) {
	testingLock.Lock()
	defer testingLock.Unlock()
	testingZoneConfig[id] = zone
}

func testingZoneConfigHook(
	_ *SystemConfig, id uint32,
) (*zonepb.ZoneConfig, *zonepb.ZoneConfig, bool, error) {
	testingLock.Lock()
	defer testingLock.Unlock()
	if zone, ok := testingZoneConfig[id]; ok {
		return &zone, nil, false, nil
	}
	return nil, nil, false, nil
}
