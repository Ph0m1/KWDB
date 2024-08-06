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

package clusterversion

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// keyedVersion associates a key to a version.
type keyedVersion struct {
	Key VersionKey
	roachpb.Version
}

// keyedVersions is a container for managing the versions of CockroachDB.
type keyedVersions []keyedVersion

// MustByKey asserts that the version specified by this key exists, and returns it.
func (kv keyedVersions) MustByKey(k VersionKey) roachpb.Version {
	key := int(k)
	if key >= len(kv) || key < 0 {
		log.Fatalf(context.Background(), "version with key %d does not exist, have:\n%s",
			key, pretty.Sprint(kv))
	}
	return kv[key].Version
}

// Validate makes sure that the keyedVersions are sorted chronologically, that
// their keys correspond to their position in the list, and that no obsolete
// versions (i.e. known to always be active) are present.
func (kv keyedVersions) Validate() error {
	type majorMinor struct {
		major, minor int32
		vs           []keyedVersion
	}
	// byRelease maps major.minor to a slice of versions that were first
	// released right after major.minor. For example, a version 2.1-12 would
	// first be released in 19.1, and would be slotted under 2.1. We'll need
	// this to determine which versions are always active with a binary built
	// from the current SHA.
	var byRelease []majorMinor
	for i, namedVersion := range kv {
		if int(namedVersion.Key) != i {
			return errors.Errorf("version %s should have key %d but has %d",
				namedVersion, i, namedVersion.Key)
		}
		if i > 0 {
			prev := kv[i-1]
			if !prev.Version.Less(namedVersion.Version) {
				return errors.Errorf("version %s must be larger than %s", namedVersion, prev)
			}
		}
		mami := majorMinor{major: namedVersion.Major, minor: namedVersion.Minor}
		n := len(byRelease)
		if n == 0 || byRelease[n-1].major != mami.major || byRelease[n-1].minor != mami.minor {
			// Add new entry to the slice.
			byRelease = append(byRelease, mami)
			n++
		}
		// Add to existing entry.
		byRelease[n-1].vs = append(byRelease[n-1].vs, namedVersion)
	}

	// Iterate through all versions known to be active. For example, if
	//
	//   byRelease = ["2.0", "2.1", "19.1"]
	//
	// then we know that the current release cycle is 19.2, so mixed version
	// clusters are running at least 19.1, so anything slotted under 2.1 (like
	// 2.1-12) and 2.0 is always-on. To avoid interfering with backports, we're
	// a bit more lenient and allow one more release cycle until validation fails.
	// In the above example, we would tolerate 2.1-x but not 2.0-x.
	if n := len(byRelease) - 4; n >= 0 {
		var buf strings.Builder
		for i, mami := range byRelease[:n+1] {
			s := "next release"
			if i+1 < len(byRelease)-1 {
				nextMM := byRelease[i+1]
				s = fmt.Sprintf("%d.%d", nextMM.major, nextMM.minor)
			}
			for _, nv := range mami.vs {
				fmt.Fprintf(&buf, "introduced in %s: %s\n", s, nv.Key)
			}
		}
		mostRecentRelease := byRelease[len(byRelease)-1]
		return errors.Errorf(
			"found versions that are always active because %d.%d is already "+
				"released; these should be removed:\n%s",
			mostRecentRelease.minor, mostRecentRelease.major,
			buf.String(),
		)
	}
	return nil
}
