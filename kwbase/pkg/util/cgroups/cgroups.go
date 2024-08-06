// Copyright 2020 The Cockroach Authors.
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

package cgroups

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	cgroupV1MemLimitFilename = "memory.stat"
	cgroupV2MemLimitFilename = "memory.max"
)

// GetMemoryLimit attempts to retrieve the cgroup memory limit for the current
// process
func GetMemoryLimit() (limit int64, warnings string, err error) {
	return getCgroupMem("/")
}

// `root` is set to "/" in production code and exists only for testing.
// cgroup memory limit detection path implemented here as
// /proc/self/cgroup file -> /proc/self/mountinfo mounts -> cgroup version -> version specific limit check
func getCgroupMem(root string) (limit int64, warnings string, err error) {
	path, err := detectMemCntrlPath(filepath.Join(root, "/proc/self/cgroup"))
	if err != nil {
		return 0, "", err
	}

	// no memory controller detected
	if path == "" {
		return 0, "no cgroup memory controller detected", nil
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, "/proc/self/mountinfo"), path)
	if err != nil {
		return 0, "", err
	}

	switch ver {
	case 1:
		limit, warnings, err = detectLimitInV1(filepath.Join(root, mount))
	case 2:
		limit, warnings, err = detectLimitInV2(filepath.Join(root, mount, path))
	default:
		limit, err = 0, fmt.Errorf("detected unknown cgroup version index: %d", ver)
	}

	return limit, warnings, err
}

// Finds memory limit for cgroup V1 via looking in [contoller mount path]/memory.stat
func detectLimitInV1(cRoot string) (limit int64, warnings string, err error) {
	statFilePath := filepath.Join(cRoot, cgroupV1MemLimitFilename)
	stat, err := os.Open(statFilePath)
	if err != nil {
		return 0, "", errors.Wrapf(err, "can't read available memory from cgroup v1 at %s", statFilePath)
	}
	defer func() {
		_ = stat.Close()
	}()

	scanner := bufio.NewScanner(stat)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) != 2 || string(fields[0]) != "hierarchical_memory_limit" {
			continue
		}

		trimmed := string(bytes.TrimSpace(fields[1]))
		limit, err = strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return 0, "", errors.Wrapf(err, "can't read available memory from cgroup v1 at %s", statFilePath)
		}

		return limit, "", nil
	}

	return 0, "", fmt.Errorf("failed to find expected memory limit for cgroup v1 in %s", statFilePath)
}

// Finds memory limit for cgroup V2 via looking into [controller mount path]/[leaf path]/memory.max
// TODO(vladdy): this implementation was based on podman+criu environment. It may cover not
// all the cases when v2 becomes more widely used in container world.
func detectLimitInV2(cRoot string) (limit int64, warnings string, err error) {
	limitFilePath := filepath.Join(cRoot, cgroupV2MemLimitFilename)

	var buf []byte
	if buf, err = ioutil.ReadFile(limitFilePath); err != nil {
		return 0, "", errors.Wrapf(err, "can't read available memory from cgroup v2 at %s", limitFilePath)
	}

	trimmed := string(bytes.TrimSpace(buf))
	if trimmed == "max" {
		return math.MaxInt64, "", nil
	}

	limit, err = strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, "", errors.Wrapf(err, "can't parse available memory from cgroup v2 in %s", limitFilePath)
	}
	return limit, "", nil
}

// The controller is defined via either type `memory` for cgroup v1 or via empty type for cgroup v2,
// where the type is the second field in /proc/[pid]/cgroup file
func detectMemCntrlPath(cgroupFilePath string) (string, error) {
	cgroup, err := os.Open(cgroupFilePath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read memory cgroup from cgroups file: %s", cgroupFilePath)
	}
	defer func() { _ = cgroup.Close() }()

	scanner := bufio.NewScanner(cgroup)
	var unifiedPathIfFound string
	for scanner.Scan() {
		fields := bytes.Split(scanner.Bytes(), []byte{':'})
		if len(fields) != 3 && len(fields) != 5 {
			// The lines should always have three fields, there's something fishy here.
			continue
		}

		f0, f1 := string(fields[0]), string(fields[1])
		// First case if v2, second - v1. We give v2 the priority here.
		// There is also a `hybrid` mode when both  versions are enabled,
		// but no known container solutions support it afaik
		if f0 == "0" && f1 == "" {
			unifiedPathIfFound = string(fields[2])
		} else if f1 == "memory" {
			return string(fields[2]), nil
		}
	}

	return unifiedPathIfFound, nil
}

// Reads /proc/[pid]/mountinfo for cgoup or cgroup2 mount which defines the used version.
// See http://man7.org/linux/man-pages/man5/proc.5.html for `mountinfo` format.
func getCgroupDetails(mountinfoPath string, cRoot string) (string, int, error) {
	info, err := os.Open(mountinfoPath)
	if err != nil {
		return "", 0, errors.Wrapf(err, "failed to read mounts info from file: %s", mountinfoPath)
	}
	defer func() {
		_ = info.Close()
	}()

	scanner := bufio.NewScanner(info)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) < 10 {
			continue
		}

		ver, ok := detectCgroupVersion(fields)
		if ok && (ver == 1 && strings.HasPrefix(string(fields[3]), cRoot)) || ver == 2 {
			return string(fields[4]), ver, nil
		}
	}

	return "", 0, fmt.Errorf("failed to detect cgroup root mount and version")
}

// Return version of cgroup mount for memory controller if found
func detectCgroupVersion(fields [][]byte) (_ int, found bool) {
	if len(fields) < 10 {
		return 0, false
	}

	// Due to strange format there can be optional fields in the middle of the set, starting
	// from the field #7. The end of the fields is marked with "-" field
	var pos = 6
	for pos < len(fields) {
		if bytes.Equal(fields[pos], []byte{'-'}) {
			break
		}

		pos++
	}

	// No optional fields separator found or there is less than 3 fields after it which is wrong
	if (len(fields) - pos - 1) < 3 {
		return 0, false
	}

	pos++

	// Check for memory controller specifically in cgroup v1 (it is listed in super options field),
	// as the limit can't be found if it is not enforced
	if bytes.Equal(fields[pos], []byte("cgroup")) && bytes.Contains(fields[pos+2], []byte("memory")) {
		return 1, true
	} else if bytes.Equal(fields[pos], []byte("cgroup2")) {
		return 2, true
	}

	return 0, false
}
