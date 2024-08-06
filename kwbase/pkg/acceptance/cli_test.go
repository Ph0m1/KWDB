// Copyright 2016 The Cockroach Authors.
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

package acceptance

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/acceptance/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

const testGlob = "../cli/interactive_tests/test*.tcl"
const containerPath = "/go/src/gitee.com/kwbasedb/kwbase/cli/interactive_tests"

var cmdBase = []string{
	"/usr/bin/env",
	"KWBASE_SKIP_UPDATE_CHECK=1",
	"KWBASE_CRASH_REPORTS=",
	"/bin/bash",
	"-c",
}

func TestDockerCLI(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = []string{"stat", cluster.CockroachBinaryInContainer}
	containerConfig.Env = []string{fmt.Sprintf("PGUSER=%s", security.RootUser)}
	ctx := context.Background()
	if err := testDockerOneShot(ctx, t, "cli_test", containerConfig); err != nil {
		t.Skipf(`TODO(dt): No binary in one-shot container, see #6086: %s`, err)
	}

	paths, err := filepath.Glob(testGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found (%v)", testGlob)
	}

	for _, p := range paths {
		testFile := filepath.Base(p)
		testPath := filepath.Join(containerPath, testFile)
		if strings.Contains(testPath, "disabled") {
			t.Logf("Skipping explicitly disabled test %s", testFile)
			continue
		}
		t.Run(testFile, func(t *testing.T) {
			log.Infof(ctx, "-- starting tests from: %s", testFile)

			// Symlink the logs directory to /logs, which is visible outside of the
			// container and preserved if the test fails. (They don't write to /logs
			// directly because they are often run manually outside of Docker, where
			// /logs is unlikely to exist.)
			cmd := "ln -s /logs logs"

			// We run the expect command using `bash -c "(expect ...)"`.
			//
			// We cannot run `expect` directly, nor `bash -c "expect ..."`,
			// because both cause Expect to become the PID 1 process inside
			// the container. On Unix, orphan processes need to be wait()ed
			// upon by the PID 1 process when they terminate, lest they
			// remain forever in the zombie state. Unfortunately, Expect
			// does not contain code to do this. Bash does.
			cmd += "; (expect"
			if log.V(2) {
				cmd = cmd + " -d"
			}
			cmd = cmd + " -f " + testPath + " " + cluster.CockroachBinaryInContainer + ")"
			containerConfig.Cmd = append(cmdBase, cmd)

			if err := testDockerOneShot(ctx, t, "cli_test", containerConfig); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestDockerUnixSocket verifies that CockroachDB initializes a unix
// socket useable by 'psql', even when the server runs insecurely.
func TestDockerUnixSocket(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = []string{"stat", cluster.CockroachBinaryInContainer}
	ctx := context.Background()

	if err := testDockerOneShot(ctx, t, "cli_test", containerConfig); err != nil {
		t.Skipf(`TODO(dt): No binary in one-shot container, see #6086: %s`, err)
	}

	containerConfig.Env = []string{fmt.Sprintf("PGUSER=%s", security.RootUser)}
	containerConfig.Cmd = append(cmdBase,
		"/mnt/data/psql/test-psql-unix.sh "+cluster.CockroachBinaryInContainer)
	if err := testDockerOneShot(ctx, t, "unix_socket_test", containerConfig); err != nil {
		t.Error(err)
	}
}
