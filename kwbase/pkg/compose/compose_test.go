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

// Package compose contains nightly tests that need docker-compose.
package compose

//import (
//	"flag"
//	"fmt"
//	"os/exec"
//	"path/filepath"
//	"testing"
//	"time"
//)
//
//var (
//	flagEach  = flag.Duration("each", 10*time.Minute, "individual test timeout")
//	flagTests = flag.String("tests", ".", "tests within docker compose to run")
//)
// TODO:fix unit test failed
//func TestComposeCompare(t *testing.T) {
//	const file = "docker-compose"
//	if _, err := exec.LookPath(file); err != nil {
//		t.Skip(err)
//	}
//	cmd := exec.Command(
//		file,
//		"-f", filepath.Join("compare", "docker-compose.yml"),
//		"--no-ansi",
//		"up",
//		"--force-recreate",
//		"--exit-code-from", "test",
//	)
//	cmd.Env = []string{
//		fmt.Sprintf("EACH=%s", *flagEach),
//		fmt.Sprintf("TESTS=%s", *flagTests),
//	}
//	out, err := cmd.CombinedOutput()
//	if err != nil {
//		t.Log(string(out))
//		t.Fatal(err)
//	}
//}
