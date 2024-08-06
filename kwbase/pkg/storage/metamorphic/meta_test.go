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

package metamorphic

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

var (
	keep    = flag.Bool("keep", false, "keep temp directories after test")
	check   = flag.String("check", "", "run operations in specified file and check output for equality")
	seed    = flag.Int64("seed", 456, "specify seed to use for random number generator")
	opCount = flag.Int("operations", 1000, "number of MVCC operations to generate and run")
)

type testRun struct {
	ctx             context.Context
	t               *testing.T
	seed            int64
	checkFile       string
	restarts        bool
	engineSequences [][]engineImpl
}

type testRunForEngines struct {
	ctx            context.Context
	t              *testing.T
	seed           int64
	restarts       bool
	checkFile      io.Reader
	outputFile     io.Writer
	engineSequence []engineImpl
}

func runMetaTestForEngines(run testRunForEngines) {
	tempDir, cleanup := testutils.TempDir(run.t)
	defer func() {
		if !*keep {
			cleanup()
		}
	}()

	testRunner := metaTestRunner{
		ctx:         run.ctx,
		t:           run.t,
		w:           run.outputFile,
		seed:        run.seed,
		restarts:    run.restarts,
		engineImpls: run.engineSequence,
		path:        filepath.Join(tempDir, "store"),
	}
	fmt.Printf("store path = %s\n", testRunner.path)

	testRunner.init()
	defer testRunner.closeAll()
	if run.checkFile != nil {
		testRunner.parseFileAndRun(run.checkFile)
	} else {
		testRunner.generateAndRun(*opCount)
	}
}

func runMetaTest(run testRun) {
	t := run.t
	outerTempDir, cleanup := testutils.TempDir(run.t)
	defer func() {
		if !*keep {
			cleanup()
		}
	}()

	// The test run with the first engine sequence writes its output to this file.
	// All subsequent engine sequence runs compare their output against this file.
	firstRunOutput := filepath.Join(outerTempDir, "output.meta")
	firstRunExecuted := false
	fmt.Printf("first run output file: %s\n", firstRunOutput)

	for _, engineSequence := range run.engineSequences {
		var engineNames []string
		for _, engineImpl := range engineSequence {
			engineNames = append(engineNames, engineImpl.name)
		}

		t.Run(strings.Join(engineNames, ","), func(t *testing.T) {
			innerTempDir, cleanup := testutils.TempDir(t)
			defer func() {
				if !*keep {
					cleanup()
				}
			}()

			// If this is not the first sequence run and a "check" file was not passed
			// in, use the first run's output file as the check file.
			var checkFileReader io.ReadCloser
			if run.checkFile == "" && firstRunExecuted {
				run.checkFile = firstRunOutput
			}
			if run.checkFile != "" {
				var err error
				checkFileReader, err = os.Open(run.checkFile)
				if err != nil {
					t.Fatal(err)
				}
				defer checkFileReader.Close()
			}

			var outputFileWriter io.WriteCloser
			outputFile := firstRunOutput
			if firstRunExecuted {
				outputFile = filepath.Join(innerTempDir, "output.meta")
			}
			var err error
			outputFileWriter, err = os.Create(outputFile)
			if err != nil {
				t.Fatal(err)
			}
			defer outputFileWriter.Close()
			fmt.Printf("check file = %s\noutput file = %s\n", run.checkFile, outputFile)
			engineRun := testRunForEngines{
				ctx:            run.ctx,
				t:              t,
				seed:           run.seed,
				restarts:       run.restarts,
				checkFile:      checkFileReader,
				outputFile:     outputFileWriter,
				engineSequence: engineSequence,
			}
			runMetaTestForEngines(engineRun)
			firstRunExecuted = true
		})
	}
}

// TestRocksPebbleEquivalence runs the MVCC Metamorphic test suite, and checks
// for matching outputs by the test suite between RocksDB and Pebble.
func TestRocksPebbleEquivalence(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()
	if util.RaceEnabled {
		// This test times out with the race detector enabled.
		return
	}

	// Have one fixed seed, one user-specified seed, and one random seed.
	seeds := []int64{123, *seed, rand.Int63()}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			run := testRun{
				ctx:      ctx,
				t:        t,
				seed:     seed,
				restarts: false,
				engineSequences: [][]engineImpl{
					{engineImplRocksDB},
					{engineImplPebble},
					{engineImplPebbleManySSTs},
					{engineImplPebbleVarOpts},
				},
			}
			runMetaTest(run)
		})
	}
}

// TestRocksPebbleRestarts runs the MVCC Metamorphic test suite with restarts
// enabled, and ensures that the output remains the same across different
// engine sequences with restarts in between.
func TestRocksPebbleRestarts(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()
	if util.RaceEnabled {
		// This test times out with the race detector enabled.
		return
	}

	// Have one fixed seed, one user-specified seed, and one random seed.
	seeds := []int64{123, *seed, rand.Int63()}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			run := testRun{
				ctx:      ctx,
				t:        t,
				seed:     seed,
				restarts: true,
				engineSequences: [][]engineImpl{
					{engineImplRocksDB},
					{engineImplPebble},
					{engineImplRocksDB, engineImplPebble},
					{engineImplRocksDB, engineImplPebbleManySSTs, engineImplPebbleVarOpts},
				},
			}
			runMetaTest(run)
		})
	}
}

// TestRocksPebbleCheck checks whether the output file specified with --check has
// matching behavior across rocks/pebble.
func TestRocksPebbleCheck(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()

	if *check != "" {
		if _, err := os.Stat(*check); os.IsNotExist(err) {
			t.Fatal(err)
		}

		run := testRun{
			ctx:       ctx,
			t:         t,
			checkFile: *check,
			restarts:  true,
			engineSequences: [][]engineImpl{
				{engineImplRocksDB},
				{engineImplPebble},
				{engineImplRocksDB, engineImplPebble},
			},
		}
		runMetaTest(run)
	}
}
