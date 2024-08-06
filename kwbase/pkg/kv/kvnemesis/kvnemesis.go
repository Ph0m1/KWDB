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

package kvnemesis

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
)

// RunNemesis generates and applies a series of Operations to exercise the KV
// api. It returns a slice of the logical failures encountered.
//
// Ideas for conditions to be added to KV nemesis:
// - Transactions being abandoned by their coordinator.
// - CPuts, and continuing after CPut errors (generally continuing after errors
// is not allowed, but it is allowed after ConditionFailedError as a special
// case).
func RunNemesis(
	ctx context.Context,
	rng *rand.Rand,
	ct ClosedTimestampTargetInterval,
	config GeneratorConfig,
	dbs ...*kv.DB,
) ([]error, error) {
	const concurrency, numSteps = 5, 30

	g, err := MakeGenerator(config, newGetReplicasFn(dbs...))
	if err != nil {
		return nil, err
	}
	a := MakeApplier(dbs...)
	w, err := Watch(ctx, dbs, ct, GeneratorDataSpan())
	if err != nil {
		return nil, err
	}
	defer func() { _ = w.Finish() }()

	var stepsStartedAtomic int64
	stepsByWorker := make([][]Step, concurrency)

	workerFn := func(ctx context.Context, workerIdx int) error {
		workerName := fmt.Sprintf(`%d`, workerIdx)
		var buf strings.Builder
		for atomic.AddInt64(&stepsStartedAtomic, 1) <= numSteps {
			step := g.RandStep(rng)

			recCtx, collect, cancel := tracing.ContextWithRecordingSpan(ctx, "txn step")
			err := a.Apply(recCtx, &step)
			log.VEventf(recCtx, 2, "step: %v", step)
			step.Trace = collect().String()
			cancel()
			if err != nil {
				buf.Reset()
				step.format(&buf, formatCtx{indent: `  ` + workerName + ` ERR `})
				log.Infof(ctx, "error: %+v\n\n%s", err, buf.String())
				return err
			}
			buf.Reset()
			fmt.Fprintf(&buf, "\n  before: %s", step.Before)
			step.format(&buf, formatCtx{indent: `  ` + workerName + ` OP  `})
			fmt.Fprintf(&buf, "\n  after: %s", step.After)
			log.Info(ctx, buf.String())
			stepsByWorker[workerIdx] = append(stepsByWorker[workerIdx], step)
		}
		return nil
	}
	if err := ctxgroup.GroupWorkers(ctx, concurrency, workerFn); err != nil {
		return nil, err
	}

	allSteps := make(steps, 0, numSteps)
	for _, steps := range stepsByWorker {
		allSteps = append(allSteps, steps...)
	}

	// TODO(dan): Also slurp the splits. The meta ranges use expiration based
	// leases, so we can't use RangeFeed/Watcher to do it. Maybe ExportRequest?
	if err := w.WaitForFrontier(ctx, allSteps.After()); err != nil {
		return nil, err
	}
	kvs := w.Finish()
	defer kvs.Close()
	failures := Validate(allSteps, kvs)

	if len(failures) > 0 {
		log.Infof(ctx, "reproduction steps:\n%s", printRepro(stepsByWorker))
		log.Infof(ctx, "kvs (recorded from rangefeed):\n%s", kvs.DebugPrint("  "))

		span := GeneratorDataSpan()
		scanKVs, err := dbs[0].Scan(ctx, span.Key, span.EndKey, -1)
		if err != nil {
			log.Infof(ctx, "could not scan actual latest values: %+v", err)
		} else {
			var kvsBuf strings.Builder
			for _, kv := range scanKVs {
				fmt.Fprintf(&kvsBuf, "  %s %s -> %s\n", kv.Key, kv.Value.Timestamp, kv.Value.PrettyPrint())
			}
			log.Infof(ctx, "kvs (scan of latest values according to kwdb):\n%s", kvsBuf.String())
		}
	}

	return failures, nil
}

func printRepro(stepsByWorker [][]Step) string {
	// TODO(dan): Make this more copy and paste, especially the error handling.
	var buf strings.Builder
	buf.WriteString("g := ctxgroup.WithContext(ctx)\n")
	for _, steps := range stepsByWorker {
		buf.WriteString("g.GoCtx(func(ctx context.Context) error {")
		for _, step := range steps {
			fctx := formatCtx{receiver: fmt.Sprintf(`db%d`, step.DBID), indent: "  "}
			buf.WriteString("\n")
			buf.WriteString(fctx.indent)
			step.Op.format(&buf, fctx)
			buf.WriteString(step.Trace)
			buf.WriteString("\n")
		}
		buf.WriteString("\n  return nil\n")
		buf.WriteString("})\n")
	}
	buf.WriteString("g.Wait()\n")
	return buf.String()
}
