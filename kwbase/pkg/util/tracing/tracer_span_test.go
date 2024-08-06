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

package tracing

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/require"
)

func TestRecordingString(t *testing.T) {
	tr := NewTracer()
	tr2 := NewTracer()

	root := tr.StartSpan("root", Recordable)
	rootSp := root.(*span)
	StartRecording(root, SnowballRecording)
	root.LogFields(otlog.String(LogMessageField, "root 1"))
	// Hackily fix the timing on the first log message, so that we can check it later.
	rootSp.mu.recordedLogs[0].Timestamp = rootSp.startTime.Add(time.Millisecond)
	// Sleep a bit so that everything that comes afterwards has higher timestamps
	// than the one we just assigned. Otherwise the sorting will be screwed up.
	time.Sleep(10 * time.Millisecond)

	carrier := make(opentracing.HTTPHeadersCarrier)
	err := tr.Inject(root.Context(), opentracing.HTTPHeaders, carrier)
	require.NoError(t, err)
	wireContext, err := tr2.Extract(opentracing.HTTPHeaders, carrier)
	remoteChild := tr2.StartSpan("remote child", opentracing.FollowsFrom(wireContext))
	root.LogFields(otlog.String(LogMessageField, "root 2"))
	remoteChild.LogFields(otlog.String(LogMessageField, "remote child 1"))
	require.NoError(t, err)
	remoteChild.Finish()
	remoteRec := GetRecording(remoteChild)
	err = ImportRemoteSpans(root, remoteRec)
	require.NoError(t, err)
	root.Finish()

	root.LogFields(otlog.String(LogMessageField, "root 3"))

	ch2 := StartChildSpan("local child", root, nil /* logTags */, false /* separateRecording */, 0)
	root.LogFields(otlog.String(LogMessageField, "root 4"))
	ch2.LogFields(otlog.String(LogMessageField, "local child 1"))
	ch2.Finish()

	root.LogFields(otlog.String(LogMessageField, "root 5"))
	root.Finish()

	rec := GetRecording(root)
	// Sanity check that the recording looks like we want. Note that this is not
	// its String() representation; this just list all the spans in order.
	err = TestingCheckRecordedSpans(rec, `
span root:
	tags: sb=1
	event: root 1
	event: root 2
	event: root 3
	event: root 4
	event: root 5
span remote child:
	tags: sb=1
	event: remote child 1
span local child:
	event: local child 1
`)
	require.NoError(t, err)

	recStr := rec.String()
	// Strip the timing info, converting rows like:
	//      0.007ms      0.007ms    event:root 1
	// into:
	//    event:root 1
	re := regexp.MustCompile(`.*s.*s\s\s\s\s`)
	stripped := string(re.ReplaceAll([]byte(recStr), nil))

	exp := `=== operation:root sb:1
event:root 1
    === operation:remote child sb:1
    event:remote child 1
event:root 2
event:root 3
    === operation:local child
    event:local child 1
event:root 4
event:root 5
`
	require.Equal(t, exp, stripped)

	// Check the timing info on the first two lines.
	lines := strings.Split(rec.String(), "\n")
	l, err := parseLine(lines[0])
	require.NoError(t, err)
	require.Equal(t, traceLine{
		timeSinceTraceStart: "0.000ms",
		timeSincePrev:       "0.000ms",
		text:                "=== operation:root sb:1",
	}, l)
	l, err = parseLine(lines[1])
	require.Equal(t, traceLine{
		timeSinceTraceStart: "1.000ms",
		timeSincePrev:       "1.000ms",
		text:                "event:root 1",
	}, l)
	require.NoError(t, err)
}

type traceLine struct {
	timeSinceTraceStart, timeSincePrev string
	text                               string
}

func parseLine(s string) (traceLine, error) {
	// Parse lines like:
	//      0.007ms      0.007ms    event:root 1
	re := regexp.MustCompile(`\s*(.*s)\s*(.*s)\s{4}(.*)`)
	match := re.FindStringSubmatch(s)
	if match == nil {
		return traceLine{}, errors.Newf("line doesn't match: %s", s)
	}
	return traceLine{
		timeSinceTraceStart: match[1],
		timeSincePrev:       match[2],
		text:                match[3],
	}, nil
}
