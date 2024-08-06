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

package tracing

import (
	"testing"

	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

func TestLogTags(t *testing.T) {
	tr := NewTracer()
	shadowTracer := mockTracer{}
	tr.setShadowTracer(&mockTracerManager{}, &shadowTracer)

	l := logtags.SingleTagBuffer("tag1", "val1")
	l = l.Add("tag2", "val2")
	sp1 := tr.StartSpan("foo", Recordable, LogTags(l))
	StartRecording(sp1, SingleNodeRecording)
	sp1.Finish()
	require.NoError(t, TestingCheckRecordedSpans(GetRecording(sp1), `
		span foo:
		  tags: tag1=val1 tag2=val2
	`))
	require.NoError(t, shadowTracer.expectSingleSpanWithTags("tag1", "tag2"))
	shadowTracer.clear()

	RegisterTagRemapping("tag1", "one")
	RegisterTagRemapping("tag2", "two")

	sp2 := tr.StartSpan("bar", Recordable, LogTags(l))
	StartRecording(sp2, SingleNodeRecording)
	sp2.Finish()
	require.NoError(t, TestingCheckRecordedSpans(GetRecording(sp2), `
		span bar:
			tags: one=val1 two=val2
	`))
	require.NoError(t, shadowTracer.expectSingleSpanWithTags("one", "two"))
	shadowTracer.clear()

	sp3 := tr.StartRootSpan("baz", l, RecordableSpan)
	StartRecording(sp3, SingleNodeRecording)
	sp3.Finish()
	require.NoError(t, TestingCheckRecordedSpans(GetRecording(sp3), `
		span baz:
			tags: one=val1 two=val2
	`))
	require.NoError(t, shadowTracer.expectSingleSpanWithTags("one", "two"))
}
