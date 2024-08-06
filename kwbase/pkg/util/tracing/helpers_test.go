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
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

type mockTracer struct {
	spans []*mockSpan
}

var _ opentracing.Tracer = &mockTracer{}

func (m *mockTracer) clear() {
	m.spans = nil
}

// expectSingleStartWithTags checks that there's been a single call to
// StartSpan() since the last clear(), and that the call specified the given tag
// names (amongst possibly more tags).
func (m *mockTracer) expectSingleSpanWithTags(tagNames ...string) error {
	if len(m.spans) != 1 {
		return errors.Newf("expected 1 StartSpan() call, had: %d", len(m.spans))
	}
	s := m.spans[0]
	for _, t := range tagNames {
		if _, ok := s.tags[t]; !ok {
			return errors.Newf("missing tag: %s", t)
		}
	}
	return nil
}

func (m *mockTracer) StartSpan(
	operationName string, opts ...opentracing.StartSpanOption,
) opentracing.Span {
	var opt opentracing.StartSpanOptions
	for _, o := range opts {
		o.Apply(&opt)
	}
	s := &mockSpan{
		tags: make(opentracing.Tags),
	}
	if opt.Tags != nil {
		s.tags = opt.Tags
	}
	m.spans = append(m.spans, s)
	return s
}

func (m *mockTracer) Inject(
	sm opentracing.SpanContext, format interface{}, carrier interface{},
) error {
	panic("unimplemented")
}

func (m *mockTracer) Extract(
	format interface{}, carrier interface{},
) (opentracing.SpanContext, error) {
	panic("unimplemented")
}

type mockTracerManager struct{}

var _ shadowTracerManager = &mockTracerManager{}

func (m *mockTracerManager) Name() string {
	return "mock"
}

func (m *mockTracerManager) Close(tr opentracing.Tracer) {}

type mockSpan struct {
	tags opentracing.Tags
}

var _ opentracing.Span = &mockSpan{}

func (m *mockSpan) Finish() {}

func (m *mockSpan) FinishWithOptions(opts opentracing.FinishOptions) {
	panic("unimplemented")
}

func (m *mockSpan) Context() opentracing.SpanContext {
	panic("unimplemented")
}

func (m *mockSpan) SetOperationName(operationName string) opentracing.Span {
	panic("unimplemented")
}

func (m *mockSpan) SetTag(key string, value interface{}) opentracing.Span {
	m.tags[key] = value
	return m
}

func (m *mockSpan) LogFields(fields ...log.Field) {
	panic("unimplemented")
}

func (m *mockSpan) LogKV(alternatingKeyValues ...interface{}) {
	panic("unimplemented")
}

func (m *mockSpan) SetBaggageItem(restrictedKey, value string) opentracing.Span {
	panic("unimplemented")
}

func (m *mockSpan) BaggageItem(restrictedKey string) string {
	panic("unimplemented")
}

func (m *mockSpan) Tracer() opentracing.Tracer {
	panic("unimplemented")
}

func (m *mockSpan) LogEvent(event string) {
	panic("unimplemented")
}

func (m *mockSpan) LogEventWithPayload(event string, payload interface{}) {
	panic("unimplemented")
}

func (m *mockSpan) Log(data opentracing.LogData) {
	panic("unimplemented")
}
