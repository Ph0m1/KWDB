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

package result

import (
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestEvalResultIsZero(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var p Result
	if !p.IsZero() {
		t.Fatalf("%v unexpectedly non-zero", p)
	}

	v := reflect.ValueOf(&p).Elem()
	for i := 0; i < v.NumField(); i++ {
		func() {
			vf := v.Field(i)
			if vf.CanAddr() {
				vf = vf.Addr()
			}
			switch f := vf.Interface().(type) {
			case *LocalResult:
				f.GossipFirstRange = true
				defer func() { f.GossipFirstRange = false }()
			case *storagepb.ReplicatedEvalResult:
				f.IsLeaseRequest = true
				defer func() { f.IsLeaseRequest = false }()
			case **storagepb.WriteBatch:
				*f = new(storagepb.WriteBatch)
				defer func() { *f = nil }()
			case **storagepb.LogicalOpLog:
				*f = new(storagepb.LogicalOpLog)
				defer func() { *f = nil }()
			case *bool:
				*f = true
				defer func() { *f = false }()
			default:
				tf := v.Type().Field(i)
				t.Fatalf("unknown field %s of type %s on %T", tf.Name, tf.Type, p)
			}

			if p.IsZero() {
				t.Fatalf("%#v unexpectedly zero", p)
			}
		}()

		if !p.IsZero() {
			t.Fatalf("%v unexpectedly non-zero", p)
		}
	}
}

func TestMergeAndDestroy(t *testing.T) {
	var r0, r1, r2 Result
	r1.Local.Metrics = new(Metrics)
	r2.Local.Metrics = new(Metrics)

	r1.Local.Metrics.LeaseRequestSuccess = 7

	r2.Local.Metrics.ResolveAbort = 13
	r2.Local.Metrics.LeaseRequestSuccess = 2

	if err := r0.MergeAndDestroy(r1); err != nil {
		t.Fatal(err)
	}

	if err := r0.MergeAndDestroy(r2); err != nil {
		t.Fatal(err)
	}

	if f, exp := *r1.Local.Metrics, (Metrics{LeaseRequestSuccess: 9, ResolveAbort: 13}); f != exp {
		t.Fatalf("expected %d, got %d", exp, f)
	}
}
