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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Result returns the Result field of the given Operation.
func (op Operation) Result() *Result {
	switch o := op.GetValue().(type) {
	case *GetOperation:
		return &o.Result
	case *PutOperation:
		return &o.Result
	case *SplitOperation:
		return &o.Result
	case *MergeOperation:
		return &o.Result
	case *BatchOperation:
		return &o.Result
	case *ClosureTxnOperation:
		return &o.Result
	default:
		panic(errors.AssertionFailedf(`unknown operation: %T %v`, o, o))
	}
}

type steps []Step

func (s steps) After() hlc.Timestamp {
	var ts hlc.Timestamp
	for _, step := range s {
		ts.Forward(step.After)
	}
	return ts
}

func (s Step) String() string {
	var fctx formatCtx
	var buf strings.Builder
	s.format(&buf, fctx)
	return buf.String()
}

type formatCtx struct {
	receiver string
	indent   string
	// TODO(dan): error handling.
}

func (s Step) format(w *strings.Builder, fctx formatCtx) {
	if fctx.receiver != `` {
		panic(`cannot specify receiver in Step.format fctx`)
	}
	fctx.receiver = fmt.Sprintf(`db%d`, s.DBID)
	w.WriteString("\n")
	w.WriteString(fctx.indent)
	s.Op.format(w, fctx)
}

func formatOps(w *strings.Builder, fctx formatCtx, ops []Operation) {
	for _, op := range ops {
		w.WriteString("\n")
		w.WriteString(fctx.indent)
		op.format(w, fctx)
	}
}

func (op Operation) String() string {
	fctx := formatCtx{receiver: `x`, indent: ``}
	var buf strings.Builder
	op.format(&buf, fctx)
	return buf.String()
}

func (op Operation) format(w *strings.Builder, fctx formatCtx) {
	switch o := op.GetValue().(type) {
	case *GetOperation:
		o.format(w, fctx)
	case *PutOperation:
		o.format(w, fctx)
	case *SplitOperation:
		o.format(w, fctx)
	case *MergeOperation:
		o.format(w, fctx)
	case *ChangeReplicasOperation:
		o.format(w, fctx)
	case *BatchOperation:
		newFctx := fctx
		newFctx.indent = fctx.indent + `  `
		newFctx.receiver = `b`
		w.WriteString(`{`)
		o.format(w, newFctx)
		w.WriteString("\n")
		w.WriteString(newFctx.indent)
		w.WriteString(fctx.receiver)
		w.WriteString(`.Run(ctx, b)`)
		o.Result.format(w)
		w.WriteString("\n")
		w.WriteString(fctx.indent)
		w.WriteString(`}`)
	case *ClosureTxnOperation:
		txnName := `txn` + o.TxnID
		newFctx := fctx
		newFctx.indent = fctx.indent + `  `
		newFctx.receiver = txnName
		w.WriteString(fctx.receiver)
		fmt.Fprintf(w, `.Txn(ctx, func(ctx context.Context, %s *kv.Txn) error {`, txnName)
		formatOps(w, newFctx, o.Ops)
		if o.CommitInBatch != nil {
			newFctx.receiver = `b`
			o.CommitInBatch.format(w, newFctx)
			newFctx.receiver = txnName
			w.WriteString("\n")
			w.WriteString(newFctx.indent)
			w.WriteString(newFctx.receiver)
			w.WriteString(`.CommitInBatch(ctx, b)`)
			o.CommitInBatch.Result.format(w)
		}
		w.WriteString("\n")
		w.WriteString(newFctx.indent)
		switch o.Type {
		case ClosureTxnType_Commit:
			w.WriteString(`return nil`)
		case ClosureTxnType_Rollback:
			w.WriteString(`return errors.New("rollback")`)
		default:
			panic(errors.AssertionFailedf(`unknown closure txn type: %s`, o.Type))
		}
		w.WriteString("\n")
		w.WriteString(fctx.indent)
		w.WriteString(`})`)
		o.Result.format(w)
		if o.Txn != nil {
			fmt.Fprintf(w, ` txnpb:(%s)`, o.Txn)
		}
	default:
		fmt.Fprintf(w, "%v", op.GetValue())
	}
}

func (op GetOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.Get(ctx, %s)`, fctx.receiver, roachpb.Key(op.Key))
	switch op.Result.Type {
	case ResultType_Error:
		err := errors.DecodeError(context.TODO(), *op.Result.Err)
		fmt.Fprintf(w, ` // (nil, %s)`, err.Error())
	case ResultType_Value:
		v := `nil`
		if len(op.Result.Value) > 0 {
			value, err := roachpb.Value{RawBytes: op.Result.Value}.GetBytes()
			if err != nil {
				v = fmt.Sprintf(`<err:%s>`, err.Error())
			} else {
				v = `"` + string(value) + `"`
			}
		}
		fmt.Fprintf(w, ` // (%s, nil)`, v)
	}
}

func (op PutOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.Put(ctx, %s, %s)`, fctx.receiver, roachpb.Key(op.Key), op.Value)
	op.Result.format(w)
}

func (op SplitOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.AdminSplit(ctx, %s)`, fctx.receiver, roachpb.Key(op.Key))
	op.Result.format(w)
}

func (op MergeOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.AdminMerge(ctx, %s)`, fctx.receiver, roachpb.Key(op.Key))
	op.Result.format(w)
}

func (op BatchOperation) format(w *strings.Builder, fctx formatCtx) {
	w.WriteString("\n")
	w.WriteString(fctx.indent)
	w.WriteString(`b := &Batch{}`)
	formatOps(w, fctx, op.Ops)
}

func (op ChangeReplicasOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.AdminChangeReplicas(ctx, %s, %s)`, fctx.receiver, roachpb.Key(op.Key), op.Changes)
	op.Result.format(w)
}

func (r Result) format(w *strings.Builder) {
	switch r.Type {
	case ResultType_NoError:
		fmt.Fprintf(w, ` // nil`)
	case ResultType_Error:
		err := errors.DecodeError(context.TODO(), *r.Err)
		fmt.Fprintf(w, ` // %s`, err.Error())
	}
}
