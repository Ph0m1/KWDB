// Copyright 2019 The Cockroach Authors.
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

package row

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// fkExistenceBatchChecker accumulates foreign key existence checks and sends
// them out as a single kv batch on demand. Checks are accumulated in
// order - the first failing check will be the one that produces an
// error report.
type fkExistenceBatchChecker struct {
	// txn captures the current transaction.
	//
	// TODO(knz): Don't do this. txn objects, like contexts,
	// should not be captured in structs.
	txn *kv.Txn

	// batch is the accumulated batch of existence checks so far.
	batch roachpb.BatchRequest

	// batchIdxToFk maps the index of the check request/response in the kv batch
	// to the fkExistenceCheckBaseHelper that created it.
	batchIdxToFk []*fkExistenceCheckBaseHelper
}

// reset starts a new batch.
func (f *fkExistenceBatchChecker) reset() {
	f.batch.Reset()
	f.batchIdxToFk = f.batchIdxToFk[:0]
}

// addCheck adds a check for the given row and fkExistenceCheckBaseHelper to the batch.
func (f *fkExistenceBatchChecker) addCheck(
	ctx context.Context, row tree.Datums, source *fkExistenceCheckBaseHelper, traceKV bool,
) error {
	span, err := source.spanForValues(row)
	if err != nil {
		return err
	}
	scan := roachpb.ScanRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(span),
	}
	if traceKV {
		log.VEventf(ctx, 2, "FKScan %s", span)
	}
	f.batch.Requests = append(f.batch.Requests, roachpb.RequestUnion{})
	f.batch.Requests[len(f.batch.Requests)-1].MustSetInner(&scan)
	f.batchIdxToFk = append(f.batchIdxToFk, source)
	return nil
}

// runCheck sends the accumulated batch of foreign key checks to kv, given the
// old and new values of the row being modified. Either oldRow or newRow can
// be set to nil in the case of an insert or a delete, respectively.
// A pgcode.ForeignKeyViolation is returned if a foreign key violation
// is detected, corresponding to the first foreign key that was violated in
// order of addition.
func (f *fkExistenceBatchChecker) runCheck(
	ctx context.Context, oldRow tree.Datums, newRow tree.Datums,
) error {
	if len(f.batch.Requests) == 0 {
		return nil
	}
	defer f.reset()

	// Run the batch.
	br, err := f.txn.Send(ctx, f.batch)
	if err != nil {
		return err.GoError()
	}

	// Process the responses.
	fetcher := SpanKVFetcher{}
	for i, resp := range br.Responses {
		fk := f.batchIdxToFk[i]
		fetcher.KVs = resp.GetInner().(*roachpb.ScanResponse).Rows
		if err := fk.rf.StartScanFrom(ctx, &fetcher); err != nil {
			return err
		}

		switch fk.dir {
		case CheckInserts:
			// If we're inserting, then there's a violation if the scan found nothing.
			if fk.rf.kvEnd {
				for valueIdx, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
					fk.valuesScratch[valueIdx] = newRow[fk.ids[colID]]
				}
				return pgerror.Newf(pgcode.ForeignKeyViolation,
					"foreign key violation: value %s not found in %s@%s %s (txn=%s)",
					fk.valuesScratch, fk.searchTable.Name, fk.searchIdx.Name,
					fk.searchIdx.ColumnNames[:fk.prefixLen], f.txn.ID())
			}

		case CheckDeletes:
			// If we're deleting, then there's a violation if the scan found something.
			if !fk.rf.kvEnd {
				if oldRow == nil {
					return pgerror.Newf(pgcode.ForeignKeyViolation,
						"foreign key violation: non-empty columns %s referenced in table %q",
						fk.mutatedIdx.ColumnNames[fk.prefixLen], fk.searchTable.Name)
				}

				for valueIdx, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
					fk.valuesScratch[valueIdx] = oldRow[fk.ids[colID]]
				}
				return pgerror.Newf(pgcode.ForeignKeyViolation,
					"foreign key violation: values %v in columns %s referenced in table %q",
					fk.valuesScratch, fk.mutatedIdx.ColumnNames[:fk.prefixLen], fk.searchTable.Name)
			}

		default:
			return errors.AssertionFailedf("impossible case: fkExistenceCheckBaseHelper has dir=%v", fk.dir)
		}
	}

	return nil
}

// SpanKVFetcher is a kvBatchFetcher that returns a set slice of kvs.
type SpanKVFetcher struct {
	KVs []roachpb.KeyValue
}

// nextBatch implements the kvBatchFetcher interface.
func (f *SpanKVFetcher) nextBatch(
	_ context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, span roachpb.Span, err error) {
	if len(f.KVs) == 0 {
		return false, nil, nil, roachpb.Span{}, nil
	}
	res := f.KVs
	f.KVs = nil
	return true, res, nil, roachpb.Span{}, nil
}

// GetRangesInfo implements the kvBatchFetcher interface.
func (f *SpanKVFetcher) GetRangesInfo() []roachpb.RangeInfo {
	panic(errors.AssertionFailedf("GetRangesInfo() called on SpanKVFetcher"))
}
