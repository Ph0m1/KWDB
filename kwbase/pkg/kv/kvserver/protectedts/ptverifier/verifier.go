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

package ptverifier

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptpb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

// verifier implements protectedts.Verifier.
type verifier struct {
	db *kv.DB
	s  protectedts.Storage
}

// New returns a new Verifier.
func New(db *kv.DB, s protectedts.Storage) protectedts.Verifier {
	return &verifier{db: db, s: s}
}

// Verify verifies that a record with the provided id is verified.
// If it is not verified this call will perform verification and mark the
// record as verified.
func (v *verifier) Verify(ctx context.Context, id uuid.UUID) error {
	// First we go read the record and note the timestamp at which we read it.
	r, ts, err := getRecordWithTimestamp(ctx, v.s, v.db, id)
	if err != nil {
		return errors.Wrapf(err, "failed to fetch record %s", id)
	}

	if r.Verified { // already verified
		return nil
	}

	b := makeVerificationBatch(r, ts)
	if err := v.db.Run(ctx, &b); err != nil {
		return err
	}

	// Check the responses and synthesize an error if one occurred.
	if err := parseResponse(&b, r); err != nil {
		return err
	}
	// Mark the record as verified.
	return errors.Wrapf(v.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return v.s.MarkVerified(ctx, txn, id)
	}), "failed to mark %v as verified", id)
}

// getRecordWithTimestamp fetches the record with the provided id and returns
// the hlc timestamp at which that read occurred.
func getRecordWithTimestamp(
	ctx context.Context, s protectedts.Storage, db *kv.DB, id uuid.UUID,
) (r *ptpb.Record, readAt hlc.Timestamp, err error) {
	if err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		r, err = s.GetRecord(ctx, txn, id)
		readAt = txn.ReadTimestamp()
		return err
	}); err != nil {
		return nil, hlc.Timestamp{}, err
	}
	return r, readAt, nil
}

func makeVerificationBatch(r *ptpb.Record, aliveAt hlc.Timestamp) kv.Batch {
	// Need to perform validation, build a batch and run it.
	mergedSpans, _ := roachpb.MergeSpans(r.Spans)
	var b kv.Batch
	for _, s := range mergedSpans {
		var req roachpb.AdminVerifyProtectedTimestampRequest
		req.RecordAliveAt = aliveAt
		req.Protected = r.Timestamp
		req.RecordID = r.ID
		req.Key = s.Key
		req.EndKey = s.EndKey
		b.AddRawRequest(&req)
	}
	return b
}

func parseResponse(b *kv.Batch, r *ptpb.Record) error {
	rawResponse := b.RawResponse()
	var failed []roachpb.RangeDescriptor
	for _, r := range rawResponse.Responses {
		resp := r.GetInner().(*roachpb.AdminVerifyProtectedTimestampResponse)
		if len(resp.FailedRanges) == 0 {
			continue
		}
		if len(failed) == 0 {
			failed = resp.FailedRanges
		} else {
			failed = append(failed, resp.FailedRanges...)
		}
	}
	if len(failed) > 0 {
		return errors.Errorf("failed to verify protection %v on %v", r, failed)
	}
	return nil
}
