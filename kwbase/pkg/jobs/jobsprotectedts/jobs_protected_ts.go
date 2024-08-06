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

package jobsprotectedts

import (
	"context"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptpb"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptreconcile"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

// MetaType is the value used in the ptpb.Record.MetaType field for records
// associated with jobs.
//
// This value must not be changed as it is used durably in the database.
const MetaType = "jobs"

// MakeStatusFunc returns a function which determines whether the job implied
// with this value of meta should be removed by the reconciler.
func MakeStatusFunc(jr *jobs.Registry) ptreconcile.StatusFunc {
	return func(ctx context.Context, txn *kv.Txn, meta []byte) (shouldRemove bool, _ error) {
		jobID, err := decodeJobID(meta)
		if err != nil {
			return false, err
		}
		j, err := jr.LoadJobWithTxn(ctx, jobID, txn)
		if jobs.HasJobNotFoundError(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		isTerminal := j.CheckTerminalStatus(ctx)
		return isTerminal, nil
	}
}

// MakeRecord makes a protected timestamp record to protect a timestamp on
// behalf of this job.
func MakeRecord(
	id uuid.UUID, jobID int64, tsToProtect hlc.Timestamp, spans []roachpb.Span,
) *ptpb.Record {
	return &ptpb.Record{
		ID:        id,
		Timestamp: tsToProtect,
		Mode:      ptpb.PROTECT_AFTER,
		MetaType:  MetaType,
		Meta:      encodeJobID(jobID),
		Spans:     spans,
	}
}

func encodeJobID(jobID int64) []byte {
	return []byte(strconv.FormatInt(jobID, 10))
}

func decodeJobID(meta []byte) (jobID int64, err error) {
	jobID, err = strconv.ParseInt(string(meta), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to interpret meta %q as bytes", meta)
	}
	return jobID, err
}
