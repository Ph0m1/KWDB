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

package jobutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/kr/pretty"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// WaitForJobToSucceed waits for the specified job ID to succeed.
func WaitForJobToSucceed(t testing.TB, db *sqlutils.SQLRunner, jobID int64) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusSucceeded)
}

// WaitForJobToPause waits for the specified job ID to be paused.
func WaitForJobToPause(t testing.TB, db *sqlutils.SQLRunner, jobID int64) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusPaused)
}

// WaitForJobToCancel waits for the specified job ID to be in a canceled state.
func WaitForJobToCancel(t testing.TB, db *sqlutils.SQLRunner, jobID int64) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusCanceled)
}

// WaitForJobToRun waits for the specified job ID to be in a running state.
func WaitForJobToRun(t testing.TB, db *sqlutils.SQLRunner, jobID int64) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusRunning)
}

// WaitForJobToFail waits for the specified job ID to be in a failed state.
func WaitForJobToFail(t testing.TB, db *sqlutils.SQLRunner, jobID int64) {
	t.Helper()
	waitForJobToHaveStatus(t, db, jobID, jobs.StatusFailed)
}

func waitForJobToHaveStatus(
	t testing.TB, db *sqlutils.SQLRunner, jobID int64, expectedStatus jobs.Status,
) {
	t.Helper()
	if err := retry.ForDuration(time.Minute*2, func() error {
		var status string
		var payloadBytes []byte
		db.QueryRow(
			t, `SELECT status, payload FROM system.jobs WHERE id = $1`, jobID,
		).Scan(&status, &payloadBytes)
		if jobs.Status(status) == jobs.StatusFailed {
			if expectedStatus == jobs.StatusFailed {
				return nil
			}
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				t.Fatalf("job failed: %s", payload.Error)
			}
			t.Fatalf("job failed")
		}
		if e, a := expectedStatus, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// WaitForJob waits for the specified job ID to terminate.
func WaitForJob(t testing.TB, db *sqlutils.SQLRunner, jobID int64) {
	t.Helper()
	if err := retry.ForDuration(time.Minute*2, func() error {
		var status string
		var payloadBytes []byte
		db.QueryRow(
			t, `SELECT status, payload FROM system.jobs WHERE id = $1`, jobID,
		).Scan(&status, &payloadBytes)
		if jobs.Status(status) == jobs.StatusFailed {
			payload := &jobspb.Payload{}
			if err := protoutil.Unmarshal(payloadBytes, payload); err == nil {
				t.Fatalf("job failed: %s", payload.Error)
			}
			t.Fatalf("job failed")
		}
		if e, a := jobs.StatusSucceeded, jobs.Status(status); e != a {
			return errors.Errorf("expected job status %s, but got %s", e, a)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// RunJob runs the provided job control statement, intializing, notifying and
// closing the chan at the passed pointer (see below for why) and returning the
// jobID and error result. PAUSE JOB and CANCEL JOB are racy in that it's hard
// to guarantee that the job is still running when executing a PAUSE or
// CANCEL--or that the job has even started running. To synchronize, we can
// install a store response filter which does a blocking receive for one of the
// responses used by our job (for example, Export for a BACKUP). Later, when we
// want to guarantee the job is in progress, we do exactly one blocking send.
// When this send completes, we know the job has started, as we've seen one
// expected response. We also know the job has not finished, because we're
// blocking all future responses until we close the channel, and our operation
// is large enough that it will generate more than one of the expected response.
func RunJob(
	t *testing.T,
	db *sqlutils.SQLRunner,
	allowProgressIota *chan struct{},
	ops []string,
	query string,
	args ...interface{},
) (int64, error) {
	*allowProgressIota = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := db.DB.ExecContext(context.TODO(), query, args...)
		errCh <- err
	}()
	select {
	case *allowProgressIota <- struct{}{}:
	case err := <-errCh:
		return 0, errors.Wrapf(err, "query returned before expected: %s", query)
	}
	var jobID int64
	db.QueryRow(t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)
	for _, op := range ops {
		db.Exec(t, fmt.Sprintf("%s JOB %d", op, jobID))
		*allowProgressIota <- struct{}{}
	}
	close(*allowProgressIota)
	return jobID, <-errCh
}

// BulkOpResponseFilter creates a blocking response filter for the responses
// related to bulk IO/backup/restore/import: Export, Import and AddSSTable. See
// discussion on RunJob for where this might be useful.
func BulkOpResponseFilter(allowProgressIota *chan struct{}) storagebase.ReplicaResponseFilter {
	return func(ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
		for _, ru := range br.Responses {
			switch ru.GetInner().(type) {
			case *roachpb.ExportResponse, *roachpb.ImportResponse, *roachpb.AddSSTableResponse:
				<-*allowProgressIota
			}
		}
		return nil
	}
}

func verifySystemJob(
	t testing.TB,
	db *sqlutils.SQLRunner,
	offset int,
	filterType jobspb.Type,
	expectedStatus string,
	expectedRunningStatus string,
	expected jobs.Record,
) error {
	var actual jobs.Record
	var rawDescriptorIDs pq.Int64Array
	var statusString string
	var runningStatus gosql.NullString
	var runningStatusString string
	// We have to query for the nth job created rather than filtering by ID,
	// because job-generating SQL queries (e.g. BACKUP) do not currently return
	// the job ID.
	db.QueryRow(t, `
		SELECT description, user_name, descriptor_ids, status, running_status
		FROM kwdb_internal.jobs WHERE job_type = $1 ORDER BY created LIMIT 1 OFFSET $2`,
		filterType.String(),
		offset,
	).Scan(
		&actual.Description, &actual.Username, &rawDescriptorIDs,
		&statusString, &runningStatus,
	)
	if runningStatus.Valid {
		runningStatusString = runningStatus.String
	}

	for _, id := range rawDescriptorIDs {
		actual.DescriptorIDs = append(actual.DescriptorIDs, sqlbase.ID(id))
	}
	sort.Sort(actual.DescriptorIDs)
	sort.Sort(expected.DescriptorIDs)
	expected.Details = nil
	if e, a := expected, actual; !reflect.DeepEqual(e, a) {
		return errors.Errorf("job %d did not match:\n%s",
			offset, strings.Join(pretty.Diff(e, a), "\n"))
	}

	if expectedStatus != statusString {
		return errors.Errorf("job %d: expected status %v, got %v", offset, expectedStatus, statusString)
	}
	if expectedRunningStatus != "" && expectedRunningStatus != runningStatusString {
		return errors.Errorf("job %d: expected running status %v, got %v",
			offset, expectedRunningStatus, runningStatusString)
	}

	return nil
}

// VerifyRunningSystemJob checks that job records are created as expected
// and is marked as running.
func VerifyRunningSystemJob(
	t testing.TB,
	db *sqlutils.SQLRunner,
	offset int,
	filterType jobspb.Type,
	expectedRunningStatus jobs.RunningStatus,
	expected jobs.Record,
) error {
	return verifySystemJob(t, db, offset, filterType, "running", string(expectedRunningStatus), expected)
}

// VerifySystemJob checks that job records are created as expected.
func VerifySystemJob(
	t testing.TB,
	db *sqlutils.SQLRunner,
	offset int,
	filterType jobspb.Type,
	expectedStatus jobs.Status,
	expected jobs.Record,
) error {
	return verifySystemJob(t, db, offset, filterType, string(expectedStatus), "", expected)
}

// GetJobFormatVersion returns the format version of a schema change job.
// Will fail the test if the jobID does not reference a schema change job.
func GetJobFormatVersion(
	t testing.TB, db *sqlutils.SQLRunner,
) jobspb.SchemaChangeDetailsFormatVersion {
	rows := db.QueryStr(t, "SELECT * FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE' AND description <> 'updating privileges' ORDER BY created DESC LIMIT 1")
	if len(rows) != 1 {
		t.Fatal("expected exactly one row when checking the format version")
	}
	jobID, err := strconv.Atoi(rows[0][0])
	if err != nil {
		t.Fatal(err)
	}

	var payloadBytes []byte
	db.QueryRow(t, `SELECT payload FROM system.jobs WHERE id = $1`, jobID).Scan(&payloadBytes)

	payload := &jobspb.Payload{}
	if err := protoutil.Unmarshal(payloadBytes, payload); err != nil {
		t.Fatal(err)
	}
	// Lease is always nil in 19.2.
	payload.Lease = nil

	details := payload.GetSchemaChange()
	return details.FormatVersion
}

// GetJobID gets a particular job's ID.
func GetJobID(t testing.TB, db *sqlutils.SQLRunner, offset int) int64 {
	var jobID int64
	db.QueryRow(t, `
	SELECT job_id FROM kwdb_internal.jobs ORDER BY created LIMIT 1 OFFSET $1`, offset,
	).Scan(&jobID)
	return jobID
}

// GetLastJobID gets the most recent job's ID.
func GetLastJobID(t testing.TB, db *sqlutils.SQLRunner) int64 {
	var jobID int64
	db.QueryRow(
		t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`,
	).Scan(&jobID)
	return jobID
}

// GetJobProgress loads the Progress message associated with the job.
func GetJobProgress(t *testing.T, db *sqlutils.SQLRunner, jobID int64) *jobspb.Progress {
	ret := &jobspb.Progress{}
	var buf []byte
	db.QueryRow(t, `SELECT progress FROM system.jobs WHERE id = $1`, jobID).Scan(&buf)
	if err := protoutil.Unmarshal(buf, ret); err != nil {
		t.Fatal(err)
	}
	return ret
}
