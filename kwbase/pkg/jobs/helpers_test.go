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

package jobs

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

func ResetConstructors() func() {
	old := make(map[jobspb.Type]Constructor)
	for k, v := range constructors {
		old[k] = v
	}
	return func() { constructors = old }
}

// FakeResumer calls optional callbacks during the job lifecycle.
type FakeResumer struct {
	OnResume     func(context.Context, chan<- tree.Datums) error
	FailOrCancel func(context.Context) error
	Success      func() error
	PauseRequest onPauseRequestFunc
}

var _ Resumer = FakeResumer{}

func (d FakeResumer) Resume(
	ctx context.Context, _ interface{}, resultsCh chan<- tree.Datums,
) error {
	if d.OnResume != nil {
		if err := d.OnResume(ctx, resultsCh); err != nil {
			return err
		}
	}
	if d.Success != nil {
		return d.Success()
	}
	return nil
}

func (d FakeResumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	if d.FailOrCancel != nil {
		return d.FailOrCancel(ctx)
	}
	return nil
}

// OnPauseRequestFunc forwards the definition for use in tests.
type OnPauseRequestFunc = onPauseRequestFunc

var _ PauseRequester = FakeResumer{}

func (d FakeResumer) OnPauseRequest(
	ctx context.Context, phs interface{}, txn *kv.Txn, details *jobspb.Progress,
) error {
	if d.PauseRequest == nil {
		return nil
	}
	return d.PauseRequest(ctx, phs, txn, details)
}

// Started is a wrapper around the internal function that moves a job to the
// started state.
func (j *Job) Started(ctx context.Context) error {
	return j.started(ctx)
}

// Created is a wrapper around the internal function that creates the initial
// job state.
func (j *Job) Created(ctx context.Context) error {
	return j.created(ctx)
}

// Paused is a wrapper around the internal function that moves a job to the
// paused state.
func (j *Job) Paused(ctx context.Context) error {
	return j.paused(ctx, nil /* fn */)
}

// Failed is a wrapper around the internal function that moves a job to the
// failed state.
func (j *Job) Failed(ctx context.Context, causingErr error) error {
	return j.failed(ctx, causingErr, nil /* fn */)
}

// Succeeded is a wrapper around the internal function that moves a job to the
// succeeded state.
func (j *Job) Succeeded(ctx context.Context) error {
	return j.succeeded(ctx, nil /* fn */)
}
