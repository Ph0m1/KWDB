// Copyright 2016 The Cockroach Authors.
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

// Package leasemanager provides functionality for acquiring and managing leases
// via the kv api for use during sqlmigrations.
package leasemanager

import (
	"context"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

// DefaultLeaseDuration is the duration a lease will be acquired for if no
// duration was specified in a LeaseManager's options.
// Exported for testing purposes.
const DefaultLeaseDuration = 1 * time.Minute

// LeaseNotAvailableError indicates that the lease the caller attempted to
// acquire is currently held by a different client.
type LeaseNotAvailableError struct {
	key        roachpb.Key
	expiration hlc.Timestamp
}

func (e *LeaseNotAvailableError) Error() string {
	return fmt.Sprintf("lease %q is not available until at least %s", e.key, e.expiration)
}

// LeaseManager provides functionality for acquiring and managing leases
// via the kv api.
type LeaseManager struct {
	db            *kv.DB
	clock         *hlc.Clock
	clientID      string
	leaseDuration time.Duration
}

// Lease contains the state of a lease on a particular key.
type Lease struct {
	key roachpb.Key
	val struct {
		sem      chan struct{}
		lease    *LeaseVal
		leaseRaw roachpb.Value
	}
}

// Options are used to configure a new LeaseManager.
type Options struct {
	// ClientID must be unique to this LeaseManager instance.
	ClientID      string
	LeaseDuration time.Duration
}

// New allocates a new LeaseManager.
func New(db *kv.DB, clock *hlc.Clock, options Options) *LeaseManager {
	if options.ClientID == "" {
		options.ClientID = uuid.MakeV4().String()
	}
	if options.LeaseDuration <= 0 {
		options.LeaseDuration = DefaultLeaseDuration
	}
	return &LeaseManager{
		db:            db,
		clock:         clock,
		clientID:      options.ClientID,
		leaseDuration: options.LeaseDuration,
	}
}

// AcquireLease attempts to grab a lease on the provided key. Returns a non-nil
// lease object if it was successful, or an error if it failed to acquire the
// lease for any reason.
//
// NB: Acquiring a non-expired lease is allowed if this LeaseManager's clientID
// matches the lease owner's ID. This behavior allows a process to re-grab
// leases without having to wait if it restarts and uses the same ID.
func (m *LeaseManager) AcquireLease(ctx context.Context, key roachpb.Key) (*Lease, error) {
	lease := &Lease{
		key: key,
	}
	lease.val.sem = make(chan struct{}, 1)
	if err := m.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var val LeaseVal
		err := txn.GetProto(ctx, key, &val)
		if err != nil {
			return err
		}
		if !m.leaseAvailable(&val) {
			return &LeaseNotAvailableError{key: key, expiration: val.Expiration}
		}
		lease.val.lease = &LeaseVal{
			Owner:      m.clientID,
			Expiration: m.clock.Now().Add(m.leaseDuration.Nanoseconds(), 0),
		}
		var leaseRaw roachpb.Value
		if err := leaseRaw.SetProto(lease.val.lease); err != nil {
			return err
		}
		if err := txn.Put(ctx, key, &leaseRaw); err != nil {
			return err
		}
		// After using newRaw as an arg to CPut, we're not allowed to modify it.
		// Passing it back to CPut again (which is the whole point of keeping it
		// around) will clear and re-init the checksum, so defensively copy it before
		// we save it.
		lease.val.leaseRaw = roachpb.Value{RawBytes: append([]byte(nil), leaseRaw.RawBytes...)}
		return nil
	}); err != nil {
		return nil, err
	}
	return lease, nil
}

func (m *LeaseManager) leaseAvailable(val *LeaseVal) bool {
	return val.Owner == m.clientID || m.timeRemaining(val) <= 0
}

// TimeRemaining returns the amount of time left on the given lease.
func (m *LeaseManager) TimeRemaining(l *Lease) time.Duration {
	l.val.sem <- struct{}{}
	defer func() { <-l.val.sem }()
	return m.timeRemaining(l.val.lease)
}

func (m *LeaseManager) timeRemaining(val *LeaseVal) time.Duration {
	maxOffset := m.clock.MaxOffset()
	return val.Expiration.GoTime().Sub(m.clock.Now().GoTime()) - maxOffset
}

// ExtendLease attempts to push the expiration time of the lease farther out
// into the future.
func (m *LeaseManager) ExtendLease(ctx context.Context, l *Lease) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.val.sem <- struct{}{}:
	}
	defer func() { <-l.val.sem }()

	if m.timeRemaining(l.val.lease) < 0 {
		return errors.Errorf("can't extend lease that expired at time %s", l.val.lease.Expiration)
	}

	newVal := &LeaseVal{
		Owner:      m.clientID,
		Expiration: m.clock.Now().Add(m.leaseDuration.Nanoseconds(), 0),
	}
	var newRaw roachpb.Value
	if err := newRaw.SetProto(newVal); err != nil {
		return err
	}
	if err := m.db.CPut(ctx, l.key, &newRaw, &l.val.leaseRaw); err != nil {
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			// Something is wrong - immediately expire the local lease state.
			l.val.lease.Expiration = hlc.Timestamp{}
			return errors.Wrapf(err, "local lease state %v out of sync with DB state", l.val.lease)
		}
		return err
	}
	l.val.lease = newVal
	// After using newRaw as an arg to CPut, we're not allowed to modify it.
	// Passing it back to CPut again (which is the whole point of keeping it
	// around) will clear and re-init the checksum, so defensively copy it before
	// we save it.
	l.val.leaseRaw = roachpb.Value{RawBytes: append([]byte(nil), newRaw.RawBytes...)}
	return nil
}

// ReleaseLease attempts to release the given lease so that another process can
// grab it.
func (m *LeaseManager) ReleaseLease(ctx context.Context, l *Lease) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.val.sem <- struct{}{}:
	}
	defer func() { <-l.val.sem }()

	return m.db.CPut(ctx, l.key, nil, &l.val.leaseRaw)
}
