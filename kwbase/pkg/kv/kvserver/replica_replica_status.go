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

package kvserver

import (
	"context"
	"sort"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"github.com/cockroachdb/errors"
)

// A ReplicaStatusCollectResult contains the outcome of a ReplicaStatusCollect call.
type ReplicaStatusCollectResult struct {
	Replica  roachpb.ReplicaDescriptor
	Response CollectReplicaStatusResponse
	Err      error
}

// GetReplicaStatus get replica status
func (r *Replica) GetReplicaStatus(
	ctx context.Context, args roachpb.GetReplicaStatusRequest,
) (roachpb.GetReplicaStatusResponse, *roachpb.Error) {
	results, err := r.RunReplicaStatusCollect(ctx)
	if err != nil {
		return roachpb.GetReplicaStatusResponse{}, roachpb.NewError(err)
	}
	var replStatus []roachpb.ReplicaStatus
	for _, status := range results {
		replStatus = append(replStatus, status.Response.ReplicaStatus)
	}
	return roachpb.GetReplicaStatusResponse{ReplicaStatus: replStatus}, nil
}

// RunReplicaStatusCollect run replica status collect
func (r *Replica) RunReplicaStatusCollect(
	ctx context.Context,
) ([]ReplicaStatusCollectResult, error) {
	// Send a ReplicaStatusCollect

	var orderedReplicas []roachpb.ReplicaDescriptor
	{
		desc := r.Desc()
		localReplica, err := r.GetReplicaDescriptor()
		if err != nil {
			return nil, errors.Wrap(err, "could not get replica descriptor")
		}

		// Move the local replica to the front (which makes it the "master"
		// we're comparing against).
		orderedReplicas = append(orderedReplicas, desc.Replicas().All()...)

		sort.Slice(orderedReplicas, func(i, j int) bool {
			return orderedReplicas[i] == localReplica
		})
	}

	resultCh := make(chan ReplicaStatusCollectResult, len(orderedReplicas))
	var results []ReplicaStatusCollectResult
	var wg sync.WaitGroup

	for _, replica := range orderedReplicas {
		wg.Add(1)
		replica := replica // per-iteration copy for the goroutine
		if err := r.store.Stopper().RunAsyncTask(ctx, "storage.Replica: checking consistency",
			func(ctx context.Context) {
				defer wg.Done()

				resp, err := r.collectStatusFromReplica(ctx, replica)

				if replica.Type != nil {
					if *replica.Type == roachpb.LEARNER {
						resp.ReplicaStatus.Learner = true
					}
				}
				resultCh <- ReplicaStatusCollectResult{
					Replica:  replica,
					Response: resp,
					Err:      err,
				}
			}); err != nil {
			wg.Done()
			// If we can't start tasks, the node is likely draining. Just return the error verbatim.
			return nil, err
		}
	}

	wg.Wait()
	close(resultCh)

	// Collect the remaining results.
	for result := range resultCh {
		results = append(results, result)
	}
	return results, nil
}

func (r *Replica) collectStatusFromReplica(
	ctx context.Context, replica roachpb.ReplicaDescriptor,
) (CollectReplicaStatusResponse, error) {
	conn, err := r.store.cfg.NodeDialer.Dial(ctx, replica.NodeID, rpc.DefaultClass)
	if err != nil {
		return CollectReplicaStatusResponse{},
			errors.Wrapf(err, "could not dial node ID %d", replica.NodeID)
	}
	client := NewPerReplicaClient(conn)
	req := &CollectReplicaStatusRequest{
		StoreRequestHeader: StoreRequestHeader{NodeID: replica.NodeID, StoreID: replica.StoreID},
		RangeID:            r.RangeID,
	}
	resp, err := client.CollectReplicaStatus(ctx, req)
	if err != nil {
		return CollectReplicaStatusResponse{}, err
	}
	return *resp, nil
}
