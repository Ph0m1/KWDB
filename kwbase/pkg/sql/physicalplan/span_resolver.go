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

package physicalplan

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan/replicaoracle"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// SpanResolver resolves key spans to their respective ranges and lease holders.
// Used for planning physical execution of distributed SQL queries.
//
// Sample usage for resolving a bunch of spans:
//
// func resolveSpans(
//   ctx context.Context,
//   it *execinfra.SpanResolverIterator,
//   spans ...spanWithDir,
// ) ([][]kv.ReplicaInfo, error) {
//   lr := execinfra.NewSpanResolver(
//     distSender, gossip, nodeDescriptor,
//     execinfra.BinPackingLeaseHolderChoice)
//   it := lr.NewSpanResolverIterator(nil)
//   res := make([][]kv.ReplicaInfo, 0)
//   for _, span := range spans {
//     repls := make([]kv.ReplicaInfo, 0)
//     for it.Seek(ctx, span.Span, span.dir); ; it.Next(ctx) {
//       if !it.Valid() {
//         return nil, it.Error()
//       }
//       repl, err := it.ReplicaInfo(ctx)
//       if err != nil {
//         return nil, err
//       }
//       repls = append(repls, repl)
//       if !it.NeedAnother() {
//         break
//       }
//     }
//     res = append(res, repls)
//   }
//   return res, nil
// }
//
//
type SpanResolver interface {
	// NewSpanResolverIterator creates a new SpanResolverIterator.
	// Txn is used for testing and for determining if follower reads are possible.
	NewSpanResolverIterator(txn *kv.Txn) SpanResolverIterator
}

// SpanResolverIterator is used to iterate over the ranges composing a key span.
type SpanResolverIterator interface {
	// Seek positions the iterator on the start of a span (span.Key or
	// span.EndKey, depending on ScanDir). Note that span.EndKey is exclusive,
	// regardless of scanDir.
	//
	// After calling this, ReplicaInfo() will return information about the range
	// containing the start key of the span (or the end key, if the direction is
	// Descending).
	//
	// NeedAnother() will return true until the iterator is positioned on or after
	// the end of the span.  Possible errors encountered should be checked for
	// with Valid().
	//
	// Seek can be called repeatedly on the same iterator. To make optimal uses of
	// caches, Seek()s should be performed on spans sorted according to the
	// scanDir (if Descending, then the span with the highest keys should be
	// Seek()ed first).
	//
	// scanDir changes the direction in which Next() will advance the iterator.
	Seek(ctx context.Context, span roachpb.Span, scanDir kvcoord.ScanDirection)

	// NeedAnother returns true if the current range is not the last for the span
	// that was last Seek()ed.
	NeedAnother() bool

	// Next advances the iterator to the next range. The next range contains the
	// last range's end key (but it does not necessarily start there, because of
	// asynchronous range splits and caching effects).
	// Possible errors encountered should be checked for with Valid().
	Next(ctx context.Context)

	// Valid returns false if an error was encountered by the last Seek() or Next().
	Valid() bool

	// Error returns any error encountered by the last Seek() or Next().
	Error() error

	// Desc returns the current RangeDescriptor.
	Desc() roachpb.RangeDescriptor

	// ReplicaInfo returns information about the replica that has been picked for
	// the current range.
	// A RangeUnavailableError is returned if there's no information in gossip
	// about any of the replicas.
	ReplicaInfo(ctx context.Context) (kvcoord.ReplicaInfo, error)
}

// spanResolver implements SpanResolver.
type spanResolver struct {
	st            *cluster.Settings
	gossip        *gossip.Gossip
	distSender    *kvcoord.DistSender
	nodeDesc      roachpb.NodeDescriptor
	oracleFactory replicaoracle.OracleFactory
}

var _ SpanResolver = &spanResolver{}

// NewSpanResolver creates a new spanResolver.
func NewSpanResolver(
	st *cluster.Settings,
	distSender *kvcoord.DistSender,
	gossip *gossip.Gossip,
	nodeDesc roachpb.NodeDescriptor,
	rpcCtx *rpc.Context,
	policy replicaoracle.Policy,
) SpanResolver {
	return &spanResolver{
		st:       st,
		nodeDesc: nodeDesc,
		oracleFactory: replicaoracle.NewOracleFactory(policy, replicaoracle.Config{
			Settings:         st,
			Gossip:           gossip,
			NodeDesc:         nodeDesc,
			RPCContext:       rpcCtx,
			LeaseHolderCache: distSender.LeaseHolderCache(),
		}),
		distSender: distSender,
		gossip:     gossip,
	}
}

// spanResolverIterator implements the SpanResolverIterator interface.
type spanResolverIterator struct {
	// it is a wrapper RangeIterator.
	it *kvcoord.RangeIterator
	// gossip is used to resolve NodeIds to addresses and node attributes, used to
	// giving preference to close-by replicas.
	gossip *gossip.Gossip
	// oracle is used to choose a lease holders for ranges when one isn't present
	// in the cache.
	oracle replicaoracle.Oracle

	curSpan roachpb.RSpan
	// dir is the direction set by the last Seek()
	dir kvcoord.ScanDirection

	queryState replicaoracle.QueryState

	err error
}

var _ SpanResolverIterator = &spanResolverIterator{}

// NewSpanResolverIterator creates a new SpanResolverIterator.
func (sr *spanResolver) NewSpanResolverIterator(txn *kv.Txn) SpanResolverIterator {
	return &spanResolverIterator{
		gossip:     sr.gossip,
		it:         kvcoord.NewRangeIterator(sr.distSender),
		oracle:     sr.oracleFactory.Oracle(txn),
		queryState: replicaoracle.MakeQueryState(),
	}
}

// Valid is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Valid() bool {
	return it.err == nil && it.it.Valid()
}

// Error is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.it.Error()
}

// Seek is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Seek(
	ctx context.Context, span roachpb.Span, scanDir kvcoord.ScanDirection,
) {
	var key, endKey roachpb.RKey
	var err error
	if key, err = keys.Addr(span.Key); err != nil {
		it.err = err
		return
	}
	if endKey, err = keys.Addr(span.EndKey); err != nil {
		it.err = err
		return
	}
	oldDir := it.dir
	it.curSpan = roachpb.RSpan{
		Key:    key,
		EndKey: endKey,
	}
	it.dir = scanDir

	var seekKey roachpb.RKey
	if scanDir == kvcoord.Ascending {
		seekKey = it.curSpan.Key
	} else {
		seekKey = it.curSpan.EndKey
	}

	// Check if the start of the span falls within the descriptor on which we're
	// already positioned. If so, and if the direction also corresponds, there's
	// no need to change the underlying iterator's state.
	if it.dir == oldDir && it.it.Valid() {
		reverse := (it.dir == kvcoord.Descending)
		desc := it.it.Desc()
		if (reverse && desc.ContainsKeyInverted(seekKey)) ||
			(!reverse && desc.ContainsKey(seekKey)) {
			if log.V(1) {
				log.Infof(ctx, "not seeking (key=%s); existing descriptor %s", seekKey, desc)
			}
			return
		}
	}
	if log.V(1) {
		log.Infof(ctx, "seeking (key=%s)", seekKey)
	}
	it.it.Seek(ctx, seekKey, scanDir)
}

// Next is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Next(ctx context.Context) {
	if !it.Valid() {
		panic(it.Error())
	}
	it.it.Next(ctx)
}

// NeedAnother is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) NeedAnother() bool {
	return it.it.NeedAnother(it.curSpan)
}

// Desc is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) Desc() roachpb.RangeDescriptor {
	return *it.it.Desc()
}

// ReplicaInfo is part of the SpanResolverIterator interface.
func (it *spanResolverIterator) ReplicaInfo(ctx context.Context) (kvcoord.ReplicaInfo, error) {
	if !it.Valid() {
		panic(it.Error())
	}

	repl, err := it.oracle.ChoosePreferredReplica(
		ctx, *it.it.Desc(), it.queryState)
	if err != nil {
		return kvcoord.ReplicaInfo{}, err
	}
	it.queryState.RangesPerNode[repl.NodeID]++
	it.queryState.AssignedRanges[it.it.Desc().RangeID] = repl
	return repl, nil
}
