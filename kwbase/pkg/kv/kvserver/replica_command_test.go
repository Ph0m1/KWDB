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

package kvserver

import (
	"context"
	"encoding/binary"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// Regression test for #38308. Summary: a non-nullable field was added to
// RangeDescriptor which broke splits, merges, and replica changes if the
// cluster had been upgraded from a previous version of kwbase.
func TestRangeDescriptorUpdateProtoChangedAcrossVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Control our own split destiny.
	args := base.TestServerArgs{Knobs: base.TestingKnobs{Store: &StoreTestingKnobs{
		DisableSplitQueue: true,
		DisableMergeQueue: true,
	}}}
	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	bKey := roachpb.Key("b")
	if err := kvDB.AdminSplit(ctx, bKey, bKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}

	// protoVarintField returns an encoded proto field of type varint with the
	// given id.
	protoVarintField := func(fieldID int) []byte {
		var scratch [binary.MaxVarintLen64]byte
		const typ = 0 // varint type field
		tag := uint64(fieldID<<3) | typ
		tagLen := binary.PutUvarint(scratch[:], tag)
		// A proto message is a series of <tag><data> where <tag> is a varint
		// including the field id and the data type and <data> depends on the type.
		buf := append([]byte(nil), scratch[:tagLen]...)
		// The test doesn't care what we use for the field data, so use the tag
		// since the data is a varint and it's already an encoded varint.
		buf = append(buf, scratch[:tagLen]...)
		return buf
	}

	// Update the serialized RangeDescriptor proto for the b to max range to have
	// an unknown proto field. Previously, this would break splits, merges,
	// replica changes. The real regression was a missing field, but an extra
	// unknown field tests the same thing.
	{
		bDescKey := keys.RangeDescriptorKey(roachpb.RKey(bKey))
		bDescKV, err := kvDB.Get(ctx, bDescKey)
		require.NoError(t, err)
		require.NotNil(t, bDescKV.Value, `could not find "b" descriptor`)

		// Update the serialized proto with a new field we don't know about. The
		// proto encoding is just a series of these, so we can do this simply by
		// appending it.
		newBDescBytes, err := bDescKV.Value.GetBytes()
		require.NoError(t, err)
		newBDescBytes = append(newBDescBytes, protoVarintField(9999)...)

		newBDescValue := roachpb.MakeValueFromBytes(newBDescBytes)
		require.NoError(t, kvDB.Put(ctx, bDescKey, &newBDescValue))
	}

	// Verify that splits still work. We could also do a similar thing to test
	// merges and replica changes, but they all go through updateRangeDescriptor
	// so it's unnecessary.
	cKey := roachpb.Key("c")
	if err := kvDB.AdminSplit(ctx, cKey, cKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}
}
