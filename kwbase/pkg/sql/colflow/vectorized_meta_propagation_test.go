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

package colflow_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/distsqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

// TestVectorizedMetaPropagation tests whether metadata is correctly propagated
// alongside columnar operators. It sets up the following "flow":
// RowSource -> metadataTestSender -> columnarizer -> noopOperator ->
// -> materializer -> metadataTestReceiver. Metadata propagation is hooked up
// manually from the columnarizer into the materializer similar to how it is
// done in setupVectorizedFlow.
func TestVectorizedMetaPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows := 10
	nCols := 1
	types := sqlbase.OneIntCol

	input := distsqlutils.NewRowBuffer(types, sqlbase.MakeIntRows(nRows, nCols), distsqlutils.RowBufferArgs{})
	mtsSpec := execinfrapb.ProcessorCoreUnion{
		MetadataTestSender: &execinfrapb.MetadataTestSenderSpec{
			ID: uuid.MakeV4().String(),
		},
	}
	mts, err := execinfra.NewMetadataTestSender(
		&flowCtx,
		0,
		input,
		&execinfrapb.PostProcessSpec{},
		nil,
		uuid.MakeV4().String(),
	)
	if err != nil {
		t.Fatal(err)
	}

	col, err := colexec.NewColumnarizer(ctx, testAllocator, &flowCtx, 1, mts)
	if err != nil {
		t.Fatal(err)
	}

	noop := colexec.NewNoop(col)
	mat, err := colexec.NewMaterializer(
		&flowCtx,
		2, /* processorID */
		noop,
		types,
		&execinfrapb.PostProcessSpec{},
		nil, /* output */
		[]execinfrapb.MetadataSource{col},
		nil, /* toClose */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}

	mtr, err := execinfra.NewMetadataTestReceiver(
		&flowCtx,
		3,
		mat,
		&execinfrapb.PostProcessSpec{},
		nil,
		[]string{mtsSpec.MetadataTestSender.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	mtr.Start(ctx)

	rowCount, metaCount := 0, 0
	for {
		row, meta := mtr.Next()
		if row == nil && meta == nil {
			break
		}
		if row != nil {
			rowCount++
		} else if meta.Err != nil {
			t.Fatal(meta.Err)
		} else {
			metaCount++
		}
	}
	if rowCount != nRows {
		t.Fatalf("expected %d rows but %d received", nRows, rowCount)
	}
	if metaCount != nRows+1 {
		// metadataTestSender sends a meta after each row plus an additional one to
		// indicate the last meta.
		t.Fatalf("expected %d meta but %d received", nRows+1, metaCount)
	}
}
