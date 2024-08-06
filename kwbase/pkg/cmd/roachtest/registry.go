// Copyright 2018 The Cockroach Authors.
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

package main

func registerTests(r *testRegistry) {
	// Helpful shell pipeline to generate the list below:
	//
	// grep -h -E 'func register[^(]+\(.*testRegistry\) {' pkg/cmd/roachtest/*.go | grep -E -o 'register[^(]+' | grep -E -v '^register(Tests|Benchmarks)$' | grep -v '^\w*Bench$' | sort -f | awk '{printf "\t%s(r)\n", $0}'

	registerAcceptance(r)
	registerActiveRecord(r)
	registerAllocator(r)
	registerAlterPK(r)
	registerAutoUpgrade(r)
	registerBackup(r)
	registerCancel(r)
	registerCDC(r)
	registerClearRange(r)
	registerClockJumpTests(r)
	registerClockMonotonicTests(r)
	registerCopy(r)
	registerDecommission(r)
	registerDiskStalledDetection(r)
	registerDjango(r)
	registerDrop(r)
	registerElectionAfterRestart(r)
	registerEncryption(r)
	registerEngineSwitch(r)
	registerFlowable(r)
	registerFollowerReads(r)
	registerGopg(r)
	registerGossip(r)
	registerHibernate(r)
	registerHotSpotSplits(r)
	registerImportTPCC(r)
	registerImportTPCH(r)
	registerInconsistency(r)
	registerIndexes(r)
	registerInterleaved(r)
	registerJepsen(r)
	registerJobsMixedVersions(r)
	registerKV(r)
	registerKVContention(r)
	registerKVQuiescenceDead(r)
	registerKVGracefulDraining(r)
	registerKVScalability(r)
	registerKVSplits(r)
	registerKVRangeLookups(r)
	registerLargeRange(r)
	registerLedger(r)
	registerLibPQ(r)
	registerNamespaceUpgrade(r)
	registerNetwork(r)
	registerPebble(r)
	registerPgjdbc(r)
	registerPgx(r)
	registerPsycopg(r)
	registerQueue(r)
	registerQuitAllNodes(r)
	registerQuitTransfersLeases(r)
	registerRebalanceLoad(r)
	registerReplicaGC(r)
	registerRestart(r)
	registerRestore(r)
	registerRoachmart(r)
	registerScaleData(r)
	registerSchemaChangeBulkIngest(r)
	registerSchemaChangeDuringKV(r)
	registerSchemaChangeIndexTPCC100(r)
	registerSchemaChangeIndexTPCC1000(r)
	registerSchemaChangeDuringTPCC1000(r)
	registerSchemaChangeInvertedIndex(r)
	registerSchemaChangeMixedVersions(r)
	registerSchemaChangeRandomLoad(r)
	registerScrubAllChecksTPCC(r)
	registerScrubIndexOnlyTPCC(r)
	registerSecondaryIndexesMultiVersionCluster(r)
	registerSQLAlchemy(r)
	registerSQLSmith(r)
	registerSyncTest(r)
	registerSysbench(r)
	registerTPCC(r)
	registerTPCDSVec(r)
	registerTPCHVec(r)
	registerKVBench(r)
	registerTypeORM(r)
	registerLoadSplits(r)
	registerVersion(r)
	registerYCSB(r)
	registerTPCHBench(r)
	registerOverload(r)
}

func registerBenchmarks(r *testRegistry) {
	// Helpful shell pipeline to generate the list below:
	//
	// grep -h -E 'func register[^(]+\(.*registry\) {' *.go | grep -E -o 'register[^(]+' | grep -v '^registerTests$' | grep '^\w*Bench$' | sort | awk '{printf "\t%s(r)\n", $0}'

	registerIndexesBench(r)
	registerTPCCBench(r)
	registerKVBench(r)
	registerTPCHBench(r)
}
