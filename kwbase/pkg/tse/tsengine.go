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

package tse

// #cgo CPPFLAGS: -I../../../kwdbts2/include
// #cgo LDFLAGS: -lkwdbts2 -lcommon  -lstdc++
// #cgo LDFLAGS: -lprotobuf
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
// #include <libkwdbts2.h>
import "C"
import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1<<50 - 1
)

const (
	compressInterval = "ts.compress_interval"
)

// A Error wraps an error returned from a TsEngine operation.
type Error struct {
	msg string
}

func (e Error) Error() string {
	return e.msg
}

// goToCTSSlice converts a go byte slice to a TSSlice. Note that this is
// potentially dangerous as the DBSlice holds a reference to the go
// byte slice memory that the Go GC does not know about. This method
// is only intended for use in converting arguments to C
// functions. The C function must copy any data that it wishes to
// retain once the function returns.
func goToCTSSlice(b []byte) C.TSSlice {
	if len(b) == 0 {
		return C.TSSlice{data: nil, len: 0}
	}
	return C.TSSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

// TsEngineConfig configuration of TsEngine
type TsEngineConfig struct {
	Attrs          roachpb.Attributes
	Dir            string
	ThreadPoolSize int
	TaskQueueSize  int
	BufferPoolSize int
	Settings       *cluster.Settings
	LogCfg         log.Config
	ExtraOptions   []byte
}

// TsQueryInfo the parameter and return value passed by the query
type TsQueryInfo struct {
	Buf      []byte
	ID       int
	UniqueID int
	TimeZone int
	Code     int
	Handle   unsafe.Pointer
	Fetcher  TsFetcher
}

// DedupResult is PutData dedup result
type DedupResult struct {
	DedupRule     int    // Deduplication mode
	DedupRows     int    // The number of inserted data rows affected
	DiscardBitmap []byte // The bitmap of discard data
}

// TsEngine is ts database instance.
type TsEngine struct {
	stopper *stop.Stopper
	cfg     TsEngineConfig
	tdb     *C.TSEngine
}

// TsWALFlushInterval indicates the WAL flush interval of TsEngine
var TsWALFlushInterval = settings.RegisterPublicDurationSetting(
	"ts.wal.flush_interval",
	"ts WAL flush interval in TsEngine. when 0, wal will be flushed on wrote. when -1, WAL is disable.",
	0,
)

// TsWALBufferSize indicates the WAL buffer size of TsEngine
var TsWALBufferSize = settings.RegisterValidatedByteSizeSetting(
	"ts.wal.buffer_size",
	"ts WAL buffer size, default 4Mib",
	4<<20,
	func(v int64) error {
		if v < 4 {
			return errors.Errorf("WAL buffer size must more than 4(Mib)")
		}
		return nil
	},
)

// TsWALFileSize indicates the WAL file size of TsEngine
var TsWALFileSize = settings.RegisterPublicValidatedByteSizeSetting(
	"ts.wal.file_size",
	"ts WAL file size, default 64Mib",
	64<<20,
	func(v int64) error {
		if v < 64<<20 {
			return errors.Errorf("WAL file size must more than 64(Mib)")
		}
		return nil
	},
)

// TsWALFilesInGroup indicates the WAL file num of in one entity group
var TsWALFilesInGroup = settings.RegisterPublicValidatedIntSetting(
	"ts.wal.files_in_group",
	"ts WAL files num of a entity group in TsEngine, default 3",
	3,
	func(v int64) error {
		if v < 3 {
			return errors.Errorf("WAL files num in group must more than 3")
		}
		return nil
	},
)

// TsWALCheckpointInterval indicates the wal checkpoint interval of TsEngine
var TsWALCheckpointInterval = settings.RegisterPublicDurationSetting(
	"ts.wal.checkpoint_interval",
	"ts WAL checkpoint interval in TsEngine",
	time.Minute,
)

// SQLTimeseriesTrace set trace for timeseries.
var SQLTimeseriesTrace = settings.RegisterStringSetting(
	"ts.trace.on_off_list",
	"collection/push switch",
	"",
)

//export isCanceledCtx
func isCanceledCtx(goCtxPtr C.uint64_t) C.bool {
	ctx := *(*context.Context)(unsafe.Pointer(uintptr(goCtxPtr)))
	select {
	case <-ctx.Done():
		return C.bool(true)
	default:
		return C.bool(false)
	}
}

// NewTsEngine new ts engine
func NewTsEngine(
	ctx context.Context, cfg TsEngineConfig, stopper *stop.Stopper, rangeIndex []roachpb.RangeIndex,
) (*TsEngine, error) {
	if cfg.Dir == "" {
		return nil, errors.New("dir must be non-empty")
	}

	r := &TsEngine{
		stopper: stopper,
		cfg:     cfg,
	}

	if err := r.open(rangeIndex); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *TsEngine) open(rangeIndex []roachpb.RangeIndex) error {
	interval := TsWALFlushInterval.Get(&r.cfg.Settings.SV)
	var walLevel uint8
	if interval < 0 {
		walLevel = 0
	} else if interval >= 0 && interval <= 200*time.Millisecond {
		walLevel = 2
	} else {
		walLevel = 1
	}

	walBufferSize := TsWALBufferSize.Get(&r.cfg.Settings.SV) >> 20
	walFileSize := TsWALFileSize.Get(&r.cfg.Settings.SV) >> 20
	walFilesInGroup := TsWALFilesInGroup.Get(&r.cfg.Settings.SV)

	traceLevel := SQLTimeseriesTrace.Get(&r.cfg.Settings.SV)
	optLog := C.TsLogOptions{
		Dir:                       goToTSSlice([]byte(r.cfg.LogCfg.Dir)),
		LogFileMaxSize:            C.long(r.cfg.LogCfg.LogFileMaxSize),
		LogFilesCombinedMaxSize:   C.long(r.cfg.LogCfg.LogFilesCombinedMaxSize),
		LogFileVerbosityThreshold: C.LgSeverity(r.cfg.LogCfg.LogFileVerbosityThreshold),
		Trace_on_off_list:         goToTSSlice([]byte(traceLevel)),
	}

	if len(rangeIndex) == 0 {
		status := C.TSOpen(&r.tdb, goToTSSlice([]byte(r.cfg.Dir)),
			C.TSOptions{
				wal_level:         C.uint8_t(walLevel),
				wal_buffer_size:   C.uint16_t(uint16(walBufferSize)),
				wal_file_size:     C.uint16_t(uint16(walFileSize)),
				wal_file_in_group: C.uint16_t(uint16(walFilesInGroup)),
				extra_options:     goToTSSlice(r.cfg.ExtraOptions),
				thread_pool_size:  C.uint16_t(uint16(r.cfg.ThreadPoolSize)),
				task_queue_size:   C.uint16_t(uint16(r.cfg.TaskQueueSize)),
				buffer_pool_size:  C.uint32_t(uint32(r.cfg.BufferPoolSize)),
				lg_opts:           optLog,
			},
			nil,
			C.uint64_t(0))
		if err := statusToError(status); err != nil {
			return errors.Wrap(err, "could not open tsengine instance")
		}
	} else {
		appliedRangeIndex := make([]C.AppliedRangeIndex, len(rangeIndex))
		for idx, rangeIdx := range rangeIndex {
			appliedRangeIndex[idx] = C.AppliedRangeIndex{
				range_id:      C.uint64_t(rangeIdx.RangeId),
				applied_index: C.uint64_t(rangeIdx.ApplyIndex),
			}
		}

		status := C.TSOpen(&r.tdb, goToTSSlice([]byte(r.cfg.Dir)),
			C.TSOptions{
				wal_level:         C.uint8_t(walLevel),
				wal_buffer_size:   C.uint16_t(uint16(walBufferSize)),
				wal_file_size:     C.uint16_t(uint16(walFileSize)),
				wal_file_in_group: C.uint16_t(uint16(walFilesInGroup)),
				extra_options:     goToTSSlice(r.cfg.ExtraOptions),
				thread_pool_size:  C.uint16_t(uint16(r.cfg.ThreadPoolSize)),
				task_queue_size:   C.uint16_t(uint16(r.cfg.TaskQueueSize)),
				buffer_pool_size:  C.uint32_t(uint32(r.cfg.BufferPoolSize)),
				lg_opts:           optLog,
			},
			&appliedRangeIndex[0],
			C.uint64_t(len(appliedRangeIndex)))
		if err := statusToError(status); err != nil {
			return errors.Wrap(err, "could not open tsengine instance")
		}
	}

	r.manageWAL()
	return nil
}

// CreateTsTable create ts table
func (r *TsEngine) CreateTsTable(tableID uint64, meta []byte, rangeGroups []api.RangeGroup) error {
	nRange := len(rangeGroups)
	cRanges := make([]C.RangeGroup, nRange)
	for i := 0; i < nRange; i++ {
		cRanges[i].range_group_id = C.uint64_t(rangeGroups[i].RangeGroupID)
		cRanges[i].typ = C.int8_t(rangeGroups[i].Type)
	}
	cRangeGroups := C.RangeGroups{
		ranges: (*C.RangeGroup)(unsafe.Pointer(&cRanges[0])),
		len:    C.int32_t(len(cRanges)),
	}
	status := C.TSCreateTsTable(r.tdb, C.TSTableID(tableID), goToTSSlice(meta), cRangeGroups)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not CreateTsTable")
	}
	return nil
}

// GetMetaData get meta from source of the snapshot
func (r *TsEngine) GetMetaData(tableID uint64, rangeGroup api.RangeGroup) ([]byte, error) {
	cRangeGroup := C.RangeGroup{
		range_group_id: C.uint64_t(rangeGroup.RangeGroupID),
	}
	var tableMeta C.TSSlice
	status := C.TSGetMetaData(r.tdb, C.TSTableID(tableID), cRangeGroup, &tableMeta)
	if err := statusToError(status); err != nil {
		return nil, errors.Wrap(err, "could not CreateTsTable")
	}
	defer C.free(unsafe.Pointer(tableMeta.data))
	meta := cSliceToGoBytes(tableMeta)
	return meta, nil
}

// TSIsTsTableExist checks if ts table exists.
func (r *TsEngine) TSIsTsTableExist(tableID uint64) (bool, error) {
	var isExist C.bool
	status := C.TSIsTsTableExist(r.tdb, C.TSTableID(tableID), &isExist)
	if err := statusToError(status); err != nil {
		return false, errors.Wrap(err, "get error")
	}
	return bool(isExist), nil
}

// GetRangeGroups gets all rangeGroups in store.
func (r *TsEngine) GetRangeGroups(tableID uint32) ([]api.RangeGroup, error) {
	cRangeGroups := C.RangeGroups{}
	status := C.TSGetRangeGroups(r.tdb, C.TSTableID(tableID), &cRangeGroups)
	if err := statusToError(status); err != nil {
		log.Errorf(context.TODO(), "could not get range groups from table %v, error: %v", tableID, err)
		return nil, errors.Wrap(err, "could not get range groups")
	}
	cRangesPtr := unsafe.Pointer(cRangeGroups.ranges)
	if cRangesPtr == nil {
		log.Errorf(context.TODO(), "get null ranges from table", tableID)
		return nil, errors.New("get null ranges")
	}
	defer C.free(cRangesPtr)
	nRange := int(cRangeGroups.len)
	cRanges := (*[math.MaxInt16]C.RangeGroup)(cRangesPtr)[0:nRange:nRange]
	rangeGroups := make([]api.RangeGroup, nRange)
	for i := 0; i < nRange; i++ {
		rangeGroups[i].RangeGroupID = api.EntityRangeGroupID(cRanges[i].range_group_id)
		rangeGroups[i].Type = api.ReplicaType(cRanges[i].typ)
	}
	return rangeGroups, nil
}

// UpdateRangeGroup updates RangeGroup messages.
func (r *TsEngine) UpdateRangeGroup(
	tableID uint32, rangeGroups []api.RangeGroup, tsMeta []byte,
) error {
	nRange := len(rangeGroups)
	if len(rangeGroups) == 0 {
		err := &Error{msg: "RangeGroup is nil"}
		return errors.Wrap(err, "could not update range group")
	}
	for i := 0; i < nRange; i++ {
		cRanges := make([]C.RangeGroup, 1)
		cRanges[0].range_group_id = C.uint64_t(rangeGroups[i].RangeGroupID)
		cRanges[0].typ = C.int8_t(rangeGroups[i].Type)
		cRangeGroups := C.RangeGroups{
			ranges: (*C.RangeGroup)(unsafe.Pointer(&cRanges[0])),
			len:    C.int32_t(len(cRanges)),
		}
		status := C.TSUpdateRangeGroup(r.tdb, C.TSTableID(tableID), cRangeGroups)
		log.Errorf(context.TODO(), "tableID: %v, rangeGroups:%v,update RangeGroups", tableID, rangeGroups)
		if err := statusToError(status); err != nil {
			if tsMeta == nil {
				log.Errorf(context.TODO(), "TSUpdateRangeGroupErr: %s", err)
				return err
			}
			log.Errorf(context.TODO(), "tableID: %v, rangeGroups:%v,could not update range group:%v,add it", tableID, rangeGroups, err)
			hasTable, err := r.TSIsTsTableExist(uint64(tableID))
			if err != nil {
				log.Errorf(context.TODO(), "getTableerror: %s", err)
			}
			var temp []api.RangeGroup
			temp = append(temp, rangeGroups[i])
			if hasTable {
				err := r.AddRangeGroup(uint64(tableID), tsMeta, temp)
				if err != nil {
					log.Errorf(context.TODO(), "AddRangeGroup: %s for table %d", err, tableID)
				}
			} else {
				err := r.CreateRangeGroup(uint64(tableID), tsMeta, temp)
				if err != nil {
					log.Errorf(context.TODO(), "CreateRangeGroup: %s for table %d", err, tableID)
				}
			}
			if err != nil {
				return errors.Errorf("Failed to get TS meta : %s", err)
			}
		}
	}
	return nil
}

// CreateRangeGroup creates meta data of RangeGroups.
func (r *TsEngine) CreateRangeGroup(
	tableID uint64, meta []byte, rangeGroups []api.RangeGroup,
) error {
	nRange := len(rangeGroups)
	cRanges := make([]C.RangeGroup, nRange)
	for i := 0; i < nRange; i++ {
		cRanges[i].range_group_id = C.uint64_t(rangeGroups[i].RangeGroupID)
		cRanges[i].typ = C.int8_t(api.ReplicaType_Follower)
	}
	cRangeGroups := C.RangeGroups{
		ranges: (*C.RangeGroup)(unsafe.Pointer(&cRanges[0])),
		len:    C.int32_t(len(cRanges)),
	}
	status := C.TSCreateTsTable(r.tdb, C.TSTableID(tableID), goToTSSlice(meta), cRangeGroups)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not create range group")
	}
	return nil
}

// AddRangeGroup adds RangeGroup.
func (r *TsEngine) AddRangeGroup(tableID uint64, meta []byte, rangeGroups []api.RangeGroup) error {
	nRange := len(rangeGroups)
	cRanges := make([]C.RangeGroup, nRange)
	for i := 0; i < nRange; i++ {
		cRanges[i].range_group_id = C.uint64_t(rangeGroups[i].RangeGroupID)
		cRanges[i].typ = C.int8_t(api.ReplicaType_Follower)
	}
	cRangeGroups := C.RangeGroups{
		ranges: (*C.RangeGroup)(unsafe.Pointer(&cRanges[0])),
		len:    C.int32_t(len(cRanges)),
	}
	status := C.TSCreateRangeGroup(r.tdb, C.TSTableID(tableID), goToTSSlice(meta), cRangeGroups)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not create range group")
	}
	return nil
}

// DropTsTable drop ts table
func (r *TsEngine) DropTsTable(tableID uint64) error {
	status := C.TSDropTsTable(r.tdb, C.TSTableID(tableID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not DropTsTable")
	}
	return nil
}

// AddTSColumn adds column for ts table.
func (r *TsEngine) AddTSColumn(
	tableID uint64, currentTSVersion, newTSVersion uint32, transactionID []byte, colMeta []byte,
) error {
	status := C.TSAddColumn(
		r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])), goToTSSlice(colMeta), C.uint32_t(currentTSVersion), C.uint32_t(newTSVersion))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not AddTsColumn")
	}
	return nil
}

// DropTSColumn drop column for ts table.
func (r *TsEngine) DropTSColumn(
	tableID uint64, currentTSVersion, newTSVersion uint32, transactionID []byte, colMeta []byte,
) error {
	status := C.TSDropColumn(
		r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])), goToTSSlice(colMeta), C.uint32_t(currentTSVersion), C.uint32_t(newTSVersion))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not DropTsColumn")
	}
	return nil
}

// AlterPartitionInterval alter partition interval for ts table.
func (r *TsEngine) AlterPartitionInterval(tableID uint64, partitionInterval uint64) error {
	status := C.TSAlterPartitionInterval(r.tdb, C.TSTableID(tableID), C.uint64_t(partitionInterval))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not AlterPartitionInterval")
	}
	return nil
}

// AlterTSColumnType alter column/tag type of ts table.
func (r *TsEngine) AlterTSColumnType(
	tableID uint64,
	currentTSVersion, newTSVersion uint32,
	transactionID []byte,
	colMeta []byte,
	originColMeta []byte,
) error {
	status := C.TSAlterColumnType(
		r.tdb,
		C.TSTableID(tableID),
		(*C.char)(unsafe.Pointer(&transactionID[0])),
		goToTSSlice(colMeta),
		goToTSSlice(originColMeta),
		C.uint32_t(currentTSVersion),
		C.uint32_t(newTSVersion),
	)
	if err := statusToError(status); err != nil {
		return err
	}
	return nil
}

// PutEntity write in, update tag data and write in ts data
func (r *TsEngine) PutEntity(
	rangeGroupID uint64, tableID uint64, payload [][]byte, tsTxnID uint64,
) error {
	if len(payload) == 0 {
		return errors.New("payload is nul")
	}

	cTsSlice := make([]C.TSSlice, len(payload))
	for i, p := range payload {
		if len(p) == 0 {
			cTsSlice[i].data = nil
			cTsSlice[i].len = 0
		} else {
			dataPtr := C.CBytes(p)
			defer C.free(dataPtr)

			cTsSlice[i].data = (*C.char)(dataPtr)
			cTsSlice[i].len = C.size_t(len(p))
		}
	}
	// mock
	cRangeGroup := C.RangeGroup{
		range_group_id: C.uint64_t(rangeGroupID),
		typ:            C.int8_t(0),
	}
	status := C.TSPutEntity(r.tdb, C.TSTableID(tableID), &cTsSlice[0], (C.size_t)(len(cTsSlice)), cRangeGroup, C.uint64_t(tsTxnID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not PutEntity")
	}
	return nil
}

// PutData write in tag data and write in ts data
func (r *TsEngine) PutData(tableID uint64, payload [][]byte, tsTxnID uint64) (DedupResult, error) {
	if len(payload) == 0 {
		return DedupResult{}, errors.New("payload is nul")
	}

	cTsSlice := make([]C.TSSlice, len(payload))
	for i, p := range payload {
		if len(p) == 0 {
			cTsSlice[i].data = nil
			cTsSlice[i].len = 0
		} else {
			dataPtr := C.CBytes(p)
			defer C.free(dataPtr)

			cTsSlice[i].data = (*C.char)(dataPtr)
			cTsSlice[i].len = C.size_t(len(p))
		}
	}
	// mock
	cRangeGroup := C.RangeGroup{
		range_group_id: C.uint64_t(101),
		typ:            C.int8_t(0),
	}

	var dedupResult C.DedupResult
	status := C.TSPutData(r.tdb, C.TSTableID(tableID), &cTsSlice[0], (C.size_t)(len(cTsSlice)), cRangeGroup, C.uint64_t(tsTxnID), &dedupResult)
	if err := statusToError(status); err != nil {
		return DedupResult{}, errors.Wrap(err, "could not PutData")
	}

	res := DedupResult{
		DedupRule:     int(dedupResult.dedup_rule),
		DedupRows:     int(dedupResult.dedup_rows),
		DiscardBitmap: cSliceToGoBytes(dedupResult.discard_bitmap),
	}
	defer C.free(unsafe.Pointer(dedupResult.discard_bitmap.data))
	return res, nil
}

// TsFetcher collect information in explain analyse
type TsFetcher struct {
	Collected bool
	CFetchers []C.TsFetcher
	Size      int
	Mu        *syncutil.Mutex
}

// TsFetcherStats collect information in explain analyse
type TsFetcherStats struct {
	ProcessorID      int32
	ProcessorName    int8
	RowNum           int64
	StallTime        int64 // time of execute
	BytesRead        int64 // byte of rows
	MaxAllocatedMem  int64 // maximum number of memory
	MaxAllocatedDisk int64 // Maximum number of disk
	OutputRowNum     int64 // row of aggregation
}

// sendDmlMsgToTs call the tsengine dml interface to issue a request and return the result
func (r *TsEngine) tsExecute(
	ctx *context.Context, tp C.EnMqType, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	if len(tsQueryInfo.Buf) == 0 {
		return tsRespInfo, errors.New("query buf is nul")
	}
	var queryInfo C.QueryInfo
	bufC := C.CBytes(tsQueryInfo.Buf)
	defer C.free(unsafe.Pointer(bufC))
	queryInfo.value = bufC
	queryInfo.len = C.uint(len(tsQueryInfo.Buf))
	queryInfo.tp = tp
	queryInfo.id = C.int(tsQueryInfo.ID)
	queryInfo.handle = tsQueryInfo.Handle
	queryInfo.unique_id = C.int(tsQueryInfo.UniqueID)
	queryInfo.time_zone = C.int(tsQueryInfo.TimeZone)
	queryInfo.relation_ctx = C.uint64_t(uintptr(unsafe.Pointer(ctx)))

	// init fetcher of analyse
	var vecFetcher C.VecTsFetcher
	vecFetcher.collected = C.bool(false)
	if tsQueryInfo.Fetcher.Collected {
		vecFetcher.collected = C.bool(true)
		vecFetcher.size = C.int8_t(tsQueryInfo.Fetcher.Size)
		// vecFetcher.TsFetchers = &tsQueryInfo.Fetcher.CFetchers[0]
		vecFetcher.goMutux = C.uint64_t(uintptr(unsafe.Pointer(&tsQueryInfo.Fetcher)))
	} else {
		tsFetchers := make([]C.TsFetcher, 1)
		tsFetchers[0].processor_id = C.int32_t(-1)
		tsQueryInfo.Fetcher.CFetchers = tsFetchers
	}
	var retInfo C.QueryInfo
	retInfo.value = nil
	C.TSExecQuery(r.tdb, &queryInfo, &retInfo, &tsQueryInfo.Fetcher.CFetchers[0], unsafe.Pointer(&vecFetcher))
	fet := tsQueryInfo.Fetcher
	tsRespInfo.Fetcher = fet
	tsRespInfo.ID = int(retInfo.id)
	tsRespInfo.UniqueID = int(retInfo.unique_id)
	tsRespInfo.Handle = unsafe.Pointer(retInfo.handle)
	tsRespInfo.Code = int(retInfo.code)
	if unsafe.Pointer(retInfo.value) != nil {
		tsRespInfo.Buf = C.GoBytes(unsafe.Pointer(retInfo.value), C.int(retInfo.len))
		C.TSFree(unsafe.Pointer(retInfo.value))
	}
	if tsRespInfo.Code > 1 {
		if unsafe.Pointer(retInfo.value) != nil {
			strCode := make([]byte, 5)
			code := tsRespInfo.Code
			for i := 0; i < 5; i++ {
				strCode[i] = byte(((code) & 0x3F) + '0')
				code = code >> 6
			}
			err = pgerror.Newf(string(strCode), string(tsRespInfo.Buf))
		} else {
			err = fmt.Errorf("Error Code: %s", strconv.Itoa(tsRespInfo.Code))
		}
	}

	return tsRespInfo, err
}

func freeTSSlice(cTsSlice []C.TSSlice) {
	for _, slice := range cTsSlice {
		if slice.data != nil {
			C.free(unsafe.Pointer(slice.data))
		}
	}
}

// DeleteEntities delete entity, containing tag data and ts data
func (r *TsEngine) DeleteEntities(
	tableID uint64, rangeGroupID uint64, primaryTags [][]byte, isDrop bool, tsTxnID uint64,
) (uint64, error) {
	if len(primaryTags) == 0 {
		return 0, errors.New("primaryTags is null")
	}

	cTsSlice := make([]C.TSSlice, len(primaryTags))
	defer freeTSSlice(cTsSlice)
	for i, p := range primaryTags {
		if len(p) == 0 {
			cTsSlice[i].data = nil
			cTsSlice[i].len = 0
		} else {
			dataPtr := C.CBytes(p)
			cTsSlice[i].data = (*C.char)(dataPtr)
			cTsSlice[i].len = C.size_t(len(p))
		}
	}

	var delCnt C.uint64_t
	status := C.TsDeleteEntities(r.tdb, C.TSTableID(tableID), &cTsSlice[0], (C.size_t)(len(cTsSlice)),
		C.uint64_t(rangeGroupID), &delCnt, C.uint64_t(tsTxnID))
	if err := statusToError(status); err != nil {
		if isDrop {
			return 0, err
		}
		log.Errorf(context.TODO(), "failed to delete ts entities")
	}
	return uint64(delCnt), nil
}

// DeleteRangeData delete entities data in the range
func (r *TsEngine) DeleteRangeData(
	tableID uint64,
	rangeGroupID uint64,
	beginHash uint64,
	endHash uint64,
	tsSpans []*roachpb.TsSpan,
	tsTxnID uint64,
) (uint64, error) {
	cKwHashIDSpans := C.HashIdSpan{
		begin: C.uint64_t(beginHash),
		end:   C.uint64_t(endHash),
	}

	cTsSpans := make([]C.KwTsSpan, len(tsSpans))
	for i := 0; i < len(tsSpans); i++ {
		cTsSpans[i].begin = C.int64_t(tsSpans[i].TsStart)
		cTsSpans[i].end = C.int64_t(tsSpans[i].TsEnd)
	}
	cKwTsSpans := C.KwTsSpans{
		spans: (*C.KwTsSpan)(unsafe.Pointer(&cTsSpans[0])),
		len:   C.int32_t(len(tsSpans)),
	}

	var delCnt C.uint64_t
	status := C.TsDeleteRangeData(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), cKwHashIDSpans, cKwTsSpans, &delCnt, C.uint64_t(tsTxnID))
	if err := statusToError(status); err != nil {
		return uint64(delCnt), errors.New("Data deletion failed or partially failed")
	}
	return uint64(delCnt), nil
}

// DeleteData delete some one entity data
func (r *TsEngine) DeleteData(
	tableID uint64, rangeGroupID uint64, primaryTag []byte, tsSpans []*roachpb.TsSpan, tsTxnID uint64,
) (uint64, error) {
	if len(primaryTag) == 0 {
		return 0, errors.New("primaryTag is null")
	}

	cTsSlice := C.TSSlice{
		data: (*C.char)(C.CBytes(primaryTag)),
		len:  C.size_t(len(primaryTag)),
	}
	defer C.free(unsafe.Pointer(cTsSlice.data))

	cTsSpans := make([]C.KwTsSpan, len(tsSpans))
	for i := 0; i < len(tsSpans); i++ {
		cTsSpans[i].begin = C.int64_t(tsSpans[i].TsStart)
		cTsSpans[i].end = C.int64_t(tsSpans[i].TsEnd)
	}
	cKwTsSpans := C.KwTsSpans{
		spans: (*C.KwTsSpan)(unsafe.Pointer(&cTsSpans[0])),
		len:   C.int32_t(len(tsSpans)),
	}

	var delCnt C.uint64_t
	status := C.TsDeleteData(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), cTsSlice, cKwTsSpans, &delCnt, C.uint64_t(tsTxnID))
	if err := statusToError(status); err != nil {
		return uint64(delCnt), errors.Wrap(err, "failed to delete ts data")
	}
	return uint64(delCnt), nil
}

// CompressTsTable compress partitions with maximum time<=ts
func (r *TsEngine) CompressTsTable(tableID uint64, ts int64) error {
	status := C.TSCompressTsTable(r.tdb, C.TSTableID(tableID), C.int64_t(ts))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to compress ts table")
	}
	return nil
}

// DeleteExpiredData delete expired data from time partitions that fall completely within the [min_int64, end) interval
func (r *TsEngine) DeleteExpiredData(tableID uint64, _ int64, end int64) error {
	status := C.TSDeleteExpiredData(r.tdb, C.TSTableID(tableID), C.int64_t(end))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to delete expired data")
	}
	return nil
}

// SetupTsFlow send timing execution plan and receive execution results
func (r *TsEngine) SetupTsFlow(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	return r.tsExecute(ctx, C.MQ_TYPE_DML_SETUP, tsQueryInfo)
}

// NextTsFlow drive timing execution plan, receive execution results
func (r *TsEngine) NextTsFlow(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	return r.tsExecute(ctx, C.MQ_TYPE_DML_NEXT, tsQueryInfo)
}

// NextTsFlowPgWire drive timing execution plan, receive execution results
func (r *TsEngine) NextTsFlowPgWire(
	ctx *context.Context, tsQueryInfo TsQueryInfo,
) (tsRespInfo TsQueryInfo, err error) {
	return r.tsExecute(ctx, C.MQ_TYPE_DML_PG_RESULT, tsQueryInfo)
}

// CloseTsFlow close the TS actuator corresponding to the current flow
func (r *TsEngine) CloseTsFlow(ctx *context.Context, tsQueryInfo TsQueryInfo) (err error) {
	_, err = r.tsExecute(ctx, C.MQ_TYPE_DML_CLOSE, tsQueryInfo)
	return err
}

// FlushBuffer flush WALs of all ts tables to files in the node
func (r *TsEngine) FlushBuffer() error {
	status := C.TSFlushBuffer(r.tdb)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to flush WAL buffer")
	}

	return nil
}

// Checkpoint create checkpoint
func (r *TsEngine) Checkpoint() error {
	status := C.TSCreateCheckpoint(r.tdb)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to create WAL checkpoint")
	}

	return nil
}

// DeleteRangeGroup Delete RangeGroup
func (r *TsEngine) DeleteRangeGroup(tableID uint64, rangeGroup api.RangeGroup) error {
	cRangeGroup := C.RangeGroup{
		range_group_id: C.uint64_t(rangeGroup.RangeGroupID),
	}
	status := C.TSDeleteRangeGroup(r.tdb, C.TSTableID(tableID), cRangeGroup)
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to delete range group")
	}
	return nil
}

// CreateSnapshot create snapshot
func (r *TsEngine) CreateSnapshot(
	tableID uint64, rangeGroupID uint64, beginHash uint64, endHash uint64,
) (uint64, error) {
	log.Info(context.TODO(), "create SnapShot, rangeGroupID: %v ", rangeGroupID)
	var snapshotID C.uint64_t
	status := C.TSCreateSnapshot(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID),
		C.uint64_t(beginHash), C.uint64_t(endHash), &snapshotID)
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "failed to create snapshot")
	}
	return uint64(snapshotID), nil
}

// DropSnapshot drops Snapshot.
func (r *TsEngine) DropSnapshot(tableID uint64, rangeGroupID uint64, snapshotID uint64) error {
	status := C.TSDropSnapshot(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(snapshotID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to drop snapshot")
	}
	return nil
}

// GetSnapshotData get data of the snapshot
func (r *TsEngine) GetSnapshotData(
	tableID uint64, rangeGroupID uint64, snapshotID uint64, offset int, limit int,
) ([]byte, int, error) {
	var data C.TSSlice
	var total C.size_t
	status := C.TSGetSnapshotData(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(snapshotID),
		C.size_t(offset), C.size_t(limit), &data, &total)
	if err := statusToError(status); err != nil {
		return nil, 0, errors.Wrap(err, "failed to get snapshot data")
	}
	defer C.free(unsafe.Pointer(data.data))
	return cSliceToGoBytes(data), int(total), nil
}

// InitSnapshotForWrite preparing for writing snapshots
func (r *TsEngine) InitSnapshotForWrite(
	tableID uint64, rangeGroupID uint64, snapshotID uint64, snapshotSize int,
) error {
	status := C.TSInitSnapshotForWrite(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(snapshotID),
		C.size_t(snapshotSize))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to init snapshot for write")
	}
	return nil
}

// WriteSnapshotData write snapshot data
func (r *TsEngine) WriteSnapshotData(
	tableID uint64, rangeGroupID uint64, snapshotID uint64, offset int, data []byte, finished bool,
) error {
	if len(data) == 0 {
		return errors.New("snapshot data is null")
	}

	cTsSlice := C.TSSlice{
		data: (*C.char)(C.CBytes(data)),
		len:  C.size_t(len(data)),
	}
	defer C.free(unsafe.Pointer(cTsSlice.data))

	status := C.TSWriteSnapshotData(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(snapshotID),
		C.size_t(offset), cTsSlice, C.bool(finished))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to write snapshot data")
	}
	return nil
}

// ApplySnapshot apply snapshot
func (r *TsEngine) ApplySnapshot(tableID uint64, rangeGroupID uint64, snapshotID uint64) error {
	status := C.TSEnableSnapshot(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(snapshotID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to apply snapshot")
	}
	return nil
}

// MtrBegin BEGIN a TS mini-transaction
func (r *TsEngine) MtrBegin(
	tableID uint64, rangeGroupID uint64, rangeID uint64, index uint64,
) (uint64, error) {
	var miniTransID C.uint64_t
	status := C.TSMtrBegin(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(rangeID),
		C.uint64_t(index), &miniTransID)
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "failed to BEGIN a TS mini-transaction")
	}
	return uint64(miniTransID), nil
}

// MtrCommit COMMIT a TS mini-transaction
func (r *TsEngine) MtrCommit(tableID uint64, rangeGroupID uint64, miniTransID uint64) error {
	status := C.TSMtrCommit(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(miniTransID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to COMMIT a TS mini-transaction")
	}
	return nil
}

// MtrRollback ROLLBACK a TS mini-transaction
func (r *TsEngine) MtrRollback(tableID uint64, rangeGroupID uint64, miniTransID uint64) error {
	status := C.TSMtrRollback(r.tdb, C.TSTableID(tableID), C.uint64_t(rangeGroupID), C.uint64_t(miniTransID))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to ROLLBACK a TS mini-transaction")
	}
	return nil
}

// TransBegin BEGIN a TS transaction
func (r *TsEngine) TransBegin(tableID uint64, transactionID []byte) error {
	status := C.TSxBegin(r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to BEGIN a TS mini-transaction")
	}
	return nil
}

// TransCommit COMMIT a TS transaction
func (r *TsEngine) TransCommit(tableID uint64, transactionID []byte) error {
	status := C.TSxCommit(r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to COMMIT a TS mini-transaction")
	}
	return nil
}

// TransRollback ROLLBACK a TS transaction
func (r *TsEngine) TransRollback(tableID uint64, transactionID []byte) error {
	status := C.TSxRollback(r.tdb, C.TSTableID(tableID), (*C.char)(unsafe.Pointer(&transactionID[0])))
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "failed to ROLLBACK a TS mini-transaction")
	}
	return nil
}

// TSGetWaitThreadNum is used to get wait thread num from time series engine
func (r *TsEngine) TSGetWaitThreadNum() (uint32, error) {
	var info C.ThreadInfo
	status := C.TSGetWaitThreadNum(r.tdb, unsafe.Pointer(&info))
	if err := statusToError(status); err != nil {
		return 0, errors.Wrap(err, "failed to get wait threads number")
	}

	return uint32(info.wait_threads), nil
}

// SetCompressInterval send compress interval to AE
func SetCompressInterval(interval []byte) {
	C.TSSetClusterSetting(goToTSSlice([]byte(compressInterval)), goToTSSlice(interval))
}

// Close close TsEngine
func (r *TsEngine) Close() {
	status := C.TSClose(r.tdb)
	if err := statusToError(status); err != nil {
		log.Errorf(context.TODO(), "could not close ts engine instance")
	}
}

func (r *TsEngine) manageWAL() {
	ctx := context.Background()
	r.stopper.RunWorker(ctx, func(ctx context.Context) {
		flushTimer := timeutil.NewTimer()
		checkpointTimer := timeutil.NewTimer()

		defer flushTimer.Stop()
		defer checkpointTimer.Stop()

		flushInterval := TsWALFlushInterval.Get(&r.cfg.Settings.SV)
		checkpointInterval := TsWALCheckpointInterval.Get(&r.cfg.Settings.SV)
		flushTimer.Reset(flushInterval)
		checkpointTimer.Reset(checkpointInterval)

		for {
			select {
			case <-r.stopper.ShouldStop():
				return
			case <-flushTimer.C:
				if flushInterval >= 0 && flushInterval <= 200*time.Millisecond {
					continue
				}
				flushTimer.Read = true
				_ = r.FlushBuffer()
				flushTimer.Reset(flushInterval)
			case <-checkpointTimer.C:
				checkpointInterval = TsWALCheckpointInterval.Get(&r.cfg.Settings.SV)
				checkpointTimer.Read = true
				_ = r.Checkpoint()
				checkpointTimer.Reset(checkpointInterval)

				newFlushInterval := TsWALFlushInterval.Get(&r.cfg.Settings.SV)
				if flushInterval != newFlushInterval {
					flushInterval = newFlushInterval
					flushTimer.Read = true
					flushTimer.Reset(flushInterval)
				}
			}
		}
	})
}

func goToTSSlice(b []byte) C.TSSlice {
	if len(b) == 0 {
		return C.TSSlice{data: nil, len: 0}
	}
	return C.TSSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

func goToTSAppliedRangeIndexe(b []byte) C.TSSlice {
	if len(b) == 0 {
		return C.TSSlice{data: nil, len: 0}
	}
	return C.TSSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

func statusToError(s C.TSStatus) error {
	if s.data == nil {
		return nil
	}
	return &Error{msg: cStringToGoString(s)}
}

func cStringToGoString(s C.TSString) string {
	if s.data == nil {
		return ""
	}
	// Reinterpret the string as a slice, then cast to string which does a copy.
	result := string(cSliceToUnsafeGoBytes(C.TSSlice{s.data, s.len}))
	C.free(unsafe.Pointer(s.data))
	return result
}

func cSliceToGoBytes(s C.TSSlice) []byte {
	if s.data == nil {
		return nil
	}
	return gobytes(unsafe.Pointer(s.data), int(s.len))
}

func cSliceToUnsafeGoBytes(s C.TSSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[MaxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

// NewTsFetcher init tsFetcher
func NewTsFetcher(specs []execinfrapb.TSProcessorSpec) []C.TsFetcher {
	i := 0
	tsFetchers := make([]C.TsFetcher, len(specs))
	for j := len(specs) - 1; j >= 0; j-- {
		tsFetchers[i].processor_id = C.int32_t(specs[j].ProcessorID)
		i++
	}
	return tsFetchers
}

// AddStatsList add data to statsList
func AddStatsList(tsFetcher TsFetcher, statss []TsFetcherStats) []TsFetcherStats {
	for i := 0; i < tsFetcher.Size; i++ {
		fetcher := tsFetcher.CFetchers[i]
		if fetcher.row_num > 0 {
			statss[i].RowNum += int64(fetcher.row_num)
		}
		if fetcher.stall_time > 0 {
			statss[i].StallTime += int64(fetcher.stall_time)
		}
		if fetcher.bytes_read > 0 {
			statss[i].BytesRead += int64(fetcher.bytes_read)
		}
		if fetcher.max_allocated_mem > 0 {
			statss[i].MaxAllocatedMem += int64(fetcher.max_allocated_mem)
		}
		if fetcher.max_allocated_disk > 0 {
			statss[i].MaxAllocatedDisk += int64(fetcher.max_allocated_disk)
		}
		if fetcher.max_allocated_disk > 0 {
			statss[i].MaxAllocatedDisk += int64(fetcher.max_allocated_disk)
		}
		if fetcher.output_row_num > 0 {
			statss[i].OutputRowNum += int64(fetcher.output_row_num)
		}
	}
	return statss
}

//export goLock
func goLock(goMutux C.uint64_t) {
	fet := *(*TsFetcher)(unsafe.Pointer(uintptr(goMutux)))
	if fet.Mu != nil {
		fet.Mu.Lock()
	}
}

//export goUnLock
func goUnLock(goMutux C.uint64_t) {
	fet := *(*TsFetcher)(unsafe.Pointer(uintptr(goMutux)))
	if fet.Mu != nil {
		fet.Mu.Unlock()
	}
}
