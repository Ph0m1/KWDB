// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/lib/pq/oid"
	"github.com/paulsmith/gogeos/geos"
)

// DirectInsertTable is related struct of ts tables in insert_direct
type DirectInsertTable struct {
	DbID, TabID uint32
	ColsDesc    []sqlbase.ColumnDescriptor
	Tname       *tree.TableName
	Desc        tree.NameList
}

// DirectInsert is related struct in insert_direct
type DirectInsert struct {
	RowNum, ColNum             int
	IDMap, PosMap              []int
	ColIndexs                  map[int]int
	PArgs                      execbuilder.PayloadArgs
	PrettyCols, PrimaryTagCols []*sqlbase.ColumnDescriptor
	Dcs                        []sqlbase.ColumnDescriptor
	PayloadNodeMap             map[int]*sqlbase.PayloadForDistTSInsert
	InputValues                []tree.Datums
}

// GetInputValues performs column type conversion and length checking
func GetInputValues(
	ptCtx tree.ParseTimeContext,
	cols *[]sqlbase.ColumnDescriptor,
	di DirectInsert,
	stmts parser.Statements,
) ([]tree.Datums, error) {
	inputValues := make([]tree.Datums, di.RowNum)
	// Apply for continuous memory space at once and allocate it to 2D slices
	preSlice := make([]tree.Datum, di.RowNum*di.ColNum)
	for i := 0; i < di.RowNum; i++ {
		inputValues[i], preSlice = preSlice[:di.ColNum:di.ColNum], preSlice[di.ColNum:]
	}
	var err error
	for row := range inputValues {
		for i := 0; i < len(di.IDMap); i++ {
			// col position in raw cols slice
			colPos := di.IDMap[i]
			// col's metadata
			column := (*cols)[colPos]
			// col position in insert cols slice
			col := di.PosMap[i]
			rawValue := stmts[0].Insertdirectstmt.InsertValues[col+row*di.ColNum]
			valueType := stmts[0].Insertdirectstmt.ValuesType[col+row*di.ColNum]
			if valueType != parser.STRINGTYPE && rawValue == "" {
				goto NullExec
			}
			switch column.Type.Oid() {
			case oid.T_timestamptz:
				var dVal *tree.DInt
				if valueType == parser.STRINGTYPE {
					t, err := tree.ParseDTimestampTZ(ptCtx, rawValue, tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
					if err != nil {
						return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
					}
					dVal = tree.NewDInt(tree.DInt(t.UnixMilli()))
					if *dVal < tree.TsMinTimestamp || *dVal > tree.TsMaxTimestamp {
						return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"value '%s' out of range for type %s", rawValue, column.Type.SQLString())
					}
				} else {
					if rawValue == "now" {
						currentTime := timeutil.Now().UnixNano() / int64(time.Millisecond)
						inputValues[row][col] = tree.NewDInt(tree.DInt(currentTime))
						continue
					}
					in, err2 := strconv.ParseInt(rawValue, 10, 64)
					if err2 != nil {
						if strings.Contains(err2.Error(), "out of range") {
							return nil, err2
						}
						return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
					}
					dVal = (*tree.DInt)(&in)
					// consistent with the timestamp range supported by savedata
					if *dVal < tree.TsMinTimestamp || *dVal > tree.TsMaxTimestamp {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
							"integer \"%s\" out of range for type %s", rawValue, column.Type.SQLString())
					}
				}
				inputValues[row][col] = dVal
				continue
			case oid.T_timestamp:
				var dVal *tree.DInt
				if valueType == parser.STRINGTYPE {
					t, err := tree.ParseDTimestamp(nil, rawValue, tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
					if err != nil {
						return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
					}
					dVal = tree.NewDInt(tree.DInt(t.UnixMilli()))
					if *dVal < tree.TsMinTimestamp || *dVal > tree.TsMaxTimestamp {
						return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"value '%s' out of range for type %s", rawValue, column.Type.SQLString())
					}
				} else {
					if rawValue == "now" {
						currentTime := timeutil.Now().UnixNano() / int64(time.Millisecond)
						inputValues[row][col] = tree.NewDInt(tree.DInt(currentTime))
						continue
					}
					in, err2 := strconv.ParseInt(rawValue, 10, 64)
					if err2 != nil {
						if strings.Contains(err2.Error(), "out of range") {
							return nil, err2
						}
						return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
					}
					dVal = (*tree.DInt)(&in)
					// consistent with the timestamp range supported by savedata
					if *dVal < tree.TsMinTimestamp || *dVal > tree.TsMaxTimestamp {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
							"integer \"%s\" out of range for type %s", rawValue, column.Type.SQLString())
					}
				}
				inputValues[row][col] = dVal
				continue
			case oid.T_int8:
				if valueType == parser.STRINGTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				in, err := strconv.Atoi(rawValue)
				if err != nil {
					dat, err := parserString2Int(rawValue, err, column)
					if err != nil {
						if valueType == parser.NORMALTYPE {
							return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
						}
						return nil, err
					}
					if dat != nil {
						inputValues[row][col] = dat
						continue
					}
				}
				if in < math.MinInt64 || in > math.MaxInt64 {
					err = pgerror.Newf(pgcode.NumericValueOutOfRange,
						"integer \"%d\" out of range for type %s", in, column.Type.SQLString())
					return nil, err
				}
				d := tree.DInt(in)
				inputValues[row][col] = &d
				continue
			case oid.T_int4:
				if valueType == parser.STRINGTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				in, err := strconv.Atoi(rawValue)
				if err != nil {
					dat, err := parserString2Int(rawValue, err, column)
					if err != nil {
						if valueType == parser.NORMALTYPE {
							return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
						}
						return nil, err
					}
					if dat != nil {
						inputValues[row][col] = dat
						continue
					}
				}
				if in < math.MinInt32 || in > math.MaxInt32 {
					err = pgerror.Newf(pgcode.NumericValueOutOfRange,
						"integer \"%d\" out of range for type %s", in, column.Type.SQLString())
					return nil, err
				}
				d := tree.DInt(in)
				inputValues[row][col] = &d
				continue
			case oid.T_int2:
				if valueType == parser.STRINGTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				in, err := strconv.Atoi(rawValue)
				if err != nil {
					dat, err := parserString2Int(rawValue, err, column)
					if err != nil {
						if valueType == parser.NORMALTYPE {
							return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
						}
						return nil, err
					}
					if dat != nil {
						inputValues[row][col] = dat
						continue
					}
				}
				if in < math.MinInt16 || in > math.MaxInt16 {
					err = pgerror.Newf(pgcode.NumericValueOutOfRange,
						"integer \"%d\" out of range for type %s", in, column.Type.SQLString())
					return nil, err
				}
				d := tree.DInt(in)
				inputValues[row][col] = &d
				continue
			case oid.T_cstring, oid.T_char:
				if valueType == parser.NUMTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				if valueType == parser.BYTETYPE {
					// convert the value of the input bytes type into a string
					rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
				}
				inputValues[row][col] = tree.NewDString(rawValue)
				continue
			case oid.T_text, oid.T_bpchar, oid.T_varchar:
				if valueType == parser.NUMTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				if valueType == parser.BYTETYPE {
					// Convert the value of the input bytes type into a string
					rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
				}
				// string(n)/char(n)/varchar(n) calculate length by byte
				if column.Type.Width() > 0 && len(rawValue) > int(column.Type.Width()) {
					return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
						"value '%s' too long for type %s", rawValue, column.Type.SQLString())
				}
				inputValues[row][col] = tree.NewDString(rawValue)
				continue
			// NCHAR or NVARCHAR
			case oid.Oid(91004), oid.Oid(91002):
				if valueType == parser.NUMTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				if valueType == parser.BYTETYPE {
					// Convert the value of the input bytes type into a string
					rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
				}
				// nchar(n)/nvarchar(n) calculate length by character
				if column.Type.Width() > 0 && utf8.RuneCountInString(rawValue) > int(column.Type.Width()) {
					return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
						"value '%s' too long for type %s", rawValue, column.Type.SQLString())
				}
				inputValues[row][col] = tree.NewDString(rawValue)
				continue
			case oid.T_bytea, oid.T_varbytea:
				if valueType == parser.NUMTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				v, err := tree.ParseDByte(rawValue)
				if err != nil {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				// bytes(n)/varbytes(n) calculate length by byte
				if column.Type.Width() > 0 && len(rawValue) > int(column.Type.Width()) {
					return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
						"value '%s' too long for type %s", rawValue, column.Type.SQLString())
				}
				inputValues[row][col] = v
				continue
			case oid.T_float4:
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				if valueType == parser.STRINGTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				var in float64
				in, err = strconv.ParseFloat(rawValue, 32)
				if err != nil {
					if strings.Contains(err.Error(), "out of range") {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
							"float \"%s\" out of range for type %s", rawValue, column.Type.SQLString())
					}
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				inputValues[row][col] = tree.NewDFloat(tree.DFloat(in))
				continue
			case oid.T_float8:
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				if valueType == parser.STRINGTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				var in float64
				in, err = strconv.ParseFloat(rawValue, 64)
				if err != nil {
					if strings.Contains(err.Error(), "out of range") {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
							"float \"%s\" out of range for type %s", rawValue, column.Type.SQLString())
					}
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				inputValues[row][col] = tree.NewDFloat(tree.DFloat(in))
			case oid.T_bool:
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				inputValues[row][col], err = tree.ParseDBool(rawValue)
				if err != nil {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
			case types.T_geometry:
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				if valueType == parser.NUMTYPE {
					return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
				}
				_, err := geos.FromWKT(rawValue)
				if err != nil {
					if strings.Contains(err.Error(), "load error") {
						return nil, err
					}
					return nil, pgerror.Newf(pgcode.DataException, "value '%s' is invalid for type %s", rawValue, column.Type.SQLString())
				}
				inputValues[row][col] = tree.NewDString(rawValue)
			default:
				return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)

			}
			if err != nil {
				return nil, err
			}
			continue
		NullExec:
			if !column.IsNullable() {
				return nil, sqlbase.NewNonNullViolationError(column.Name)
			}
			// attempting to insert a NULL value when no value is specified
			inputValues[row][col] = tree.DNull
		}
	}
	return inputValues, nil
}

func parserString2Int(
	rawValue string, err error, column sqlbase.ColumnDescriptor,
) (tree.Datum, error) {
	if strings.Contains(err.Error(), "out of range") {
		err = pgerror.New(pgcode.NumericValueOutOfRange, "numeric constant out of int64 range")
		return nil, err
	}
	if rawValue == "true" {
		dat := tree.NewDInt(tree.DInt(1))
		return dat, nil
	} else if rawValue == "false" {
		dat := tree.NewDInt(tree.DInt(0))
		return dat, nil
	}
	err = tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
	return nil, err
}

// BuildPayload is used to BuildPayloadForTsInsert
func BuildPayload(
	EvalContext *tree.EvalContext, priTagRowIdx []int, di *DirectInsert, dit DirectInsertTable,
) error {
	payload, _, err := execbuilder.BuildPayloadForTsInsert(
		EvalContext,
		EvalContext.Txn,
		di.InputValues,
		priTagRowIdx,
		di.PrettyCols,
		di.ColIndexs,
		di.PArgs,
		dit.DbID,
		dit.TabID,
	)
	if err != nil {
		return err
	}
	hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
	primaryTagKey := sqlbase.MakeTsPrimaryTagKey(sqlbase.ID(dit.TabID), hashPoints)
	BuildPerNodePayloads(di.PayloadNodeMap, []roachpb.NodeID{1}, payload, priTagRowIdx, primaryTagKey)
	return nil
}

// BuildPreparePayload is used to BuildPayloadForTsInsert
func BuildPreparePayload(
	EvalContext *tree.EvalContext,
	inputValues [][][]byte,
	priTagRowIdx []int,
	di *DirectInsert,
	dit DirectInsertTable,
	qargs [][]byte,
) error {
	payload, _, err := execbuilder.BuildPreparePayloadForTsInsert(
		EvalContext,
		EvalContext.Txn,
		inputValues,
		priTagRowIdx,
		di.PrettyCols,
		di.ColIndexs,
		di.PArgs,
		dit.DbID,
		dit.TabID,
		qargs,
		di.ColNum,
	)
	if err != nil {
		return err
	}
	hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
	primaryTagKey := sqlbase.MakeTsPrimaryTagKey(sqlbase.ID(dit.TabID), hashPoints)
	BuildPerNodePayloads(di.PayloadNodeMap, []roachpb.NodeID{1}, payload, priTagRowIdx, primaryTagKey)
	return nil
}

// BuildPerNodePayloads is used to PerNodePayloads
func BuildPerNodePayloads(
	payloadNodeMap map[int]*sqlbase.PayloadForDistTSInsert,
	nodeID []roachpb.NodeID,
	payload []byte,
	priTagRowIdx []int,
	primaryTagKey roachpb.Key,
) {
	if val, ok := payloadNodeMap[int(nodeID[0])]; ok {
		val.PerNodePayloads = append(val.PerNodePayloads, &sqlbase.SinglePayloadInfo{
			Payload:       payload,
			RowNum:        uint32(len(priTagRowIdx)),
			PrimaryTagKey: primaryTagKey,
		})
	} else {
		rowVal := &sqlbase.PayloadForDistTSInsert{
			NodeID: nodeID[0],
			PerNodePayloads: []*sqlbase.SinglePayloadInfo{{
				Payload:       payload,
				RowNum:        uint32(len(priTagRowIdx)),
				PrimaryTagKey: primaryTagKey,
			}}}
		payloadNodeMap[int(nodeID[0])] = rowVal
	}
}

// NumofInsertDirect id used to calculate colNum, insertLength, rowNum
func NumofInsertDirect(
	ins *tree.Insert, colsDesc []sqlbase.ColumnDescriptor, stmts parser.Statements, di *DirectInsert,
) int {
	// insert number of columns
	if ins.Columns != nil && len(ins.Columns) > 0 {
		di.ColNum = len(ins.Columns)
	} else {
		di.ColNum = len(colsDesc)
	}
	insertLength := len(stmts[0].Insertdirectstmt.InsertValues)
	//rowNum := insertLength / colNum
	di.RowNum = int(stmts[0].Insertdirectstmt.RowsAffected)
	return insertLength
}

// BuildpriTagValMap groups input values by primary tag
func BuildpriTagValMap(di DirectInsert) map[string][]int {
	// group the input values by primary tag
	priTagValMap := make(map[string][]int, len(di.InputValues))
	for i := range di.InputValues {
		var priVal string
		for _, col := range di.PrimaryTagCols {
			priVal += sqlbase.DatumToString(di.InputValues[i][di.ColIndexs[int(col.ID)]])
		}
		priTagValMap[priVal] = append(priTagValMap[priVal], i)
	}
	return priTagValMap
}

// BuildPreparepriTagValMap groups the values entered in prepare by primary tag
func BuildPreparepriTagValMap(qargs [][]byte, di DirectInsert) map[string][]int {
	// group the input values by primary tag
	rowNum := di.RowNum / di.ColNum

	// Calculate column indexes in advance
	var colIndexes []int
	for _, col := range di.PrimaryTagCols {
		colIndexes = append(colIndexes, di.ColIndexs[int(col.ID)])
	}

	priTagValMap := make(map[string][]int, 10)
	for i := 0; i < rowNum; i++ {
		var priVal []byte
		for _, idx := range colIndexes {
			priVal = append(priVal, qargs[i*di.ColNum+idx]...)
		}
		priValStr := string(priVal)
		priTagValMap[priValStr] = append(priTagValMap[priValStr], i)
	}
	return priTagValMap
}

// BuildRowBytesForPrepareTsInsert build rows for PrepareTsInsert
func BuildRowBytesForPrepareTsInsert(
	ptCtx tree.ParseTimeContext,
	Args [][]byte,
	Dit DirectInsertTable,
	di *DirectInsert,
	EvalContext tree.EvalContext,
	table *sqlbase.ImmutableTableDescriptor,
	nodeID roachpb.NodeID,
	rowTimestamps []int64,
) error {

	rowNum := di.RowNum / di.ColNum
	rowBytes, dataOffset, varDataOffset, err := preAllocateDataRowBytes(rowNum, &di.Dcs)
	if err != nil {
		return err
	}
	// Define the required variables
	var curColLength int
	bitmapOffset := execbuilder.DataLenSize
	for i := 0; i < rowNum; i++ {
		Payload := rowBytes[i]
		offset := dataOffset
		for j, col := range di.PrettyCols {
			colIdx := di.ColIndexs[int(col.ID)]
			isLastDataCol := false
			if colIdx < 0 {
				if !col.Nullable {
					return sqlbase.NewNonNullViolationError(col.Name)
				} else if col.IsTagCol() {
					continue
				}
			}
			// tag列不用拼到payload里
			if !col.IsTagCol() {
				// dataColIdx是第几个data
				dataColIdx := j - di.PArgs.PTagNum - di.PArgs.AllTagNum
				isLastDataCol = dataColIdx == di.PArgs.DataColNum-1

				if int(col.TsCol.VariableLengthType) == sqlbase.StorageTuple {
					curColLength = int(col.TsCol.StorageLen)
				} else {
					curColLength = execbuilder.VarColumnSize
				}
				// deal with NULL value
				if colIdx < 0 {
					Payload[bitmapOffset+dataColIdx/8] |= 1 << (dataColIdx % 8)
					offset += curColLength
					// Fill the length of rowByte
					if isLastDataCol {
						binary.LittleEndian.PutUint32(Payload[0:], uint32(varDataOffset-execbuilder.DataLenSize))
					}
					continue
				}
				dataLen := len(Args[i*di.ColNum+colIdx])
				copy(Payload[offset:offset+dataLen], Args[i*di.ColNum+colIdx])
				offset += curColLength
			}
			if isLastDataCol {
				// The length of the data is placed in the first 4 bytes of the payload
				binary.LittleEndian.PutUint32(Payload[0:], uint32(varDataOffset-execbuilder.DataLenSize))
			}
		}
	}

	//	The following code uniformly calculates priTagValMap
	var colIndexes []int
	for _, col := range di.PrimaryTagCols {
		colIndexes = append(colIndexes, di.ColIndexs[int(col.ID)])
	}

	priTagValMap := make(map[string][]int, 10)
	for i := 0; i < rowNum; i++ {
		var priVal []byte
		for _, idx := range colIndexes {
			priVal = append(priVal, Args[i*di.ColNum+idx]...)
		}
		priValStr := string(priVal)
		priTagValMap[priValStr] = append(priTagValMap[priValStr], i)
	}
	di.PArgs.DataColNum, di.PArgs.DataColSize, di.PArgs.PreAllocColSize = 0, 0, 0
	allPayloads := make([]*sqlbase.SinglePayloadInfo, len(priTagValMap))
	count := 0
	for _, priTagRowIdx := range priTagValMap {
		payload, _, err := execbuilder.BuildPreparePayloadForTsInsert(
			&EvalContext,
			EvalContext.Txn,
			nil,
			priTagRowIdx[:1],
			di.PArgs.PrettyCols[:di.PArgs.PTagNum+di.PArgs.AllTagNum],
			di.ColIndexs,
			di.PArgs,
			Dit.DbID,
			Dit.TabID,
			Args,
			di.ColNum,
		)
		if err != nil {
			return err
		}
		// Make primaryTag key.
		hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
		primaryTagKey := sqlbase.MakeTsPrimaryTagKey(table.ID, hashPoints)
		groupRowBytes := make([][]byte, len(priTagRowIdx))
		groupRowTime := make([]int64, len(priTagRowIdx))
		// TsRowPutRequest need min and max timestamp.
		minTimestamp := int64(math.MaxInt64)
		maxTimeStamp := int64(math.MinInt64)
		valueSize := int32(0)
		for i, idx := range priTagRowIdx {
			groupRowBytes[i] = rowBytes[idx]
			groupRowTime[i] = rowTimestamps[idx]
			if rowTimestamps[idx] > maxTimeStamp {
				maxTimeStamp = rowTimestamps[idx]
			}
			if rowTimestamps[idx] < minTimestamp {
				minTimestamp = rowTimestamps[idx]
			}
			valueSize += int32(len(groupRowBytes[i]))
		}
		startKey := sqlbase.MakeTsRangeKey(table.ID, uint64(hashPoints[0]), minTimestamp)
		endKey := sqlbase.MakeTsRangeKey(table.ID, uint64(hashPoints[0]), maxTimeStamp+1)
		allPayloads[count] = &sqlbase.SinglePayloadInfo{
			Payload:       payload,
			RowNum:        uint32(len(priTagRowIdx)),
			PrimaryTagKey: primaryTagKey,
			RowBytes:      groupRowBytes,
			RowTimestamps: groupRowTime,
			StartKey:      startKey,
			EndKey:        endKey,
			ValueSize:     valueSize,
		}
		count++
	}
	di.PayloadNodeMap[int(EvalContext.NodeID)] = &sqlbase.PayloadForDistTSInsert{
		NodeID: nodeID, PerNodePayloads: allPayloads,
	}
	return nil
}

// TsprepareTypeCheck performs args conversion based on input type and column type
func TsprepareTypeCheck(
	ptCtx tree.ParseTimeContext,
	Args [][]byte,
	inferTypes []oid.Oid,
	ArgFormatCodes []pgwirebase.FormatCode,
	cols []sqlbase.ColumnDescriptor,
	di DirectInsert,
) ([][][]byte, []int64, error) {
	rowNum := di.RowNum / di.ColNum
	rowTimestamps := make([]int64, 0, rowNum)
	if di.RowNum%di.ColNum != 0 {
		return nil, nil, pgerror.Newf(
			pgcode.Syntax,
			"insert (row %d) has more expressions than target columns, %d expressions for %d targets",
			rowNum, di.RowNum, di.ColNum)
	}
	isFirstCols := false
	for row := 0; row < rowNum; row++ {
		for col := 0; col < di.ColNum; col++ {
			colPos := di.IDMap[col]
			column := cols[colPos]
			// 通过列ID确定是第一列，也就是时间戳列
			if int(column.ID) == 1 {
				isFirstCols = true
			}
			idx := di.PosMap[col] + di.ColNum*row
			if Args[idx] == nil {
				if column.IsNullable() {
					continue
				} else {
					return nil, nil, sqlbase.NewNonNullViolationError(column.Name)
				}
			}
			switch inferTypes[idx] {
			case oid.T_timestamptz:
				if ArgFormatCodes[idx] == pgwirebase.FormatText {
					// string type
					t, err := tree.ParseDTimestampTZ(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
					if err != nil {
						return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
					}
					tum := t.UnixMilli()
					if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
						return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"value '%s' out of range for type %s", t.String(), column.Type.SQLString())
					}
					Args[idx] = make([]byte, 8)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
					if isFirstCols {
						rowTimestamps = append(rowTimestamps, tum)
					}
				} else {
					i := int64(binary.BigEndian.Uint64(Args[idx]))
					nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
					second := execbuilder.PgBinaryToTime(i).Unix()
					tum := second*1000 + int64(nanosecond/1000000)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
					if isFirstCols {
						rowTimestamps = append(rowTimestamps, tum)
					}
				}
			case oid.T_timestamp:
				if ArgFormatCodes[idx] == pgwirebase.FormatText {
					// string type
					t, err := tree.ParseDTimestamp(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
					if err != nil {
						return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
					}
					tum := t.UnixMilli()
					if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
						return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"value %s out of range for type %s", t.String(), column.Type.SQLString())
					}
					Args[idx] = make([]byte, 8)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
					if isFirstCols {
						rowTimestamps = append(rowTimestamps, tum)
					}
				} else {
					i := int64(binary.BigEndian.Uint64(Args[idx]))
					nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
					second := execbuilder.PgBinaryToTime(i).Unix()
					tum := second*1000 + int64(nanosecond/1000000)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
					if isFirstCols {
						rowTimestamps = append(rowTimestamps, tum)
					}
				}
			case oid.T_int8:
				switch column.Type.Family() {
				case types.IntFamily, types.TimestampTZFamily, types.TimestampFamily, types.BoolFamily:
					switch column.Type.Oid() {
					case oid.T_int2:
						var intValue int32
						for _, b := range Args[idx] {
							// Split each byte into two 4-bit values and combine them into one integer
							high := int32(b >> 4)
							low := int32(b & 0x0F)
							intValue = (intValue << 8) | (high << 4) | low
						}
						if intValue < math.MinInt16 || intValue > math.MaxInt16 {
							return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
								"integer out of range for type %s (column %q)",
								column.Type.SQLString(), column.Name)
						}
						binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
					case oid.T_int4:
						if len(Args[idx]) > 4 {
							var intValue int64
							for _, b := range Args[idx] {
								intValue = (intValue << 8) | int64(b)
							}
							if intValue < math.MinInt32 || intValue > math.MaxInt32 {
								return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
									"integer out of range for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
						}
						binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
					case oid.T_int8:
						binary.LittleEndian.PutUint64(Args[idx], binary.BigEndian.Uint64(Args[idx]))
					case oid.T_timestamptz, oid.T_timestamp:
						tum := binary.BigEndian.Uint64(Args[idx])
						binary.LittleEndian.PutUint64(Args[idx], tum)
						if isFirstCols {
							rowTimestamps = append(rowTimestamps, int64(tum))
						}
					case oid.T_bool:
						num := binary.BigEndian.Uint32(Args[idx])
						// Determine whether the specific positioning is 1 based on bit and operation
						bitIndex := 0 // The bit index to be determined, counting from right to left, starting from 0
						bitValue := num & (1 << bitIndex)
						Args[idx] = make([]byte, 1)
						if bitValue != 0 {
							Args[idx][0] = 1
						} else {
							Args[idx][0] = 0
						}
					default:
						return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
					}
				case types.StringFamily:
					num := binary.BigEndian.Uint32(Args[idx])
					str := strconv.FormatUint(uint64(num), 10)
					Args[idx] = make([]byte, len(str))
					copy(Args[idx][0:], str)
					continue
				default:
					return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
			case oid.T_int4, oid.T_int2:
				switch column.Type.Family() {
				case types.IntFamily, types.TimestampTZFamily, types.TimestampFamily, types.BoolFamily:
					switch column.Type.Oid() {
					case oid.T_int2:
						var intValue int32
						for _, b := range Args[idx] {
							// Determine whether the specific positioning is 1 based on bit and operation
							high := int32(b >> 4)
							low := int32(b & 0x0F)
							intValue = (intValue << 8) | (high << 4) | low
						}
						if intValue < math.MinInt16 || intValue > math.MaxInt16 {
							return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
								"integer out of range for type %s (column %q)",
								column.Type.SQLString(), column.Name)
						}
						Args[idx] = make([]uint8, len(Args[idx]))
						binary.LittleEndian.PutUint16(Args[idx], uint16(intValue))
					case oid.T_int4:
						if len(Args[idx]) > 4 {
							var intValue int64
							for _, b := range Args[idx] {
								intValue = (intValue << 8) | int64(b)
							}
							if intValue < math.MinInt32 || intValue > math.MaxInt32 {
								return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
									"integer out of range for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
						}
						binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
					case oid.T_int8:
						binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
					case oid.T_timestamptz, oid.T_timestamp:
						tum := binary.BigEndian.Uint64(Args[idx])
						binary.LittleEndian.PutUint64(Args[idx], tum)
						if isFirstCols {
							rowTimestamps = append(rowTimestamps, int64(tum))
						}
					case oid.T_bool:
						num := binary.BigEndian.Uint32(Args[idx])
						bitIndex := 0
						bitValue := num & (1 << bitIndex)
						Args[idx] = make([]byte, 1)
						if bitValue != 0 {
							Args[idx][0] = 1
						} else {
							Args[idx][0] = 0
						}
					default:
						return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
					}
				case types.StringFamily:
					num := binary.BigEndian.Uint32(Args[idx])
					str := strconv.FormatUint(uint64(num), 10)
					Args[idx] = make([]byte, len(str))
					copy(Args[idx][0:], str)
					continue
				default:
					return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
			case oid.T_float8:
				if column.Type.Family() != types.FloatFamily {
					return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
				switch column.Type.Oid() {
				case oid.T_float4:
					f64 := math.Float64frombits(binary.BigEndian.Uint64(Args[idx]))
					str := strconv.FormatFloat(float64(f64), 'f', -1, 64)
					f32, err := strconv.ParseFloat(str, 32)
					if err != nil {
						return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
							"float \"%g\" out of range for type float4", f64)
					}
					Args[idx] = make([]byte, 4)
					binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32((f32))))))
				case oid.T_float8:
					Args[idx] = bigEndianToLittleEndian(Args[idx])
				}
			case oid.T_float4:
				if column.Type.Family() != types.FloatFamily {
					return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
				switch column.Type.Oid() {
				case oid.T_float4:
					f := math.Float32frombits(binary.BigEndian.Uint32(Args[idx]))
					if f < math.SmallestNonzeroFloat32 || f > math.MaxFloat32 {
						return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
							"float \"%g\" out of range for type float4", f)
					}
					binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32((f))))))
				case oid.T_float8:
					f32 := math.Float32frombits(binary.BigEndian.Uint32(Args[idx]))
					str := strconv.FormatFloat(float64(f32), 'f', -1, 32)
					f64, err := strconv.ParseFloat(str, 64)
					if err != nil {
						return nil, nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
							"float \"%g\" out of range for type float8", f32)
					}
					Args[idx] = make([]byte, 8)
					binary.LittleEndian.PutUint64(Args[idx], uint64(int64(math.Float64bits(float64(f64)))))
				}
			case oid.T_varchar, oid.T_bpchar, oid.T_varbytea:
				switch column.Type.Family() {
				case types.StringFamily, types.BytesFamily, types.TimestampFamily, types.TimestampTZFamily, types.BoolFamily:
					switch column.Type.Oid() {
					case oid.T_varchar, oid.T_bpchar, oid.T_text, oid.T_bytea, oid.T_varbytea:
						if column.Type.Width() > 0 && len(string(Args[idx])) > int(column.Type.Width()) {
							return nil, nil, pgerror.Newf(pgcode.StringDataRightTruncation,
								"value too long for type %s (column %q)",
								column.Type.SQLString(), column.Name)
						}
					case types.T_nchar, types.T_nvarchar:
						if column.Type.Width() > 0 && utf8.RuneCountInString(string(Args[idx])) > int(column.Type.Width()) {
							return nil, nil, pgerror.Newf(pgcode.StringDataRightTruncation,
								"value too long for type %s (column %q)",
								column.Type.SQLString(), column.Name)
						}
					case oid.T_timestamptz:
						if ArgFormatCodes[idx] == pgwirebase.FormatText {
							// string type
							t, err := tree.ParseDTimestampTZ(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
							if err != nil {
								return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
							}
							tum := t.UnixMilli()
							if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
								if column.Type.Oid() == oid.T_timestamptz {
									return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
										"value '%s' out of range for type %s", t.String(), column.Type.SQLString())
								}
								return nil, nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
									"value '%s' out of range for type %s", t.String(), column.Type.SQLString())
							}
							Args[idx] = make([]byte, 8)
							binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
							if isFirstCols {
								rowTimestamps = append(rowTimestamps, tum)
							}
						} else {
							i := int64(binary.BigEndian.Uint64(Args[idx]))
							nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
							second := execbuilder.PgBinaryToTime(i).Unix()
							tum := second*1000 + int64(nanosecond/1000000) - 8*60*60*1000
							binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
							if isFirstCols {
								rowTimestamps = append(rowTimestamps, tum)
							}
						}
					case oid.T_bool:
						davl, err := tree.ParseDBool(string(Args[idx]))
						if err != nil {
							return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
						}
						Args[idx] = make([]byte, 1)
						if *davl {
							Args[idx][0] = 1
						} else {
							Args[idx][0] = 0
						}
					case types.T_geometry:
						_, err := geos.FromWKT(string(Args[idx]))
						if err != nil {
							if strings.Contains(err.Error(), "load error") {
								return nil, nil, err
							}
							return nil, nil, pgerror.Newf(pgcode.DataException, "value '%s' is invalid for type %s", string(Args[idx]), column.Type.SQLString())
						}
						if len(string(Args[idx])) > int(column.Type.Width()) {
							return nil, nil, pgerror.Newf(pgcode.StringDataRightTruncation,
								"value '%s' too long for type %s", string(Args[idx]), column.Type.SQLString())
						}
					}
				default:
					return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
			case oid.T_bool:
				davl, err := tree.ParseDBool(string(Args[idx]))
				if err != nil {
					return nil, nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
				Args[idx] = make([]byte, 1)
				if *davl {
					Args[idx][0] = 1
				} else {
					Args[idx][0] = 0
				}
			}
		}
	}

	return nil, rowTimestamps, nil
}

func bigEndianToLittleEndian(bigEndian []byte) []byte {
	// Reverse the byte slice of the large endings
	// to obtain the byte slice of the small endings
	littleEndian := make([]byte, len(bigEndian))
	for i := 0; i < len(bigEndian); i++ {
		littleEndian[i] = bigEndian[len(bigEndian)-1-i]
	}
	return littleEndian
}

// GetColsInfo to obtain relevant column information
func GetColsInfo(
	tsColsDesc *[]sqlbase.ColumnDescriptor, ins *tree.Insert, di *DirectInsert,
) (err error) {
	var otherTagCols, dataCols []*sqlbase.ColumnDescriptor
	var ptID, tID, dID []int
	var ptPos, tPos, dPos []int
	colsDesc := *tsColsDesc
	di.ColIndexs = make(map[int]int, len(colsDesc))
	fixColsDesc := make([]sqlbase.ColumnDescriptor, 0, 22)
	di.Dcs = make([]sqlbase.ColumnDescriptor, 0, 11)
	tagCols := make([]sqlbase.ColumnDescriptor, 0, 11)
	samecol := make([]bool, len(ins.Columns))
	for i := 0; i < len(colsDesc); i++ {
		col := (colsDesc)[i]
		if col.IsTagCol() {
			tagCols = append(tagCols, col)
		} else {
			di.Dcs = append(di.Dcs, col)
		}
	}
	fixColsDesc = append(fixColsDesc, di.Dcs...)
	fixColsDesc = append(fixColsDesc, tagCols...)
	colsDesc = fixColsDesc
	*tsColsDesc = fixColsDesc
	if ins.Columns != nil && len(ins.Columns) > 0 {
		haveOtherTag, haveDataCol := false, false
		isInsert := false
		for i := 0; i < len(colsDesc); i++ {
			isInsert = false
			var insertPos int
			for idx, name := range ins.Columns {
				if !samecol[idx] && string(name) == colsDesc[i].Name {
					isInsert = true
					insertPos = idx
					samecol[idx] = true
					break
				}
			}

			colID := opt.ColumnID(colsDesc[i].ID)
			if colsDesc[i].IsPrimaryTagCol() {
				di.PrimaryTagCols = append(di.PrimaryTagCols, &colsDesc[i])
				if isInsert {
					di.ColIndexs[int(colID)] = insertPos
				}
			}
			if colsDesc[i].IsTagCol() {
				otherTagCols = append(otherTagCols, &colsDesc[i])
				if isInsert {
					di.ColIndexs[int(colID)] = insertPos
					haveOtherTag = true
					tID = append(tID, i)
					tPos = append(tPos, insertPos)
				}
			} else {
				dataCols = append(dataCols, &colsDesc[i])
				if isInsert {
					haveDataCol = true
					di.ColIndexs[int(colID)] = insertPos
					dID = append(dID, i)
					dPos = append(dPos, insertPos)
				}
			}
			if !isInsert {
				di.ColIndexs[int(colsDesc[i].ID)] = -1
			}
		}
		for idx, name := range ins.Columns {
			if !samecol[idx] {
				for i := 0; i < len(colsDesc); i++ {
					if colsDesc[i].Name == string(ins.Columns[idx]) {
						err = pgerror.Newf(pgcode.DuplicateColumn, "multiple assignments to the same column \"%s\"", string(ins.Columns[idx]))
						return
					}
				}
				err = sqlbase.NewUndefinedColumnError(string(name))
				return
			}
		}
		if !haveDataCol {
			dataCols = dataCols[:0]
		}
		if !haveOtherTag && haveDataCol {
			otherTagCols = otherTagCols[:0]
		}
	} else {
		for i := 0; i < len(colsDesc); i++ {
			if colsDesc[i].IsPrimaryTagCol() {
				di.PrimaryTagCols = append(di.PrimaryTagCols, &colsDesc[i])
			}
			if colsDesc[i].IsTagCol() {
				otherTagCols = append(otherTagCols, &colsDesc[i])
				tID = append(tID, i)
				tPos = append(tPos, i)
			} else {
				dataCols = append(dataCols, &colsDesc[i])
				dID = append(dID, i)
				dPos = append(dPos, i)
			}
			di.ColIndexs[int(colsDesc[i].ID)] = i
		}
	}
	tsVersion := uint32(1)
	di.PArgs, err = execbuilder.BuildPayloadArgs(tsVersion, di.PrimaryTagCols, otherTagCols, dataCols)
	di.PrettyCols = di.PArgs.PrettyCols
	di.IDMap = make([]int, 0, 22)
	di.IDMap = append(di.IDMap, ptID...)
	di.IDMap = append(di.IDMap, tID...)
	di.IDMap = append(di.IDMap, dID...)
	di.PosMap = make([]int, 0, 22)
	di.PosMap = append(di.PosMap, ptPos...)
	di.PosMap = append(di.PosMap, tPos...)
	di.PosMap = append(di.PosMap, dPos...)
	// check for not null columns
	for i := 0; i < len(di.PrettyCols); i++ {
		if di.ColIndexs[int(di.PrettyCols[i].ID)] < 0 {
			if di.PrettyCols[i].IsPrimaryTagCol() {
				var priTagNames []string
				for _, ptCols := range di.PrimaryTagCols {
					priTagNames = append(priTagNames, (*ptCols).Name)
				}
				err = pgerror.Newf(pgcode.Syntax, "need to specify all primary tag %v", priTagNames)
				return
			}
			if di.PrettyCols[i].IsOrdinaryTagCol() && !di.PrettyCols[i].Nullable {
				err = sqlbase.NewNonNullViolationError(di.PrettyCols[i].Name)
				return
			}
			if !di.PrettyCols[i].Nullable && len(dataCols) != 0 {
				err = sqlbase.NewNonNullViolationError(di.PrettyCols[i].Name)
				return
			}
		}
	}
	return
}

// preAllocateDataRowBytes calculates the memory size required by rowBytes based on the data columns
// and preAllocates the memory space.
func preAllocateDataRowBytes(
	rowNum int, dataCols *[]sqlbase.ColumnDescriptor,
) (rowBytes [][]byte, dataOffset, varDataOffset int, err error) {
	dataRowSize, preSize, err := computeColumnSize(dataCols)
	if err != nil {
		return
	}
	bitmapLen := (len(*dataCols) + 7) / 8
	singleRowSize := execbuilder.DataLenSize + bitmapLen + dataRowSize + preSize
	rowBytesSize := singleRowSize * rowNum
	rowBytes = make([][]byte, rowNum)
	// allocate memory for two nested slices, for better performance
	preBytes := make([]byte, rowBytesSize)
	for i := 0; i < rowNum; i++ {
		rowBytes[i], preBytes = preBytes[:singleRowSize:singleRowSize], preBytes[singleRowSize:]
	}
	bitmapOffset := execbuilder.DataLenSize
	dataOffset = bitmapOffset + bitmapLen
	varDataOffset = dataOffset + dataRowSize
	return
}

// ComputeColumnSize computes colSize
func computeColumnSize(cols *[]sqlbase.ColumnDescriptor) (int, int, error) {
	colSize := 0
	preAllocSize := 0
	for i := range *cols {
		col := (*cols)[i]
		switch col.Type.Oid() {
		case oid.T_int2:
			colSize += 2
		case oid.T_int4, oid.T_float4:
			colSize += 4
		case oid.T_int8, oid.T_float8, oid.T_timestamp, oid.T_timestamptz:
			if !col.IsTagCol() && i == 0 {
				// The first timestamp column in the data column needs to reserve 16 bytes for LSN
				colSize += sqlbase.FirstTsDataColSize
			} else {
				colSize += 8
			}
		case oid.T_bool:
			colSize++
		case oid.T_char, types.T_nchar, oid.T_text, oid.T_bpchar, oid.T_bytea, types.T_geometry:
			colSize += int(col.TsCol.StorageLen)
		case oid.T_varchar, types.T_nvarchar, types.T_varbytea:
			if col.TsCol.VariableLengthType == sqlbase.StorageTuple || col.IsPrimaryTagCol() {
				colSize += int(col.TsCol.StorageLen)
			} else {
				// pre allocate paylaod space for var-length colums
				// here we use some fixed-rule to preallocate more space to improve efficiency
				// StorageLen = userWidth+1
				// varDataLen = StorageLen+2
				if col.TsCol.StorageLen < 68 {
					// 100%
					preAllocSize += int(col.TsCol.StorageLen)
				} else if col.TsCol.StorageLen < 260 {
					// 60%
					preAllocSize += int(col.TsCol.StorageLen/5) * 3
				} else {
					// 30%
					preAllocSize += int(col.TsCol.StorageLen/10) * 3
				}
				colSize += execbuilder.VarColumnSize
			}
		default:
			return 0, 0, pgerror.Newf(pgcode.DatatypeMismatch, "unsupported input type oid %d", col.Type.Oid())
		}
	}
	return colSize, preAllocSize, nil
}

// GetRowBytesForTsInsert performs column type conversion and length checking
func GetRowBytesForTsInsert(
	ptCtx tree.ParseTimeContext,
	cols *[]sqlbase.ColumnDescriptor,
	di *DirectInsert,
	stmts parser.Statements,
	rowTimestamps []int64,
) ([]tree.Datums, map[string][]int, [][]byte, error) {
	inputValues := make([]tree.Datums, di.RowNum)
	// Apply for continuous memory space at once and allocate it to 2D slices
	preSlice := make([]tree.Datum, di.RowNum*di.ColNum)
	for i := 0; i < di.RowNum; i++ {
		inputValues[i], preSlice = preSlice[:di.ColNum:di.ColNum], preSlice[di.ColNum:]
	}
	tp := execbuilder.NewTsPayload()
	rowBytes, dataOffset, independentOffset, err := preAllocateDataRowBytes(di.RowNum, &di.Dcs)
	if err != nil {
		return nil, nil, nil, err
	}
	// Define the required variables
	var curColLength, dataColIdx int
	var isDataCol, isLastDataCol bool
	bitmapOffset := execbuilder.DataLenSize
	// partition input data based on primary tag values
	priTagValMap := make(map[string][]int)
	// Type check for input values.
	var buf strings.Builder
	for row := range inputValues {
		tp.SetPayload(rowBytes[row])
		offset := dataOffset
		varDataOffset := independentOffset
		for i, column := range di.PrettyCols {
			colIdx := di.ColIndexs[int(column.ID)]
			isDataCol = !column.IsTagCol()
			isLastDataCol = false
			if colIdx < 0 {
				if !column.IsNullable() {
					return nil, nil, nil, sqlbase.NewNonNullViolationError(column.Name)
				} else if column.IsTagCol() {
					continue
				}
				dataColIdx = i
				if column.IsTagCol() {
					continue
				}
			}
			if isDataCol {
				dataColIdx = i - di.PArgs.PTagNum - di.PArgs.AllTagNum
				isLastDataCol = dataColIdx == di.PArgs.DataColNum-1

				if int(column.TsCol.VariableLengthType) == sqlbase.StorageTuple {
					curColLength = int(column.TsCol.StorageLen)
				} else {
					curColLength = execbuilder.VarColumnSize
				}
				// deal with NULL value
				if colIdx < 0 {
					execbuilder.SetBit(tp, bitmapOffset, dataColIdx)
					offset += curColLength
					// Fill the length of rowByte
					if isLastDataCol {
						execbuilder.WriteUint32ToPayload(tp, uint32(varDataOffset-execbuilder.DataLenSize))
						rowBytes[row] = tp.GetPayload(varDataOffset)
					}
					continue
				}
			}
			col := colIdx
			rawValue := stmts[0].Insertdirectstmt.InsertValues[col+row*di.ColNum]
			valueType := stmts[0].Insertdirectstmt.ValuesType[col+row*di.ColNum]
			if valueType != parser.STRINGTYPE && rawValue == "" {
				if !column.IsNullable() {
					return nil, nil, nil, sqlbase.NewNonNullViolationError(column.Name)
				}
				// attempting to insert a NULL value when no value is specified
				inputValues[row][col] = tree.DNull
			} else {
				inputValues[row][col], err = GetSingleDatum(ptCtx, *column, valueType, rawValue)
			}

			if err != nil {
				return nil, nil, nil, err
			}
			if i < di.PArgs.PTagNum {
				buf.WriteString(sqlbase.DatumToString(inputValues[row][col]))
			}
			if isDataCol {
				if inputValues[row][col] == tree.DNull {
					execbuilder.SetBit(tp, bitmapOffset, dataColIdx)
					offset += curColLength
					// Fill the length of rowByte
					if isLastDataCol {
						execbuilder.WriteUint32ToPayload(tp, uint32(varDataOffset-execbuilder.DataLenSize))
						rowBytes[row] = tp.GetPayload(varDataOffset)
					}
					continue
				}

				if dataColIdx == 0 {
					rowTimestamps[row] = int64(*inputValues[row][col].(*tree.DInt))
				}
				if varDataOffset, err = tp.FillColData(
					inputValues[row][col],
					column, false, false,
					offset, varDataOffset, bitmapOffset,
				); err != nil {
					return nil, nil, nil, err
				}
				offset += curColLength
				if isLastDataCol {
					tp.SetPayload(tp.GetPayload(varDataOffset))
					execbuilder.WriteUint32ToPayload(tp, uint32(varDataOffset-execbuilder.DataLenSize))
					rowBytes[row] = tp.GetPayload(varDataOffset)
				}
			}
			continue
			// TODO 需要处理空值(data的空值)
			//NullExec:
			//	if !column.IsNullable() {
			//		return nil, nil, nil, sqlbase.NewNonNullViolationError(column.Name)
			//	}
			//	// attempting to insert a NULL value when no value is specified
			//	inputValues[row][col] = tree.DNull
		}
		priTagValMap[buf.String()] = append(priTagValMap[buf.String()], row)
		buf.Reset()
	}

	return inputValues, priTagValMap, rowBytes, nil
}

// GetSingleDatum gets single datum by columnDesc
func GetSingleDatum(
	ptCtx tree.ParseTimeContext,
	column sqlbase.ColumnDescriptor,
	valueType parser.TokenType,
	rawValue string,
) (tree.Datum, error) {
	var err error
	switch column.Type.Oid() {
	case oid.T_timestamptz:
		var dVal *tree.DInt
		if valueType == parser.STRINGTYPE {
			t, err := tree.ParseDTimestampTZ(ptCtx, rawValue, tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
			if err != nil {
				return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
			}
			dVal = tree.NewDInt(tree.DInt(t.UnixMilli()))
			if *dVal < tree.TsMinTimestamp || *dVal > tree.TsMaxTimestamp {
				return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
					"value '%s' out of range for type %s", rawValue, column.Type.SQLString())
			}
		} else {
			if rawValue == "now" {
				currentTime := timeutil.Now().UnixNano() / int64(time.Millisecond)
				return tree.NewDInt(tree.DInt(currentTime)), nil
				//continue
			}
			in, err2 := strconv.ParseInt(rawValue, 10, 64)
			if err2 != nil {
				if strings.Contains(err2.Error(), "out of range") {
					return nil, err2
				}
				return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
			}
			dVal = (*tree.DInt)(&in)
			// consistent with the timestamp range supported by savedata
			if *dVal < tree.TsMinTimestamp || *dVal > tree.TsMaxTimestamp {
				return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
					"integer \"%s\" out of range for type %s", rawValue, column.Type.SQLString())
			}
		}
		return dVal, nil
		//continue
	case oid.T_timestamp:
		var dVal *tree.DInt
		if valueType == parser.STRINGTYPE {
			t, err := tree.ParseDTimestamp(nil, rawValue, tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
			if err != nil {
				return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
			}
			dVal = tree.NewDInt(tree.DInt(t.UnixMilli()))
			if *dVal < tree.TsMinTimestamp || *dVal > tree.TsMaxTimestamp {
				return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
					"value '%s' out of range for type %s", rawValue, column.Type.SQLString())
			}
		} else {
			if rawValue == "now" {
				currentTime := timeutil.Now().UnixNano() / int64(time.Millisecond)
				return tree.NewDInt(tree.DInt(currentTime)), nil
				//continue
			}
			in, err2 := strconv.ParseInt(rawValue, 10, 64)
			if err2 != nil {
				if strings.Contains(err2.Error(), "out of range") {
					return nil, err2
				}
				return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
			}
			dVal = (*tree.DInt)(&in)
			// consistent with the timestamp range supported by savedata
			if *dVal < tree.TsMinTimestamp || *dVal > tree.TsMaxTimestamp {
				return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
					"integer \"%s\" out of range for type %s", rawValue, column.Type.SQLString())
			}
		}
		return dVal, nil
		//continue
	case oid.T_int8:
		if valueType == parser.STRINGTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		in, err := strconv.Atoi(rawValue)
		if err != nil {
			dat, err := parserString2Int(rawValue, err, column)
			if err != nil {
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				return nil, err
			}
			if dat != nil {
				return dat, nil
				//continue
			}
		}
		if in < math.MinInt64 || in > math.MaxInt64 {
			err = pgerror.Newf(pgcode.NumericValueOutOfRange,
				"integer \"%d\" out of range for type %s", in, column.Type.SQLString())
			return nil, err
		}
		d := tree.DInt(in)
		return &d, nil
		//continue
	case oid.T_int4:
		if valueType == parser.STRINGTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		in, err := strconv.Atoi(rawValue)
		if err != nil {
			dat, err := parserString2Int(rawValue, err, column)
			if err != nil {
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				return nil, err
			}
			if dat != nil {
				return dat, nil
				//continue
			}
		}
		if in < math.MinInt32 || in > math.MaxInt32 {
			err = pgerror.Newf(pgcode.NumericValueOutOfRange,
				"integer \"%d\" out of range for type %s", in, column.Type.SQLString())
			return nil, err
		}
		d := tree.DInt(in)
		return &d, nil
		//continue
	case oid.T_int2:
		if valueType == parser.STRINGTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		in, err := strconv.Atoi(rawValue)
		if err != nil {
			dat, err := parserString2Int(rawValue, err, column)
			if err != nil {
				if valueType == parser.NORMALTYPE {
					return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
				}
				return nil, err
			}
			if dat != nil {
				return dat, nil
				//continue
			}
		}
		if in < math.MinInt16 || in > math.MaxInt16 {
			err = pgerror.Newf(pgcode.NumericValueOutOfRange,
				"integer \"%d\" out of range for type %s", in, column.Type.SQLString())
			return nil, err
		}
		d := tree.DInt(in)
		return &d, nil
		//continue
	case oid.T_cstring, oid.T_char:
		if valueType == parser.NUMTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		if valueType == parser.NORMALTYPE {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
		}
		if valueType == parser.BYTETYPE {
			// convert the value of the input bytes type into a string
			rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
		}
		return tree.NewDString(rawValue), nil
		//continue
	case oid.T_text, oid.T_bpchar, oid.T_varchar:
		if valueType == parser.NUMTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		if valueType == parser.NORMALTYPE {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
		}
		if valueType == parser.BYTETYPE {
			// Convert the value of the input bytes type into a string
			rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
		}
		// string(n)/char(n)/varchar(n) calculate length by byte
		if column.Type.Width() > 0 && len(rawValue) > int(column.Type.Width()) {
			return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
				"value '%s' too long for type %s", rawValue, column.Type.SQLString())
		}
		return tree.NewDString(rawValue), nil
		//continue
	// NCHAR or NVARCHAR
	case oid.Oid(91004), oid.Oid(91002):
		if valueType == parser.NUMTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		if valueType == parser.NORMALTYPE {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
		}
		if valueType == parser.BYTETYPE {
			// Convert the value of the input bytes type into a string
			rawValue = strings.Trim(tree.NewDBytes(tree.DBytes(rawValue)).String(), "'")
		}
		// nchar(n)/nvarchar(n) calculate length by character
		if column.Type.Width() > 0 && utf8.RuneCountInString(rawValue) > int(column.Type.Width()) {
			return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
				"value '%s' too long for type %s", rawValue, column.Type.SQLString())
		}
		return tree.NewDString(rawValue), nil
		//continue
	case oid.T_bytea, oid.T_varbytea:
		if valueType == parser.NUMTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		if valueType == parser.NORMALTYPE {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
		}
		v, err := tree.ParseDByte(rawValue)
		if err != nil {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		// bytes(n)/varbytes(n) calculate length by byte
		if column.Type.Width() > 0 && len(rawValue) > int(column.Type.Width()) {
			return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
				"value '%s' too long for type %s", rawValue, column.Type.SQLString())
		}
		return v, nil
		//continue
	case oid.T_float4:
		if valueType == parser.NORMALTYPE {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
		}
		if valueType == parser.STRINGTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		var in float64
		in, err = strconv.ParseFloat(rawValue, 32)
		if err != nil {
			if strings.Contains(err.Error(), "out of range") {
				return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
					"float \"%s\" out of range for type %s", rawValue, column.Type.SQLString())
			}
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		return tree.NewDFloat(tree.DFloat(in)), nil
		//continue
	case oid.T_float8:
		if valueType == parser.NORMALTYPE {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
		}
		if valueType == parser.STRINGTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		var in float64
		in, err = strconv.ParseFloat(rawValue, 64)
		if err != nil {
			if strings.Contains(err.Error(), "out of range") {
				return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
					"float \"%s\" out of range for type %s", rawValue, column.Type.SQLString())
			}
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		return tree.NewDFloat(tree.DFloat(in)), nil
	case oid.T_bool:
		if valueType == parser.NORMALTYPE {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
		}
		dBool, err := tree.ParseDBool(rawValue)
		if err != nil {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		return dBool, nil
	case types.T_geometry:
		if valueType == parser.NORMALTYPE {
			return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)
		}
		if valueType == parser.NUMTYPE {
			return nil, tree.NewDatatypeMismatchError(rawValue, column.Type.SQLString())
		}
		_, err := geos.FromWKT(rawValue)
		if err != nil {
			if strings.Contains(err.Error(), "load error") {
				return nil, err
			}
			return nil, pgerror.Newf(pgcode.DataException, "value '%s' is invalid for type %s", rawValue, column.Type.SQLString())
		}
		return tree.NewDString(rawValue), nil
	default:
		return nil, pgerror.Newf(pgcode.Syntax, "unsupported input type relation \"%s\"", rawValue)

	}
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// GetPayloadMapForMuiltNode builds payloads for distributed insertion
func GetPayloadMapForMuiltNode(
	ptCtx tree.ParseTimeContext,
	dit DirectInsertTable,
	di *DirectInsert,
	stmts parser.Statements,
	EvalContext tree.EvalContext,
	table *sqlbase.ImmutableTableDescriptor,
	nodeid roachpb.NodeID,
) error {
	rowTimestamps := make([]int64, di.RowNum)
	inputValues, priTagValMap, rowBytes, err := GetRowBytesForTsInsert(ptCtx, &dit.ColsDesc, di, stmts, rowTimestamps)
	if err != nil {
		return err
	}
	di.PArgs.DataColNum, di.PArgs.DataColSize, di.PArgs.PreAllocColSize = 0, 0, 0
	allPayloads := make([]*sqlbase.SinglePayloadInfo, len(priTagValMap))
	count := 0
	for _, priTagRowIdx := range priTagValMap {
		// Payload is the encoding of a complete line, which is the first line in a line with the same ptag
		payload, _, err := execbuilder.BuildPayloadForTsInsert(
			&EvalContext,
			EvalContext.Txn,
			inputValues,
			priTagRowIdx[:1],
			di.PArgs.PrettyCols[:di.PArgs.PTagNum+di.PArgs.AllTagNum],
			di.ColIndexs,
			di.PArgs,
			dit.DbID,
			uint32(table.ID),
		)
		if err != nil {
			return err
		}

		// Make primaryTag key.
		hashPoints := sqlbase.DecodeHashPointFromPayload(payload)
		primaryTagKey := sqlbase.MakeTsPrimaryTagKey(table.ID, hashPoints)
		groupRowBytes := make([][]byte, len(priTagRowIdx))
		groupRowTime := make([]int64, len(priTagRowIdx))
		// TsRowPutRequest need min and max timestamp.
		minTimestamp := int64(math.MaxInt64)
		maxTimeStamp := int64(math.MinInt64)
		valueSize := int32(0)
		for i, idx := range priTagRowIdx {
			groupRowBytes[i] = rowBytes[idx]
			groupRowTime[i] = rowTimestamps[idx]
			if rowTimestamps[idx] > maxTimeStamp {
				maxTimeStamp = rowTimestamps[idx]
			}
			if rowTimestamps[idx] < minTimestamp {
				minTimestamp = rowTimestamps[idx]
			}
			valueSize += int32(len(groupRowBytes[i]))
		}
		var startKey roachpb.Key
		var endKey roachpb.Key
		if di.PArgs.RowType == execbuilder.OnlyTag {
			startKey = sqlbase.MakeTsHashPointKey(sqlbase.ID(dit.TabID), uint64(hashPoints[0]))
			endKey = sqlbase.MakeTsRangeKey(sqlbase.ID(dit.TabID), uint64(hashPoints[0]), math.MaxInt64)
		} else {
			startKey = sqlbase.MakeTsRangeKey(sqlbase.ID(dit.TabID), uint64(hashPoints[0]), minTimestamp)
			endKey = sqlbase.MakeTsRangeKey(sqlbase.ID(dit.TabID), uint64(hashPoints[0]), maxTimeStamp+1)
		}
		allPayloads[count] = &sqlbase.SinglePayloadInfo{
			Payload:       payload,
			RowNum:        uint32(len(priTagRowIdx)),
			PrimaryTagKey: primaryTagKey,
			RowBytes:      groupRowBytes,
			RowTimestamps: groupRowTime,
			StartKey:      startKey,
			EndKey:        endKey,
			ValueSize:     valueSize,
		}
		count++
	}
	di.PayloadNodeMap = make(map[int]*sqlbase.PayloadForDistTSInsert)
	di.PayloadNodeMap[int(EvalContext.NodeID)] = &sqlbase.PayloadForDistTSInsert{
		NodeID: nodeid, PerNodePayloads: allPayloads,
	}
	return err
}
