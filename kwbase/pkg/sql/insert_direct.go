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
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
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

// GetInputValues performs column type conversion and length checking
func GetInputValues(
	ptCtx tree.ParseTimeContext,
	rowNum int,
	cols *[]sqlbase.ColumnDescriptor,
	IDMap []int,
	PosMap []int,
	stmts parser.Statements,
	colNum int,
) ([]tree.Datums, error) {
	inputValues := make([]tree.Datums, rowNum)
	// Apply for continuous memory space at once and allocate it to 2D slices
	preSlice := make([]tree.Datum, rowNum*colNum)
	for i := 0; i < rowNum; i++ {
		inputValues[i], preSlice = preSlice[:colNum:colNum], preSlice[colNum:]
	}
	var err error
	for row := range inputValues {
		for i := 0; i < len(IDMap); i++ {
			// col position in raw cols slice
			colPos := IDMap[i]
			// col's metadata
			column := (*cols)[colPos]
			// col position in insert cols slice
			col := PosMap[i]
			rawValue := stmts[0].Insertdirectstmt.InsertValues[col+row*colNum]
			valueType := stmts[0].Insertdirectstmt.ValuesType[col+row*colNum]
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
	payloadNodeMap map[int]*sqlbase.PayloadForDistTSInsert,
	EvalContext *tree.EvalContext,
	inputValues []tree.Datums,
	priTagRowIdx []int,
	prettyCols []*sqlbase.ColumnDescriptor,
	colIndexs map[int]int,
	pArgs execbuilder.PayloadArgs,
	dbID, tabID uint32,
	hashRouter api.HashRouter,
) error {
	payload, primaryTagVal, err := execbuilder.BuildPayloadForTsInsert(
		EvalContext,
		EvalContext.Txn,
		inputValues,
		priTagRowIdx,
		prettyCols,
		colIndexs,
		pArgs,
		dbID,
		tabID,
		hashRouter,
	)
	if err != nil {
		return err
	}
	var nodeID []roachpb.NodeID
	if EvalContext.StartSinglenode {
		nodeID = []roachpb.NodeID{1}
	} else {
		nodeID, err = hashRouter.GetNodeIDByPrimaryTag(EvalContext.Context, primaryTagVal)
		if err != nil {
			return err
		}
	}
	primaryTagKey := sqlbase.MakeTsPrimaryTagKey(sqlbase.ID(tabID), primaryTagVal)
	BuildPerNodePayloads(payloadNodeMap, nodeID, payload, priTagRowIdx, primaryTagKey)
	return nil
}

// BuildPreparePayload is used to BuildPayloadForTsInsert
func BuildPreparePayload(
	payloadNodeMap map[int]*sqlbase.PayloadForDistTSInsert,
	EvalContext *tree.EvalContext,
	inputValues [][][]byte,
	priTagRowIdx []int,
	prettyCols []*sqlbase.ColumnDescriptor,
	colIndexs map[int]int,
	pArgs execbuilder.PayloadArgs,
	dbID, tabID uint32,
	hashRouter api.HashRouter,
	qargs [][]byte,
	colnum int,
) error {
	payload, primaryTagVal, err := execbuilder.BuildPreparePayloadForTsInsert(
		EvalContext,
		EvalContext.Txn,
		inputValues,
		priTagRowIdx,
		prettyCols,
		colIndexs,
		pArgs,
		dbID,
		tabID,
		hashRouter,
		qargs,
		colnum,
	)
	if err != nil {
		return err
	}
	var nodeID []roachpb.NodeID
	if EvalContext.StartSinglenode {
		nodeID = []roachpb.NodeID{1}
	} else {
		nodeID, err = hashRouter.GetNodeIDByPrimaryTag(EvalContext.Context, primaryTagVal)
		if err != nil {
			return err
		}
	}
	primaryTagKey := sqlbase.MakeTsPrimaryTagKey(sqlbase.ID(tabID), primaryTagVal)
	BuildPerNodePayloads(payloadNodeMap, nodeID, payload, priTagRowIdx, primaryTagKey)
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
	ins *tree.Insert, colsDesc []sqlbase.ColumnDescriptor, stmts parser.Statements,
) (int, int, int) {
	// insert number of columns
	var colNum int
	if ins.Columns != nil && len(ins.Columns) > 0 {
		colNum = len(ins.Columns)
	} else {
		colNum = len(colsDesc)
	}
	insertLength := len(stmts[0].Insertdirectstmt.InsertValues)
	//rowNum := insertLength / colNum
	rowNum := int(stmts[0].Insertdirectstmt.RowsAffected)
	return colNum, insertLength, rowNum
}

// BuildpriTagValMap groups input values by primary tag
func BuildpriTagValMap(
	inputValues []tree.Datums, primaryTagCols []*sqlbase.ColumnDescriptor, colIndexs map[int]int,
) map[string][]int {
	// group the input values by primary tag
	priTagValMap := make(map[string][]int, len(inputValues))
	for i := range inputValues {
		var priVal string
		for _, col := range primaryTagCols {
			priVal += sqlbase.DatumToString(inputValues[i][colIndexs[int(col.ID)]])
		}
		priTagValMap[priVal] = append(priTagValMap[priVal], i)
	}
	return priTagValMap
}

// BuildPreparepriTagValMap groups the values entered in prepare by primary tag
func BuildPreparepriTagValMap(
	qargs [][]byte,
	primaryTagCols []*sqlbase.ColumnDescriptor,
	colIndexs map[int]int,
	valueNum, colNum int,
) map[string][]int {
	// group the input values by primary tag
	rowNum := valueNum / colNum

	// Calculate column indexes in advance
	var colIndexes []int
	for _, col := range primaryTagCols {
		colIndexes = append(colIndexes, colIndexs[int(col.ID)])
	}

	priTagValMap := make(map[string][]int, 10)
	for i := 0; i < rowNum; i++ {
		var priVal []byte
		for _, idx := range colIndexes {
			priVal = append(priVal, qargs[i*colNum+idx]...)
		}
		priValStr := string(priVal)
		priTagValMap[priValStr] = append(priTagValMap[priValStr], i)
	}
	return priTagValMap
}

// TsprepareTypeCheck performs args conversion based on input type and column type
func TsprepareTypeCheck(
	ptCtx tree.ParseTimeContext,
	Args [][]byte,
	inferTypes []oid.Oid,
	ArgFormatCodes []pgwirebase.FormatCode,
	cols []sqlbase.ColumnDescriptor,
	IDMap, PosMap []int,
	valueNum, colNum int,
) ([][][]byte, error) {
	rowNum := valueNum / colNum

	if valueNum%colNum != 0 {
		return nil, pgerror.Newf(
			pgcode.Syntax,
			"insert (row %d) has more expressions than target columns, %d expressions for %d targets",
			rowNum, valueNum, colNum)
	}

	for row := 0; row < rowNum; row++ {
		for col := 0; col < colNum; col++ {
			colPos := IDMap[col]
			column := cols[colPos]
			idx := PosMap[col] + colNum*row
			if Args[idx] == nil {
				if column.IsNullable() {
					continue
				} else {
					return nil, sqlbase.NewNonNullViolationError(column.Name)
				}
			}
			switch inferTypes[idx] {
			case oid.T_timestamptz:
				if ArgFormatCodes[idx] == pgwirebase.FormatText {
					// string type
					t, err := tree.ParseDTimestampTZ(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
					if err != nil {
						return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
					}
					tum := t.UnixMilli()
					if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
						return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"value '%s' out of range for type %s", t.String(), column.Type.SQLString())
					}
					Args[idx] = make([]byte, 8)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
				} else {
					i := int64(binary.BigEndian.Uint64(Args[idx]))
					nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
					second := execbuilder.PgBinaryToTime(i).Unix()
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(second*1000+int64(nanosecond/1000000)))
				}
			case oid.T_timestamp:
				if ArgFormatCodes[idx] == pgwirebase.FormatText {
					// string type
					t, err := tree.ParseDTimestamp(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
					if err != nil {
						return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
					}
					tum := t.UnixMilli()
					if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
						return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
							"value %s out of range for type %s", t.String(), column.Type.SQLString())
					}
					Args[idx] = make([]byte, 8)
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
				} else {
					i := int64(binary.BigEndian.Uint64(Args[idx]))
					nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
					second := execbuilder.PgBinaryToTime(i).Unix()
					binary.LittleEndian.PutUint64(Args[idx][0:], uint64(second*1000+int64(nanosecond/1000000)))
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
							return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
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
								return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
									"integer out of range for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
						}
						binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
					case oid.T_int8:
						binary.LittleEndian.PutUint64(Args[idx], binary.BigEndian.Uint64(Args[idx]))
					case oid.T_timestamptz, oid.T_timestamp:
						binary.LittleEndian.PutUint64(Args[idx], binary.BigEndian.Uint64(Args[idx]))
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
						return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
					}
				case types.StringFamily:
					num := binary.BigEndian.Uint32(Args[idx])
					str := strconv.FormatUint(uint64(num), 10)
					Args[idx] = make([]byte, len(str))
					copy(Args[idx][0:], str)
					continue
				default:
					return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
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
							return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
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
								return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
									"integer out of range for type %s (column %q)",
									column.Type.SQLString(), column.Name)
							}
						}
						binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
					case oid.T_int8:
						binary.LittleEndian.PutUint32(Args[idx], binary.BigEndian.Uint32(Args[idx]))
					case oid.T_timestamptz, oid.T_timestamp:
						binary.LittleEndian.PutUint64(Args[idx], binary.BigEndian.Uint64(Args[idx]))
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
						return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
					}
				case types.StringFamily:
					num := binary.BigEndian.Uint32(Args[idx])
					str := strconv.FormatUint(uint64(num), 10)
					Args[idx] = make([]byte, len(str))
					copy(Args[idx][0:], str)
					continue
				default:
					return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
			case oid.T_float8:
				if column.Type.Family() != types.FloatFamily {
					return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
				switch column.Type.Oid() {
				case oid.T_float4:
					f64 := math.Float64frombits(binary.BigEndian.Uint64(Args[idx]))
					str := strconv.FormatFloat(float64(f64), 'f', -1, 64)
					f32, err := strconv.ParseFloat(str, 32)
					if err != nil {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
							"float \"%g\" out of range for type float4", f64)
					}
					Args[idx] = make([]byte, 4)
					binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32((f32))))))
				case oid.T_float8:
					Args[idx] = bigEndianToLittleEndian(Args[idx])
				}
			case oid.T_float4:
				if column.Type.Family() != types.FloatFamily {
					return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
				switch column.Type.Oid() {
				case oid.T_float4:
					f := math.Float32frombits(binary.BigEndian.Uint32(Args[idx]))
					if f < math.SmallestNonzeroFloat32 || f > math.MaxFloat32 {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
							"float \"%g\" out of range for type float4", f)
					}
					binary.LittleEndian.PutUint32(Args[idx], uint32(int32(math.Float32bits(float32((f))))))
				case oid.T_float8:
					f32 := math.Float32frombits(binary.BigEndian.Uint32(Args[idx]))
					str := strconv.FormatFloat(float64(f32), 'f', -1, 32)
					f64, err := strconv.ParseFloat(str, 64)
					if err != nil {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange,
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
							return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
								"value too long for type %s (column %q)",
								column.Type.SQLString(), column.Name)
						}
					case types.T_nchar, types.T_nvarchar:
						if column.Type.Width() > 0 && utf8.RuneCountInString(string(Args[idx])) > int(column.Type.Width()) {
							return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
								"value too long for type %s (column %q)",
								column.Type.SQLString(), column.Name)
						}
					case oid.T_timestamptz:
						if ArgFormatCodes[idx] == pgwirebase.FormatText {
							// string type
							t, err := tree.ParseDTimestampTZ(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
							if err != nil {
								return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
							}
							tum := t.UnixMilli()
							if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
								return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
									"value '%s' out of range for type %s", t.String(), column.Type.SQLString())
							}
							Args[idx] = make([]byte, 8)
							binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
						} else {
							i := int64(binary.BigEndian.Uint64(Args[idx]))
							nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
							second := execbuilder.PgBinaryToTime(i).Unix()
							binary.LittleEndian.PutUint64(Args[idx][0:], uint64(second*1000+int64(nanosecond/1000000)))
						}
					case oid.T_timestamp:
						if ArgFormatCodes[idx] == pgwirebase.FormatText {
							// string type
							t, err := tree.ParseDTimestamp(ptCtx, string(Args[idx]), tree.TimeFamilyPrecisionToRoundDuration(column.Type.Precision()))
							if err != nil {
								return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
							}
							tum := t.UnixMilli()
							if tum < tree.TsMinTimestamp || tum > tree.TsMaxTimestamp {
								return nil, pgerror.Newf(pgcode.StringDataLengthMismatch,
									"value '%s' out of range for type %s", t.String(), column.Type.SQLString())
							}
							Args[idx] = make([]byte, 8)
							binary.LittleEndian.PutUint64(Args[idx][0:], uint64(tum))
						} else {
							i := int64(binary.BigEndian.Uint64(Args[idx]))
							nanosecond := execbuilder.PgBinaryToTime(i).Nanosecond()
							second := execbuilder.PgBinaryToTime(i).Unix()
							binary.LittleEndian.PutUint64(Args[idx][0:], uint64(second*1000+int64(nanosecond/1000000)))
						}
					case oid.T_bool:
						davl, err := tree.ParseDBool(string(Args[idx]))
						if err != nil {
							return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
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
								return nil, err
							}
							return nil, pgerror.Newf(pgcode.DataException, "value '%s' is invalid for type %s", string(Args[idx]), column.Type.SQLString())
						}
						if len(string(Args[idx])) > int(column.Type.Width()) {
							return nil, pgerror.Newf(pgcode.StringDataRightTruncation,
								"value '%s' too long for type %s", string(Args[idx]), column.Type.SQLString())
						}
					}
				default:
					return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
				}
			case oid.T_bool:
				davl, err := tree.ParseDBool(string(Args[idx]))
				if err != nil {
					return nil, tree.NewDatatypeMismatchError(string(Args[idx]), column.Type.SQLString())
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

	return nil, nil
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
	tsColsDesc *[]sqlbase.ColumnDescriptor, ins *tree.Insert,
) (
	IDMap, PosMap []int,
	colIndexs map[int]int,
	pArgs execbuilder.PayloadArgs,
	prettyCols, primaryTagCols []*sqlbase.ColumnDescriptor,
	err error,
) {

	var otherTagCols, dataCols []*sqlbase.ColumnDescriptor
	var ptID, tID, dID []int
	var ptPos, tPos, dPos []int
	colsDesc := *tsColsDesc
	colIndexs = make(map[int]int, len(colsDesc))
	fixColsDesc := make([]sqlbase.ColumnDescriptor, 0, 22)
	dcs := make([]sqlbase.ColumnDescriptor, 0, 11)
	tagCols := make([]sqlbase.ColumnDescriptor, 0, 11)
	samecol := make([]bool, len(ins.Columns))
	for i := 0; i < len(colsDesc); i++ {
		col := (colsDesc)[i]
		if col.IsTagCol() {
			tagCols = append(tagCols, col)
		} else {
			dcs = append(dcs, col)
		}
	}
	fixColsDesc = append(fixColsDesc, dcs...)
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
				primaryTagCols = append(primaryTagCols, &colsDesc[i])
				if isInsert {
					colIndexs[int(colID)] = insertPos
				}
			}
			if colsDesc[i].IsTagCol() {
				otherTagCols = append(otherTagCols, &colsDesc[i])
				if isInsert {
					colIndexs[int(colID)] = insertPos
					haveOtherTag = true
					tID = append(tID, i)
					tPos = append(tPos, insertPos)
				}
			} else {
				dataCols = append(dataCols, &colsDesc[i])
				if isInsert {
					haveDataCol = true
					colIndexs[int(colID)] = insertPos
					dID = append(dID, i)
					dPos = append(dPos, insertPos)
				}
			}
			if !isInsert {
				colIndexs[int(colsDesc[i].ID)] = -1
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
				primaryTagCols = append(primaryTagCols, &colsDesc[i])
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
			colIndexs[int(colsDesc[i].ID)] = i
		}
	}
	prettyCols = make([]*sqlbase.ColumnDescriptor, 0, 22)
	prettyCols = append(prettyCols, primaryTagCols...)
	prettyCols = append(prettyCols, otherTagCols...)
	prettyCols = append(prettyCols, dataCols...)
	IDMap = make([]int, 0, 22)
	IDMap = append(IDMap, ptID...)
	IDMap = append(IDMap, tID...)
	IDMap = append(IDMap, dID...)
	PosMap = make([]int, 0, 22)
	PosMap = append(PosMap, ptPos...)
	PosMap = append(PosMap, tPos...)
	PosMap = append(PosMap, dPos...)
	// check for not null columns
	for i := 0; i < len(prettyCols); i++ {
		if colIndexs[int(prettyCols[i].ID)] < 0 {
			if prettyCols[i].IsPrimaryTagCol() {
				var priTagNames []string
				for _, ptCols := range primaryTagCols {
					priTagNames = append(priTagNames, (*ptCols).Name)
				}
				err = pgerror.Newf(pgcode.Syntax, "need to specify all primary tag %v", priTagNames)
				return
			}
			if prettyCols[i].IsOrdinaryTagCol() && !prettyCols[i].Nullable {
				err = sqlbase.NewNonNullViolationError(prettyCols[i].Name)
				return
			}
			if !prettyCols[i].Nullable && len(dataCols) != 0 {
				err = sqlbase.NewNonNullViolationError(prettyCols[i].Name)
				return
			}
		}
	}

	tsVersion := uint32(1)
	pArgs, err = execbuilder.BuildPayloadArgs(tsVersion, primaryTagCols, otherTagCols, dataCols)
	return
}
