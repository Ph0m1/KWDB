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

package memo

import (
	"hash/fnv"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/lib/pq/oid"
)

// ExprPos type of expr position
// used for bo push down logical
// flag the expr position, for example ExprPosSelect flag expr exists
// selectExpr filters. ExprPosProjList flag expr exists projectExpr
// projections. push down can use it for can push down to bo
type ExprPos uint32

// flag of the expr position
const (
	ExprPosSelect   = 0
	ExprPosProjList = 1
	ExprPosGroupBy  = 2
	ExprPosNone     = 3
)

// ExprType all expr type
// used for bo push down logical
// push down can use it for can push down to bo
type ExprType uint32

// expr type flag
const (
	ExprTypConst   = 0  // const scalar, a value
	ExprTypCol     = 1  // single column, an original column
	ExprTypeFuncOp = 2  // function, a func expr
	ExprTypBinOp   = 3  // binary operate
	ExprTypCompOp  = 4  // compare operate
	ExprTypeAggOp  = 5  // aggregation operate
	ExprTypeNone   = 10 // unexpected flag
)

// ExprInfo to facilitate pushdown logic during SQL compilation
type ExprInfo struct {
	Alias        string   //name of expr
	Type         ExprType //type of expr
	Pos          ExprPos  //the position where expr appear, for whitelist checking
	Hash         uint32   //hash value for searching in white list
	IsTimeBucket bool     //IsTimeBucket is true when the expr is time_bucket function.
}

// PushHelper to associate column ID and expressions
type PushHelper struct {
	MetaMap MetaInfoMap

	// lock used when operate on map
	lock syncutil.Mutex
}

// Find finds the exprInfo obj from PushHelper.
// id is the key of MetaMap.
func (p *PushHelper) Find(id opt.ColumnID) (ExprInfo, bool) {
	p.lock.Lock()
	if v, ok := p.MetaMap[id]; ok {
		p.lock.Unlock()
		return v, ok
	}
	p.lock.Unlock()
	return ExprInfo{}, false
}

// MetaInfoMap records all logical column info
type MetaInfoMap map[opt.ColumnID]ExprInfo

// StringHash get hashcode from string
func StringHash(s string) uint32 {
	h := fnv.New32a()
	if _, err := h.Write([]byte(s)); err != nil {
		panic(err)
	}
	return h.Sum32()
}

// GetDataTypeName get data type name
// string family need detailed type string, other need family string
func GetDataTypeName(t *types.T) string {
	return strconv.Itoa(int(t.Oid()))
}

func getChildStr(src opt.Expr) string {
	str := ""
	if src.ChildCount() > 0 {
		for i := 0; i < src.ChildCount(); i++ {
			param, ok := src.Child(i).(opt.ScalarExpr)
			if !ok {
				return ""
			}
			// We need to handle Coalece expressions specifically here,
			// because the time series engine does not support more than two parameters
			if _, ok1 := src.(*CoalesceExpr); ok1 {
				if args, ok2 := param.(*ScalarListExpr); ok2 && len(*args) >= 3 {
					return ""
				}
			}
			// if any of the children is of time data type, check if it's interval data type
			// if it IS interval data type and includes month/year, we cannot push it down
			if param.DataType().SupportTimeCalc() {
				if timeCanPush(param) {
					str += GetDataTypeName(param.DataType())
				} else {
					str += GetDataTypeName(types.Interval)
				}
			} else if value, ok1 := param.(*TupleExpr); ok1 {
				for c := 0; c < value.Elems.ChildCount(); c++ {
					str += GetDataTypeName(value.Elems.Child(0).(opt.ScalarExpr).DataType())
				}
			} else {
				str += GetDataTypeName(param.DataType())
			}
		}
	}

	return str
}

// timeCanPush return true if interval type does not have month or year.
func timeCanPush(param opt.ScalarExpr) bool {
	if conExpr, ok := param.(*ConstExpr); ok {
		if interval, ok := conExpr.Value.(*tree.DInterval); ok && interval.Months != 0 {
			return false
		}
	}
	return true
}

// GetExprString gets the expr type name of itself and its children nodes, and return as string
func GetExprString(src opt.Expr) string {
	var str string
	if opt.IsComparisonOp(src) || opt.IsBinaryOp(src) {
		if opt.IsComparisonOp(src) {
			treeOp := opt.ComparisonOpReverseMap[src.Op()]
			str += treeOp.String()
		} else if opt.IsBinaryOp(src) {
			treeOp := opt.BinaryOpReverseMap[src.Op()]
			str += treeOp.String()
		}
		return str + getChildStr(src)
	} else if opt.IsAggregateOp(src) {
		return opt.AggregateOpReverseMap[src.Op()] + getChildStr(src)
	}

	switch source := src.(type) {
	case *VariableExpr:
		str = "column"
	case *ConstExpr:
		str = GetDataTypeName(source.Typ)
	case *FunctionExpr:
		str = source.FunctionPrivate.Name
		for _, param := range source.Args {
			str += GetDataTypeName(param.DataType())
		}
	default:
		str = src.Op().String()

		str += getChildStr(src)
	}

	return str
}

// GetExprHash get expr hash code
func GetExprHash(src opt.Expr) uint32 {
	if val, ok := src.(opt.ScalarExpr); ok {
		str := GetExprString(val)
		return StringHash(str)
	}
	return 0
}

// GetExprType get expr type code
func GetExprType(src opt.Expr) ExprType {
	if opt.IsConstValueOp(src) {
		return ExprTypConst
	} else if src.Op() == opt.VariableOp {
		return ExprTypCol
	} else if opt.IsBinaryOp(src) {
		return ExprTypBinOp
	} else if opt.IsComparisonOp(src) {
		return ExprTypCompOp
	} else if opt.IsAggregateOp(src) {
		return ExprTypeAggOp
	} else if src.Op() == opt.FunctionOp {
		if e, ok := src.(*FunctionExpr); ok && e.IsConstForLogicPlan {
			return ExprTypConst
		}
		return ExprTypeFuncOp
	}

	return ExprTypeNone
}

// CheckFunc is used in whitelist checking for given function
type CheckFunc func(key uint32, pos uint32) bool

// These types support exec in ts engine in castExpr
var castSupportType = []oid.Oid{oid.T_timestamp, oid.T_timestamptz, oid.T_int2, oid.T_int4, oid.T_int8,
	oid.T_float4, oid.T_float8, oid.T_bool, oid.T_bpchar, oid.T_bytea, types.T_nchar, oid.T_varchar, types.T_nvarchar, oid.T_varbytea}

// CheckExprCanExecInTSEngine checks whether expr and it's children exprs can be pushed down
// tupleCanExecInTSEngine is true when the tuple use in InExpr or NotInExpr, tuple expr only can exec in ts engine in this case,
// such as col in (1,2,3) or col in (col1,col2); other case such as COUNT(DISTINCT (channel, companyid)) can not exec in ts engine.
func CheckExprCanExecInTSEngine(
	src opt.Expr, pos int8, f CheckFunc, tupleCanExecInTSEngine bool,
) (bool, uint32) {
	// const and column can exec in ts engine
	if opt.IsConstValueOp(src) || src.Op() == opt.VariableOp ||
		(src.Op() == opt.TupleOp && src.ChildCount() > 0 && src.Child(0).ChildCount() > 0 && tupleCanExecInTSEngine) {
		return true, 0
	}

	if src.Op() == opt.SubqueryOp {
		return false, 0
	}
	// add the check for (not) in operator, that white list can add common, otherwise can only check (not )in any
	if src.Op() == opt.InOp || src.Op() == opt.NotInOp {
		for i := 0; i < src.ChildCount(); i++ {
			can, _ := CheckExprCanExecInTSEngine(src.Child(i), pos, f, true)
			if !can { // can not push down
				return false, 0
			}
		}
		treeOp := opt.ComparisonOpReverseMap[src.Op()]
		str := treeOp.String()
		if src.ChildCount() > 1 {
			if param, ok := src.Child(1).(*TupleExpr); ok && param.ChildCount() > 0 {
				val := param.Child(0)
				if list, ok1 := val.(*ScalarListExpr); ok1 {
					str += getChildStr(list.Child(0))
				}
			}
		}

		hashCode := StringHash(str)
		if nil != f && f(hashCode, uint32(pos)) {
			return true, hashCode
		}
		return false, 0
	}

	switch source := src.(type) {
	case *ScalarListExpr:
		for i := 0; i < len(*source); i++ {
			can, _ := CheckExprCanExecInTSEngine((*source)[i], pos, f, false)
			if !can { // can not push down
				return false, 0
			}
		}
		return true, 0
	case *FunctionExpr:
		if source.Properties.ForbiddenExecInTSEngine {
			return false, 0
		}
	case *CastExpr:
		typFlag := false
		// check returnType in CastExpr
		for _, tmp := range castSupportType {
			if tmp == source.Typ.Oid() {
				typFlag = true
				break
			}
		}
		if !typFlag {
			return false, 0
		}
	}

	if scalar, ok := src.(opt.ScalarExpr); ok && scalar.CheckConstDeductionEnabled() {
		return true, 0
	}

	// check white list to find out if src can be pushed down
	hashCode := GetExprHash(src)
	if nil != f && !f(hashCode, uint32(pos)) { // can not push down
		return false, hashCode
	}

	// check whether child exprs can exec in ts engine
	for i := 0; i < src.ChildCount(); i++ {
		if can, _ := CheckExprCanExecInTSEngine(src.Child(i), pos, f, false); !can { // can not push down
			return false, hashCode
		}
	}
	return true, hashCode
}

// CheckFilterExprCanExecInTSEngine  check filter condition can exec on ts engine
//
// params:
// - src: the expr that filter expr
// - pos: reference positions referencing this expr in where/having expressions
// - f: check func, for function checking in white list
//
// return
// - can execute on ts engine
func CheckFilterExprCanExecInTSEngine(src opt.Expr, pos ExprPos, f CheckFunc) bool {
	// operator and , or , range check is child can exec on ts engine
	if src.Op() == opt.AndOp || src.Op() == opt.OrOp || src.Op() == opt.RangeOp {
		var i int
		for i = 0; i < src.ChildCount(); i++ {
			if !CheckFilterExprCanExecInTSEngine(src.Child(i), pos, f) {
				return false
			}
		}

		return true
	}

	// check white list that can support by ts engine
	if can, _ := CheckExprCanExecInTSEngine(src, int8(pos), f, false); !can {
		return false
	}

	return true
}

// SplitTagExpr find all ts engine tag filter expr for ts engine execute tag index
// eg: create table t1(k_timestamp timestamptz not null, a int, b int) tags (type int not null, location varchar(10))
//
//	primary tags(type)
//	select sum(a) from t1 where type=1 and a = 10;
//	the expr is  [type=1 and location = 'shanghai']
//	 src is                                 AndExpr
//	            left: EqExpr (type=1)[tag filter]           right: EqExpr (a = 10)[leave]
//	  left:VariableExpr right:constExpr[int]        left:VariableExpr right:constExpr[int]
//	 leave expr is EqExpr (a = 10)[leave], tag filter is EqExpr (type=1)
//
// params:
// - src: memo expr struct
// - f: check func, for check in white list
//
// returns:
// - leave: leave expr for exec at up engine
// - tagFilter: tag expr filter, eg a > 10, b < 100, that can compose andexpr
func (m *Memo) SplitTagExpr(src opt.Expr, colMap TagColMap) (leave opt.Expr, tagFilter []opt.Expr) {
	// split and expr
	switch source := src.(type) {
	case *FiltersExpr:
		switch len(*source) {
		case 0:
			return src, nil
		default:
			var lev FiltersExpr
			for i := 0; i < len(*source); i++ {
				lea, tags := m.SplitTagExpr((*source)[i].Condition.(opt.Expr), colMap)
				if lea != nil {
					(*source)[i].Condition = lea.(opt.ScalarExpr)
					lev = append(lev, (*source)[i])
				}

				tagFilter = append(tagFilter, tags...)
			}
			if lev == nil {
				return nil, tagFilter
			}
			return &lev, tagFilter
		}
	case *FiltersItem:
		// Pass through the call.
		return m.SplitTagExpr(source.Condition, colMap)
	case *RangeExpr:
		return m.SplitTagExpr(source.And, colMap)
	case *AndExpr:
		lLeave, lTag := m.SplitTagExpr(source.Left, colMap)
		rLeave, rTag := m.SplitTagExpr(source.Right, colMap)
		if lLeave == nil && rLeave == nil {
			lTag = append(lTag, rTag...)
			return nil, lTag
		}
		for i := 0; i < len(lTag); i++ {
			tagFilter = append(tagFilter, lTag[i])
		}
		for i := 0; i < len(rTag); i++ {
			tagFilter = append(tagFilter, rTag[i])
		}
		if nil == lLeave {
			leave = rLeave
		} else if nil == rLeave {
			leave = lLeave
		} else {
			source.Left = lLeave.(opt.ScalarExpr)
			source.Right = rLeave.(opt.ScalarExpr)
			leave = source
		}
		return leave, tagFilter
	case *OrExpr:
		lLeave, _ := m.SplitTagExpr(source.Left, colMap)
		rLeave, _ := m.SplitTagExpr(source.Right, colMap)
		if lLeave == nil && rLeave == nil {
			// all or is a tag filter
			return nil, []opt.Expr{source}
		}

		return source, nil
	default:
		// check white list that can push down
		if push, _ := CheckExprCanExecInTSEngine(src, ExprPosSelect, m.GetWhiteList().CheckWhiteListParam, false); !push {
			return source, nil
		}

		mode := checkTagExpr(src, colMap)
		if mode == 1<<hasTag || mode == (1<<hasTag+1<<hasConst) {
			return nil, []opt.Expr{source}
		}

		return source, nil
	}
}

// constValueToString get const value to string slice
func constValueToString(expr *ConstExpr) string {
	resValue := expr.Value.String()
	switch expr := expr.Value.(type) {
	case *tree.DString:

		resValue = string(*expr)
	}
	return resValue
}

// appendToPTagValues append string to primary tag values
func appendToPTagValues(col uint32, expr *ConstExpr, val *PTagValues) {
	if val != nil {
		(*val)[col] = append((*val)[col], constValueToString(expr))
	}
}

// addConstValueToPTagValues get const value and add to PTagValues
// returning true if append happens, else false
func addConstValueToPTagValues(
	src1 opt.Expr, src2 opt.Expr, primaryTagCol TagColMap, val *PTagValues,
) bool {
	if col, ok := src1.(*VariableExpr); ok {
		switch src := src2.(type) {
		case *ConstExpr:
			if _, find := primaryTagCol[col.Col]; !find {
				return false
			}
			appendToPTagValues(uint32(col.Col), src, val)
			return true
		case *FalseExpr:
			if _, find := primaryTagCol[col.Col]; !find {
				return false
			}
			(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], tree.DBoolFalse.String())
			return true
		case *TrueExpr:
			if _, find := primaryTagCol[col.Col]; !find {
				return false
			}
			(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], tree.DBoolTrue.String())
			return true
		}
		return false
	}
	return false
}

// GetPrimaryTagValues get primary tag value
//
// params:
// - src: memo expr struct
// - primaryTagCol: primary tag col map, key is column id[logical] value is empty
// - PTagValues: primary tag value map, key is column id[logical] value is string
//
// returns:
// - get primary tag flag
func GetPrimaryTagValues(src opt.Expr, primaryTagCol TagColMap, val *PTagValues) bool {
	switch source := src.(type) {
	case *EqExpr:
		return addConstValueToPTagValues(source.Left, source.Right, primaryTagCol, val) ||
			addConstValueToPTagValues(source.Right, source.Left, primaryTagCol, val)
	case *InExpr:
		if col, ok := source.Left.(*VariableExpr); ok {
			if _, find := primaryTagCol[col.Col]; !find {
				return false
			}
			if tuple, ok1 := source.Right.(*TupleExpr); ok1 {
				var values []string
				for i := 0; i < len(tuple.Elems); i++ {
					if col1, ok1 := tuple.Elems[i].(*ConstExpr); ok1 {
						values = append(values, constValueToString(col1))
					} else {
						return false
					}
				}
				if val != nil {
					(*val)[uint32(col.Col)] = append((*val)[uint32(col.Col)], values...)
				}
				return true
			}
		}
	}

	return false
}

// tagExprMode tag expr mode
type tagExprMode uint32

// TagColMap tag col map
type TagColMap map[opt.ColumnID]struct{}

const (
	// hasConst expr of const flag
	hasConst = 1
	// hasTag expr of tag flag
	hasTag = 2
	// hasColumn expr of column flag
	hasColumn = 3
	// hasSubQuery sub query expr flag
	hasSubQuery = 4
)

// checkTagExpr check if expr can push down, including src expr self and children
func checkTagExpr(src opt.Expr, colMap TagColMap) tagExprMode {
	switch source := src.(type) {
	case *VariableExpr:
		if _, ok := colMap[source.Col]; ok {
			return 1 << hasTag
		}

		return 1 << hasColumn
	case *ConstExpr:
		return 1 << hasConst
	case *SubqueryExpr:
		return 1 << hasSubQuery
	default:
		var rMode tagExprMode
		for i := 0; i < src.ChildCount(); i++ {
			mode := checkTagExpr(src.Child(i), colMap)
			rMode |= mode
		}
		return rMode
	}
}
