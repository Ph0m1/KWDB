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

package rules

import (
	"context"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

const (
	// AllStmt represents all statements
	AllStmt string = "ALL"
	// AllTar represents all target objects
	AllTar string = "ALL"
	// AllUser represents all users
	AllUser string = "ALL" // username
)

// AuditInquirers represents audit strategies
type AuditInquirers struct {
	Ie  *sql.InternalExecutor // used to wrap operation without transaction
	ctx context.Context       // accept context
}

// NewAuditStrategyInquirers constructs a new NewAuditStrategyInquirers.
func NewAuditStrategyInquirers(ctx context.Context, ie *sql.InternalExecutor) AuditInquirers {
	return AuditInquirers{
		ctx: ctx,
		Ie:  ie,
	}
}

// Strategy represents params of audit strategy
type Strategy struct {
	Name        string
	TargetType  string
	TargetID    uint32
	Operations  []string
	Operators   []string
	EventResult string
	Condition   int64
	Action      int64
	Level       int64
}

// QueryAuditPolicy query all audit strategies
func (ar AuditInquirers) QueryAuditPolicy(
	ctx context.Context, txn *kv.Txn,
) (map[string]Strategy, error) {
	strMap := map[string]Strategy{}
	var str Strategy
	query := `SELECT * FROM system.audits WHERE enable = true`
	rows, err := ar.Ie.Query(ctx, "init-audit", txn, query)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		str = queryResultToAuditPolicy(row)
		strMap[str.Name] = str
	}

	return strMap, nil
}

func queryResultToAuditPolicy(row tree.Datums) Strategy {
	name := string(*row[0].(*tree.DString))
	eventType := string(*row[1].(*tree.DString))
	id := uint32(*row[2].(*tree.DInt))
	operations := row[3].(*tree.DArray).Array
	operators := row[4].(*tree.DArray).Array
	condition := int64(*row[5].(*tree.DInt))
	eventResult := string(*row[6].(*tree.DString))
	action := int64(*row[7].(*tree.DInt))
	level := int64(*row[8].(*tree.DInt))

	s1 := make([]string, len(operations))
	for i, v := range operations {
		s1[i] = string(*v.(*tree.DString))
	}
	s2 := make([]string, len(operators))
	for i, v := range operators {
		s2[i] = string(*v.(*tree.DString))
	}

	return Strategy{
		Name:        name,
		TargetType:  eventType,
		TargetID:    id,
		Operations:  s1,
		Operators:   s2,
		EventResult: eventResult,
		Condition:   condition,
		Action:      action,
		Level:       level,
	}
}

// CheckAuditTargetID returns true if targetID matches
func (st *Strategy) CheckAuditTargetID(id uint32) bool {
	if st.TargetID == id {
		return true
	}
	return false
}

// CheckAuditTargetIDs returns true if targetIDs matches
func (st *Strategy) CheckAuditTargetIDs(ids []uint32) bool {
	for _, id := range ids {
		if st.CheckAuditTargetID(id) {
			return true
		}
	}

	return false
}

// CheckAuditTargetType returns true if targetType matches
func (st *Strategy) CheckAuditTargetType(targetType target.AuditObjectType) bool {
	if strings.ToUpper(st.TargetType) == AllTar {
		return true
	}
	if strings.ToUpper(st.TargetType) == strings.ToUpper(string(targetType)) {
		return true
	}
	return false
}

// CheckAuditOperation returns true if operation matches
func (st *Strategy) CheckAuditOperation(operation target.OperationType) bool {
	for _, op := range st.Operations {
		userOper := strings.ToUpper(op)
		if userOper == string(target.AllOper) || userOper == strings.ToUpper(string(operation)) {
			return true
		}
	}
	return false
}

// CheckAuditUser returns true if user matches
func (st *Strategy) CheckAuditUser(user string) bool {
	for _, uname := range st.Operators {
		if uname == AllUser { // all users
			return true
		}
		if uname == user {
			return true
		}
	}
	return false
}

// CheckEventResult returns true if result matches
func (st *Strategy) CheckEventResult(result string) bool {
	if strings.ToUpper(st.EventResult) == AllStmt {
		return true
	}
	return strings.Contains(strings.ToUpper(result), strings.ToUpper(st.EventResult))
}
