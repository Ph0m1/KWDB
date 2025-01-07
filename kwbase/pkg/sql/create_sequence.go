// Copyright 2015 The Cockroach Authors.
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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

type createSequenceNode struct {
	n      *tree.CreateSequence
	dbDesc *sqlbase.DatabaseDescriptor
}

func (p *planner) CreateSequence(ctx context.Context, n *tree.CreateSequence) (planNode, error) {
	dbDesc, err := p.ResolveUncachedDatabase(ctx, &n.Name)
	if err != nil {
		return nil, err
	}

	if err1 := TSDatabaseUnsupportedErr(dbDesc.EngineType, "create sequence"); err1 != nil {
		return nil, err1
	}
	if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createSequenceNode{
		n:      n,
		dbDesc: dbDesc,
	}, nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE SEQUENCE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createSequenceNode) ReadingOwnWrites() {}

func (n *createSequenceNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("sequence"))
	isTemporary := n.n.Temporary

	_, schemaID, err := getTableCreateParams(params, n.dbDesc.ID, isTemporary, n.n.Name)
	if err != nil {
		if sqlbase.IsRelationAlreadyExistsError(err) && n.n.IfNotExists {
			return nil
		}
		return err
	}

	return doCreateSequence(
		params, n.n.String(), n.dbDesc, schemaID, &n.n.Name, isTemporary, n.n.Options,
		tree.AsStringWithFQNames(n.n, params.Ann()), false,
	)
}

// doCreateSequence performs the creation of a sequence in KV. The
// context argument is a string to use in the event log.
func doCreateSequence(
	params runParams,
	context string,
	dbDesc *DatabaseDescriptor,
	schemaID sqlbase.ID,
	name *ObjectName,
	isTemporary bool,
	opts tree.SequenceOptions,
	jobDesc string,
	isSerial bool,
) error {
	id, err := GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB)
	if err != nil {
		return err
	}

	// Inherit permissions from the database descriptor.
	privs := dbDesc.GetPrivileges()

	if isTemporary {
		telemetry.Inc(sqltelemetry.CreateTempSequenceCounter)
	}

	time, err := params.creationTimeForNewTableDescriptor()
	if err != nil {
		return err
	}

	desc, err := MakeSequenceTableDesc(
		name.Table(),
		opts,
		dbDesc.ID,
		schemaID,
		id,
		time,
		privs,
		isTemporary,
		&params,
	)
	if err != nil {
		return err
	}
	desc.SequenceOpts.IsSerial = isSerial
	// makeSequenceTableDesc already validates the table. No call to
	// desc.ValidateTable() needed here.

	key := sqlbase.MakeObjectNameKey(
		params.ctx,
		params.ExecCfg().Settings,
		dbDesc.ID,
		schemaID,
		name.Table(),
	).Key()
	if err = params.p.createDescriptorWithID(
		params.ctx, key, id, &desc, params.EvalContext().Settings, jobDesc,
	); err != nil {
		return err
	}

	// Initialize the sequence value.
	seqValueKey := keys.MakeSequenceKey(uint32(id))
	b := &kv.Batch{}
	b.Inc(seqValueKey, desc.SequenceOpts.Start-desc.SequenceOpts.Increment)
	if err := params.p.txn.Run(params.ctx, b); err != nil {
		return err
	}

	if err := desc.Validate(params.ctx, params.p.txn); err != nil {
		return err
	}

	// Log Create Sequence event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	params.p.SetAuditTarget(uint32(desc.GetID()), desc.GetName(), nil)
	return nil
}

func (*createSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (*createSequenceNode) Values() tree.Datums          { return tree.Datums{} }
func (*createSequenceNode) Close(context.Context)        {}

const (
	sequenceColumnID   = 1
	sequenceColumnName = "value"
)

// MakeSequenceTableDesc creates a sequence descriptor.
func MakeSequenceTableDesc(
	sequenceName string,
	sequenceOptions tree.SequenceOptions,
	parentID sqlbase.ID,
	schemaID sqlbase.ID,
	id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	isTemporary bool,
	params *runParams,
) (sqlbase.MutableTableDescriptor, error) {
	desc := InitTableDescriptor(
		id,
		parentID,
		schemaID,
		sequenceName,
		creationTime,
		privileges,
		isTemporary,
		tree.RelationalTable,
		"",
	)

	// Mimic a table with one column, "value".
	desc.Columns = []sqlbase.ColumnDescriptor{
		{
			ID:   1,
			Name: sequenceColumnName,
			Type: *types.Int,
		},
	}
	desc.PrimaryIndex = sqlbase.IndexDescriptor{
		ID:               keys.SequenceIndexID,
		Name:             sqlbase.PrimaryKeyIndexName,
		ColumnIDs:        []sqlbase.ColumnID{sqlbase.ColumnID(1)},
		ColumnNames:      []string{sequenceColumnName},
		ColumnDirections: []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC},
	}
	desc.Families = []sqlbase.ColumnFamilyDescriptor{
		{
			ID:              keys.SequenceColumnFamilyID,
			ColumnIDs:       []sqlbase.ColumnID{1},
			ColumnNames:     []string{sequenceColumnName},
			Name:            "primary",
			DefaultColumnID: sequenceColumnID,
		},
	}

	// Fill in options, starting with defaults then overriding.
	opts := &sqlbase.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	err := assignSequenceOptions(opts, sequenceOptions, true /* setDefaults */, params, id)
	if err != nil {
		return desc, err
	}
	desc.SequenceOpts = opts

	// A sequence doesn't have dependencies and thus can be made public
	// immediately.
	desc.State = sqlbase.TableDescriptor_PUBLIC

	return desc, desc.ValidateTable()
}
