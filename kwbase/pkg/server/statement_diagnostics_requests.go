// Copyright 2020 The Cockroach Authors.
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

package server

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

type stmtDiagnosticsRequest struct {
	ID                     int
	StatementFingerprint   string
	Completed              bool
	StatementDiagnosticsID int
	RequestedAt            time.Time
}

type stmtDiagnostics struct {
	ID                   int
	StatementFingerprint string
	Trace                string
	CollectedAt          time.Time
}

func (request *stmtDiagnosticsRequest) toProto() serverpb.StatementDiagnosticsReport {
	resp := serverpb.StatementDiagnosticsReport{
		Id:                     int64(request.ID),
		Completed:              request.Completed,
		StatementFingerprint:   request.StatementFingerprint,
		StatementDiagnosticsId: int64(request.StatementDiagnosticsID),
		RequestedAt:            request.RequestedAt,
	}
	return resp
}

func (diagnostics *stmtDiagnostics) toProto() serverpb.StatementDiagnostics {
	resp := serverpb.StatementDiagnostics{
		Id:                   int64(diagnostics.ID),
		StatementFingerprint: diagnostics.StatementFingerprint,
		CollectedAt:          diagnostics.CollectedAt,
		Trace:                diagnostics.Trace,
	}
	return resp
}

// CreateStatementDiagnosticsRequest creates a statement diagnostics
// request in the `system.statement_diagnostics_requests` table
// to trace the next query matching the provided fingerprint.
func (s *statusServer) CreateStatementDiagnosticsReport(
	ctx context.Context, req *serverpb.CreateStatementDiagnosticsReportRequest,
) (*serverpb.CreateStatementDiagnosticsReportResponse, error) {
	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.CreateStatementDiagnosticsReportResponse{
		Report: &serverpb.StatementDiagnosticsReport{},
	}

	err := s.stmtDiagnosticsRequester.InsertRequest(ctx, req.StatementFingerprint)
	if err != nil {
		return nil, err
	}

	response.Report.StatementFingerprint = req.StatementFingerprint
	return response, nil
}

// StatementDiagnosticsRequests retrieves all of the statement
// diagnostics requests in the `system.statement_diagnostics_requests` table.
func (s *statusServer) StatementDiagnosticsRequests(
	ctx context.Context, req *serverpb.StatementDiagnosticsReportsRequest,
) (*serverpb.StatementDiagnosticsReportsResponse, error) {
	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	var err error

	// TODO(davidh): Add pagination to this request.
	rows, err := s.internalExecutor.QueryEx(ctx, "stmt-diag-get-all", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{
			User: security.RootUser,
		},
		`SELECT
			id,
			statement_fingerprint,
			completed,
			statement_diagnostics_id,
			requested_at
		FROM
			system.statement_diagnostics_requests`)
	if err != nil {
		return nil, err
	}

	requests := make([]stmtDiagnosticsRequest, len(rows))
	for i, row := range rows {
		id := int(*row[0].(*tree.DInt))
		statementFingerprint := string(*row[1].(*tree.DString))
		completed := bool(*row[2].(*tree.DBool))
		req := stmtDiagnosticsRequest{
			ID:                   id,
			StatementFingerprint: statementFingerprint,
			Completed:            completed,
		}

		if row[3] != tree.DNull {
			sdi := int(*row[3].(*tree.DInt))
			req.StatementDiagnosticsID = sdi
		}

		if requestedAt, ok := row[4].(*tree.DTimestampTZ); ok {
			req.RequestedAt = requestedAt.Time
		}

		requests[i] = req
	}

	response := &serverpb.StatementDiagnosticsReportsResponse{
		Reports: make([]serverpb.StatementDiagnosticsReport, len(requests)),
	}

	for i, request := range requests {
		response.Reports[i] = request.toProto()
	}
	return response, nil
}

// StatementDiagnostics retrieves a statement diagnostics instance
// identified by the given ID. These are in the
// `system.statement_diagnostics` table.
//
// This is generated once the trace is completed on a request created
// by the CreateStatementDiagnosticsRequest call and is linked to
// the original request with its ID.
func (s *statusServer) StatementDiagnostics(
	ctx context.Context, req *serverpb.StatementDiagnosticsRequest,
) (*serverpb.StatementDiagnosticsResponse, error) {
	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	var err error
	row, err := s.internalExecutor.QueryRowEx(ctx, "stmt-diag-get-one", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{
			User: security.RootUser,
		},
		`SELECT
			id,
			statement_fingerprint,
			collected_at,
			trace
		FROM
			system.statement_diagnostics
		WHERE
			id = $1`, req.StatementDiagnosticsId)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Newf(
			"requested a statement diagnostic (%d) that does not exist",
			req.StatementDiagnosticsId,
		)
	}

	diagnostics := stmtDiagnostics{
		ID: int(req.StatementDiagnosticsId),
	}

	if statementFingerprint, ok := row[1].(*tree.DString); ok {
		diagnostics.StatementFingerprint = statementFingerprint.String()
	}

	if collectedAt, ok := row[2].(*tree.DTimestampTZ); ok {
		diagnostics.CollectedAt = collectedAt.Time
	}

	if traceJSON, ok := row[3].(*tree.DJSON); ok {
		traceJSONString, err := traceJSON.AsText()
		if err != nil {
			return nil, err
		}
		diagnostics.Trace = *traceJSONString
	}

	diagnosticsProto := diagnostics.toProto()
	response := &serverpb.StatementDiagnosticsResponse{
		Diagnostics: &diagnosticsProto,
	}

	return response, nil
}
