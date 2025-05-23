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

package sql

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

// setExplainBundleResult creates the diagnostics and returns the bundle
// information for an EXPLAIN ANALYZE (DEBUG) statement.
//
// Returns an error if information rows couldn't be added to the result.
func setExplainBundleResult(
	ctx context.Context,
	res RestrictedCommandResult,
	ast tree.Statement,
	trace tracing.Recording,
	plan *planTop,
	placeholders *tree.PlaceholderInfo,
	ie *InternalExecutor,
	execCfg *ExecutorConfig,
) error {
	res.ResetStmtType(&tree.ExplainAnalyzeDebug{})
	res.SetColumns(ctx, sqlbase.ExplainAnalyzeDebugColumns)

	var text []string
	func() {
		bundle, err := buildStatementBundle(ctx, execCfg.DB, ie, plan, trace, placeholders)
		if err != nil {
			// TODO(radu): we cannot simply set an error on the result here without
			// changing the executor logic (e.g. an implicit transaction could have
			// committed already). Just show the error in the result.
			text = []string{fmt.Sprintf("Error generating bundle: %v", err)}
			return
		}

		fingerprint := tree.AsStringWithFlags(ast, tree.FmtHideConstants)
		stmtStr := tree.AsString(ast)

		diagID, err := execCfg.StmtDiagnosticsRecorder.InsertStatementDiagnostics(
			ctx,
			fingerprint,
			stmtStr,
			bundle.trace,
			bundle.zip,
		)
		if err != nil {
			text = []string{fmt.Sprintf("Error recording bundle: %v", err)}
			return
		}

		text = []string{
			"Statement diagnostics bundle generated. Download from the Admin UI (Advanced",
			"Debug -> Statement Diagnostics History), via the direct link below, or using",
			"the command line.",
			fmt.Sprintf("Admin UI: %s", execCfg.AdminURL()),
			fmt.Sprintf("Direct link: %s/_admin/v1/stmtbundle/%d", execCfg.AdminURL(), diagID),
			"Command line: kwbase statement-diag list / download",
		}
	}()

	if err := res.Err(); err != nil {
		// Add the bundle information as a detail to the query error.
		//
		// TODO(radu): if the statement gets auto-retried, we will generate a
		// bundle for each iteration. If the statement eventually succeeds we
		// will have a link to the last iteration's bundle. It's not clear what
		// the ideal behavior is here; if we keep all bundles we should try to
		// list them all in the final message.
		res.SetError(errors.WithDetail(err, strings.Join(text, "\n")))
		return nil
	}

	for _, line := range text {
		if err := res.AddRow(ctx, tree.Datums{tree.NewDString(line)}); err != nil {
			return err
		}
	}
	return nil
}

// traceToJSON converts a trace to a JSON datum suitable for the
// system.statement_diagnostics.trace column. In case of error, the returned
// datum is DNull. Also returns the string representation of the trace.
//
// traceToJSON assumes that the first span in the recording contains all the
// other spans.
func traceToJSON(trace tracing.Recording) (tree.Datum, string, error) {
	root := normalizeSpan(trace[0], trace)
	marshaller := jsonpb.Marshaler{
		Indent: "\t",
	}
	str, err := marshaller.MarshalToString(&root)
	if err != nil {
		return tree.DNull, "", err
	}
	d, err := tree.ParseDJSON(str)
	if err != nil {
		return tree.DNull, "", err
	}
	return d, str, nil
}

func normalizeSpan(s tracing.RecordedSpan, trace tracing.Recording) tracing.NormalizedSpan {
	var n tracing.NormalizedSpan
	n.Operation = s.Operation
	n.StartTime = s.StartTime
	n.Duration = s.Duration
	n.Tags = s.Tags
	n.Logs = s.Logs

	for _, ss := range trace {
		if ss.ParentSpanID != s.SpanID {
			continue
		}
		n.Children = append(n.Children, normalizeSpan(ss, trace))
	}
	return n
}

// diagnosticsBundle contains diagnostics information collected for a statement.
type diagnosticsBundle struct {
	zip   []byte
	trace tree.Datum
}

// buildStatementBundle collects metadata related the planning and execution of
// the statement. It generates a bundle for storage in
// system.statement_diagnostics.
func buildStatementBundle(
	ctx context.Context,
	db *kv.DB,
	ie *InternalExecutor,
	plan *planTop,
	trace tracing.Recording,
	placeholders *tree.PlaceholderInfo,
) (diagnosticsBundle, error) {
	if plan == nil {
		return diagnosticsBundle{}, errors.AssertionFailedf("execution terminated early")
	}
	b := makeStmtBundleBuilder(db, ie, plan, trace, placeholders)

	b.addStatement()
	b.addOptPlans()
	b.addExecPlan()
	b.addDistSQLDiagrams()
	traceJSON := b.addTrace()
	b.addEnv(ctx)

	buf, err := b.finalize()
	if err != nil {
		return diagnosticsBundle{}, err
	}
	return diagnosticsBundle{trace: traceJSON, zip: buf.Bytes()}, nil
}

// stmtBundleBuilder is a helper for building a statement bundle.
type stmtBundleBuilder struct {
	db *kv.DB
	ie *InternalExecutor

	plan         *planTop
	trace        tracing.Recording
	placeholders *tree.PlaceholderInfo

	z memZipper
}

func makeStmtBundleBuilder(
	db *kv.DB,
	ie *InternalExecutor,
	plan *planTop,
	trace tracing.Recording,
	placeholders *tree.PlaceholderInfo,
) stmtBundleBuilder {
	b := stmtBundleBuilder{db: db, ie: ie, plan: plan, trace: trace, placeholders: placeholders}
	b.z.Init()
	return b
}

// addStatement adds the pretty-printed statement as file statement.txt.
func (b *stmtBundleBuilder) addStatement() {
	cfg := tree.DefaultPrettyCfg()
	cfg.UseTabs = false
	cfg.LineWidth = 100
	cfg.TabWidth = 2
	cfg.Simplify = true
	cfg.Align = tree.PrettyNoAlign
	cfg.JSONFmt = true
	var output string
	// If we hit an early error, stmt or stmt.AST might not be initialized yet.
	switch {
	case b.plan.stmt == nil:
		output = "No Statement."
	case b.plan.stmt.AST == nil:
		output = "No AST."
	default:
		output = cfg.Pretty(b.plan.stmt.AST)
	}

	if b.placeholders != nil && len(b.placeholders.Values) != 0 {
		var buf bytes.Buffer
		buf.WriteString(output)
		buf.WriteString("\n\nArguments:\n")
		for i, v := range b.placeholders.Values {
			fmt.Fprintf(&buf, "  %s: %v\n", tree.PlaceholderIdx(i), v)
		}
		output = buf.String()
	}

	b.z.AddFile("statement.txt", output)
}

// addOptPlans adds the EXPLAIN (OPT) variants as files opt.txt, opt-v.txt,
// opt-vv.txt.
func (b *stmtBundleBuilder) addOptPlans() {
	if b.plan.mem == nil || b.plan.mem.RootExpr() == nil {
		// No optimizer plans; an error must have occurred during planning.
		return
	}

	formatOptPlan := func(flags memo.ExprFmtFlags) string {
		f := memo.MakeExprFmtCtx(flags, b.plan.mem, b.plan.catalog)
		f.FormatExpr(b.plan.mem.RootExpr())
		return f.Buffer.String()
	}

	b.z.AddFile("opt.txt", formatOptPlan(memo.ExprFmtHideAll))
	b.z.AddFile("opt-v.txt", formatOptPlan(
		memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes,
	))
	b.z.AddFile("opt-vv.txt", formatOptPlan(memo.ExprFmtHideQualifications))
}

// addExecPlan adds the EXPLAIN (VERBOSE) plan as file plan.txt.
func (b *stmtBundleBuilder) addExecPlan() {
	if plan := b.plan.instrumentation.planString; plan != "" {
		b.z.AddFile("plan.txt", plan)
	}
}

func (b *stmtBundleBuilder) addDistSQLDiagrams() {
	for i, d := range b.plan.distSQLDiagrams {
		d.AddSpans(b.trace, nil)
		_, url, err := d.ToURL()

		var contents string
		if err != nil {
			contents = err.Error()
		} else {
			contents = fmt.Sprintf(`<meta http-equiv="Refresh" content="0; url=%s">`, url.Fragment)
		}

		var filename string
		if len(b.plan.distSQLDiagrams) == 1 {
			filename = "distsql.html"
		} else {
			// TODO(radu): it would be great if we could distinguish between
			// subqueries/main query/postqueries here.
			filename = fmt.Sprintf("distsql-%d.html", i+1)
		}
		b.z.AddFile(filename, contents)
	}
}

// addTrace adds two files to the bundle: one is a json representation of the
// trace, the other one is a human-readable representation.
func (b *stmtBundleBuilder) addTrace() tree.Datum {
	traceJSON, traceJSONStr, err := traceToJSON(b.trace)
	if err != nil {
		b.z.AddFile("trace.json", err.Error())
	} else {
		b.z.AddFile("trace.json", traceJSONStr)
	}

	cfg := tree.DefaultPrettyCfg()
	cfg.UseTabs = false
	cfg.LineWidth = 100
	cfg.TabWidth = 2
	cfg.Simplify = true
	cfg.Align = tree.PrettyNoAlign
	cfg.JSONFmt = true
	stmt := cfg.Pretty(b.plan.stmt.AST)

	// The JSON is not very human-readable, so we include another format too.
	b.z.AddFile("trace.txt", fmt.Sprintf("%s\n\n\n\n%s", stmt, b.trace.String()))

	// Note that we're going to include the non-anonymized statement in the trace.
	// But then again, nothing in the trace is anonymized.
	jaegerJSON, err := b.trace.ToJaegerJSON(stmt)
	if err != nil {
		b.z.AddFile("trace-jaeger.txt", err.Error())
	} else {
		b.z.AddFile("trace-jaeger.json", jaegerJSON)
	}

	return traceJSON
}

func (b *stmtBundleBuilder) addEnv(ctx context.Context) {
	c := makeStmtEnvCollector(ctx, b.ie)

	var buf bytes.Buffer
	if err := c.PrintVersion(&buf); err != nil {
		fmt.Fprintf(&buf, "-- error getting version: %v\n", err)
	}
	fmt.Fprintf(&buf, "\n")

	// Show the values of any non-default session variables that can impact
	// planning decisions.
	if err := c.PrintSettings(&buf); err != nil {
		fmt.Fprintf(&buf, "-- error getting settings: %v\n", err)
	}
	b.z.AddFile("env.sql", buf.String())

	mem := b.plan.mem
	if mem == nil {
		// No optimizer plans; an error must have occurred during planning.
		return
	}
	buf.Reset()

	var tables, sequences, views []tree.TableName
	err := b.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		tables, sequences, views, err = mem.Metadata().AllDataSourceNames(
			func(ds cat.DataSource) (cat.DataSourceName, error) {
				return b.plan.catalog.fullyQualifiedNameWithTxn(ctx, ds, txn)
			},
		)
		return err
	})
	if err != nil {
		b.z.AddFile("schema.sql", fmt.Sprintf("-- error getting data source names: %v\n", err))
		return
	}

	if len(tables) == 0 && len(sequences) == 0 && len(views) == 0 {
		return
	}

	first := true
	blankLine := func() {
		if !first {
			buf.WriteByte('\n')
		}
		first = false
	}
	for i := range sequences {
		blankLine()
		if err := c.PrintCreateSequence(&buf, &sequences[i]); err != nil {
			fmt.Fprintf(&buf, "-- error getting schema for sequence %s: %v\n", sequences[i].String(), err)
		}
	}
	for i := range tables {
		blankLine()
		if err := c.PrintCreateTable(&buf, &tables[i]); err != nil {
			fmt.Fprintf(&buf, "-- error getting schema for table %s: %v\n", tables[i].String(), err)
		}
	}
	for i := range views {
		blankLine()
		if err := c.PrintCreateView(&buf, &views[i]); err != nil {
			fmt.Fprintf(&buf, "-- error getting schema for view %s: %v\n", views[i].String(), err)
		}
	}
	b.z.AddFile("schema.sql", buf.String())
	for i := range tables {
		buf.Reset()
		if err := c.PrintTableStats(&buf, &tables[i], false /* hideHistograms */); err != nil {
			fmt.Fprintf(&buf, "-- error getting statistics for table %s: %v\n", tables[i].String(), err)
		}
		b.z.AddFile(fmt.Sprintf("stats-%s.sql", tables[i].String()), buf.String())
	}
}

// finalize generates the zipped bundle and returns it as a buffer.
func (b *stmtBundleBuilder) finalize() (*bytes.Buffer, error) {
	return b.z.Finalize()
}

// memZipper builds a zip file into an in-memory buffer.
type memZipper struct {
	buf *bytes.Buffer
	z   *zip.Writer
	err error
}

func (z *memZipper) Init() {
	z.buf = &bytes.Buffer{}
	z.z = zip.NewWriter(z.buf)
}

func (z *memZipper) AddFile(name string, contents string) {
	if z.err != nil {
		return
	}
	w, err := z.z.CreateHeader(&zip.FileHeader{
		Name:     name,
		Method:   zip.Deflate,
		Modified: timeutil.Now(),
	})
	if err != nil {
		z.err = err
		return
	}
	_, z.err = w.Write([]byte(contents))
}

func (z *memZipper) Finalize() (*bytes.Buffer, error) {
	if z.err != nil {
		return nil, z.err
	}
	if err := z.z.Close(); err != nil {
		return nil, err
	}
	buf := z.buf
	*z = memZipper{}
	return buf, nil
}

// stmtEnvCollector helps with gathering information about the "environment" in
// which a statement was planned or run: version, relevant session settings,
// schema, table statistics.
type stmtEnvCollector struct {
	ctx context.Context
	ie  *InternalExecutor
}

func makeStmtEnvCollector(ctx context.Context, ie *InternalExecutor) stmtEnvCollector {
	return stmtEnvCollector{ctx: ctx, ie: ie}
}

// environmentQuery is a helper to run a query that returns a single string
// value.
func (c *stmtEnvCollector) query(query string) (string, error) {
	var row tree.Datums
	row, err := c.ie.QueryRowEx(
		c.ctx,
		"stmtEnvCollector",
		nil, /* txn */
		sqlbase.NoSessionDataOverride,
		query,
	)
	if err != nil {
		return "", err
	}

	if len(row) != 1 {
		return "", errors.AssertionFailedf(
			"expected env query %q to return a single column, returned %d",
			query, len(row),
		)
	}

	s, ok := row[0].(*tree.DString)
	if !ok {
		return "", errors.AssertionFailedf(
			"expected env query %q to return a DString, returned %T",
			query, row[0],
		)
	}

	return string(*s), nil
}

var testingOverrideExplainEnvVersion string

// TestingOverrideExplainEnvVersion overrides the version reported by
// EXPLAIN (OPT, ENV). Used for testing.
func TestingOverrideExplainEnvVersion(ver string) func() {
	prev := testingOverrideExplainEnvVersion
	testingOverrideExplainEnvVersion = ver
	return func() { testingOverrideExplainEnvVersion = prev }
}

// PrintVersion appends a row of the form:
//
//	-- Version: CockroachDB CCL v20.1.0 ...
func (c *stmtEnvCollector) PrintVersion(w io.Writer) error {
	version, err := c.query("SELECT version()")
	if err != nil {
		return err
	}
	if testingOverrideExplainEnvVersion != "" {
		version = testingOverrideExplainEnvVersion
	}
	fmt.Fprintf(w, "-- Version: %s\n", version)
	return err
}

// PrintSettings appends information about session settings that can impact
// planning decisions.
func (c *stmtEnvCollector) PrintSettings(w io.Writer) error {
	relevantSettings := []struct {
		sessionSetting string
		clusterSetting settings.WritableSetting
	}{
		{sessionSetting: "reorder_joins_limit", clusterSetting: ReorderJoinsLimitClusterValue},
		{sessionSetting: "multi_model_reorder_joins_limit", clusterSetting: MultiModelReorderJoinsLimitClusterValue},
		{sessionSetting: "hash_scan_mode", clusterSetting: hashScanMode},
		{sessionSetting: "enable_multimodel", clusterSetting: multiModelClusterMode},
		{sessionSetting: "enable_zigzag_join", clusterSetting: zigzagJoinClusterMode},
		{sessionSetting: "optimizer_foreign_keys", clusterSetting: optDrivenFKClusterMode},
	}

	for _, s := range relevantSettings {
		value, err := c.query(fmt.Sprintf("SHOW %s", s.sessionSetting))
		if err != nil {
			return err
		}
		// Get the default value for the cluster setting.
		def := s.clusterSetting.EncodedDefault()
		// Convert true/false to on/off to match what SHOW returns.
		switch def {
		case "true":
			def = "on"
		case "false":
			def = "off"
		}

		if value == def {
			fmt.Fprintf(w, "-- %s has the default value: %s\n", s.sessionSetting, value)
		} else {
			fmt.Fprintf(w, "SET %s = %s;  -- default value: %s\n", s.sessionSetting, value, def)
		}
	}
	return nil
}

func (c *stmtEnvCollector) PrintCreateTable(w io.Writer, tn *tree.TableName) error {
	createStatement, err := c.query(
		fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE TABLE %s]", tn.String()),
	)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", createStatement)
	return nil
}

func (c *stmtEnvCollector) PrintCreateSequence(w io.Writer, tn *tree.TableName) error {
	createStatement, err := c.query(fmt.Sprintf(
		"SELECT create_statement FROM [SHOW CREATE SEQUENCE %s]", tn.String(),
	))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", createStatement)
	return nil
}

func (c *stmtEnvCollector) PrintCreateView(w io.Writer, tn *tree.TableName) error {
	createStatement, err := c.query(fmt.Sprintf(
		"SELECT create_statement FROM [SHOW CREATE VIEW %s]", tn.String(),
	))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", createStatement)
	return nil
}

func (c *stmtEnvCollector) PrintTableStats(
	w io.Writer, tn *tree.TableName, hideHistograms bool,
) error {
	var maybeRemoveHistoBuckets string
	if hideHistograms {
		maybeRemoveHistoBuckets = " - 'histo_buckets'"
	}

	stats, err := c.query(fmt.Sprintf(
		`SELECT jsonb_pretty(COALESCE(json_agg(stat), '[]'))
		 FROM (
			 SELECT json_array_elements(statistics)%s AS stat
			 FROM [SHOW STATISTICS USING JSON FOR TABLE %s]
		 )`,
		maybeRemoveHistoBuckets, tn.String(),
	))
	if err != nil {
		return err
	}

	stats = strings.Replace(stats, "'", "''", -1)
	fmt.Fprintf(w, "ALTER TABLE %s INJECT STATISTICS '%s';\n", tn.String(), stats)
	return nil
}
