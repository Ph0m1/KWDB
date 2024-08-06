// Copyright 2016 The Cockroach Authors.
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

package cli

import (
	"fmt"
	"os"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/workload"
	// Register the relevant examples
	_ "gitee.com/kwbasedb/kwbase/pkg/workload/examples"
	"gitee.com/kwbasedb/kwbase/pkg/workload/workloadsql"
	"github.com/spf13/cobra"
)

var genExamplesCmd = &cobra.Command{
	Use:   "example-data",
	Short: "generate example SQL code suitable for use with KwDB",
	Long: `This command generates example SQL code that shows various KwDB features and
is suitable to populate an example database for demonstration and education purposes.
`,
}

func init() {
	for _, meta := range workload.Registered() {
		gen := meta.New()
		genExampleCmd := &cobra.Command{
			Use:   meta.Name,
			Short: meta.Description,
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				runGenExamplesCmd(gen)
				return nil
			},
		}
		if f, ok := gen.(workload.Flagser); ok {
			genExampleCmd.Flags().AddFlagSet(f.Flags().FlagSet)
		}
		genExamplesCmd.AddCommand(genExampleCmd)
	}
}

func runGenExamplesCmd(gen workload.Generator) {
	w := os.Stdout

	meta := gen.Meta()
	fmt.Fprintf(w, "CREATE DATABASE IF NOT EXISTS %s;\n", meta.Name)
	fmt.Fprintf(w, "SET DATABASE=%s;\n", meta.Name)
	for _, table := range gen.Tables() {
		fmt.Fprintf(w, "DROP TABLE IF EXISTS \"%s\";\n", table.Name)
		fmt.Fprintf(w, "CREATE TABLE \"%s\" %s;\n", table.Name, table.Schema)
		for rowIdx := 0; rowIdx < table.InitialRows.NumBatches; rowIdx++ {
			for _, row := range table.InitialRows.BatchRows(rowIdx) {
				rowTuple := strings.Join(workloadsql.StringTuple(row), `,`)
				fmt.Fprintf(w, "INSERT INTO \"%s\" VALUES (%s);\n", table.Name, rowTuple)
			}
		}
	}

	fmt.Fprint(w, footerComment)
}

const footerComment = `--
--
-- If you can see this message, you probably want to redirect the output of
-- 'kwbase gen example-data' to a file, or pipe it as input to 'kwbase sql'.
`
