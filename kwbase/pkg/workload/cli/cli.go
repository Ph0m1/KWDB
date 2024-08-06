// Copyright 2018 The Cockroach Authors.
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
	"os"

	"gitee.com/kwbasedb/kwbase/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// WorkloadCmd returns a new command that can serve as the root of the workload
// command tree.
func WorkloadCmd(userFacing bool) *cobra.Command {
	rootCmd := SetCmdDefaults(&cobra.Command{
		Use:   `workload`,
		Short: `generators for data and query loads`,
	})
	for _, subCmdFn := range subCmdFns {
		rootCmd.AddCommand(subCmdFn(userFacing))
	}
	if userFacing {
		allowlist := map[string]struct{}{
			`workload`: {},
			`init`:     {},
			`run`:      {},
		}
		for _, m := range workload.Registered() {
			allowlist[m.Name] = struct{}{}
		}
		var hideNonPublic func(c *cobra.Command)
		hideNonPublic = func(c *cobra.Command) {
			if _, ok := allowlist[c.Name()]; !ok {
				c.Hidden = true
			}
			for _, sub := range c.Commands() {
				hideNonPublic(sub)
			}
		}
		hideNonPublic(rootCmd)
	}
	return rootCmd
}

var subCmdFns []func(userFacing bool) *cobra.Command

// AddSubCmd adds a sub-command closure to the workload cli.
//
// Most commands are global singletons and hooked together in init functions but
// the workload ones do fancy things with making each generator in
// `workload.Registered` into a subcommand. This means the commands need to be
// generated after `workload.Registered` is fully populated, but the latter is
// now sometimes done in the outermost possible place (main.go) and so its init
// runs last. Instead, we return a closure that is called in the main function,
// which is guaranteed to run after all inits.
func AddSubCmd(fn func(userFacing bool) *cobra.Command) {
	subCmdFns = append(subCmdFns, fn)
}

// HandleErrs wraps a `RunE` cobra function to print an error but not the usage.
func HandleErrs(
	f func(cmd *cobra.Command, args []string) error,
) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		err := f(cmd, args)
		if err != nil {
			hint := errors.FlattenHints(err)
			cmd.Println("Error:", err.Error())
			if hint != "" {
				cmd.Println("Hint:", hint)
			}
			os.Exit(1)
		}
	}
}
