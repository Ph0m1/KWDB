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
	"context"
	gosql "database/sql"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/workload"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func init() {
	AddSubCmd(func(userFacing bool) *cobra.Command {
		var checkCmd = SetCmdDefaults(&cobra.Command{
			Use:   `check`,
			Short: `check a running cluster's data for consistency`,
		})
		for _, meta := range workload.Registered() {
			gen := meta.New()
			if hooks, ok := gen.(workload.Hookser); !ok || hooks.Hooks().CheckConsistency == nil {
				continue
			}

			var genFlags *pflag.FlagSet
			if f, ok := gen.(workload.Flagser); ok {
				genFlags = f.Flags().FlagSet
				// Hide irrelevant flags so they don't clutter up the help text, but
				// don't remove them entirely so if someone switches from
				// `./workload run` to `./workload check` they don't have to remove
				// them from the invocation.
				for flagName, meta := range f.Flags().Meta {
					if meta.RuntimeOnly && !meta.CheckConsistencyOnly {
						_ = genFlags.MarkHidden(flagName)
					}
				}
			}

			genCheckCmd := SetCmdDefaults(&cobra.Command{
				Use:  meta.Name + ` [KWDB URI]`,
				Args: cobra.RangeArgs(0, 1),
			})
			genCheckCmd.Flags().AddFlagSet(genFlags)
			genCheckCmd.Run = CmdHelper(gen, check)
			checkCmd.AddCommand(genCheckCmd)
		}
		return checkCmd
	})
}

func check(gen workload.Generator, urls []string, dbName string) error {
	ctx := context.Background()

	var fn func(context.Context, *gosql.DB) error
	if hooks, ok := gen.(workload.Hookser); ok {
		fn = hooks.Hooks().CheckConsistency
	}
	if fn == nil {
		return errors.Errorf(`no consistency checks are defined for %s`, gen.Meta().Name)
	}

	sqlDB, err := gosql.Open(`kwbase`, strings.Join(urls, ` `))
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	if err := sqlDB.Ping(); err != nil {
		return err
	}
	return fn(ctx, sqlDB)
}
