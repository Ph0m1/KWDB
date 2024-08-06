// Copyright 2019 The Cockroach Authors.
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

// cr2pg is a program that reads CockroachDB-formatted SQL files on stdin,
// modifies them to be Postgres compatible, and outputs them to stdout.
package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"

	"gitee.com/kwbasedb/kwbase/pkg/cmd/cr2pg/sqlstream"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)
	done := ctx.Done()

	readStmts := make(chan tree.Statement, 100)
	writeStmts := make(chan tree.Statement, 100)
	// Divide up work between parsing, filtering, and writing.
	g.Go(func() error {
		defer close(readStmts)
		stream := sqlstream.NewStream(os.Stdin)
		for {
			stmt, err := stream.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			select {
			case readStmts <- stmt:
			case <-done:
				return nil
			}
		}
		return nil
	})
	g.Go(func() error {
		defer close(writeStmts)
		newstmts := make([]tree.Statement, 8)
		for stmt := range readStmts {
			newstmts = newstmts[:1]
			newstmts[0] = stmt
			switch stmt := stmt.(type) {
			case *tree.CreateTable:
				stmt.Interleave = nil
				stmt.PartitionBy = nil
				var newdefs tree.TableDefs
				for _, def := range stmt.Defs {
					switch def := def.(type) {
					case *tree.FamilyTableDef:
						// skip
					case *tree.IndexTableDef:
						// Postgres doesn't support
						// indexes in CREATE TABLE,
						// so split them out to their
						// own statement.
						newstmts = append(newstmts, &tree.CreateIndex{
							Name:     def.Name,
							Table:    stmt.Table,
							Inverted: def.Inverted,
							Columns:  def.Columns,
							Storing:  def.Storing,
						})
					case *tree.UniqueConstraintTableDef:
						if def.PrimaryKey {
							// Postgres doesn't support descending PKs.
							for i, col := range def.Columns {
								if col.Direction != tree.Ascending {
									return errors.New("PK directions not supported by postgres")
								}
								def.Columns[i].Direction = tree.DefaultDirection
							}
							// Unset Name here because
							// constaint names cannot
							// be shared among tables,
							// so multiple PK constraints
							// named "primary" is an error.
							def.Name = ""
							newdefs = append(newdefs, def)
							break
						}
						newstmts = append(newstmts, &tree.CreateIndex{
							Name:     def.Name,
							Table:    stmt.Table,
							Unique:   true,
							Inverted: def.Inverted,
							Columns:  def.Columns,
							Storing:  def.Storing,
						})
					default:
						newdefs = append(newdefs, def)
					}
				}
				stmt.Defs = newdefs
			}
			for _, stmt := range newstmts {
				select {
				case writeStmts <- stmt:
				case <-done:
					return nil
				}
			}
		}
		return nil
	})
	g.Go(func() error {
		w := bufio.NewWriterSize(os.Stdout, 1024*1024)
		fmtctx := tree.NewFmtCtx(tree.FmtSimple)
		for stmt := range writeStmts {
			stmt.Format(fmtctx)
			_, _ = w.WriteString(fmtctx.CloseAndGetString())
			_, _ = w.WriteString(";\n\n")
		}
		w.Flush()
		return nil
	})
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
}
