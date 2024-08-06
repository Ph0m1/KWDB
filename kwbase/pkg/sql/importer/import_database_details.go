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

package importer

import (
	"context"
	"io/ioutil"
	"os"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// checkAndGetDetailsInDatabase parse files[0] to stmts.
// The first return parameter indicates whether time series data is being imported.
// The second return parameter represents the database name.
// The third return parameter represents the schema names.
// The fourth return parameter represents tableDetails.
// If time series data is imported, all SQL statements are written in the meta.sql directory in the root directory.
// If the data is imported relationally, the SQL of each table is written in the meta.sql directory of the corresponding subdirectory/.
func checkAndGetDetailsInDatabase(
	ctx context.Context, p sql.PlanHookState, files []string,
) (bool, string, []string, []sqlbase.ImportTable, error) {
	var err error
	// The whole database import supports only one file directory
	if len(files) != 1 {
		return false, "", nil, nil, errors.Errorf("Database import does not support multiple file paths，%q", files)
	}
	dbPath := files[0]

	externalStorageFromURI := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	dbStore, err := externalStorageFromURI(ctx, dbPath+string(os.PathSeparator))
	if err != nil {
		return false, "", nil, nil, err
	}
	defer dbStore.Close()
	// read DB SQL file
	reader, err := dbStore.ReadFile(ctx, "meta.sql")
	if err != nil {
		return false, "", nil, nil, err
	}
	defer reader.Close()
	databaseDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return false, "", nil, nil, err
	}
	stmts, err := parser.Parse(string(databaseDefStr))
	if err != nil {
		return false, "", nil, nil, err
	}
	dbCreate, ok := stmts[0].AST.(*tree.CreateDatabase)
	if !ok {
		return false, "", nil, nil, errors.New("The first sql CREATE DATABASE SQL has errors")
	}
	// timeseries database
	if dbCreate.EngineType == tree.EngineTypeTimeseries {
		var tableDetails []sqlbase.ImportTable
		for i := range stmts {
			if i == 0 {
				// skip create db stmt
				continue
			}
			tbCreate, ok := stmts[i].AST.(*tree.CreateTable)
			if !ok {
				return false, "", nil, nil, errors.New("expected CREATE TABLE statement in database file")
			}
			tableDetails = append(tableDetails, sqlbase.ImportTable{Create: stmts[i].SQL, IsNew: true, TableName: tbCreate.Table.Table(), UsingSource: tbCreate.UsingSource.Table(), TableType: tbCreate.TableType})
		}
		return true, dbCreate.Name.String(), nil, tableDetails, nil
	}
	// relational database
	scNames, tableDetails, err := readTablesInDbFromStore(ctx, p, dbPath, stmts)
	if err != nil {
		return false, "", nil, nil, err
	}
	return false, dbCreate.Name.String(), scNames, tableDetails, nil
}

// readTablesInDbFromStore reads the SQL file from the subdirectory and generate tableDetails.
func readTablesInDbFromStore(
	ctx context.Context, p sql.PlanHookState, dbPath string, stmts parser.Statements,
) ([]string, []sqlbase.ImportTable, error) {
	var scNames []string
	var tableDetails []sqlbase.ImportTable
	externalStorageFromURI := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	dbPath = dbPath + string(os.PathSeparator)
	dbStore, err := externalStorageFromURI(ctx, dbPath)
	if err != nil {
		return nil, nil, err
	}
	defer dbStore.Close()
	if len(stmts) != 1 {
		return nil, nil, errors.Errorf("expected 1 create database statement, found %d", len(stmts))
	}
	// dbPath, get all files in dbPath
	filesInDbDir, err := dbStore.ListFiles(ctx, "*")
	if err != nil {
		return nil, nil, err
	}
	for _, scFolderName := range filesInDbDir {
		// find dbSQL
		if scFolderName == "meta.sql" {
			continue
		}
		// skip public, because we don't need to create public schema
		// In the scenario of exporting DB, each schema does not read meta.sql
		// and names schemaName directly with the folder.
		if scFolderName != tree.PublicSchema {
			scNames = append(scNames, scFolderName)
		}
		// schemaPath
		scPath := dbPath + scFolderName + string(os.PathSeparator)
		scStore, err := externalStorageFromURI(ctx, scPath)
		if err != nil {
			return nil, nil, err
		}
		defer scStore.Close()
		// dbPath, get all files in dbPath
		filesInScDir, err := scStore.ListFiles(ctx, "*")
		if err != nil {
			return nil, nil, err
		}
		for _, tbFolderName := range filesInScDir {
			if tbFolderName == "meta.sql" { // SC SQL
				continue
			}
			// table SQL
			tbReader, err := scStore.ReadFile(ctx, tbFolderName+string(os.PathSeparator)+"meta.sql")
			if err != nil {
				return nil, nil, err
			}
			defer tbReader.Close()
			tableDefStr, err := ioutil.ReadAll(tbReader)
			if err != nil {
				return nil, nil, err
			}
			stmts, err = parser.Parse(string(tableDefStr))
			if err != nil {
				return nil, nil, err
			}
			if len(stmts) != 1 {
				return nil, nil, errors.Errorf("expected 1 create table statement, found %d", len(stmts))
			}
			tbCreate, ok := stmts[0].AST.(*tree.CreateTable)
			if !ok {
				return nil, nil, errors.New("expected CREATE TABLE statement in database file")
			}
			if err = checkCreateTableLegal(ctx, tbCreate); err != nil {
				return nil, nil, err
			}
			tableDetails = append(tableDetails, sqlbase.ImportTable{Create: stmts[0].SQL, IsNew: true, TableName: tbCreate.Table.Table(), SchemaName: scFolderName})
		}
	}
	if tableDetails == nil {
		return nil, nil, errors.Errorf("cannot import an empty database")
	}
	return scNames, tableDetails, nil
}
