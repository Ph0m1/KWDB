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

package cli

import (
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const (
	chunkSize = 4 * 1024
)

var nodeLocalUploadCmd = &cobra.Command{
	Use:   "upload <source> <destination>",
	Short: "Upload file from source to destination",
	Long: `
Uploads a file to a gateway node's local file system using a SQL connection.
`,
	Args: cobra.MinimumNArgs(2),
	RunE: maybeShoutError(runUpload),
}

func runUpload(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("kwbase nodelocal", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	source := args[0]
	destination := args[1]
	reader, err := openSourceFile(source)
	if err != nil {
		return err
	}
	defer reader.Close()
	return uploadFile(conn, reader, destination)
}

func openSourceFile(source string) (io.ReadCloser, error) {
	f, err := os.Open(source)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get source file stats for %s", source)
	}
	if stat.IsDir() {
		return nil, fmt.Errorf("source file %s is a directory, not a file", source)
	}
	return f, nil
}

func uploadFile(conn *sqlConn, reader io.Reader, destination string) error {
	if err := conn.ensureConn(); err != nil {
		return err
	}

	if _, err := conn.conn.Exec(`BEGIN`, nil); err != nil {
		return err
	}

	stmt, err := conn.conn.Prepare(sql.CopyInFileStmt(destination, "kwdb_internal", "file_upload"))
	if err != nil {
		return err
	}

	defer func() {
		if stmt != nil {
			_ = stmt.Close()
			_, _ = conn.conn.Exec(`ROLLBACK`, nil)
		}
	}()

	send := make([]byte, chunkSize)
	for {
		n, err := reader.Read(send)
		if n > 0 {
			_, err = stmt.Exec([]driver.Value{string(send[:n])})
			if err != nil {
				return err
			}
		} else if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	if err := stmt.Close(); err != nil {
		return err
	}
	stmt = nil

	if _, err := conn.conn.Exec(`COMMIT`, nil); err != nil {
		return err
	}

	nodeID, _, _, err := conn.getServerMetadata()
	if err != nil {
		return errors.Wrap(err, "unable to get node id")
	}
	fmt.Printf("successfully uploaded to nodelocal://%s\n", filepath.Join(nodeID.String(), destination))
	return nil
}

var nodeLocalCmds = []*cobra.Command{
	nodeLocalUploadCmd,
}

var nodeLocalCmd = &cobra.Command{
	Use:   "nodelocal [command]",
	Short: "upload and delete nodelocal files",
	Long:  "Upload and delete files on the gateway node's local file system.",
	RunE:  usageAndErr,
}

func init() {
	nodeLocalCmd.AddCommand(nodeLocalCmds...)
}
