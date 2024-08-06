// Copyright 2020 The Cockroach Authors.
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

package fs

import "io"

// File and FS are a partial attempt at offering the Pebble vfs.FS interface. Given the constraints
// of the RocksDB Env interface we've chosen to only include what is easy to implement. Additionally,
// it does not try to subsume all the file related functionality already in the Engine interface.
// It seems preferable to do a final cleanup only when the implementation can simply use Pebble's
// implementation of vfs.FS. At that point the following interface will become a superset of vfs.FS.
type File interface {
	io.ReadWriteCloser
	io.ReaderAt
	Sync() error
}

// FS provides a filesystem interface.
type FS interface {
	// CreateFile creates the named file for writing, truncating it if it already
	// exists.
	CreateFile(name string) (File, error)

	// CreateFileWithSync is similar to CreateFile, but the file is periodically
	// synced whenever more than bytesPerSync bytes accumulate. This syncing
	// does not provide any persistency guarantees, but can prevent latency
	// spikes.
	CreateFileWithSync(name string, bytesPerSync int) (File, error)

	// LinkFile creates newname as a hard link to the oldname file.
	LinkFile(oldname, newname string) error

	// OpenFile opens the named file for reading.
	OpenFile(name string) (File, error)

	// OpenDir opens the named directory for syncing.
	OpenDir(name string) (File, error)

	// DeleteFile removes the named file. If the file with given name doesn't exist, return an error
	// that returns true from os.IsNotExist().
	DeleteFile(name string) error

	// RenameFile renames a file. It overwrites the file at newname if one exists,
	// the same as os.Rename.
	RenameFile(oldname, newname string) error

	// CreateDir creates the named dir. Does nothing if the directory already
	// exists.
	CreateDir(name string) error

	// DeleteDir removes the named dir.
	DeleteDir(name string) error

	// DeleteDirAndFiles deletes the directory and any files it contains but
	// not subdirectories. If dir does not exist, DeleteDirAndFiles returns nil
	// (no error).
	DeleteDirAndFiles(dir string) error

	// ListDir returns a listing of the given directory. The names returned are
	// relative to the directory.
	ListDir(name string) ([]string, error)
}
