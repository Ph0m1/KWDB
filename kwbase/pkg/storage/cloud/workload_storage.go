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

package cloud

import (
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/workload"
	"github.com/cockroachdb/errors"
)

type workloadStorage struct {
	conf  *roachpb.ExternalStorage_Workload
	gen   workload.Generator
	table workload.Table
}

var _ ExternalStorage = &workloadStorage{}

func makeWorkloadStorage(conf *roachpb.ExternalStorage_Workload) (ExternalStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("workload upload requested but info missing")
	}
	if strings.ToLower(conf.Format) != `csv` {
		return nil, errors.Errorf(`unsupported format: %s`, conf.Format)
	}
	meta, err := workload.Get(conf.Generator)
	if err != nil {
		return nil, err
	}
	// Different versions of the workload could generate different data, so
	// disallow this.
	if meta.Version != conf.Version {
		return nil, errors.Errorf(
			`expected %s version "%s" but got "%s"`, meta.Name, conf.Version, meta.Version)
	}
	gen := meta.New()
	if f, ok := gen.(workload.Flagser); ok {
		if err := f.Flags().Parse(conf.Flags); err != nil {
			return nil, errors.Wrapf(err, `parsing parameters %s`, strings.Join(conf.Flags, ` `))
		}
	}
	s := &workloadStorage{
		conf: conf,
		gen:  gen,
	}
	for _, t := range gen.Tables() {
		if t.Name == conf.Table {
			s.table = t
			break
		}
	}
	if s.table.Name == `` {
		return nil, errors.Wrapf(err, `unknown table %s for generator %s`, conf.Table, meta.Name)
	}
	return s, nil
}

func (s *workloadStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:       roachpb.ExternalStorageProvider_Workload,
		WorkloadConfig: s.conf,
	}
}

func (s *workloadStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	if basename != `` {
		return nil, errors.Errorf(`basenames are not supported by workload storage`)
	}
	r := workload.NewCSVRowsReader(s.table, int(s.conf.BatchBegin), int(s.conf.BatchEnd))
	return ioutil.NopCloser(r), nil
}

func (s *workloadStorage) Seek(
	ctx context.Context, filename string, offset int64, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return nil, errors.New("workloadStorage does not support seek")
}

func (s *workloadStorage) WriteFile(_ context.Context, _ string, _ io.ReadSeeker) error {
	return errors.Errorf(`workload storage does not support writes`)
}

func (s *workloadStorage) AppendWrite(
	ctx context.Context, file string, content io.ReadSeeker,
) error {
	return errors.New("workload does not support additional writing")
}

func (s *workloadStorage) ListFiles(_ context.Context, _ string) ([]string, error) {
	return nil, errors.Errorf(`workload storage does not support listing files`)
}

func (s *workloadStorage) Delete(_ context.Context, _ string) error {
	return errors.Errorf(`workload storage does not support deletes`)
}
func (s *workloadStorage) Size(_ context.Context, _ string) (int64, error) {
	return 0, errors.Errorf(`workload storage does not support sizing`)
}
func (s *workloadStorage) Close() error {
	return nil
}

// ParseWorkloadConfig parses a workload config URI to a proto config.
func ParseWorkloadConfig(uri *url.URL) (*roachpb.ExternalStorage_Workload, error) {
	c := &roachpb.ExternalStorage_Workload{}
	pathParts := strings.Split(strings.Trim(uri.Path, `/`), `/`)
	if len(pathParts) != 3 {
		return nil, errors.Errorf(
			`path must be of the form /<format>/<generator>/<table>: %s`, uri.Path)
	}
	c.Format, c.Generator, c.Table = pathParts[0], pathParts[1], pathParts[2]
	q := uri.Query()
	if _, ok := q[`version`]; !ok {
		return nil, errors.New(`parameter version is required`)
	}
	c.Version = q.Get(`version`)
	q.Del(`version`)
	if s := q.Get(`row-start`); len(s) > 0 {
		q.Del(`row-start`)
		var err error
		if c.BatchBegin, err = strconv.ParseInt(s, 10, 64); err != nil {
			return nil, err
		}
	}
	if e := q.Get(`row-end`); len(e) > 0 {
		q.Del(`row-end`)
		var err error
		if c.BatchEnd, err = strconv.ParseInt(e, 10, 64); err != nil {
			return nil, err
		}
	}
	for k, vs := range q {
		for _, v := range vs {
			c.Flags = append(c.Flags, `--`+k+`=`+v)
		}
	}
	return c, nil
}
