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

package cloud

import (
	"context"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/sysutil"
	"github.com/stretchr/testify/require"
)

var econnrefused = &net.OpError{Err: &os.SyscallError{
	Syscall: "test",
	Err:     sysutil.ECONNREFUSED,
}}

var econnreset = &net.OpError{Err: &os.SyscallError{
	Syscall: "test",
	Err:     sysutil.ECONNRESET,
}}

type antagonisticDialer struct {
	net.Dialer
	rnd               *rand.Rand
	numRepeatFailures *int
}

type antagonisticConn struct {
	net.Conn
	rnd               *rand.Rand
	numRepeatFailures *int
}

func (d *antagonisticDialer) DialContext(
	ctx context.Context, network, addr string,
) (net.Conn, error) {
	if network == "tcp" && addr == "storage.googleapis.com:443" {
		if *d.numRepeatFailures < maxNoProgressReads && d.rnd.Int()%2 == 0 {
			*(d.numRepeatFailures)++
			return nil, econnrefused
		}
		c, err := d.Dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		return &antagonisticConn{Conn: c, rnd: d.rnd, numRepeatFailures: d.numRepeatFailures}, nil
	}
	return d.Dialer.DialContext(ctx, network, addr)
}

func (c *antagonisticConn) Read(b []byte) (int, error) {
	if *c.numRepeatFailures < maxNoProgressReads && c.rnd.Int()%2 == 0 {
		*(c.numRepeatFailures)++
		return 0, econnreset
	}
	return c.Conn.Read(b)
}

func TestAntagonisticRead(t *testing.T) {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		// This test requires valid GS credential file.
		return
	}

	rnd, _ := randutil.NewPseudoRand()
	failures := 0
	// Override DialContext implementation in http transport.
	dialer := &antagonisticDialer{rnd: rnd, numRepeatFailures: &failures}

	// Override transport to return antagonistic connection.
	transport := http.DefaultTransport.(*http.Transport)
	transport.DialContext =
		func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		}
	transport.DisableKeepAlives = true

	defer func() {
		transport.DialContext = nil
		transport.DisableKeepAlives = false
	}()

	gsFile := "gs://kwbase-fixtures/tpch-csv/sf-1/region.tbl?AUTH=implicit"
	conf, err := ExternalStorageConfFromURI(gsFile)
	require.NoError(t, err)

	s, err := MakeExternalStorage(
		context.TODO(), conf, base.ExternalIOConfig{}, testSettings, nil)
	require.NoError(t, err)
	stream, err := s.ReadFile(context.TODO(), "")
	require.NoError(t, err)
	defer stream.Close()
	_, err = ioutil.ReadAll(stream)
	require.NoError(t, err)
}
