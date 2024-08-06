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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestInitInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := startCmd.Flags()

	testCases := []struct {
		args     []string
		insecure bool
		expected string
	}{
		{[]string{}, false, ""},
		{[]string{"--insecure"}, true, ""},
		{[]string{"--insecure=true"}, true, ""},
		{[]string{"--insecure=false"}, false, ""},
		{[]string{"--listen-addr", "localhost"}, false, ""},
		{[]string{"--listen-addr", "127.0.0.1"}, false, ""},
		{[]string{"--listen-addr", "[::1]"}, false, ""},
		{[]string{"--listen-addr", "192.168.1.1"}, false,
			`specify --insecure to listen on external address 192\.168\.1\.1`},
		{[]string{"--insecure", "--listen-addr", "192.168.1.1"}, true, ""},
		{[]string{"--listen-addr", "localhost", "--advertise-addr", "192.168.1.1"}, false, ""},
		{[]string{"--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.1.1"}, false, ""},
		{[]string{"--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.1.1", "--advertise-port", "36259"}, false, ""},
		{[]string{"--listen-addr", "[::1]", "--advertise-addr", "192.168.1.1"}, false, ""},
		{[]string{"--listen-addr", "[::1]", "--advertise-addr", "192.168.1.1", "--advertise-port", "36259"}, false, ""},
		{[]string{"--insecure", "--listen-addr", "192.168.1.1", "--advertise-addr", "192.168.1.1"}, true, ""},
		{[]string{"--insecure", "--listen-addr", "192.168.1.1", "--advertise-addr", "192.168.2.2"}, true, ""},
		{[]string{"--insecure", "--listen-addr", "192.168.1.1", "--advertise-addr", "192.168.2.2", "--advertise-port", "36259"}, true, ""},
		// Clear out the flags when done to avoid affecting other tests that rely on the flag state.
		{[]string{"--listen-addr", "", "--advertise-addr", ""}, false, ""},
		{[]string{"--listen-addr", "", "--advertise-addr", "", "--advertise-port", ""}, false, ""},
	}
	for i, c := range testCases {
		// Reset the context and for every test case.
		startCtx.serverInsecure = false

		if err := f.Parse(c.args); err != nil {
			t.Fatal(err)
		}
		if c.insecure != startCtx.serverInsecure {
			t.Fatalf("%d: expected %v, but found %v", i, c.insecure, startCtx.serverInsecure)
		}
	}
}

func TestStartArgChecking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Avoid leaking configuration changes after the tests end.
	// In addition to the usual initCLIDefaults, we need to reset
	// the serverCfg because --store modifies it in a way that
	// initCLIDefaults does not restore.
	defer func(save server.Config) { serverCfg = save }(serverCfg)
	defer initCLIDefaults()

	f := startCmd.Flags()

	testCases := []struct {
		args     []string
		expected string
	}{
		{[]string{}, ``},
		{[]string{`--insecure=blu`}, `parsing "blu": invalid syntax`},
		{[]string{`--store=path=`}, `no value specified`},
		{[]string{`--store=path=blah,path=blih`}, `field was used twice`},
		{[]string{`--store=path=~/blah`}, `path cannot start with '~': ~/blah`},
		{[]string{`--store=path=./~/blah`}, ``},
		{[]string{`--store=size=blih`}, `could not parse store size`},
		{[]string{`--store=size=0.005`}, `store size \(0.005\) must be between 1.000000% and 100.000000%`},
		{[]string{`--store=size=100.5`}, `store size \(100.5\) must be between 1.000000% and 100.000000%`},
		{[]string{`--store=size=0.5%`}, `store size \(0.5%\) must be between 1.000000% and 100.000000%`},
		{[]string{`--store=size=0.5%`}, `store size \(0.5%\) must be between 1.000000% and 100.000000%`},
		{[]string{`--store=size=500.0%`}, `store size \(500.0%\) must be between 1.000000% and 100.000000%`},
		{[]string{`--store=size=.5,path=.`}, ``},
		{[]string{`--store=size=50.%,path=.`}, ``},
		{[]string{`--store=size=50%,path=.`}, ``},
		{[]string{`--store=size=50.5%,path=.`}, ``},
		{[]string{`--store=size=-1231MB`}, `store size \(-1231MB\) must be larger than`},
		{[]string{`--store=size=1231B`}, `store size \(1231B\) must be larger than`},
		{[]string{`--store=size=1231BLA`}, `unhandled size name: bla`},
		{[]string{`--store=attrs=bli:bli`}, `duplicate attribute`},
		{[]string{`--store=type=bli`}, `bli is not a valid store type`},
		{[]string{`--store=bla=bli`}, `bla is not a valid store field`},
		{[]string{`--store=size=123gb`}, `no path specified`},
		{[]string{`--store=type=mem`}, `size must be specified for an in memory store`},
		{[]string{`--store=type=mem,path=blah`}, `path specified for in memory store`},
		{[]string{"--store=type=mem,size=1GiB"}, ``},
	}
	for i, c := range testCases {
		// Reset the context and insecure flag for every test case.
		err := f.Parse(c.args)
		if !testutils.IsError(err, c.expected) {
			t.Errorf("%d: expected %q, but found %v", i, c.expected, err)
		}
	}
}

func TestAddrWithDefaultHost(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		inAddr  string
		outAddr string
	}{
		{"localhost:123", "localhost:123"},
		{":123", "localhost:123"},
		{"[::1]:123", "[::1]:123"},
	}

	for _, test := range testData {
		addr, err := addrWithDefaultHost(test.inAddr)
		if err != nil {
			t.Error(err)
		} else if addr != test.outAddr {
			t.Errorf("expected %q, got %q", test.outAddr, addr)
		}
	}
}
