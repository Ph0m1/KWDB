// Copyright 2015 The Cockroach Authors.
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

package resolver

import (
	"context"
	"errors"
	"net"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"github.com/stretchr/testify/require"
)

func TestParseResolverAddress(t *testing.T) {
	def := ensureHostPort(":", base.DefaultPort)
	testCases := []struct {
		input           string
		success         bool
		resolverType    string
		resolverAddress string
	}{
		// Ports are not checked at parsing time. They are at GetAddress time though.
		{"127.0.0.1:26222", true, "tcp", "127.0.0.1:26222"},
		{":" + base.DefaultPort, true, "tcp", def},
		{"127.0.0.1", true, "tcp", "127.0.0.1:" + base.DefaultPort},
		{"", false, "", ""},
		{"", false, "tcp", ""},
		{":", true, "tcp", def},
	}

	for tcNum, tc := range testCases {
		resolver, err := NewResolver(tc.input)
		if (err == nil) != tc.success {
			t.Errorf("#%d: expected success=%t, got err=%v", tcNum, tc.success, err)
		}
		if err != nil {
			continue
		}
		if resolver.Type() != tc.resolverType {
			t.Errorf("#%d: expected resolverType=%s, got %+v", tcNum, tc.resolverType, resolver)
		}
		if resolver.Addr() != tc.resolverAddress {
			t.Errorf("#%d: expected resolverAddress=%s, got %+v", tcNum, tc.resolverAddress, resolver)
		}
	}
}

func TestGetAddress(t *testing.T) {
	testCases := []struct {
		address      string
		success      bool
		addressType  string
		addressValue string
	}{
		{"127.0.0.1:26222", true, "tcp", "127.0.0.1:26222"},
		{"127.0.0.1", true, "tcp", "127.0.0.1:" + base.DefaultPort},
		{"localhost:80", true, "tcp", "localhost:80"},
	}

	for tcNum, tc := range testCases {
		resolver, err := NewResolver(tc.address)
		if err != nil {
			t.Fatal(err)
		}
		address, err := resolver.GetAddress()
		if (err == nil) != tc.success {
			t.Errorf("#%d: expected success=%t, got err=%v", tcNum, tc.success, err)
		}
		if err != nil {
			continue
		}
		if address.Network() != tc.addressType {
			t.Errorf("#%d: expected address type=%s, got %+v", tcNum, tc.addressType, address)
		}
		if address.String() != tc.addressValue {
			t.Errorf("#%d: expected address value=%s, got %+v", tcNum, tc.addressValue, address)
		}
	}
}

func TestSRV(t *testing.T) {
	type lookupFunc func(service, proto, name string) (string, []*net.SRV, error)

	lookupWithErr := func(err error) lookupFunc {
		return func(service, proto, name string) (string, []*net.SRV, error) {
			if service != "" || proto != "" {
				t.Errorf("unexpected params in erroring LookupSRV() call")
			}
			return "", nil, err
		}
	}

	dnsErr := &net.DNSError{Err: "no such host", Name: "", Server: "", IsTimeout: false}

	lookupSuccess := func(service, proto, name string) (string, []*net.SRV, error) {
		if service != "" || proto != "" {
			t.Errorf("unexpected params in successful LookupSRV() call")
		}

		srvs := []*net.SRV{
			{Target: "node1", Port: 26222},
			{Target: "node2", Port: 35222},
			{Target: "node3", Port: 0},
		}

		return "cluster", srvs, nil
	}

	expectedAddrs := []string{"node1:26222", "node2:35222"}

	testCases := []struct {
		address  string
		lookuper lookupFunc
		want     []string
	}{
		{":26222", nil, nil},
		{"some.host", lookupWithErr(dnsErr), nil},
		{"some.host", lookupWithErr(errors.New("another error")), nil},
		{"some.host", lookupSuccess, expectedAddrs},
		{"some.host:26222", lookupSuccess, expectedAddrs},
		// "real" `lookupSRV` returns "no such host" when resolving IP addresses
		{"127.0.0.1", lookupWithErr(dnsErr), nil},
		{"127.0.0.1:26222", lookupWithErr(dnsErr), nil},
		{"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]", lookupWithErr(dnsErr), nil},
		{"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:26222", lookupWithErr(dnsErr), nil},
	}

	for tcNum, tc := range testCases {
		func() {
			defer TestingOverrideSRVLookupFn(tc.lookuper)()

			resolvers, err := SRV(context.TODO(), tc.address)

			if err != nil {
				t.Errorf("#%d: expected success, got err=%v", tcNum, err)
			}

			require.Equal(t, tc.want, resolvers, "Test #%d failed", tcNum)

		}()
	}
}
