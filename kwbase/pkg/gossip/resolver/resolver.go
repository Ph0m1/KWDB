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
	"fmt"
	"net"
	"os"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/netutil"
	"github.com/pkg/errors"
)

// Resolver is an interface which provides an abstract factory for
// net.Addr addresses. Resolvers are not thread safe.
type Resolver interface {
	Type() string
	Addr() string
	GetAddress() (net.Addr, error)
}

// NewResolver takes an address and returns a new resolver.
func NewResolver(address string) (Resolver, error) {
	if len(address) == 0 {
		return nil, errors.Errorf("invalid address value: %q", address)
	}

	// Ensure addr has port and host set.
	address = ensureHostPort(address, base.DefaultPort)
	return &socketResolver{typ: "tcp", addr: address}, nil
}

// SRV returns a slice of addresses from SRV record lookup
func SRV(ctx context.Context, name string) ([]string, error) {
	// Ignore port
	name, _, err := netutil.SplitHostPort(name, base.DefaultPort)
	if err != nil {
		return nil, err
	}

	if name == "" {
		return nil, nil
	}

	// "" as the addr and proto forces the direct look up of the name
	_, recs, err := lookupSRV("", "", name)
	if err != nil {
		if dnsErr, ok := err.(*net.DNSError); ok && dnsErr.Err == "no such host" {
			return nil, nil
		}

		if log.V(1) {
			log.Infof(context.TODO(), "failed to lookup SRV record for %q: %v", name, err)
		}

		return nil, nil
	}

	addrs := []string{}
	for _, r := range recs {
		if r.Port != 0 {
			addrs = append(addrs, net.JoinHostPort(r.Target, fmt.Sprintf("%d", r.Port)))
		}
	}

	return addrs, nil
}

// NewResolverFromAddress takes a net.Addr and constructs a resolver.
func NewResolverFromAddress(addr net.Addr) (Resolver, error) {
	switch addr.Network() {
	case "tcp":
		return &socketResolver{typ: addr.Network(), addr: addr.String()}, nil
	default:
		return nil, errors.Errorf("unknown address network %q for %v", addr.Network(), addr)
	}
}

// NewResolverFromUnresolvedAddr takes a util.UnresolvedAddr and constructs a resolver.
func NewResolverFromUnresolvedAddr(addr util.UnresolvedAddr) (Resolver, error) {
	return NewResolverFromAddress(&addr)
}

// ensureHostPort takes a host:port addr, where the host and port are optional. If host and port are
// present, the output is equal to addr. If port is not present, defaultPort is used. If host is not
// present, hostname (or "127.0.0.1" as a fallback) is used.
func ensureHostPort(addr string, defaultPort string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return net.JoinHostPort(addr, defaultPort)
	}
	if host == "" {
		host, err = os.Hostname()
		if err != nil {
			host = "127.0.0.1"
		}
	}
	if port == "" {
		port = defaultPort
	}

	return net.JoinHostPort(host, port)
}

var (
	lookupSRV = net.LookupSRV
)

// TestingOverrideSRVLookupFn enables a test to temporarily override
// the SRV lookup function.
func TestingOverrideSRVLookupFn(
	fn func(service, proto, name string) (cname string, addrs []*net.SRV, err error),
) func() {
	prevFn := lookupSRV
	lookupSRV = fn
	return func() { lookupSRV = prevFn }
}
