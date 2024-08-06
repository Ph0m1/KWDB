// Copyright 2017 The Cockroach Authors.
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

package security_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestManagerWithEmbedded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cm, err := security.NewCertificateManager("test_certs")
	if err != nil {
		t.Error(err)
	}

	// Verify loaded certs.
	if cm.CACert() == nil {
		t.Error("expected non-nil CACert")
	}
	if cm.NodeCert() == nil {
		t.Error("expected non-nil NodeCert")
	}
	clientCerts := cm.ClientCerts()
	if a, e := len(clientCerts), 2; a != e {
		t.Errorf("expected %d client certs, found %d", e, a)
	}

	if _, ok := clientCerts[security.RootUser]; !ok {
		t.Error("no client cert for root user found")
	}

	// Verify that we can build tls.Config objects.
	if _, err := cm.GetServerTLSConfig(); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(security.NodeUser); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig(security.RootUser); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig("testuser"); err != nil {
		t.Error(err)
	}
	if _, err := cm.GetClientTLSConfig("my-random-user"); err == nil {
		t.Error("unexpected success")
	}
}

func TestManagerWithPrincipalMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Do not mock cert access for this test.
	security.ResetAssetLoader()
	defer ResetTest()

	defer func() { _ = security.SetCertPrincipalMap(nil) }()
	defer func() {
		_ = os.Setenv("KWBASE_CERT_NODE_USER", security.NodeUser)
		envutil.ClearEnvCache()
	}()
	require.NoError(t, os.Setenv("KWBASE_CERT_NODE_USER", "node.kwdb.io"))

	certsDir, err := ioutil.TempDir("", "certs_test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(certsDir))
	}()

	caKey := filepath.Join(certsDir, "ca.key")
	require.NoError(t, security.CreateCAPair(
		certsDir, caKey, testKeySize, time.Hour*96, true, true,
	))
	require.NoError(t, security.CreateClientPair(
		certsDir, caKey, testKeySize, time.Hour*48, true, "testuser", false,
	))
	require.NoError(t, security.CreateNodePair(
		certsDir, caKey, testKeySize, time.Hour*48, true, []string{"127.0.0.1", "foo"},
	))

	setCertPrincipalMap := func(s string) {
		require.NoError(t, security.SetCertPrincipalMap(strings.Split(s, ",")))
	}
	newCertificateManager := func() error {
		_, err := security.NewCertificateManager(certsDir)
		return err
	}
	loadUserCert := func(user string) error {
		cm, err := security.NewCertificateManager(certsDir)
		if err != nil {
			return err
		}
		ci := cm.ClientCerts()[user]
		if ci == nil {
			return fmt.Errorf("user %q not found", user)
		}
		return ci.Error
	}

	setCertPrincipalMap("")
	require.Regexp(t, `node certificate has principals \["node.kwdb.io" "foo"\]`, newCertificateManager())

	// We can map the "node.kwdb.io" principal to "node".
	setCertPrincipalMap("node.kwdb.io:node")
	require.NoError(t, newCertificateManager())

	// We can map the "foo" principal to "node".
	setCertPrincipalMap("foo:node")
	require.NoError(t, newCertificateManager())

	// Mapping the "testuser" principal to a different name should result in an
	// error as it no longer matches the file name.
	setCertPrincipalMap("testuser:foo,node.kwdb.io:node")
	require.Regexp(t, `client certificate has principals \["foo"\], expected "testuser"`, loadUserCert("testuser"))

	// Renaming "client.testuser.crt" to "client.foo.crt" allows us to load it
	// under that name.
	require.NoError(t, os.Rename(filepath.Join(certsDir, "client.testuser.crt"),
		filepath.Join(certsDir, "client.foo.crt")))
	require.NoError(t, os.Rename(filepath.Join(certsDir, "client.testuser.key"),
		filepath.Join(certsDir, "client.foo.key")))
	setCertPrincipalMap("testuser:foo,node.kwdb.io:node")
	require.NoError(t, loadUserCert("foo"))
}
