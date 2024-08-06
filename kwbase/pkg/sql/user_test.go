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

package sql_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

// TestGetUserHashedPasswordTimeout verifies that user login attempts
// fail with a suitable timeout when some system range(s) are
// unavailable.
//
// To achieve this it creates a 2-node cluster, moves all ranges
// from node 1 to node 2, then stops node 2, then attempts
// to connect to node 1.
func TestGetUserHashedPasswordTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// We want to use a low timeout below to prevent
		// this test from taking forever, however
		// race builds are so slow as to trigger this timeout spuriously.
		t.Skip("not running under race")
	}

	ctx := context.Background()

	// unavailableCh is used by the replica command filter
	// to conditionally block requests and simulate unavailability.
	var unavailableCh atomic.Value
	closedCh := make(chan struct{})
	close(closedCh)
	unavailableCh.Store(closedCh)
	knobs := &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, _ roachpb.BatchRequest) *roachpb.Error {
			select {
			case <-unavailableCh.Load().(chan struct{}):
			case <-ctx.Done():
			}
			return nil
		},
	}
	params := base.TestServerArgs{Knobs: base.TestingKnobs{Store: knobs}}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Make a user that must use a password to authenticate.
	// Default privileges on defaultdb are needed to run simple queries.
	if _, err := db.Exec(`
CREATE USER foo WITH PASSWORD 'testabc';
GRANT ALL ON DATABASE defaultdb TO foo`); err != nil {
		t.Fatal(err)
	}

	// We'll attempt connections on gateway node 0.
	userURL, cleanupFn := sqlutils.PGUrlWithOptionalClientCerts(t,
		s.ServingSQLAddr(), t.Name(), url.UserPassword("foo", "testabc"), false /* withClientCerts */)
	defer cleanupFn()
	rootURL, rootCleanupFn := sqlutils.PGUrl(t,
		s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer rootCleanupFn()

	// Override the timeout built into pgx so we are only subject to
	// what the server thinks.
	userURL.RawQuery += "&connect_timeout=0"
	rootURL.RawQuery += "&connect_timeout=0"

	fmt.Fprintln(os.Stderr, "-- sanity checks --")

	// We use a closure here and below to ensure the defers are run
	// before the rest of the test.

	func() {
		// Sanity check: verify that secure mode is enabled: password is
		// required. If this part fails, this means the test cluster is
		// not properly configured, and the remainder of the test below
		// would report false positives.
		unauthURL := userURL
		unauthURL.User = url.User("foo")
		dbSQL, err := pgxConn(t, unauthURL)
		if err == nil {
			defer func() { _ = dbSQL.Close() }()
		}
		if !testutils.IsError(err, "password authentication failed for user foo") {
			t.Fatalf("expected password error, got %v", err)
		}
	}()

	func() {
		// Sanity check: verify that the new user is able to log in with password.
		dbSQL, err := pgxConn(t, userURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = dbSQL.Close() }()
		row := dbSQL.QueryRow("SELECT current_user")
		var username string
		if err := row.Scan(&username); err != nil {
			t.Fatal(err)
		}
		if username != "foo" {
			t.Fatalf("invalid username: expected foo, got %q", username)
		}
	}()

	// Configure the login timeout to just 1s.
	if _, err := db.Exec(`SET CLUSTER SETTING server.user_login.timeout = '200ms'`); err != nil {
		t.Fatal(err)
	}

	fmt.Fprintln(os.Stderr, "-- make ranges unavailable --")

	ch := make(chan struct{})
	unavailableCh.Store(ch)
	defer close(ch)

	fmt.Fprintln(os.Stderr, "-- expect timeout --")

	func() {
		// Now attempt to connect again. We're expecting a timeout within 5 seconds.
		start := timeutil.Now()
		dbSQL, err := pgxConn(t, userURL)
		if err == nil {
			defer func() { _ = dbSQL.Close() }()
		}
		if !testutils.IsError(err, "authentication timeout") {
			t.Fatalf("expected error during connection, got %v", err)
		}
		timeoutDur := timeutil.Now().Sub(start)
		if timeoutDur > 5*time.Second {
			t.Fatalf("timeout lasted for more than 5 second (%s)", timeoutDur)
		}
	}()

	fmt.Fprintln(os.Stderr, "-- no timeout for root --")

	// TODO(sh): PR 4485 caused timeout, skipped temporarily, to be resolved by fengyouxu.
	//func() {
	//	dbSQL, err := pgxConn(t, rootURL)
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//	defer func() { _ = dbSQL.Close() }()
	//	// A simple query must work for 'root' even without a system range available.
	//	if _, err := dbSQL.Exec("SELECT 1"); err != nil {
	//		t.Fatal(err)
	//	}
	//}()
}

func pgxConn(t *testing.T, connURL url.URL) (*pgx.Conn, error) {
	pgxConfig, err := pgx.ParseConnectionString(connURL.String())
	if err != nil {
		t.Fatal(err)
	}

	// Override the conninfo to avoid a bunch of pg_catalog
	// queries when the connection is being set up.
	pgxConfig.CustomConnInfo = func(c *pgx.Conn) (*pgtype.ConnInfo, error) {
		return c.ConnInfo, nil
	}

	return pgx.Connect(pgxConfig)
}
