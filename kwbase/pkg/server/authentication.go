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

package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// authPrefix is the prefix for RESTful endpoints used to provide
	// authentication methods.
	loginPath  = "/login"
	logoutPath = "/logout"
	// secretLength is the number of random bytes generated for session secrets.
	secretLength = 16
	// SessionCookieName is the name of the cookie used for HTTP auth.
	SessionCookieName = "session"
)

var webSessionTimeout = settings.RegisterPublicNonNegativeDurationSetting(
	"server.web_session_timeout",
	"the duration that a newly created web session will be valid",
	7*24*time.Hour,
)

// LoginManager to record failed user login time
type LoginManager struct {
	mu       syncutil.Mutex
	failures map[string][]time.Time
}

type authenticationServer struct {
	server     *Server
	memMetrics *sql.MemoryMetrics
	lm         *LoginManager
}

// newAuthenticationServer allocates and returns a new REST server for
// authentication APIs.
func newAuthenticationServer(s *Server) *authenticationServer {
	return &authenticationServer{
		server:     s,
		memMetrics: &s.adminMemMetrics,
		lm: &LoginManager{
			failures: make(map[string][]time.Time),
		},
	}
}

// RegisterService registers the GRPC service.
func (s *authenticationServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterLogInServer(g, s)
	serverpb.RegisterLogOutServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *authenticationServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	if err := serverpb.RegisterLogInHandler(ctx, mux, conn); err != nil {
		return err
	}
	return serverpb.RegisterLogOutHandler(ctx, mux, conn)
}

// UserLogin verifies an incoming request by a user to create an web
// authentication session. It checks the provided credentials against the
// system.users table, and if successful creates a new authentication session.
// The session's ID and secret are returned to the caller as an HTTP cookie,
// added via a "Set-Cookie" header.
func (s *authenticationServer) UserLogin(
	ctx context.Context, req *serverpb.UserLoginRequest,
) (*serverpb.UserLoginResponse, error) {
	username := req.Username
	if username == "" {
		return nil, status.Errorf(
			codes.Unauthenticated,
			"no username was provided",
		)
	}

	// Verify the provided username/password pair.
	verified, expired, err := s.verifyPassword(ctx, username, req.Password)
	if err != nil {
		return nil, apiInternalError(ctx, err)
	}
	if expired {
		return nil, status.Errorf(
			codes.Unauthenticated,
			"the password for %s has expired",
			username,
		)
	}
	if !verified {
		return nil, status.Errorf(
			codes.Unauthenticated,
			"the provided username and password did not match any credentials on the server",
		)
	}

	// Create a new database session, generating an ID and secret key.
	id, secret, err := s.newAuthSession(ctx, username)
	if err != nil {
		return nil, apiInternalError(ctx, err)
	}

	// Generate and set a session cookie for the response. Because HTTP cookies
	// must be strings, the cookie value (a marshaled protobuf) is encoded in
	// base64.
	cookieValue := &serverpb.SessionCookie{
		ID:     id,
		Secret: secret,
	}
	cookie, err := EncodeSessionCookie(cookieValue, !s.server.cfg.DisableTLSForHTTP)
	if err != nil {
		return nil, apiInternalError(ctx, err)
	}

	// Set the cookie header on the outgoing response.
	if err := grpc.SetHeader(ctx, metadata.Pairs("set-cookie", cookie.String())); err != nil {
		return nil, apiInternalError(ctx, err)
	}

	return &serverpb.UserLoginResponse{}, nil
}

// UserLogout allows a user to terminate their currently active session.
func (s *authenticationServer) UserLogout(
	ctx context.Context, req *serverpb.UserLogoutRequest,
) (*serverpb.UserLogoutResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, apiInternalError(ctx, fmt.Errorf("couldn't get incoming context"))
	}
	sessionIDs := md.Get(webSessionIDKeyStr)
	if len(sessionIDs) != 1 {
		return nil, apiInternalError(ctx, fmt.Errorf("couldn't get incoming context"))
	}

	sessionID, err := strconv.Atoi(sessionIDs[0])
	if err != nil {
		return nil, fmt.Errorf("invalid session id: %d", sessionID)
	}

	// Revoke the session.
	if n, err := s.server.internalExecutor.ExecEx(
		ctx,
		"revoke-auth-session",
		nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.web_sessions SET "revokedAt" = now() WHERE id = $1`,
		sessionID,
	); err != nil {
		return nil, apiInternalError(ctx, err)
	} else if n == 0 {
		msg := fmt.Sprintf("session with id %d nonexistent", sessionID)
		log.Info(ctx, msg)
		return nil, fmt.Errorf(msg)
	}

	// Send back a header which will cause the browser to destroy the cookie.
	// See https://tools.ietf.org/search/rfc6265, page 7.
	cookie := makeCookieWithValue("", false /* forHTTPSOnly */)
	cookie.MaxAge = -1

	// Set the cookie header on the outgoing response.
	if err := grpc.SetHeader(ctx, metadata.Pairs("set-cookie", cookie.String())); err != nil {
		return nil, apiInternalError(ctx, err)
	}

	return &serverpb.UserLogoutResponse{}, nil
}

// verifySession verifies the existence and validity of the session claimed by
// the supplied SessionCookie. Returns three parameters: a boolean indicating if
// the session was valid, the username associated with the session (if
// validated), and an error for any internal errors which prevented validation.
func (s *authenticationServer) verifySession(
	ctx context.Context, cookie *serverpb.SessionCookie,
) (bool, string, error) {
	// Look up session in database and verify hashed secret value.
	const sessionQuery = `
SELECT "hashedSecret", "username", "expiresAt", "revokedAt"
FROM system.web_sessions
WHERE id = $1`

	var (
		hashedSecret []byte
		username     string
		expiresAt    time.Time
		isRevoked    bool
	)

	row, err := s.server.internalExecutor.QueryRowEx(
		ctx,
		"lookup-auth-session",
		nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sessionQuery, cookie.ID)
	if row == nil || err != nil {
		return false, "", err
	}

	if row.Len() != 4 ||
		row[0].ResolvedType().Family() != types.BytesFamily ||
		row[1].ResolvedType().Family() != types.StringFamily ||
		row[2].ResolvedType().Family() != types.TimestampFamily {
		return false, "", errors.Errorf("values returned from auth session lookup do not match expectation")
	}

	// Extract datum values.
	hashedSecret = []byte(*row[0].(*tree.DBytes))
	username = string(*row[1].(*tree.DString))
	expiresAt = row[2].(*tree.DTimestamp).Time
	isRevoked = row[3].ResolvedType().Family() != types.UnknownFamily

	if isRevoked {
		return false, "", nil
	}

	if now := s.server.clock.PhysicalTime(); !now.Before(expiresAt) {
		return false, "", nil
	}

	hasher := sha256.New()
	_, _ = hasher.Write(cookie.Secret)
	hashedCookieSecret := hasher.Sum(nil)
	if !bytes.Equal(hashedSecret, hashedCookieSecret) {
		return false, "", nil
	}

	return true, username, nil
}

// verifyPassword verifies the passed username/password pair against the
// system.users table. The returned boolean indicates whether or not the
// verification succeeded; an error is returned if the validation process could
// not be completed.
func (s *authenticationServer) verifyPassword(
	ctx context.Context, username string, password string,
) (valid bool, expired bool, err error) {
	exists, canLogin, pwRetrieveFn, validUntilFn, err := sql.GetUserHashedPassword(
		ctx, s.server.execCfg.InternalExecutor, username,
	)
	if err != nil {
		return false, false, err
	}
	if !exists || !canLogin {
		return false, false, nil
	}
	hashedPassword, err := pwRetrieveFn(ctx)
	if err != nil {
		return false, false, err
	}

	validUntil, err := validUntilFn(ctx)
	if err != nil {
		return false, false, err
	}
	if validUntil != nil {
		if validUntil.Time.Sub(timeutil.Now()) < 0 {
			return false, true, nil
		}
	}

	return security.CompareHashAndPassword(hashedPassword, password) == nil, false, nil
}

// CreateAuthSecret creates a secret, hash pair to populate a session auth token.
func CreateAuthSecret() (secret, hashedSecret []byte, err error) {
	secret = make([]byte, secretLength)
	if _, err := rand.Read(secret); err != nil {
		return nil, nil, err
	}

	hasher := sha256.New()
	_, _ = hasher.Write(secret)
	hashedSecret = hasher.Sum(nil)
	return secret, hashedSecret, nil
}

// newAuthSession attempts to create a new authentication session for the given
// user. If successful, returns the ID and secret value for the new session.
func (s *authenticationServer) newAuthSession(
	ctx context.Context, username string,
) (int64, []byte, error) {
	secret, hashedSecret, err := CreateAuthSecret()
	if err != nil {
		return 0, nil, err
	}

	expiration := s.server.clock.PhysicalTime().Add(webSessionTimeout.Get(&s.server.st.SV))

	insertSessionStmt := `
INSERT INTO system.web_sessions ("hashedSecret", username, "expiresAt")
VALUES($1, $2, $3)
RETURNING id
`
	var id int64

	row, err := s.server.internalExecutor.QueryRowEx(
		ctx,
		"create-auth-session",
		nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		insertSessionStmt,
		hashedSecret,
		username,
		expiration,
	)
	if err != nil {
		return 0, nil, err
	}
	if row.Len() != 1 || row[0].ResolvedType().Family() != types.IntFamily {
		return 0, nil, errors.Errorf(
			"expected create auth session statement to return exactly one integer, returned %v",
			row,
		)
	}

	// Extract integer value from single datum.
	id = int64(*row[0].(*tree.DInt))

	return id, secret, nil
}

// authenticationMux implements http.Handler, and is used to provide session
// authentication for an arbitrary "inner" handler.
type authenticationMux struct {
	server *authenticationServer
	inner  http.Handler

	// allowAnonymous, if true, indicates that the authentication mux should
	// call its inner HTTP handler even if the request doesn't have a valid
	// session. If there is a valid session, the mux calls its inner handler
	// with a context containing the username and session ID.
	//
	// If allowAnonymous is false, the mux returns an error if there is no
	// valid session.
	allowAnonymous bool
}

// restAuthMux is used to manage and handle RESTful-style HTTP requests
type restAuthMux struct {
	server         *authenticationServer
	mux            safeServeMux
	allowAnonymous bool
}

// ServeHTTP processes an HTTP request, it retrieves user authentication information, sets context properties
func (rm *restAuthMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	dbName := ""

	// find usr,pwd and make key
	username, passwd, key, method, err := rm.getIdentifier(req, dbName)
	if err != nil {
		rm.server.server.restful.sendJSONResponse(req.Context(), w, -1, nil, err.Error())
		return
	}

	// fill ctx attr.
	ctx := req.Context()
	ctx = context.WithValue(ctx, webSessionUserKey{}, username)
	ctx = context.WithValue(ctx, webSessionPassKey{}, passwd)
	ctx = context.WithValue(ctx, webCacheKey{}, key)
	ctx = context.WithValue(ctx, webCacheMethodKey{}, method)
	req = req.WithContext(ctx)

	rm.mux.ServeHTTP(w, req)
}

func (rm *restAuthMux) Handle(pattern string, handler http.Handler) {
	rm.mux.Handle(pattern, handler)
}

func newAuthenticationMuxAllowAnonymous(
	s *authenticationServer, inner http.Handler,
) *authenticationMux {
	return &authenticationMux{
		server:         s,
		inner:          inner,
		allowAnonymous: true,
	}
}

func newAuthenticationMux(s *authenticationServer, inner http.Handler) *authenticationMux {
	return &authenticationMux{
		server:         s,
		inner:          inner,
		allowAnonymous: false,
	}
}

func newRestAuthMux(s *authenticationServer) *restAuthMux {
	return &restAuthMux{
		server:         s,
		allowAnonymous: false,
	}
}

type webSessionUserKey struct{}
type webSessionPassKey struct{}
type webSessionIDKey struct{}
type webSessionDBKey struct{}
type webCacheKey struct{}
type webCacheMethodKey struct{}
type restfulAuthKey struct{}

const webSessionUserKeyStr = "websessionuser"
const webSessionPassKeyStr = "websessionpasswd"
const webSessionIDKeyStr = "websessionid"
const webSessionDBKeyStr = "websessiondb"
const webCacheKeyStr = "webcache"
const restfulAuthKeyStr = "restfulauth"

func (am *authenticationMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	username, cookie, err := am.getSession(w, req)
	if err == nil {
		ctx := req.Context()
		ctx = context.WithValue(ctx, webSessionUserKey{}, username)
		ctx = context.WithValue(ctx, webSessionIDKey{}, cookie.ID)
		req = req.WithContext(ctx)
	} else if !am.allowAnonymous {
		log.Infof(req.Context(), "Web session error: %s", err)
		http.Error(w, "a valid authentication cookie is required", http.StatusUnauthorized)
		return
	}
	am.inner.ServeHTTP(w, req)
}

// EncodeSessionCookie encodes a SessionCookie proto into an http.Cookie.
// The flag forHTTPSOnly, if set, produces the "Secure" flag on the
// resulting HTTP cookie, which means the cookie should only be
// transmitted over HTTPS channels. Note that a cookie without
// the "Secure" flag can be transmitted over either HTTP or HTTPS channels.
func EncodeSessionCookie(
	sessionCookie *serverpb.SessionCookie, forHTTPSOnly bool,
) (*http.Cookie, error) {
	cookieValueBytes, err := protoutil.Marshal(sessionCookie)
	if err != nil {
		return nil, errors.Wrap(err, "session cookie could not be encoded")
	}
	value := base64.StdEncoding.EncodeToString(cookieValueBytes)
	return makeCookieWithValue(value, forHTTPSOnly), nil
}

func makeCookieWithValue(value string, forHTTPSOnly bool) *http.Cookie {
	return &http.Cookie{
		Name:     SessionCookieName,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   forHTTPSOnly,
	}
}

// getSession decodes the cookie from the request, looks up the corresponding session, and
// returns the logged in user name. If there's an error, it returns an error value and the
// HTTP error code.
func (am *authenticationMux) getSession(
	w http.ResponseWriter, req *http.Request,
) (string, *serverpb.SessionCookie, error) {
	// Validate the returned cookie.
	rawCookie, err := req.Cookie(SessionCookieName)
	if err != nil {
		return "", nil, err
	}

	cookie, err := decodeSessionCookie(rawCookie)
	if err != nil {
		err = errors.Wrap(err, "a valid authentication cookie is required")
		return "", nil, err
	}

	valid, username, err := am.server.verifySession(req.Context(), cookie)
	if err != nil {
		err := apiInternalError(req.Context(), err)
		return "", nil, err
	}
	if !valid {
		err := errors.New("the provided authentication session could not be validated")
		return "", nil, err
	}

	return username, cookie, nil
}

func decodeSessionCookie(encodedCookie *http.Cookie) (*serverpb.SessionCookie, error) {
	// Cookie value should be a base64 encoded protobuf.
	cookieBytes, err := base64.StdEncoding.DecodeString(encodedCookie.Value)
	if err != nil {
		return nil, errors.Wrap(err, "session cookie could not be decoded")
	}
	var sessionCookieValue serverpb.SessionCookie
	if err := protoutil.Unmarshal(cookieBytes, &sessionCookieValue); err != nil {
		return nil, errors.Wrap(err, "session cookie could not be unmarshaled")
	}
	return &sessionCookieValue, nil
}

// authenticationHeaderMatcher is a GRPC header matcher function, which provides
// a conversion from GRPC headers to HTTP headers. This function is needed to
// attach the "set-cookie" header to the response; by default, Grpc-Gateway
// adds a prefix to all GRPC headers before adding them to the response.
func authenticationHeaderMatcher(key string) (string, bool) {
	// GRPC converts all headers to lower case.
	if key == "set-cookie" {
		return key, true
	}
	// This is the default behavior of GRPC Gateway when matching headers -
	// it adds a constant prefix to the HTTP header so that by default they
	// do not conflict with any HTTP headers that might be used by the
	// browser.
	// TODO(mrtracy): A function "DefaultOutgoingHeaderMatcher" should
	// likely be added to GRPC Gateway so that the logic does not have to be
	// duplicated here.
	return fmt.Sprintf("%s%s", gwruntime.MetadataHeaderPrefix, key), true
}

func forwardAuthenticationMetadata(ctx context.Context, _ *http.Request) metadata.MD {
	md := metadata.MD{}
	if user := ctx.Value(webSessionUserKey{}); user != nil {
		md.Set(webSessionUserKeyStr, user.(string))
	}
	if sessionID := ctx.Value(webSessionIDKey{}); sessionID != nil {
		md.Set(webSessionIDKeyStr, fmt.Sprintf("%v", sessionID))
	}
	return md
}

func (rm *restAuthMux) getIdentifier(
	req *http.Request, dbName string,
) (userName string, passWd string, cacheKey string, methodKey string, err error) {
	// way of make Key : format[username:token] temply for base64
	token := ""
	header := req.Header

	// parse input.
	// case1: Authority.
	// case 1: from authorization
	tokenFromHeader := rm.server.server.restful.authorization
	tokenWithBaseAu := header.Get("Authorization")

	tokenFinal := ""
	if tokenWithBaseAu != "" {
		tokenFinal = tokenWithBaseAu
	} else if tokenFromHeader != "" {
		tokenFinal = tokenFromHeader
	}

	if tokenFinal != "" {
		tokenSlice := strings.Split(tokenFinal, " ")
		if len(tokenSlice) != 2 {
			return "", "", "", "", fmt.Errorf("can not find Basic attribute, please check")
		}
		if tokenSlice[0] != "Basic" {
			return "", "", "", "", fmt.Errorf("can not find Basic attribute, please check")
		}
		token = tokenSlice[1]
	}

	// case2: need to make token
	if token == "" {
		return "", "", "", "", fmt.Errorf("wrong username or password, please check")
	}

	_, err = hex.DecodeString(token)
	if err == nil {
		return "", "", token, "token", nil
	}

	decodedData, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", "", "", "", err
	}
	slice := strings.Split(string(decodedData), ":")
	if len(slice) != 2 {
		return "", "", "", "", fmt.Errorf("wrong username or password, please check")
	}
	return slice[0], slice[1], "", "password", nil
}
