// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"context"
	"crypto/md5"
	"crypto/sha256"
	gosql "database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/lib/pq"
)

// DDlIncluded use for ddl .
var DDlIncluded = []string{
	"create",
	"drop",
	"delete",
	"use",
	"alter",
	"update",
	"grant",
	"revoke"}

// queryKeyWords to be filtered out when query
var queryKeyWords = []string{
	"insert",
	"set",
	"drop",
	"alter",
	"delete",
	"update",
}

var transTypeToLength = map[string]int64{
	"BOOL":        1,
	"INT2":        2,
	"INT4":        4,
	"INT8":        8,
	"_INT8":       8,
	"FLOAT4":      4,
	"FLOAT8":      8,
	"TIMESTAMP":   8,
	"TIMESTAMPTZ": 8,
	"INTERVAL":    8,
	"BIT":         8,
	"VARBIT":      8,
	"DATE":        8,
	"TIME":        8,
	"JSONB":       8,
	"INET":        8,
	"UUID":        8,
	"GEOMETRY":    9223372036854775807,
	"_TEXT":       9223372036854775807,
	"NAME":        9223372036854775807}

const (
	//insert_type_str = "INSERT INTO"
	insertTypeStrLowercase = "insert into"
	//ddl_exclude_str = "SHOW CREATE"
	ddlExcludeStrLowercase = "show create"
	//without schema
	insertWithoutSchema = "insert without schema into"
	// maxtimes of retry
	maxRetries = 10
	// varchar length
	varcharlen = 254
)

type colMetaInfo struct {
	Name   string
	Type   string
	Length int64
}

type pgConnection struct {
	db            *gosql.DB
	username      string
	maxLifeTime   int64
	sessionid     string
	lastLoginTime int64
	isAdmin       bool
	loginValid    bool
	lastStartTime time.Time
}

// RestfulUser provides login user
type RestfulUser struct {
	UserName  string
	LoginTime int64
}

// A restfulServer provides a RESTful HTTP API to administration of
// the kwbase cluster.
type restfulServer struct {
	server        *Server
	insertNotices *pq.Error
	connCache     map[string]*pgConnection
	authorization string
	ifByLogin     bool
}

// SQLRestfulTimeOut maximum overdue time
var SQLRestfulTimeOut = settings.RegisterPublicIntSetting(
	"server.rest.timeout",
	"time out for restful api(in minutes)",
	60,
)

// SQLRestfulTimeZone information of timezone
var SQLRestfulTimeZone = settings.RegisterValidatedIntSetting(
	"server.restful_service.default_request_timezone",
	"set time zone for restful api",
	0,
	func(v int64) error {
		if v < -12 || v > 14 {
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"server.restful_service.default_request_timezone must be set between -12 and 14")
		}
		return nil
	},
)

// loginResponseSuccess is use for return login success
type loginResponseSuccess struct {
	Code  int    `json:"code"`
	Token string `json:"token"`
}

type baseResponse struct {
	Code int     `json:"code"`
	Desc string  `json:"desc"`
	Time float64 `json:"time"`
}

type ddlResponse struct {
	*baseResponse
}

type insertResponse struct {
	*baseResponse
	Notice string `json:"notice"`
	Rows   int64  `json:"rows"`
}

type teleInsertResponse struct {
	*baseResponse
	Rows int64 `json:"rows"`
}

type queryResponse struct {
	*baseResponse
	ColumnMeta []colMetaInfo `json:"column_meta"`
	Data       [][]string    `json:"data"`
	Rows       int           `json:"rows"`
}

// loginResponseFail returns login fail
type showAllSuccess struct {
	Code  int           `json:"code"`
	Conns []sessionInfo `json:"conns"`
}

// sessionInfo shows seesion infos
type sessionInfo struct {
	Connid         string
	Username       string
	Token          string
	MaxLifeTime    int64
	LastLoginTime  string
	ExpirationTime string
}

type resultToken struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}

func (col colMetaInfo) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`["%s", "%s", %d]`, col.Name, col.Type, col.Length)), nil
}

func (inStr insertResponse) MarshalJSON() ([]byte, error) {
	results := strings.Split(inStr.Desc, ",")
	resultStr := "["
	for _, result := range results {
		resultStr = resultStr + fmt.Sprintf(`"%s",`, result)
	}
	// erase the redundant symbols.
	resultStr = strings.TrimRight(resultStr, ",")
	resultStr = resultStr + "]"
	inStr.Desc = resultStr

	if "" == inStr.Notice {
		inStr.Notice = fmt.Sprintf(`null`)
	} else {
		notices := strings.Split(inStr.Notice, ",")
		noticeStr := "["
		for _, nResult := range notices {
			noticeStr = noticeStr + fmt.Sprintf(`%s,`, nResult)
		}
		// erase the redundant symbols.
		noticeStr = strings.TrimRight(noticeStr, ",")
		noticeStr = noticeStr + "]"
		inStr.Notice = noticeStr
	}

	str := fmt.Sprintf(`{"code":%d,"desc":%s,"rows":%d,"notice":%s,"time":%f}`,
		inStr.Code,
		inStr.Desc,
		inStr.Rows,
		inStr.Notice,
		inStr.Time)

	return []byte(str), nil
}

func (ddlStr ddlResponse) MarshalJSON() ([]byte, error) {
	results := strings.Split(ddlStr.Desc, ",")
	resultStr := "["
	for _, result := range results {
		resultStr = resultStr + fmt.Sprintf(`"%s",`, result)
	}
	// erase the redundant symbols.
	resultStr = strings.TrimRight(resultStr, ",")
	resultStr = resultStr + "]"
	ddlStr.Desc = resultStr
	return []byte(fmt.Sprintf(`{"code":%d,"desc":%s,"time":%f}`,
		ddlStr.Code,
		ddlStr.Desc,
		ddlStr.Time)), nil
}

// newRestfulServer allocates and returns a new REST server for
// Restful APIs.
func newRestfulServer(s *Server) *restfulServer {
	server := &restfulServer{server: s, connCache: make(map[string]*pgConnection)}
	return server
}

func (s *restfulServer) handleNotice(notice *pq.Error) {
	s.insertNotices = notice
	return
}

// getPgConnection gets db connections
func (s *restfulServer) getPgConnection(
	ctx context.Context, user string, passwd string,
) (*gosql.DB, error) {
	url, _ := s.server.cfg.PGURL(url.UserPassword(user, passwd))
	var err error
	var db *gosql.DB
	var base *pq.Connector
	base, err = pq.NewConnector(url.String())
	if err != nil {
		log.Errorf(ctx, "pg conn err: %s \n", err.Error())
		return nil, err
	}
	connector := pq.ConnectorWithNoticeHandler(base, func(notice *pq.Error) {
		s.handleNotice(notice)
	})
	db = gosql.OpenDB(connector)
	if err != nil {
		log.Errorf(ctx, "conn open db err: %s \n", err.Error())
		return nil, err
	}
	// set max connections 100
	db.SetMaxOpenConns(100)
	return db, nil
}

func ifContainsType(target []string, src string) bool {
	for _, t := range target {
		re := regexp.MustCompile(`\b` + t + `\b`)
		if re.MatchString(src) {
			return true
		}
	}
	return false
}

// handleLogin handles authentication when login
func (s *restfulServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	desc := "success"
	// the same rule of td.
	code := 0
	token := ""
	var err error
	// Extract the authentication info from request header
	ctx := r.Context()

	time := SQLRestfulTimeOut.Get(&s.server.cfg.Settings.SV) * 60
	// db is illegal.
	// get dbname by context.
	paraDbName := r.FormValue("db")
	// get dbname by path.
	if paraDbName != "" {
		desc := "wrong db parameter for login."
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}

	// check if it is GET.
	if r.Method != "GET" {
		desc := "support only GET method."
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}
	// Read the request body
	_, err = ioutil.ReadAll(r.Body)
	if err != nil {
		desc := "body err:" + err.Error()
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}
	var username, password string
	// case 1: -H "Authorization:Basic d3k5OTk6MTIzNDU2Nzg5"
	usr, pass, err := s.getUserWIthPass(r)
	if err != nil {
		s.sendJSONResponse(ctx, w, -1, nil, err.Error())
		return
	}
	username, password = usr, pass

	// Call the verifySession/verifyPassword function from authentication.go
	valid, expired, err := s.server.authentication.verifyPassword(ctx, username, password)
	if err != nil {
		desc := "auth err:" + err.Error()
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}

	if expired {
		desc := "the password for user has expired."
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}

	if !valid {
		desc = "the provided username and password did not match any credentials on the server."
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}
	tNow := timeutil.Now().Unix()
	role, err := s.isAdminRole(ctx, username)
	if err != nil {
		desc = "query users" + err.Error()
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}
	token, err = s.generateKey(username, tNow)
	if err != nil {
		desc = "Failed to encode struct" + err.Error()
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}

	if _, ok := s.connCache[token]; !ok {
		db, err := s.getPgConnection(ctx, username, password)
		if err != nil {
			desc = "database connection error: " + err.Error()
			s.sendJSONResponse(ctx, w, -1, nil, desc)
			return
		}
		sessionid, err := generateSessionID()
		if err != nil {
			desc = "generate session id error: " + err.Error()
			s.sendJSONResponse(ctx, w, -1, nil, desc)
			return
		}
		s.connCache[token] = &pgConnection{
			db:            db,
			sessionid:     sessionid,
			maxLifeTime:   time,
			lastLoginTime: tNow,
			username:      username,
			isAdmin:       role,
		}
		log.Infof(ctx, "session %s has established", sessionid)
	}

	responseSuccess := loginResponseSuccess{code, token}
	s.sendJSONResponse(ctx, w, code, responseSuccess, desc)

	// critical: must clean the basic field.
	s.ifByLogin = false
	s.authorization = ""
}

// checkUser checks user information
func (s *restfulServer) checkUser(ctx context.Context, username string, password string) error {
	// Call the verifySession/verifyPassword function from authentication.go
	valid, expired, err := s.server.authentication.verifyPassword(ctx, username, password)
	if err != nil {
		desc := "auth err:" + err.Error()
		return fmt.Errorf(desc)
	}

	if expired {
		desc := "the password for user has expired."
		return fmt.Errorf(desc)
	}

	if !valid {
		desc := "the provided username and password did not match any credentials on the server."
		return fmt.Errorf(desc)
	}
	return nil
}

func (s *restfulServer) getSQLFromReqBody(r *http.Request) (string, error) {
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		return "", err
	}
	sql := string(body)
	return sql, nil
}

// handleDDL handles DDL SQL interface
func (s *restfulServer) handleDDL(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := 0
	desc := ""

	restDDL, err := s.checkFormat(ctx, w, r, "POST")
	if err != nil {
		return
	}

	connCache, db, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}

	// Calculate the execution time if needed
	executionTime := float64(0)
	// split the stmts.
	ddlStmts := parseSQL(restDDL)
	for _, stmt := range ddlStmts {
		if stmt == "" {
			continue
		}
		// ddl includes
		includeDDlflag := ifContainsType(DDlIncluded, strings.ToLower(stmt))
		excludeDDlCount := strings.Count(strings.ToLower(stmt), ddlExcludeStrLowercase)
		if !includeDDlflag || 0 < excludeDDlCount {
			desc = desc + "wrong statement for ddl interface and please check" + ","
			code = -1
			continue
		}
		DDLStartTime := timeutil.Now()
		_, err = db.Exec(stmt)
		if err != nil {
			errStr := strings.ReplaceAll(err.Error(), `"`, `\"`)
			desc = desc + errStr + ","
			code = -1
		} else {
			desc = desc + "success" + ","
		}
		duration := timeutil.Now().Sub(DDLStartTime)
		executionTime = float64(duration) / float64(time.Second)
	}

	ddldesc := parseDesc(desc)

	// Create the response struct
	response := &ddlResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: ddldesc,
			Time: executionTime,
		},
	}
	s.sendJSONResponse(ctx, w, 0, response, ddldesc)
	connCache.lastLoginTime = timeutil.Now().Unix()
	clear(ctx, db)
}

// clear clears db connection
func clear(ctx context.Context, db *gosql.DB) {
	method := ctx.Value(webCacheMethodKey{}).(string)
	if method == "password" {
		if err := db.Close(); err != nil {
			log.Error(ctx, "restful api close db err: %v", err.Error())
		}
	}
}

// handleInsert handles insert interface
func (s *restfulServer) handleInsert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := 0
	desc := ""

	restInsert, err := s.checkFormat(ctx, w, r, "POST")
	if err != nil {
		return
	}

	connCache, db, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}

	// Calculate the execution time if needed
	executionTime := float64(0)
	var rowsAffected int64
	rowsAffected = 0
	notice := ""
	var result gosql.Result
	// split the stmts.
	insertStmts := parseSQL(restInsert)
	s.insertNotices = nil
	for _, stmt := range insertStmts {
		if stmt == "" {
			continue
		}
		insertflag := strings.Contains(strings.ToLower(stmt), insertTypeStrLowercase)
		if !insertflag {
			desc = desc + "can not find insert statement and please check" + ","
			code = -1
			continue
		}
		InsertStartTime := timeutil.Now()
		result, err = db.Exec(stmt)
		duration := timeutil.Now().Sub(InsertStartTime)
		executionTime = float64(duration) / float64(time.Second)
		if err != nil {
			errStr := strings.ReplaceAll(err.Error(), `"`, `\"`)
			desc = desc + errStr + ","
			code = -1
		} else {
			curRowsAffected, err := result.RowsAffected()
			if err != nil {
				errStr := strings.ReplaceAll(err.Error(), `"`, `\"`)
				desc = desc + errStr + ","
				code = -1
			} else {
				desc = desc + "success" + ","
				rowsAffected += curRowsAffected
			}
		}
		// collect notice.
		if s.insertNotices != nil {
			notice = notice + fmt.Sprintf(`"%v",`, s.insertNotices)
			notice = strings.ReplaceAll(notice, "\r\n", " ")
			notice = strings.ReplaceAll(notice, "\n", " ")
			s.insertNotices = nil
		}
	}
	insertdesc := parseDesc(desc)

	// erase the last ","
	if "" != notice {
		notice = strings.TrimRight(notice, ",")
	}

	// Create the response struct
	response := insertResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: insertdesc,
			Time: executionTime,
		},
		Notice: notice,
		Rows:   rowsAffected,
	}
	s.sendJSONResponse(ctx, w, 0, response, insertdesc)
	connCache.lastLoginTime = timeutil.Now().Unix()
	clear(ctx, db)
}

// handleQuery handles query interface
func (s *restfulServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := 0
	desc := "success"

	restQuery, err := s.checkFormat(ctx, w, r, "POST")
	if err != nil {
		return
	}

	connCache, db, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}

	// Execute the query
	resultsCount := 0
	var columnMeta = []colMetaInfo{}
	var restData [][]string
	executionTime := float64(0)
	var cloLength int64
	mistakeTypeCount := 0

	queryStmtCount := strings.Count(restQuery, ";")
	createFlag := strings.Contains(strings.ToLower(restQuery), "create")
	showCreateFlag := strings.Contains(strings.ToLower(restQuery), "show create")

	if queryStmtCount > 1 {
		desc = "only support single statement for each query interface, please check."
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}
	if createFlag == true && !showCreateFlag {
		desc = "do not support create statement for query interface, please check."
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}

	if ifKey, keyword := startsWithKeywords(restQuery); ifKey {
		desc = "do not support " + keyword + " statement for query interface, please check."
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}

	// Calculate the execution time if needed
	QueryStartTime := timeutil.Now()
	rows, err := db.Query(restQuery)
	if err != nil {
		desc = err.Error()
		code = -1
	} else {
		defer rows.Close()
		duration := timeutil.Now().Sub(QueryStartTime)
		executionTime = float64(duration) / float64(time.Second)

		// Get column meta.
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			desc = err.Error()
			code = -1
		}
		for _, colMeta := range colTypes {
			colName := colMeta.Name()
			colType := colMeta.DatabaseTypeName()
			if colType == "BYTEA" {
				colType = "BYTES"
			} else if colType == "VARBYTEA" {
				colType = "VARBYTES"
			}
			originalLen, hasLen := colMeta.Length()
			if hasLen {
				cloLength = originalLen
			} else {
				value, ifexist := transTypeToLength[colType]
				if ifexist {
					cloLength = value
				} else {
					var ifsupport bool
					cloLength, ifsupport = colMeta.Length()
					if !ifsupport {
						if colType != "NUMERIC" {
							cloLength = 0
							mistakeTypeCount++
							if mistakeTypeCount == 1 {
								desc = ""
							}
							desc += "the type's description " + colType + " and length of column " + colName + " can not be displayed completely for current version"
							code = -1
						}
					}
				}
			}

			if colType == "BPCHAR" {
				colType = "CHAR"
			}
			columnMeta = append(columnMeta, colMetaInfo{colName, colType, cloLength})
		}

		// get row data.
		restData, err = sqlutils.GetDataValue(rows)
		if err != nil {
			desc = err.Error()
			code = -1
		} else {
			resultsCount = len(restData)
		}
	}

	for row := range restData {
		for col := range restData[row] {
			// replace "\n"
			restData[row][col] = strings.ReplaceAll(restData[row][col], "\n", "")
			// replace "\t"
			restData[row][col] = strings.ReplaceAll(restData[row][col], "\t", "")
		}
	}

	// Create the response struct
	response := queryResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: desc,
			Time: executionTime,
		},
		Rows:       resultsCount,
		ColumnMeta: columnMeta,
		Data:       restData,
	}

	s.sendJSONResponse(ctx, w, code, response, desc)
	connCache.lastLoginTime = timeutil.Now().Unix()
	clear(ctx, db)
}

// handleTelegraf handle telegraf interface
func (s *restfulServer) handleTelegraf(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := 0
	desc := "success"

	restTelegraph, err := s.checkFormat(ctx, w, r, "POST")
	if err != nil {
		return
	}

	connCache, db, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}

	var rowsAffected int64
	rowsAffected = 0
	var teleResult gosql.Result
	// Calculate the execution time if needed
	executionTime := float64(0)
	// the program will get a batch of data at once, so it needs to handle it first
	statements := strings.Split(strings.ReplaceAll(restTelegraph, "\r\n", "\n"), "\n")
	numOfStmts := len(statements)
	for i := 0; i < numOfStmts; i++ {
		insertTelegraphStmt := makeInsertStmt(statements[i])

		TeleInsertStartTime := timeutil.Now()
		insertflag := strings.Contains(strings.ToLower(insertTelegraphStmt), insertTypeStrLowercase)
		if insertTelegraphStmt == "" {
			desc = "wrong telegraf insert statement, please check."
			code = -1
		} else if !insertflag {
			desc = "can not find insert statement, please check."
			code = -1
		} else {
			teleStmtCount := strings.Count(insertTelegraphStmt, ";")
			if teleStmtCount > 1 {
				desc = "only support single statement for each telegraf interface, please check."
				code = -1
			} else {
				teleResult, err = db.Exec(insertTelegraphStmt)
				if err != nil {
					desc = err.Error()
					code = -1
				} else {
					rowsAffected, err = teleResult.RowsAffected()
					if err != nil {
						desc = err.Error()
						code = -1
						rowsAffected = 0
					} else {
						duration := timeutil.Now().Sub(TeleInsertStartTime)
						executionTime = float64(duration) / float64(time.Second)
					}
				}
			}
		}
	}

	response := teleInsertResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: desc,
			Time: executionTime,
		},
		Rows: rowsAffected,
	}

	s.sendJSONResponse(ctx, w, 0, response, desc)
	connCache.lastLoginTime = timeutil.Now().Unix()
	clear(ctx, db)
}

// checkConn checks connection of users
func (s *restfulServer) checkConn(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) (*pgConnection, *gosql.DB, error) {
	connCache, err := s.pickConnCache(ctx)
	if err != nil {
		// s.authorization = ""
		desc := err.Error()
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return &pgConnection{}, nil, err
	}
	db := connCache.db

	// get dbname by context.
	paraDbName := r.FormValue("db")
	// get dbname by path.
	if paraDbName == "" {
		paraDbName = "defaultdb"
	}

	if _, err := db.Exec("USE " + paraDbName); err != nil {
		desc := err.Error()
		clear(ctx, db)
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return &pgConnection{}, nil, err
	}

	// get timezone by context.
	paraTimeZone := r.FormValue("tz")
	// get dbname by path.
	if paraTimeZone == "" {
		timezone := SQLRestfulTimeZone.Get(&s.server.cfg.Settings.SV)
		paraTimeZone = fmt.Sprintf("%d", timezone)
	}
	if _, err := db.Exec(fmt.Sprintf("set time zone %s", paraTimeZone)); err != nil {
		desc := err.Error()
		clear(ctx, db)
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return &pgConnection{}, nil, err
	}

	return connCache, db, nil
}

// checkFormat checks format of input
func (s *restfulServer) checkFormat(
	ctx context.Context, w http.ResponseWriter, r *http.Request, method string,
) (sql string, err error) {
	if s.server.restful.authorization != "" {
		if restAuth := r.Header.Get("Authorization"); restAuth != "" {
			s.server.restful.authorization = restAuth
		}
	}

	if r.Method != method {
		desc := "only support " + method + " method"
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return "", fmt.Errorf(desc)
	}

	// Get SQL statement from the body
	sqlValue, err := s.getSQLFromReqBody(r)
	if err != nil {
		desc := "invalid request body"
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return "", fmt.Errorf("invalid request body")
	}
	return sqlValue, nil
}

// handleSession handles session info
func (s *restfulServer) handleSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.Method != "GET" && r.Method != "DELETE" {
		desc := "only support GET/DELETE method."
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}

	if r.Method == "GET" {
		s.handleUserShow(w, r)
	} else if r.Method == "DELETE" {
		s.handleUserDelete(w, r)
	}
}

// handleUserDelete deletes conn by session id for your API endpoint
// If the current user is an admin, they can delete all connections
// If the current user is a regular user, they can only delete their own connections
func (s *restfulServer) handleUserDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := 0
	desc := "delete success"

	uuid, err := s.checkFormat(ctx, w, r, "DELETE")
	if err != nil {
		return
	}

	isAdmin, _, _, username, err := s.verifyUser(ctx)
	if err != nil {
		desc = err.Error()
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}
	found := false
	if isAdmin {
		// If it's an admin user, directly delete the session for that connid
		for key, value := range s.connCache {
			if value.sessionid == uuid {
				if err := value.db.Close(); err != nil {
					log.Error(ctx, "restful api close db err: %v", err.Error())
				}
				delete(s.connCache, key)
				found = true
				break
			}
		}
	} else {
		// If it's a regular user, they can only delete sessions that they themselves have created
		for key, value := range s.connCache {
			if value.sessionid == uuid {
				if value.username == username {
					if err := value.db.Close(); err != nil {
						log.Error(ctx, "restful api close db err: %v", err.Error())
					}
					delete(s.connCache, key)
					found = true
					break
				} else {
					// If it's not created by themselves, an error will occur
					desc = "do not have authority, please check."
					s.sendJSONResponse(ctx, w, -1, nil, desc)
					return
				}
			}
		}
	}
	if !found {
		desc = "no connid matching the given one was found."
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}

	responseSuccess := resultToken{Code: code, Desc: desc}
	s.sendJSONResponse(ctx, w, 0, responseSuccess, "")
}

// handleUserShow shows session info by session id for your API endpoint
func (s *restfulServer) handleUserShow(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var tokens []sessionInfo
	code := 0
	desc := "success"

	_, err := s.checkFormat(ctx, w, r, "GET")
	if err != nil {
		return
	}

	isAdmin, method, key, username, err := s.verifyUser(ctx)
	if err != nil {
		desc = err.Error()
		s.sendJSONResponse(ctx, w, -1, nil, desc)
		return
	}

	if isAdmin {
		for token, value := range s.connCache {
			s.addSessionInfo(&tokens, *value, token)
		}
	} else if method == "password" {
		for token, value := range s.connCache {
			if value.username == username {
				s.addSessionInfo(&tokens, *value, token)
			}
		}
	} else if method == "token" {
		if value, ok := s.connCache[key]; ok {
			s.addSessionInfo(&tokens, *value, key)
		}
	}

	responseSuccess := showAllSuccess{Code: code, Conns: tokens}
	s.sendJSONResponse(ctx, w, 0, responseSuccess, "")
}

// addSessionInfo adds session infos
func (s *restfulServer) addSessionInfo(tokens *[]sessionInfo, value pgConnection, token string) {
	lastLoginTime := transtUnixTime(value.lastLoginTime)
	expirationTime := transtUnixTime(value.lastLoginTime + value.maxLifeTime)
	truncated := token[:8]

	showtoken := truncated + strings.Repeat("*", 1)
	*tokens = append(*tokens, sessionInfo{
		Connid:         value.sessionid,
		Username:       value.username,
		Token:          showtoken,
		MaxLifeTime:    value.maxLifeTime,
		LastLoginTime:  lastLoginTime,
		ExpirationTime: expirationTime,
	})
}

// sendJSONResponse returns JSON format information
func (s *restfulServer) sendJSONResponse(
	ctx context.Context, w http.ResponseWriter, code int, responseSuccess interface{}, desc string,
) {
	if code == -1 {
		responseFail := resultToken{Code: code, Desc: desc}
		jsonResponse, err := json.Marshal(responseFail)
		if err != nil {
			log.Error(ctx, "marshal response err: %v \n", err.Error())
			return
		}
		// Set the content type header to JSON
		w.Header().Set("Content-Type", "application/json")
		// Write the JSON response
		if _, err := w.Write(jsonResponse); err != nil {
			log.Error(ctx, "write to json err: %v \n", err.Error())
			return
		}
	} else {
		jsonResponse, err := json.Marshal(responseSuccess)
		if err != nil {
			log.Error(ctx, "marshal response err: %v \n", err.Error())
			return
		}
		// Set the content type header to JSON
		w.Header().Set("Content-Type", "application/json")
		// Write the JSON response
		if _, err := w.Write(jsonResponse); err != nil {
			log.Error(ctx, "write to json err: %v \n", err.Error())
			return
		}
	}
}

// generateKey generates token by username and time when login
func (s *restfulServer) generateKey(userName string, tNow int64) (key string, err error) {
	user := RestfulUser{UserName: userName, LoginTime: tNow}

	jsonData, err := json.Marshal(user)
	if err != nil {
		return "", err
	}
	hash := md5.New()
	hash.Write(jsonData)
	hashValue := hash.Sum(nil)

	hashStr := hex.EncodeToString(hashValue)

	return hashStr[:32], nil
}

// pickConnCache finds existed db, or makes a new db for the user.
func (s *restfulServer) pickConnCache(ctx context.Context) (*pgConnection, error) {
	key := ctx.Value(webCacheKey{}).(string)
	username := ctx.Value(webSessionUserKey{}).(string)
	password := ctx.Value(webSessionPassKey{}).(string)
	method := ctx.Value(webCacheMethodKey{}).(string)
	if method == "token" {
		if key != "" {
			var ok bool
			var pgconn *pgConnection
			if pgconn, ok = s.connCache[key]; !ok {
				return &pgConnection{}, fmt.Errorf("can not find token, need login first")
			}
			return pgconn, nil
		}
	} else if method == "password" {
		err := s.checkUser(ctx, username, password)
		if err == nil {
			var pg pgConnection
			db, err := s.getPgConnection(ctx, username, password)
			if err != nil {
				return &pgConnection{}, err
			}
			pg.db = db
			return &pg, nil
		}
	}
	return &pgConnection{}, fmt.Errorf("can not find token, need login first")
}

// verifyUser verifys if the user has logged in.
func (s *restfulServer) verifyUser(ctx context.Context) (bool, string, string, string, error) {
	key := ctx.Value(webCacheKey{}).(string)
	username := ctx.Value(webSessionUserKey{}).(string)
	password := ctx.Value(webSessionPassKey{}).(string)
	method := ctx.Value(webCacheMethodKey{}).(string)
	if method == "token" {
		if pgconn, ok := s.connCache[key]; ok {
			if pgconn.isAdmin {
				return true, method, key, username, nil
			}
			return false, method, key, username, nil
		}
	} else if method == "password" {
		if s.checkUser(ctx, username, password) == nil {
			role, err := s.isAdminRole(ctx, username)
			if err == nil {
				return role, method, key, username, nil
			}
		}
	}
	return false, "", "", "", fmt.Errorf("can not verify")
}

// anythingToNumeric cleans numeric values, and add \' (speech marks) between non-numeric values.
func anythingToNumeric(input string) (output string) {
	// regular expression for numbers.
	re := regexp.MustCompile(`^[+-]?[0-9]*[.]?[0-9]+[i]?$`)

	if re.MatchString(input) == false {
		if strings.HasPrefix(input, "'") && strings.HasSuffix(input, "'") {
			output = input
			return output
		}
		output = "'" + input + "'"
		return output
	}

	numbers := re.FindAllString(input, -1)
	// numbers should only be one match, otherwise it may not be a number
	if len(numbers) > 1 {
		output = "'" + input + "'"
		return output
	}

	output = numbers[0]
	if output[len(output)-1] == 'i' {
		output = output[0 : len(output)-1]
	}
	return output
}

// makeInsertStmt makes insert statement when telegraf.
func makeInsertStmt(stmtOriginal string) (teleInsertStmt string) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("invalid data for telegraf insert, please check the format.")
		}
	}()

	// eg. swap,host='123456' k_timestamp=123,inn=27451392,out=194539520 1687898010000000000
	// slice[slice[0] slice[1] slice[2]]

	// find truncation eg. host=
	slice := strings.Split(stmtOriginal, " ")

	// slice[0] = tableName, host, ...
	attribute := strings.Split(slice[0], ",")
	tblName := attribute[0]

	var colKey []string
	var colValue []string

	for index, keyWithValue := range attribute {
		if index >= 1 {
			initObj := strings.Split(keyWithValue, "=")
			// init clokey first
			colKey = append(colKey, initObj[0])
			// init value first
			colValue = append(colValue, anythingToNumeric(initObj[1]))
		}
	}

	colkeyValue := strings.Split(slice[1], ",")
	for _, keyValue := range colkeyValue {
		obj := strings.Split(keyValue, "=")
		colKey = append(colKey, obj[0])
		colValue = append(colValue, anythingToNumeric(obj[1]))
	}
	timeStamp := slice[2]
	if len(timeStamp) > 13 {
		timeStamp = timeStamp[0:13]
	}

	// construct insert stmt
	// eg. insert into table1(field1,field2) values(value1,value2)
	// insert keys
	insertKeyStmt := "("
	insertKeyStmt += "k_timestamp,"
	for _, insertKey := range colKey {
		insertKeyStmt += insertKey
		insertKeyStmt += ","
	}
	// insertKeyStmt += hostKey
	// drop the last character.
	insertKeyStmt = strings.TrimRight(insertKeyStmt, ",")
	insertKeyStmt += ")"

	// insert values
	insertValueStmt := "("
	insertValueStmt += timeStamp
	insertValueStmt += ","
	for _, insertValue := range colValue {
		insertValueStmt += insertValue
		insertValueStmt += ","
	}
	// drop the last character.
	// insertValueStmt += hostValue
	insertValueStmt = strings.TrimRight(insertValueStmt, ",")
	insertValueStmt += ")"
	// insert stmt
	stmtRet := "insert into " + tblName + insertKeyStmt + " values" + insertValueStmt

	return stmtRet
}

// transtUnixTime formats display time.
func transtUnixTime(timestamp int64) string {
	t := timeutil.Unix(timestamp, 0)

	return t.Format("2006-01-02 15:04:05")
}

// generateSessionID generates Session ID
func generateSessionID() (string, error) {
	uuid, err := uuid.NewV1()
	if err != nil {
		return "", err
	}
	return uuid.String(), nil
}

// isAdminRole determines whether the user is a member of the admin role
func (s *restfulServer) isAdminRole(ctx context.Context, member string) (bool, error) {
	ret := map[string]bool{}

	// Keep track of members we looked up.
	visited := map[string]struct{}{}
	toVisit := []string{member}
	lookupRolesStmt := `SELECT "role", "isAdmin" FROM system.role_members WHERE "member" = $1`

	for len(toVisit) > 0 {
		// Pop first element.
		m := toVisit[0]
		toVisit = toVisit[1:]
		if _, ok := visited[m]; ok {
			continue
		}
		visited[m] = struct{}{}

		rows, err := s.server.execCfg.InternalExecutor.Query(
			ctx, "expand-roles", nil, lookupRolesStmt, m,
		)
		if err != nil {
			return false, err
		}

		for _, row := range rows {
			roleName := tree.MustBeDString(row[0])
			isAdmin := row[1].(*tree.DBool)

			ret[string(roleName)] = bool(*isAdmin)

			// We need to expand this role. Let the "pop" worry about already-visited elements.
			toVisit = append(toVisit, string(roleName))
		}
	}

	if _, ok := ret[sqlbase.AdminRole]; ok {
		return true, nil
	}
	return false, nil
}

// parseSQL parses SQL of insert
func parseSQL(restInsert string) []string {
	restInsert = strings.ReplaceAll(restInsert, "\r\n", "")
	restInsert = strings.ReplaceAll(restInsert, "\n", "")
	insertStmts := strings.Split(restInsert, ";")
	return insertStmts
}

// parseSQL parses SQL of desc
func parseDesc(desc string) string {
	// erase the redundant symbols.
	desc = strings.ReplaceAll(desc, `"""`, `"`)
	desc = strings.ReplaceAll(desc, `""`, `"`)
	desc = strings.TrimRight(desc, ",")
	return desc
}

func (s *restfulServer) getUserWIthPass(
	r *http.Request,
) (userName string, passWd string, err error) {
	tokenFromHeader := s.server.restful.authorization
	tokenWithBaseAu := r.Header.Get("Authorization")

	tokenStr := ""
	if tokenWithBaseAu != "" {
		tokenStr = tokenWithBaseAu
	} else if tokenFromHeader != "" {
		tokenStr = tokenFromHeader
	}
	// token : format[Basic base64codes]
	token := ""
	// get token.
	if tokenStr == "" {
		return "", "", fmt.Errorf("can not find Basic attribute, please check")
	}

	tokenSlice := strings.Split(tokenStr, " ")
	if tokenSlice[0] != "Basic" || tokenSlice[1] == "" {
		return "", "", fmt.Errorf("can not find Basic attribute, please check")
	}
	token = tokenSlice[1]

	usernamePassword, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", "", fmt.Errorf("wrong username or password, please check")
	}
	slice := strings.Split(string(usernamePassword), ":")
	if len(slice) != 2 {
		return "", "", fmt.Errorf("wrong username or password, please check")
	}
	return slice[0], slice[1], nil
}

// determineType returns the corresponding type based on the input string
func determineType(input string) (string, string) {
	input = strings.TrimSpace(input)

	// Regular expression matching for numbers
	isFloat := regexp.MustCompile(`^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$`)
	isNumericWithU := regexp.MustCompile(`^[-+]?[0-9]+u$`)
	isNumericWithI := regexp.MustCompile(`^[-+]?[0-9]+i$`)

	isBool := map[string]bool{
		"true":  true,
		"t":     true,
		"T":     true,
		"True":  true,
		"TRUE":  true,
		"false": false,
		"f":     false,
		"F":     false,
		"False": false,
		"FALSE": false,
	}

	if val, ok := isBool[input]; ok {
		return fmt.Sprintf("%t", val), "bool"
	}

	switch {
	case len(input) > 1 && input[0] == '"' && input[len(input)-1] == '"':
		length := len(input) - 2
		if length > varcharlen {
			return "'" + input[1:len(input)-1] + "'", "varchar" + "(" + strconv.Itoa(length) + ")"
		}
		return "'" + input[1:len(input)-1] + "'", "varchar"
	case isNumericWithU.MatchString(input), isNumericWithI.MatchString(input):
		input = input[:len(input)-1]
		return input, "int8"
	case isFloat.MatchString(input):
		return input, "float8"
	default:
		return "", "UNKNOWN"
	}
}

// makeInfluxDBStmt makes insert statement when telegraf.
func makeInfluxDBStmt(stmtOriginal string) (teleInsertStmt string, teleCreateStmt string) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("invalid data for Influxdb protocol, please check the format.")
		}
	}()

	slice := strings.Split(stmtOriginal, " ")
	attribute := strings.Split(slice[0], ",")
	tblName := attribute[0]

	var colKey, colValue, coltagName, coltagType, colvalueType, colvalueName, hashtag []string

	for index, keyWithValue := range attribute {
		if index >= 1 {
			hashtag = append(hashtag, keyWithValue)
			initObj := strings.Split(keyWithValue, "=")
			colKey = append(colKey, initObj[0])
			coltagName = append(coltagName, initObj[0])
			if len(initObj[1]) > varcharlen {
				typ := "varchar" + "(" + strconv.Itoa(len(initObj[1])) + ")"
				coltagType = append(coltagType, typ)
			} else {
				coltagType = append(coltagType, "varchar")
			}
			initObj[1] = "'" + initObj[1] + "'"
			colValue = append(colValue, initObj[1])
		}
	}
	tagNum := len(coltagType)
	createTagStmt := "(primary_tag varchar not null"
	for index, createKey := range coltagName {
		createTagStmt += ","
		createTagStmt += createKey
		createTagStmt += " "
		createTagStmt += coltagType[index]
	}
	createTagStmt += ")"

	colkeyValue := strings.Split(slice[1], ",")
	for _, keyValue := range colkeyValue {
		obj := strings.Split(keyValue, "=")
		colKey = append(colKey, obj[0])
		colvalueName = append(colvalueName, obj[0])
		value, col := determineType(obj[1])
		if col == "UNKNOWN" {
			return "", ""
		}
		colValue = append(colValue, value)
		colvalueType = append(colvalueType, col)
		coltagType = append(coltagType, col)
	}

	timeStamp := "now()"
	if len(slice) >= 3 {
		timeStamp = slice[2]
		if len(timeStamp) > 13 {
			timeStamp = timeStamp[0:13]
		}
	}

	createColStmt := "(k_timestamp timestamptz not null"
	for index, createKey := range colvalueName {
		createColStmt += ","
		createColStmt += createKey
		createColStmt += " "
		createColStmt += colvalueType[index]
	}
	createColStmt += ")"

	insertKeyStmt := "(primary_tag varchar tag,k_timestamp timestamptz column,"
	for index, insertKey := range colKey {
		if index < tagNum {
			insertKeyStmt += insertKey
			insertKeyStmt += " "
			insertKeyStmt += coltagType[index]
			insertKeyStmt += " tag"
			insertKeyStmt += ","
		} else {
			insertKeyStmt += insertKey
			insertKeyStmt += " "
			insertKeyStmt += coltagType[index]
			insertKeyStmt += " column"
			insertKeyStmt += ","
		}

	}
	// insertKeyStmt += hostKey
	insertKeyStmt = strings.TrimRight(insertKeyStmt, ",")
	insertKeyStmt += ")"

	// insert values
	insertValueStmt := "("
	insertValueStmt += "'" + generateHashString(hashtag) + "'"
	insertValueStmt += ","
	insertValueStmt += timeStamp
	for _, insertValue := range colValue {
		insertValueStmt += ","
		insertValueStmt += insertValue
	}
	insertValueStmt += ")"
	stmtRet := "insert without schema into " + tblName + insertKeyStmt + " values" + insertValueStmt
	createRet := "create table " + tblName + createColStmt + "tags" + createTagStmt + "primary tags(primary_tag)"
	return stmtRet, createRet
}

// handleTelegraf handle telegraf interface
func (s *restfulServer) handleInfluxDB(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := 0
	desc := ""

	restTelegraph, err := s.checkFormat(ctx, w, r, "POST")
	if err != nil {
		return
	}

	connCache, db, err := s.checkConn(ctx, w, r)
	if err != nil {
		return
	}

	var rowsAffected int64
	rowsAffected = 0
	var teleResult gosql.Result
	// Calculate the execution time if needed
	executionTime := float64(0)
	// the program will get a batch of data at once, so it needs to handle it first
	statements := strings.Split(strings.ReplaceAll(restTelegraph, "\r\n", "\n"), "\n")
	numOfStmts := len(statements)

	for i := 0; i < numOfStmts; i++ {
		insertTelegraphStmt, createTelegrafStmt := makeInfluxDBStmt(statements[i])
		TeleInsertStartTime := timeutil.Now()

		if insertTelegraphStmt == "" {
			desc += "wrong influxdb insert statement and please check" + ";"
			code = -1
			continue
		}

		insertflag := strings.Contains(strings.ToLower(insertTelegraphStmt), insertWithoutSchema)
		if !insertflag {
			desc += "can not find insert statement and please check" + ";"
			code = -1
			continue
		}

		teleStmtCount := strings.Count(insertTelegraphStmt, ";")
		if teleStmtCount > 1 {
			desc += "only support single statement for each influxdb interface and please check" + ";"
			code = -1
			continue
		}

		teleResult, err = s.executeWithRetry(db, insertTelegraphStmt, createTelegrafStmt)
		if err != nil {
			desc += err.Error() + ";"
			code = -1
			continue
		}

		curRowsAffected, err := teleResult.RowsAffected()
		if err != nil {
			desc += err.Error() + ";"
			code = -1
			curRowsAffected = 0
			continue
		}

		duration := timeutil.Now().Sub(TeleInsertStartTime)
		executionTime = float64(duration) / float64(time.Second)
		desc += "success" + ";"
		rowsAffected += curRowsAffected
	}

	response := teleInsertResponse{
		baseResponse: &baseResponse{
			Code: code,
			Desc: desc,
			Time: executionTime,
		},
		Rows: rowsAffected,
	}

	s.sendJSONResponse(ctx, w, 0, response, desc)
	connCache.lastLoginTime = timeutil.Now().Unix()
	clear(ctx, db)
}

func (s *restfulServer) executeWithRetry(
	db *gosql.DB, insertStmt, createStmt string,
) (gosql.Result, error) {
	var result gosql.Result
	var err error
	retryDelay := 1 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		result, err = db.Exec(insertStmt)
		if err == nil {
			return result, nil
		}

		influxdbErr := err.Error()
		noExist := `^pq: relation .* does not exist$`
		reNoExist, _ := regexp.Compile(noExist)
		waitForSuc := `^pq:.* Please wait for success$`
		rewaitForSuc, _ := regexp.Compile(waitForSuc)

		if !reNoExist.MatchString(influxdbErr) && !rewaitForSuc.MatchString(influxdbErr) {
			return nil, err
		}
		if reNoExist.MatchString(influxdbErr) {
			_, err = db.Exec(createStmt)
			if err != nil {
				createTableErr := err.Error()
				alExist := `^pq: relation .* already exists$`
				reAlExist, _ := regexp.Compile(alExist)
				reWaitForSuccess, _ := regexp.Compile(waitForSuc)
				if !reAlExist.MatchString(createTableErr) && !reWaitForSuccess.MatchString(createTableErr) {
					return nil, err
				}
			}
		}

		time.Sleep(retryDelay)
		retryDelay *= 2
	}

	return nil, err
}

// generateHashString generate hash byte(64)
func generateHashString(hashtag []string) string {
	sort.Strings(hashtag)

	hash := sha256.New()
	for _, str := range hashtag {
		hash.Write([]byte(str))
	}
	hashSum := hash.Sum(nil)
	return fmt.Sprintf("%x", hashSum)
}

// startsWithKeywords check key words
func startsWithKeywords(s string) (bool, string) {
	s = strings.ToLower(s)

	for _, keyword := range queryKeyWords {
		if strings.HasPrefix(s, keyword) {
			return true, keyword
		}
	}
	return false, ""
}
