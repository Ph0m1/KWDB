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

#include <stdio.h>
#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>

#define NUM_PARAMS 2

void CheckRC(SQLRETURN rc, SQLSMALLINT HandleType, SQLHANDLE Handle)
{
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
    {
        fprintf(stderr, "Error %d in %s\n", (int)rc,
                (HandleType == SQL_HANDLE_DBC) ? "DBC Function" : (HandleType == SQL_HANDLE_STMT) ? "STMT Function"
                                                                                                  : "Unknown Function");
        exit(1);
    }
}

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;
    SQLINTEGER ParamValues[NUM_PARAMS] = {1, 2};
    SQLLEN ParamIndicators[NUM_PARAMS] = {0, 0};
    SQLCHAR SQLStmt[1024] = "INSERT INTO d1.t1 (id) VALUES (?, ?)";

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    CheckRC(ret, SQL_HANDLE_ENV, NULL);

    // set odbc version 3.x
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    CheckRC(ret, SQL_HANDLE_ENV, NULL);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    CheckRC(ret, SQL_HANDLE_DBC, NULL);
    ret = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, NULL, 0, NULL, 0);
    CheckRC(ret, SQL_HANDLE_DBC, hdbc);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    CheckRC(ret, SQL_HANDLE_STMT, NULL);

    // set SQL_ATTR_PARAM_BIND_TYPE SQL_PARAM_BIND_BY_COLUMN
    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)SQL_PARAM_BIND_BY_COLUMN, 0);
    CheckRC(ret, SQL_HANDLE_STMT, hstmt);
    printf("SQL_ATTR_PARAM_BIND_TYPE set to SQL_PARAM_BIND_BY_COLUMN successfully\n");

    ret = SQLPrepare(hstmt, SQLStmt, SQL_NTS);
    CheckRC(ret, SQL_HANDLE_STMT, hstmt);

    for (int i = 0; i < NUM_PARAMS; ++i)
    {
        ret = SQLBindParameter(hstmt, i + 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
                               &ParamValues[i], sizeof(SQLINTEGER), &ParamIndicators[i]);
        CheckRC(ret, SQL_HANDLE_STMT, hstmt);
    }
}
