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

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>

#define MAX_COL_NAME_LEN 256

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;
    SQLLEN dataType, colSize;
    SQLSMALLINT numCols;
    SQLUSMALLINT pfExists;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);


    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    ret = SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)30, 0);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLSetConnectAttr\n");
    }
    else
    {
        printf("Connected to the SQLSetConnectAttr\n");
    }

    // connect database
    // Construct the connection string
    SQLWCHAR connStrIn[] = L"DSN=PostgreSQL35W;UID=root;PWD=123456";

    // Connect to the database
    ret = SQLDriverConnect(hdbc, NULL, connStrIn, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the database\n");
    }
    else
    {
        printf("Connected to the database\n");
    }
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    SQLWCHAR pre[] = L"select * from t2 where 1 = ?;";
    ret = SQLPrepare(hstmt, pre, SQL_NTS);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the database\n");
    }
    else
    {
        printf("SUCCEED to the pre\n");
    }

    int param1 = 1;

    // binding Parameters
    ret = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &param1, 0, NULL);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to bind parameter 1\n");
    }
    else
    {
        printf("succeed to bind parameter 1\n");
    }

    ret = SQLExecute(hstmt);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the excute\n");
    }
    else
    {
        printf("SUCCEED to the excute\n");
    }

    // free
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
