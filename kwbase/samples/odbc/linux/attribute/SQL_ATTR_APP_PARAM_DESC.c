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
#include <sql.h>
#include <sqlext.h>

#define BUFSIZE 1024

typedef struct
{
    SQLSMALLINT SqlType;
    SQLULEN ParamSize;
    SQLSMALLINT DecimalDigits;
} MyParamDesc;

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLAllocHandle(ENV) failed\n");
        return 1;
    }

    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLSetEnvAttr failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLAllocHandle(DBC) failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    char dsn[BUFSIZE] = "kwdb";
    char uid[BUFSIZE] = "root";
    char pwd[BUFSIZE] = "123456";
    ret = SQLConnect(hdbc, (SQLCHAR *)dsn, SQL_NTS, (SQLCHAR *)uid, SQL_NTS, (SQLCHAR *)pwd, SQL_NTS);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLConnect failed\n");
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLAllocHandle(STMT) failed\n");
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    MyParamDesc paramDescArray[1];
    paramDescArray[0].SqlType = SQL_INTEGER;
    paramDescArray[0].ParamSize = 0;
    paramDescArray[0].DecimalDigits = 0;

    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_APP_PARAM_DESC, (SQLPOINTER)paramDescArray, SQL_IS_POINTER);
    if (ret == SQL_SUCCESS)
    {
        printf("SQL_ATTR_APP_PARAM_DESC set successfully\n");
    }
    else if (ret == SQL_SUCCESS_WITH_INFO)
    {
        printf("SQL_ATTR_APP_PARAM_DESC set with information\n");
    }
    else
    {
        printf("SQLSetStmtAttr(SQL_ATTR_APP_PARAM_DESC)\n");
    }
}
