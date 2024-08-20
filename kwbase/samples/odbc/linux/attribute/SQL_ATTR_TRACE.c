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

#define BUFSIZE 1024

void PrintError(SQLSMALLINT HandleType, SQLHANDLE Handle)
{
    SQLCHAR SqlState[6], Message[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER NativeError;
    SQLSMALLINT i, MessageLength;
    SQLRETURN ret;

    i = 1;
    while ((ret = SQLGetDiagRec(HandleType, Handle, i, SqlState, &NativeError, Message, sizeof(Message), &MessageLength)) == SQL_SUCCESS)
    {
        printf("Error %d: %s\n", i, Message);
        i++;
    }
}

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLRETURN ret;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLAllocHandle(env) failed\n");
        return 1;
    }

    // set odbc version
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLSetEnvAttr(ODBC version) failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLAllocHandle(dbc) failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_TRACE, SQL_OPT_TRACE_ON, SQL_IS_INTEGER);
    if (ret == SQL_SUCCESS)
    {
        printf("SQL_ATTR_TRACE set successfully\n");
    }
    else
    {
        printf("SQLSetConnectAttr(SQL_ATTR_TRACE) failed\n");
        PrintError(SQL_HANDLE_DBC, hdbc);
    }

    char dsn[BUFSIZE] = "kwdb";
    char uid[BUFSIZE] = "root";
    char pwd[BUFSIZE] = "123456";
    ret = SQLConnect(hdbc, (SQLCHAR *)dsn, SQL_NTS, (SQLCHAR *)uid, SQL_NTS, (SQLCHAR *)pwd, SQL_NTS);
    if (ret == SQL_SUCCESS)
    {
        printf("Connected to the data source successfully\n");
        SQLDisconnect(hdbc);
    }
    else if (ret == SQL_SUCCESS_WITH_INFO)
    {
        printf("Connected to the data source with some warnings\n");
        SQLDisconnect(hdbc);
    }
    else
    {
        printf("Connection to the data source failed\n");
    }

    // clear
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
