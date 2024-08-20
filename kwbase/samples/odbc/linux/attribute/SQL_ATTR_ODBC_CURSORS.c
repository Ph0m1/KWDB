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
#define MAX_MSG 512

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
        printf("SQLAllocHandle(ENV) failed\n");
        return 1;
    }

    // set odbc version 3.0
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

    SQLULEN cursorType = SQL_CUR_USE_DRIVER;
    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_ODBC_CURSORS, (SQLPOINTER)&cursorType, SQL_IS_UINTEGER);
    if (ret == SQL_SUCCESS)
    {
        printf("SQL_ATTR_ODBC_CURSORS set successfully to: %lu\n", cursorType);
    }
    else
    {
        printf("SQLSetConnectAttr(SQL_ATTR_ODBC_CURSORS) failed\n");
        PrintError(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // clear
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
