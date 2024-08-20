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

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLRETURN retcode;

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        printf("SQLAllocHandle(SQL_HANDLE_ENV) failed\n");
        return 1;
    }

    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        printf("SQLSetEnvAttr(SQL_ATTR_ODBC_VERSION) failed\n");
        return 1;
    }

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        printf("SQLAllocHandle(SQL_HANDLE_DBC) failed\n");
        return 1;
    }

    SQLWCHAR connStrIn[] = L"DSN=PostgreSQL35W;UID=root;PWD=123456";

    // connect database
    retcode = SQLDriverConnect(hdbc, NULL, connStrIn, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        printf("SQLDriverConnect failed\n");
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    SQLULEN cursorType = SQL_CUR_USE_DRIVER;

    retcode = SQLSetConnectAttr(hdbc, SQL_ATTR_ODBC_CURSORS, (SQLPOINTER)&cursorType, SQL_IS_UINTEGER);
    if (retcode == SQL_SUCCESS)
    {
        printf("SQL_ATTR_ODBC_CURSORS set successfully to: %lu\n", cursorType);
    }
    else
    {
        printf("SQLSetConnectAttr(SQL_ATTR_ODBC_CURSORS) failed\n");
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // disconnect conn
    retcode = SQLDisconnect(hdbc);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        printf("SQLDisconnect failed\n");
    }

    // free
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
