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

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLRETURN ret;
    SQLSMALLINT access_mode;

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

    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_WRITE, 0);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLSetConnectAttr(SQL_ATTR_ACCESS_MODE) failed\n");
        // handle error...
    }
    else
    {
        printf("SQL_ATTR_ACCESS_MODE set to SQL_MODE_READ_WRITE successfully\n");
    }

    ret = SQLGetConnectAttr(hdbc, SQL_ATTR_ACCESS_MODE, &access_mode, 0, NULL);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLGetConnectAttr(SQL_ATTR_ACCESS_MODE) failed\n");
    }
    else
    {
        printf("Current access mode: %d\n", access_mode);
    }

    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
