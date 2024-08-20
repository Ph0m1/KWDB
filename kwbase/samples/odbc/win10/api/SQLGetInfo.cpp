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
    SQLHENV envHandle = SQL_NULL_HANDLE;
    SQLHDBC connHandle = SQL_NULL_HANDLE;
    SQLRETURN retcode;
    SQLSMALLINT outLength;
    SQLWCHAR buffer[256];

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &envHandle);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(env) failed\n");
        return 1;
    }

    // set odbc version 3.x
    SQLSetEnvAttr(envHandle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, envHandle, &connHandle);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(conn) failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, envHandle);
        return 1;
    }

    SQLWCHAR connString[] = L"DSN=PostgreSQL35W;UID=root;PWD=123456";
    retcode = SQLDriverConnect(connHandle, NULL, connString, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        printf("SQLConnectW failed\n");
        SQLFreeHandle(SQL_HANDLE_DBC, connHandle);
        SQLFreeHandle(SQL_HANDLE_ENV, envHandle);
        return 1;
    }

    retcode = SQLGetInfoW(connHandle, SQL_DBMS_VER, buffer, sizeof(buffer) / sizeof(SQLWCHAR), &outLength);
    if (SQL_SUCCESS == retcode || SQL_SUCCESS_WITH_INFO == retcode)
    {
        printf("DBMS Version: %ls\n", buffer);
    }
    else
    {
        printf("SQLGetInfoW failed\n");
    }

    // free
    SQLDisconnect(connHandle);
    SQLFreeHandle(SQL_HANDLE_DBC, connHandle);
    SQLFreeHandle(SQL_HANDLE_ENV, envHandle);

    return 0;
}
