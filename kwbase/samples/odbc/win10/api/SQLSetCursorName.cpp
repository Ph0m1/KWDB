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
    SQLHENV envHandle;
    SQLHDBC connHandle;
    SQLHSTMT stmtHandle;
    SQLRETURN retcode;
    SQLWCHAR cursorName[] = L"MyCursor";

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &envHandle);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(SQL_HANDLE_ENV) failed\n");
        return 1;
    }

    // set odbc version 3.0
    retcode = SQLSetEnvAttr(envHandle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLSetEnvAttr failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, envHandle);
        return 1;
    }

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, envHandle, &connHandle);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(SQL_HANDLE_DBC) failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, envHandle);
        return 1;
    }

    SQLWCHAR dsn[] = L"PostgreSQL35W";
    SQLWCHAR uid[] = L"root";
    SQLWCHAR pwd[] = L"123456";
    retcode = SQLConnectW(connHandle, dsn, SQL_NTS, uid, SQL_NTS, pwd, SQL_NTS);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        printf("SQLConnectW failed\n");
        SQLFreeHandle(SQL_HANDLE_DBC, connHandle);
        SQLFreeHandle(SQL_HANDLE_ENV, envHandle);
        return 1;
    }

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, connHandle, &stmtHandle);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(SQL_HANDLE_STMT) failed\n");
        SQLDisconnect(connHandle);
        SQLFreeHandle(SQL_HANDLE_DBC, connHandle);
        SQLFreeHandle(SQL_HANDLE_ENV, envHandle);
        return 1;
    }

    retcode = SQLSetCursorNameW(stmtHandle, cursorName, SQL_NTS);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        printf("SQLSetCursorNameW failed\n");
        // handle error
    }
    else
    {
        printf("SQLSetCursorNameW succeed\n");
    }

    // clear
    SQLFreeHandle(SQL_HANDLE_STMT, stmtHandle);
    SQLDisconnect(connHandle);
    SQLFreeHandle(SQL_HANDLE_DBC, connHandle);
    SQLFreeHandle(SQL_HANDLE_ENV, envHandle);

    return 0;
}
