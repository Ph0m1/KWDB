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
    SQLHSTMT stmtHandle = SQL_NULL_HANDLE;
    SQLRETURN retcode;
    SQLWCHAR dsn[] = L"PostgreSQL35W";
    SQLWCHAR uid[] = L"root";
    SQLWCHAR pwd[] = L"123456";
    SQLSMALLINT rowCount = 0;

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &envHandle);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(SQL_HANDLE_ENV) failed\n");
    }

    // set odbc version 3.0
    retcode = SQLSetEnvAttr(envHandle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLSetEnvAttr failed\n");
    }

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, envHandle, &connHandle);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(SQL_HANDLE_DBC) failed\n");
    }

    retcode = SQLConnectW(connHandle, dsn, SQL_NTS, uid, SQL_NTS, pwd, SQL_NTS);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        printf("SQLConnectW failed\n");
    }

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, connHandle, &stmtHandle);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(SQL_HANDLE_STMT) failed\n");
    }

    // exec sql
    SQLWCHAR sqlStatement[] = L"SELECT * FROM t2";
    retcode = SQLExecDirectW(stmtHandle, sqlStatement, SQL_NTS);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        printf("SQLExecDirectW failed\n");
    }

    while (SQLFetchScroll(stmtHandle, SQL_FETCH_NEXT, 0) != SQL_NO_DATA)
    {
        SQLCHAR id[1024];
        SQLLEN idIndicator, nameIndicator;

        retcode = SQLGetData(stmtHandle, 1, SQL_C_CHAR, id, 1024, &idIndicator);
        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
        {
            // printf ID
            printf("%s\n", id);
        }
    }


    return 0;
}
