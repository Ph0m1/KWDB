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
    SQLSMALLINT outLength;
    SQLINTEGER rowCount = 0;

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

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, connHandle, &stmtHandle);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(stmt) failed\n");
        SQLDisconnect(connHandle);
        SQLFreeHandle(SQL_HANDLE_DBC, connHandle);
        SQLFreeHandle(SQL_HANDLE_ENV, envHandle);
        return 1;
    }

    SQLWCHAR *catalogName = NULL;
    SQLSMALLINT cchCatalogName = SQL_NTS;
    SQLWCHAR *schemaName = NULL;
    SQLSMALLINT cchSchemaName = SQL_NTS;
    SQLWCHAR tableName[] = L"t2";
    SQLSMALLINT cchTableName = SQL_NTS;
    SQLUSMALLINT unique = SQL_FALSE;
    SQLUSMALLINT accuracy = SQL_ENSURE;

    retcode = SQLStatisticsW(stmtHandle,
                             catalogName,
                             cchCatalogName,
                             schemaName,
                             cchSchemaName,
                             tableName,
                             cchTableName,
                             SQL_INDEX_UNIQUE | SQL_INDEX_ALL,
                             accuracy);

    if (SQL_SUCCESS == retcode || SQL_SUCCESS_WITH_INFO == retcode)
    {
        printf("Statistics retrieved for table %ls\n", tableName);
    }
    else
    {
        printf("SQLStatisticsW failed\n");
    }

    // free
    SQLFreeHandle(SQL_HANDLE_STMT, stmtHandle);
    SQLDisconnect(connHandle);
    SQLFreeHandle(SQL_HANDLE_DBC, connHandle);
    SQLFreeHandle(SQL_HANDLE_ENV, envHandle);

    return 0;
}
