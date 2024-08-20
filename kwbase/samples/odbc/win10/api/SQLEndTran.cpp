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

#pragma comment(lib, "odbc32.lib")

int main()
{
    SQLHENV envHandle;
    SQLHDBC connHandle;
    SQLHSTMT stmtHandle;
    SQLRETURN retcode;

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &envHandle);
    if (SQL_SUCCESS != retcode)
    {
        // handle error
        return 1;
    }

    // set odbc version 3.0
    retcode = SQLSetEnvAttr(envHandle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (SQL_SUCCESS != retcode)
    {
        // handle error
        return 1;
    }

    // alloc conn handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, envHandle, &connHandle);
    if (SQL_SUCCESS != retcode)
    {
        // handle error
        return 1;
    }

    // connect database
    // Construct the connection string
    SQLWCHAR connStrIn[] = L"DSN=PostgreSQL35W;UID=root;PWD=123456";

    // Connect to the database
    retcode = SQLDriverConnect(connHandle, NULL, connStrIn, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        // handle error
        return 1;
    }

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, connHandle, &stmtHandle);
    if (SQL_SUCCESS != retcode)
    {
        // handle error
        return 1;
    }

    SQLWCHAR query[1024] = L"SELECT a FROM t2"; // table2
    retcode = SQLExecDirect(stmtHandle, query, SQL_NTS);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        // handle error
        return 1;
    }

    SQLSMALLINT completionType = SQL_COMMIT;
    retcode = SQLEndTran(SQL_HANDLE_DBC, connHandle, completionType);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        // handle error
        wprintf(L"SQLEndTran FAIL\n");
        return 1;
    }
    else
    {
        wprintf(L"SQLEndTran success\n");
    }

    // clear
    SQLFreeHandle(SQL_HANDLE_STMT, stmtHandle);
    SQLDisconnect(connHandle);
    SQLFreeHandle(SQL_HANDLE_DBC, connHandle);
    SQLFreeHandle(SQL_HANDLE_ENV, envHandle);

    return 0;
}
