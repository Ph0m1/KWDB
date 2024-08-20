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

#define BUFSIZE 1024

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

    SQLPOINTER access_mode;
    retcode = SQLSetConnectAttr(connHandle, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_WRITE, 0);
    if (retcode != SQL_SUCCESS)
    {
        printf("SQLSetConnectAttr(SQL_ATTR_ACCESS_MODE) failed\n");
        // handle error...
    }
    else
    {
        printf("SQL_ATTR_ACCESS_MODE set to SQL_MODE_READ_WRITE successfully\n");
    }

    retcode = SQLGetConnectAttr(connHandle, SQL_ATTR_ACCESS_MODE, &access_mode, 0, NULL);
    if (retcode != SQL_SUCCESS)
    {
        printf("SQLGetConnectAttr(SQL_ATTR_ACCESS_MODE) failed\n");
    }
    else
    {
        printf("Current access mode: %d\n", access_mode);
    }

    // clear
    SQLDisconnect(connHandle);
    SQLFreeHandle(SQL_HANDLE_DBC, connHandle);
    SQLFreeHandle(SQL_HANDLE_ENV, envHandle);

    return 0;
}
