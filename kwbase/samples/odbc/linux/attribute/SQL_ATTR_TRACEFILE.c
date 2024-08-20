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

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLRETURN ret;
    SQLCHAR traceFile[1024] = "/root/odbc/log";

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

    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_TRACEFILE, traceFile, SQL_NTS);
    if (ret == SQL_SUCCESS)
    {
        printf("SQL_ATTR_TRACEFILE set successfully to %s\n", traceFile);
    }
    else
    {
        printf("SQLSetConnectAttr(SQL_ATTR_TRACEFILE) failed\n");
    }

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
