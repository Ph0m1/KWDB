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
    SQLWCHAR inputSql[1024];
    SQLWCHAR outputSql[4096];
    SQLINTEGER outputLength;

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(ENV) failed\n");
        // handle error
        return 1;
    }

    // set odbc version
    SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(DBC) failed\n");
        // handle error
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // connect database
    retcode = SQLConnect(hdbc, (SQLWCHAR *)L"PostgreSQL35W", SQL_NTS, (SQLWCHAR *)L"root", SQL_NTS, (SQLWCHAR *)L"123456", SQL_NTS);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        printf("SQLConnect failed\n");
        // handle error
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    swprintf_s(inputSql, sizeof(inputSql) / sizeof(inputSql[0]), L"SELECT * FROM t1");

    outputLength = sizeof(outputSql) / sizeof(outputSql[0]);
    retcode = SQLNativeSql(hdbc, inputSql, SQL_NTS, outputSql, outputLength, &outputLength);
    if (SQL_SUCCESS != retcode && SQL_SUCCESS_WITH_INFO != retcode)
    {
        printf("SQLNativeSql failed\n");
        // handle error
    }
    else
    {
        wprintf(L"Native SQL: %s\n", outputSql);
    }

    // clear
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
