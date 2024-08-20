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

#define BUFFER_LEN 256

void handleOdbcError(SQLHANDLE handle, SQLSMALLINT handleType, SQLRETURN retCode)
{
    SQLCHAR sqlState[6];
    SQLINTEGER nativeError;
    SQLCHAR errMsg[BUFFER_LEN];
    SQLSMALLINT errMsgLen;

    SQLGetDiagRec(handleType, handle, 1, sqlState, &nativeError, errMsg, BUFFER_LEN, &errMsgLen);
    printf("SQL Error: %s - %d - %s\n", sqlState, nativeError, errMsg);
}

int main()
{
    SQLHENV henv;
    SQLRETURN ret;
    SQLUINTEGER cpMatch;

    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(henv, SQL_HANDLE_ENV, ret);
        return 1;
    }

    // Set ODBC version to 3
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(henv, SQL_HANDLE_ENV, ret);
        return 1;
    }

    // Get current setting of SQL_ATTR_CP_MATCH
    ret = SQLGetEnvAttr(henv, SQL_ATTR_CP_MATCH, &cpMatch, 0, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(henv, SQL_HANDLE_ENV, ret);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    printf("Current setting of SQL_ATTR_CP_MATCH: %d\n", (int)cpMatch);

    // Set SQL_ATTR_CP_MATCH to a new value
    ret = SQLSetEnvAttr(henv, SQL_ATTR_CP_MATCH, (SQLPOINTER)SQL_CB_DELETE, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(henv, SQL_HANDLE_ENV, ret);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    printf("SQL_ATTR_CP_MATCH set to SQL_CP_RELABEL\n");

    // Free the environment handle
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
