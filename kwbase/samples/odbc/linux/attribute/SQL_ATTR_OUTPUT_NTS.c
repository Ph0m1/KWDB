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
    SQLUINTEGER outputNts;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        printf("Error: SQLAllocHandle(SQL_HANDLE_ENV) failed\n");
        return 1;
    }

    // set odbc version
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3_80, SQL_IS_INTEGER);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        printf("Error: SQLSetEnvAttr(SQL_ATTR_ODBC_VERSION) failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }
    printf("SQLSetEnvAttr(SQL_ATTR_ODBC_VERSION) success\n");

    // Get current setting of SQL_ATTR_OUTPUT_NTS
    ret = SQLGetEnvAttr(henv, SQL_ATTR_OUTPUT_NTS, &outputNts, 0, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(henv, SQL_HANDLE_ENV, ret);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    printf("Current setting of SQL_ATTR_OUTPUT_NTS: %d\n", (int)outputNts);

    // Set SQL_ATTR_OUTPUT_NTS to a new value
    ret = SQLSetEnvAttr(henv, SQL_ATTR_OUTPUT_NTS, (SQLPOINTER)SQL_TRUE, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(henv, SQL_HANDLE_ENV, ret);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // free
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
