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

void handleOdbcError(SQLHANDLE handle, SQLSMALLINT handleType, SQLRETURN retCode)
{
    SQLCHAR sqlState[6];
    SQLINTEGER nativeError;
    SQLCHAR errMsg[1024];
    SQLSMALLINT errMsgLen;

    SQLGetDiagRec(handleType, handle, 1, sqlState, &nativeError, errMsg, 1024, &errMsgLen);
    printf("SQL Error: %s - %d - %s\n", sqlState, nativeError, errMsg);
}

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;
    SQLULEN concurrency;

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

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(hdbc, SQL_HANDLE_DBC, ret);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Connect to the data source
    ret = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(hdbc, SQL_HANDLE_DBC, ret);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Allocate statement handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(hstmt, SQL_HANDLE_STMT, ret);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Set SQL_ATTR_CONCURRENCY
    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY, (SQLPOINTER)SQL_CONCUR_READ_ONLY, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(hstmt, SQL_HANDLE_STMT, ret);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Get current concurrency attribute
    ret = SQLGetStmtAttr(hstmt, SQL_ATTR_CONCURRENCY, &concurrency, 0, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(hstmt, SQL_HANDLE_STMT, ret);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    printf("Concurrency Attribute: %lu\n", concurrency);

    // Free the statement, connection, and environment handles
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
