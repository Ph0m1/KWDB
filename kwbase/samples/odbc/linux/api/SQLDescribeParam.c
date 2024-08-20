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

#define BUFFER_SIZE 100

void handle_error(SQLHANDLE handle, SQLSMALLINT handle_type, SQLRETURN retcode)
{
    SQLCHAR sqlstate[6];
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER native_error;
    SQLSMALLINT message_length;

    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        SQLGetDiagRec(handle_type, handle, 1, sqlstate, &native_error, message, sizeof(message), &message_length);
        printf("SQL error %s: %s\n", sqlstate, message);
    }
}

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN retcode;
    SQLCHAR connStr[] = "DSN=kwdb;UID=root;PWD=123456";
    SQLCHAR query[] = "SELECT * FROM d1.t1 WHERE id = ?";
    SQLSMALLINT param_count;
    SQLSMALLINT data_type;
    SQLULEN column_size;
    SQLSMALLINT decimal_digits;
    SQLSMALLINT nullable;

    // Allocate environment handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    handle_error(henv, SQL_HANDLE_ENV, retcode);

    // Set ODBC version
    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    handle_error(henv, SQL_HANDLE_ENV, retcode);

    // Allocate connection handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    handle_error(hdbc, SQL_HANDLE_DBC, retcode);

    // Connect to the database
    retcode = SQLDriverConnect(hdbc, NULL, connStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    handle_error(hdbc, SQL_HANDLE_DBC, retcode);

    // Allocate statement handle
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    handle_error(hstmt, SQL_HANDLE_STMT, retcode);

    // Prepare the SQL statement
    retcode = SQLPrepare(hstmt, query, SQL_NTS);
    handle_error(hstmt, SQL_HANDLE_STMT, retcode);

    // Describe parameters
    retcode = SQLNumParams(hstmt, &param_count);
    handle_error(hstmt, SQL_HANDLE_STMT, retcode);

    for (SQLSMALLINT i = 1; i <= param_count; ++i)
    {
        retcode = SQLDescribeParam(hstmt, i, &data_type, &column_size, &decimal_digits, &nullable);
        handle_error(hstmt, SQL_HANDLE_STMT, retcode);

        printf("Parameter %d:\n", i);
        printf("  Data type: %d\n", data_type);
        printf("  Column size: %lu\n", column_size);
        printf("  Decimal digits: %d\n", decimal_digits);
        printf("  Nullable: %d\n", nullable);
    }

    // Clean up
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
