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

void handle_error(SQLHANDLE handle, SQLRETURN ret)
{
    SQLCHAR sql_state[6];
    SQLINTEGER native_error;
    SQLCHAR message_text[256];
    SQLSMALLINT text_length;

    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        SQLGetDiagRec(SQL_HANDLE_STMT, handle, 1, sql_state, &native_error, message_text, sizeof(message_text), &text_length);
        printf("Error %s: %s\n", sql_state, message_text);
        exit(1);
    }
}

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    handle_error(henv, ret);

    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    handle_error(henv, ret);

    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    handle_error(hdbc, ret);

    // connect database
    ret = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    handle_error(hdbc, ret);

    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    handle_error(hstmt, ret);

    // exec sql
    SQLCHAR *sql_query = (SQLCHAR *)"SELECT * FROM d1.t1";
    ret = SQLExecDirect(hstmt, sql_query, SQL_NTS);
    handle_error(hstmt, ret);

    SQLSMALLINT col_count;
    ret = SQLNumResultCols(hstmt, &col_count);
    handle_error(hstmt, ret);

    for (SQLSMALLINT i = 1; i <= col_count; i++)
    {
        SQLCHAR col_name[256];
        SQLSMALLINT col_name_length;
        SQLSMALLINT data_type;
        SQLULEN column_size;
        SQLSMALLINT decimal_digits;
        SQLSMALLINT nullable;

        ret = SQLDescribeCol(hstmt, i, col_name, sizeof(col_name), &col_name_length, &data_type, &column_size, &decimal_digits, &nullable);
        handle_error(hstmt, ret);

        printf("Column %d Name: %s\n", i, col_name);
        printf("Data Type: %d\n", data_type);
        printf("Column Size: %lu\n", column_size);
        printf("Decimal Digits: %d\n", decimal_digits);
        printf("Nullable: %d\n", nullable);
    }

    // free
    ret = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    handle_error(hstmt, ret);

    ret = SQLDisconnect(hdbc);
    handle_error(hdbc, ret);

    ret = SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    handle_error(hdbc, ret);

    ret = SQLFreeHandle(SQL_HANDLE_ENV, henv);
    handle_error(henv, ret);

    return 0;
}
