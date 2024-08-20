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

void handle_error(SQLHANDLE handle, SQLRETURN ret, SQLSMALLINT handle_type)
{
    SQLCHAR sql_state[6];
    SQLINTEGER native_error;
    SQLCHAR message_text[256];
    SQLSMALLINT text_length;

    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_error, message_text, sizeof(message_text), &text_length);
        printf("Error %s: %s\n", sql_state, message_text);
        exit(1);
    }
}

int main()
{
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;
    SQLRETURN ret;

    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    handle_error(env, ret, SQL_HANDLE_ENV);

    // Set the ODBC version to be used
    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    handle_error(env, ret, SQL_HANDLE_ENV);

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    handle_error(dbc, ret, SQL_HANDLE_ENV);

    // Connect to data source
    ret = SQLConnect(dbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        handle_error(dbc, ret, SQL_HANDLE_DBC);
    }

    // Allocate statement handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    handle_error(stmt, ret, SQL_HANDLE_DBC);

    // Input SQL query
    SQLCHAR *in_query = (SQLCHAR *)"SELECT * FROM d1.t1 WHERE id = 1";
    SQLCHAR out_query[1024];
    SQLSMALLINT out_query_len;

    // Convert SQL to native SQL
    ret = SQLNativeSql(dbc, in_query, SQL_NTS, out_query, sizeof(out_query), &out_query_len);
    handle_error(stmt, ret, SQL_HANDLE_STMT);
    printf("Original SQL: %s\n", in_query);
    printf("Native SQL: %s\n", out_query);

    // Free the statement handle
    ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    handle_error(stmt, ret, SQL_HANDLE_STMT);

    // Disconnect from data source
    ret = SQLDisconnect(dbc);
    handle_error(dbc, ret, SQL_HANDLE_DBC);

    // Free the connection handle
    ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    handle_error(dbc, ret, SQL_HANDLE_DBC);

    // Free the environment handle
    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    handle_error(env, ret, SQL_HANDLE_ENV);

    return 0;
}
