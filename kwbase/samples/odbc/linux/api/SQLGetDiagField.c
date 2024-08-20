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
    SQLRETURN ret;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    handle_error(env, ret, SQL_HANDLE_ENV);

    // set env attr
    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    handle_error(env, ret, SQL_HANDLE_ENV);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    handle_error(dbc, ret, SQL_HANDLE_DBC);

    ret = SQLConnect(dbc, (SQLCHAR *)"db", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        SQLCHAR sql_state[6];
        SQLINTEGER native_error;
        SQLCHAR message_text[256];
        SQLSMALLINT text_length;

        SQLGetDiagField(SQL_HANDLE_DBC, dbc, 1, SQL_DIAG_SQLSTATE, sql_state, sizeof(sql_state), &text_length);
        SQLGetDiagField(SQL_HANDLE_DBC, dbc, 1, SQL_DIAG_NATIVE, &native_error, sizeof(native_error), &text_length);
        SQLGetDiagField(SQL_HANDLE_DBC, dbc, 1, SQL_DIAG_MESSAGE_TEXT, message_text, sizeof(message_text), &text_length);

        printf("Connection Error %s (%d): %s\n", sql_state, (int)native_error, message_text);
    }
    else
    {
        printf("Connected to the database\n");
    }

    // free
    ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    handle_error(dbc, ret, SQL_HANDLE_DBC);

    // free
    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    handle_error(env, ret, SQL_HANDLE_ENV);

    return 0;
}
