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

    // prepare sql
    SQLCHAR *sql_query = (SQLCHAR *)"INSERT INTO d1.t1 (id, name) VALUES (?, ?)";

    // binding Parameters
    SQLINTEGER param1 = 123;
    SQLLEN param1_len = SQL_NTS;
    SQLINTEGER param2 = 456;
    SQLLEN param2_len = SQL_NTS;

    ret = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &param1, 0, &param1_len);
    handle_error(hstmt, ret);

    ret = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &param2, 0, &param2_len);
    handle_error(hstmt, ret);

    SQLULEN param_operations[2] = {SQL_PARAM_INPUT, SQL_PARAM_INPUT};

    // set SQL_ATTR_PARAM_OPERATION_PTR
    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_OPERATION_PTR, param_operations, 0);
    handle_error(hstmt, ret);

    // exec sql
    ret = SQLExecDirect(hstmt, sql_query, SQL_NTS);
    handle_error(hstmt, ret);

    printf("Record inserted successfully!\n");

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
