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

    SQLCHAR *table_name = (SQLCHAR *)"d1.t1";
    ret = SQLStatistics(hstmt, NULL, 0, NULL, 0, table_name, SQL_NTS, SQL_INDEX_UNIQUE, SQL_ENSURE);
    handle_error(hstmt, ret);

    SQLCHAR index_name[256];
    SQLSMALLINT index_type;
    SQLSMALLINT seq_in_index;
    SQLCHAR column_name[256];
    SQLSMALLINT collation;
    SQLSMALLINT cardinality;
    SQLSMALLINT pages;

    // while (SQLFetch(hstmt) == SQL_SUCCESS) {
    SQLGetData(hstmt, 6, SQL_C_CHAR, index_name, sizeof(index_name), NULL);
    SQLGetData(hstmt, 7, SQL_C_SSHORT, &index_type, 0, NULL);
    SQLGetData(hstmt, 8, SQL_C_SSHORT, &seq_in_index, 0, NULL);
    SQLGetData(hstmt, 9, SQL_C_CHAR, column_name, sizeof(column_name), NULL);
    SQLGetData(hstmt, 10, SQL_C_SSHORT, &collation, 0, NULL);
    SQLGetData(hstmt, 11, SQL_C_SSHORT, &cardinality, 0, NULL);
    SQLGetData(hstmt, 12, SQL_C_SSHORT, &pages, 0, NULL);

    printf("Index Name: %s, Index Type: %d, Sequence in Index: %d, Column Name: %s, Collation: %d, Cardinality: %d, Pages: %d\n", index_name, index_type, seq_in_index, column_name, collation, cardinality, pages);

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
