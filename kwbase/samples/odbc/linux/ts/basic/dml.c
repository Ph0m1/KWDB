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

#include <sql.h>
#include <sqlext.h>
#include <stdio.h>

// Function to display error messages
void show_error(SQLHANDLE handle, SQLSMALLINT handle_type) {
    SQLWCHAR sql_state[6];
    SQLINTEGER native_error;
    SQLWCHAR error_msg[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT msg_len;
    SQLGetDiagRecW(handle_type, handle, 1, sql_state, &native_error, error_msg, sizeof(error_msg), &msg_len);
    printf("SQL Error: %s - %s\n", sql_state, error_msg);
}

int main() {
    // Initialize ODBC environment and connection handles
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN retcode;

    // Allocate environment handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error allocating environment handle\n");
        return 1;
    }

    // Set environment attribute to use ODBC 3.x
    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error setting environment attribute\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Allocate connection handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error allocating connection handle\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Connect to the data source
    retcode = SQLConnect(hdbc, (SQLCHAR*)"kwdb", SQL_NTS, (SQLCHAR*)"root", SQL_NTS, (SQLCHAR*)"123456", SQL_NTS);   
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error connecting to data source\n");
        show_error(hdbc, SQL_HANDLE_DBC);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Allocate statement handle
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error allocating statement handle\n");
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Prepare the statement
    SQLCHAR *sql_query = (SQLCHAR *)"DROP DATABASE if exists test cascade; CREATE ts DATABASE test; CREATE TABLE test.t1(k_timestamp TIMESTAMPTZ NOT NULL,id INT NOT NULL,e1 INT2,e2 INT4) ATTRIBUTES (code1 INT2 NOT NULL,code2 INT4) PRIMARY TAGS(code1);";
    retcode = SQLPrepare(hstmt, sql_query, SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error preparing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Execute the statement
    retcode = SQLExecute(hstmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error executing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }
    printf("create ts table successfully\n");

    // Prepare the statement
    SQLCHAR *query2 = (SQLCHAR *)"INSERT INTO test.t1 VALUES ('2023-10-23 12:34:56.789+08', 1, 10, 20, 10, 20);";
    retcode = SQLPrepare(hstmt, query2, SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error preparing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Execute the statement
    retcode = SQLExecute(hstmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error executing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }
    printf("insert into ts table successfully\n");

    // Prepare the statement
    SQLCHAR *query3 = (SQLCHAR *)"UPDATE test.t1 SET code2 = 15 WHERE code1 = 10;";
    retcode = SQLPrepare(hstmt, query3, SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error preparing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Execute the statement
    retcode = SQLExecute(hstmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error executing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }
    printf("update ts table successfully\n");

    // Prepare the statement
    SQLCHAR *query4 = (SQLCHAR *)"DELETE FROM test.t1 WHERE code1 = 10;";
    retcode = SQLPrepare(hstmt, query4, SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error preparing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Execute the statement
    retcode = SQLExecute(hstmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error executing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }
    printf("delete ts table successfully\n");

    // Prepare the statement
    SQLCHAR *query5 = (SQLCHAR *)"SELECT * FROM test.t1;";
    retcode = SQLPrepare(hstmt, query5, SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error preparing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Execute the statement
    retcode = SQLExecute(hstmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error executing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }
    printf("select ts table successfully\n");

    // Free resources and disconnect
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
