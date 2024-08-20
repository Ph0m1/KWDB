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
    //retcode = SQLPrepareW(hstmt, sqlStatement, SQL_NTS);
    SQLCHAR *sql_query = (SQLCHAR *)"DROP DATABASE if exists test_multi_select cascade;CREATE ts DATABASE test_multi_select;CREATE TABLE test_multi_select.t1(k_timestamp TIMESTAMPTZ NOT NULL,id INT NOT NULL,e1 INT2,e2 INT4,e3 INT8,e4 FLOAT4,e5 FLOAT8,e6 BOOL,e7 TIMESTAMPTZ,e8 CHAR(1023),e9 NCHAR(255),e10 VARCHAR(4096),e11 CHAR,e12 CHAR(255),e13 NCHAR,e14 NVARCHAR(4096),e15 VARCHAR(1023), e16 NVARCHAR(200),e17 NCHAR(255),e18 CHAR(200),e19 VARBYTES,e20 VARBYTES(60),e21 VARCHAR,e22 NVARCHAR) ATTRIBUTES (code1 INT2 NOT NULL,code2 INT4,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 VARBYTES(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL) PRIMARY TAGS(code1,code14,code8,code16);";
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

    SQLCHAR *sql_query2 = (SQLCHAR *)"INSERT INTO test_multi_select.t1 VALUES('1976-10-20 12:00:12.123',3,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');";
    retcode = SQLPrepare(hstmt, sql_query2, SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        printf("Error preparing statement\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    printf("insert into ts table successfully\n");

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

    // Free resources and disconnect
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}