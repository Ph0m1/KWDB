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

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>

int main() {
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLRETURN ret;
    SQLLEN id;
    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS) {
        // Handle error
    }

    // Set ODBC version
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS) {
        // Handle error
    }

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret != SQL_SUCCESS) {
        // Handle error
    }

    // Construct the connection string
    SQLWCHAR connStrIn[] = L"DSN=wyA;UID=root;PWD=123456;";

    // Connect to the database
    ret = SQLDriverConnect(hdbc, NULL, connStrIn, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (ret != SQL_SUCCESS) {
        printf("Failed to the database\n");
    }
    else {
        printf("Connected to the database\n");
    }

    // Execute SQL query
    SQLHSTMT hstmt;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS) {
        // Handle error
    }

    SQLHSTMT hstmt8;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt8);
    if (ret != SQL_SUCCESS) {
        // Handle error
    }

    SQLHSTMT hstmt9;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt9);
    if (ret != SQL_SUCCESS) {
        // Handle error
    }

    SQLWCHAR query[] = L"set client_encoding to 'gbk';";
    SQLWCHAR query8[] = L"DROP DATABASE if exists test_multi_select cascade;CREATE ts DATABASE test_multi_select;CREATE TABLE test_multi_select.t1(k_timestamp TIMESTAMPTZ NOT NULL,id INT NOT NULL,e1 INT2,e2 INT4,e3 INT8,e4 FLOAT4,e5 FLOAT8,e6 BOOL,e7 TIMESTAMPTZ,e8 CHAR(1023),e9 NCHAR(255),e10 VARCHAR(4096),e11 CHAR,e12 CHAR(255),e13 NCHAR,e14 NVARCHAR(4096),e15 VARCHAR(1023), e16 NVARCHAR(200),e17 NCHAR(255),e18 CHAR(200),e19 VARBYTES,e20 VARBYTES(60),e21 VARCHAR,e22 NVARCHAR) ATTRIBUTES (code1 INT2 NOT NULL,code2 INT4,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 VARBYTES(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL) PRIMARY TAGS(code1,code14,code8,code16);";
    SQLWCHAR query9[] = L";";
    printf("Test SQL1----------------------\n");
    ret = SQLExecDirect(hstmt, query, SQL_NTS);
    if (ret != SQL_SUCCESS) {
        printf("Failed to the set\n");
    }
    else {
        printf("Successed to the set\n");
    }
    printf("Test SQL9----------------------\n");
    ret = SQLExecDirect(hstmt8, query8, SQL_NTS);
    if (ret != SQL_SUCCESS) {
        printf("Failed to the drop\n");
    }
    else {
        printf("Successed to the drop\n");
    }
    ret = SQLExecDirect(hstmt8, query9, SQL_NTS);
    if (ret != SQL_SUCCESS) {
        printf("Failed to the insert\n");
    }
    else {
        printf("Successed to the insert\n");
    }

    // Free statement handle
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt8);
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt9);
    // Disconnect from the database
    SQLDisconnect(hdbc);

    // Clean up
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
