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
#include <iostream>

#define MAX_COL_NAME_LEN 256

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;
    SQLCHAR colName[256];
    SQLSMALLINT colNameLen;
    SQLLEN dataType, colSize;
    SQLSMALLINT numCols;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);


    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    ret = SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)30, 0);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLSetConnectAttr\n");
    }
    else
    {
        printf("Connected to the SQLSetConnectAttr\n");
    }

    // connect database
    // Construct the connection string
    SQLWCHAR connStrIn[] = L"DSN=PostgreSQL35W;UID=root;PWD=123456";

    // Connect to the database
    ret = SQLDriverConnect(hdbc, NULL, connStrIn, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the database\n");
    }
    else
    {
        printf("Connected to the database\n");
    }

    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS)
    {
        // Handle error
    }
    SQLWCHAR query[] = L"select * from t12;";
    ret = SQLExecDirect(hstmt, query, SQL_NTS);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLExecDirect\n");
    }
    else
    {
        printf("Successed to the SQLExecDirect\n");
    }
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_NAME, colName, sizeof(colName), &colNameLen, NULL);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLColAttribute\n");
    }
    else
    {
        printf("Successed to the SQLColAttribute\n");
    }
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_TYPE, NULL, 0, NULL, &dataType);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLColAttribute\n");
    }
    else
    {
        printf("Successed to the SQLColAttribute\n");
    }
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_LENGTH, NULL, 0, NULL, &colSize);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLColAttribute\n");
    }
    else
    {
        printf("Successed to the SQLColAttribute\n");
    }
    SQLWCHAR szBaseTableName[256];
    SQLSMALLINT cbBaseTableName;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_BASE_TABLE_NAME, szBaseTableName, sizeof(szBaseTableName), &cbBaseTableName, NULL);
    if (ret == SQL_SUCCESS)
    {
        wprintf(L"Column is from table: %s\n", szBaseTableName);
    }
    else
    {
        printf("Failed to get base table name for column \n");
    }
    SQLLEN scale;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_FIXED_PREC_SCALE, NULL, 0, NULL, &scale);
    if (ret == SQL_SUCCESS)
    {
        printf("Scale of the column: %d\n", scale);
    }
    else
    {
        printf("Failed to get scale for column \n");
    }

    SQLLEN displaySize;
    // Get the display size of the first column
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &displaySize);
    if (ret == SQL_SUCCESS)
    {
        printf("Display size of the first column: %lld\n", displaySize);
    }
    else
    {
        printf("Failed to get display size for column \n");
    }
    SQLLEN caseSensitive;
    // Get the case sensitivity attribute for the column
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_CASE_SENSITIVE, NULL, 0, NULL, &caseSensitive);
    if (ret == SQL_SUCCESS)
    {
        printf("Case sensitivity of the column: %d\n", caseSensitive);
    }
    else
    {
        printf("Failed to get case sensitivity for column \n");
    }
    SQLLEN dataTypedesc;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &dataTypedesc);
    if (ret == SQL_SUCCESS)
    {
        printf("Data type of the column: %d\n", dataType);
    }
    else
    {
        printf("Failed to get data type for column \n");
    }

    // Print column attributes
    printf("Column 1: Name=%s, DataType=%d, Size=%d\n", colName, dataType, colSize);

    ret = SQLNumResultCols(hstmt, &numCols);

    // Print the number of result columns
    printf("Number of result columns: %d\n", numCols);

    // free
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
