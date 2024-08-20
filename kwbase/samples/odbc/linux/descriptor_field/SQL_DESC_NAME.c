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

#define MAX_COL_NAME_LEN 256

// Function to display error messages
void show_error(SQLHANDLE handle, SQLSMALLINT handle_type)
{
    SQLWCHAR sql_state[6];
    SQLINTEGER native_error;
    SQLWCHAR error_msg[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT msg_len;
    SQLGetDiagRecW(handle_type, handle, 1, sql_state, &native_error, error_msg, sizeof(error_msg), &msg_len);
    printf("SQL Error: %s - %s\n", sql_state, error_msg);
}

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
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLSetConnectAttr\n");
    }
    else
    {
        printf("Connected to the SQLSetConnectAttr\n");
    }

    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLSetConnectAttr\n");
    }
    else
    {
        printf("Connected to the SQLSetConnectAttr\n");
    }
    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLSetConnectAttr\n");
    }
    else
    {
        printf("Connected to the SQLSetConnectAttr\n");
    }
    ret = SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)30, 0);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLSetConnectAttr\n");
    }
    else
    {
        printf("Connected to the SQLSetConnectAttr\n");
    }

    // Connect to the data source
    ret = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        printf("Error connecting to data source\n");
        show_error(hdbc, SQL_HANDLE_DBC);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS)
    {
        // Handle error
    }

    SQLCHAR *sql_query = (SQLCHAR *)"SELECT * FROM d1.t1";
    ret = SQLExecDirect(hstmt, sql_query, SQL_NTS);
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
        printf("Set SQLColAttribute(SQL_DESC_NAME) successfully\n");
    }

    ret = SQLColAttribute(hstmt, 1, SQL_DESC_TYPE, NULL, 0, NULL, &dataType);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLColAttribute\n");
    }
    else
    {
        printf("Set SQLColAttribute(SQL_DESC_TYPE) successfully\n");
    }

    ret = SQLColAttribute(hstmt, 1, SQL_DESC_LENGTH, NULL, 0, NULL, &colSize);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLColAttribute\n");
    }
    else
    {
        printf("Set SQLColAttribute(SQL_DESC_LENGTH) successfully\n");
    }

    // Print column attributes
    printf("Column 1: Name=%s, DataType=%d, Size=%d\n", colName, dataType, colSize);

    ret = SQLNumResultCols(hstmt, &numCols);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLColAttribute\n");
    }
    else
    {
        printf("Set SQLColAttribute(SQL_DESC_NAME) successfully\n");
    }
    // Print the number of result columns
    printf("Number of result columns: %d\n", numCols);

    // free
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
