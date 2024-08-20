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
#include <sql.h>
#include <sqlext.h>

void handleOdbcError(SQLRETURN ret, SQLSMALLINT handleType, SQLHANDLE handle)
{
    SQLCHAR sqlstate[6];
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT messageLength;

    if (ret == SQL_INVALID_HANDLE)
    {
        printf("Invalid handle!\n");
        return;
    }

    if (ret == SQL_SUCCESS_WITH_INFO || ret == SQL_ERROR)
    {
        SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeError, message, SQL_MAX_MESSAGE_LENGTH, &messageLength);
        printf("SQLSTATE: %s\n", sqlstate);
        printf("Native error: %d\n", nativeError);
        printf("Message: %s\n", message);
    }
}

int main()
{
    SQLSMALLINT fieldNumber = 1; // Field number to retrieve information for
    SQLCHAR name[SQL_MAX_OPTION_STRING_LENGTH];
    SQLSMALLINT nameLength;
    SQLSMALLINT dataType;
    SQLULEN columnSize;
    SQLSMALLINT decimalDigits;
    SQLSMALLINT nullable;

    SQLHSTMT hstmt;
    SQLHDESC hdesc;
    SQLHDBC hdbc1 = SQL_NULL_HDBC;
    SQLHENV henv = SQL_NULL_HENV;
    RETCODE retcode;

    // Allocate the ODBC environment and save handle.
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv);
    if ((retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS))
    {
        printf("SQLAllocHandle(Env) Failed\n\n");
        // Cleanup();
    }

    // Notify ODBC that this is an ODBC 3.0 app.
    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER);
    if ((retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS))
    {
        printf("SQLSetEnvAttr(ODBC version) Failed\n\n");
        // Cleanup();
    }

    // Allocate ODBC connection handle and connect.
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc1);
    if ((retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS))
    {
        printf("SQLAllocHandle(hdbc1) Failed\n\n");
        // Cleanup();
    }

    // Sample uses Integrated Security, create SQL Server DSN using Windows NT authentication.
    retcode = SQLConnect(hdbc1, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    if ((retcode != SQL_SUCCESS) && (retcode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLConnect Failed\n\n");
        // Cleanup();
    }

    // alloc handle
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DESC, hdbc1, &hdesc);
    if (!SQL_SUCCEEDED(ret))
    {
        handleOdbcError(ret, SQL_HANDLE_DESC, hdesc);
        return 1;
    }

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc1, &hstmt);
    if (!SQL_SUCCEEDED(ret))
    {
        handleOdbcError(ret, SQL_HANDLE_STMT, hstmt);
        return 1;
    }

    // Set the record number to retrieve (1 for the first record)
    ret = SQLGetDescField(hdesc, fieldNumber, SQL_DESC_NAME, name, sizeof(name), &nameLength);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
        printf("SQLGetDescField failed\n");
        printf("ret: %d\n", ret);
        handleOdbcError(ret, SQL_HANDLE_DESC, hdesc);
    }

    // Retrieve data type
    ret = SQLGetDescField(hdesc, fieldNumber, SQL_DESC_CONCISE_TYPE, &dataType, 0, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
        printf("SQLGetDescField failed\n");
    }

    // Retrieve column size
    ret = SQLGetDescField(hdesc, fieldNumber, SQL_DESC_LENGTH, &columnSize, 0, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
        printf("SQLGetDescField failed\n");
    }

    // Retrieve decimal digits
    ret = SQLGetDescField(hdesc, fieldNumber, SQL_DESC_SCALE, &decimalDigits, 0, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
        printf("SQLGetDescField failed\n");
    }

    // Retrieve nullable
    ret = SQLGetDescField(hdesc, fieldNumber, SQL_DESC_NULLABLE, &nullable, 0, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
        printf("SQLGetDescField failed\n");
    }

    // Display retrieved information
    printf("Field Name: %s\n", name);
    printf("Data Type: %d\n", dataType);
    printf("Column Size: %lu\n", columnSize);
    printf("Decimal Digits: %d\n", decimalDigits);
    printf("Nullable: %d\n", nullable);

    // Cleanup: Free descriptor handle
    SQLFreeHandle(SQL_HANDLE_DESC, hdesc);

    return 0;
}
