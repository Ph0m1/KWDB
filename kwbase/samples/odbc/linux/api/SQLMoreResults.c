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

void ShowError(SQLSMALLINT handleType, SQLHANDLE handle)
{
    SQLCHAR sqlstate[6];
    SQLINTEGER errNum;
    SQLCHAR errMsg[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT errMsgLen;
    SQLGetDiagRecA(handleType, handle, 1, sqlstate, &errNum, errMsg, sizeof(errMsg), &errMsgLen);
    printf("SQL error: %s - %d - %s\n", sqlstate, (int)errNum, errMsg);
}

void handleOdbcError(SQLRETURN ret, SQLSMALLINT handleType, SQLHANDLE handle)
{
    if (ret == SQL_INVALID_HANDLE)
    {
        printf("Invalid handle!\n");
        return;
    }
}

int main()
{
    SQLHENV henv;   // Environment Handle
    SQLHDBC hdbc;   // Connection Handle
    SQLHSTMT hstmt; // Statement Handle
    SQLRETURN retcode;

    // Allocate environment handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (retcode != SQL_SUCCESS)
    {
        ShowError(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Set the ODBC version environment attribute
    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER);
    if (retcode != SQL_SUCCESS)
    {
        ShowError(SQL_HANDLE_ENV, henv);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Allocate connection handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (retcode != SQL_SUCCESS)
    {
        ShowError(SQL_HANDLE_ENV, henv);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Connect to the data source
    retcode = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(hdbc, SQL_HANDLE_DBC, retcode);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Allocate statement handle
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (retcode != SQL_SUCCESS)
    {
        ShowError(SQL_HANDLE_DBC, hdbc);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Execute a SELECT statement
    // SQLWCHAR sqlStatement[] = "SELECT * FROM t1";
    retcode = SQLExecDirect(hstmt, (SQLWCHAR *)"SELECT * FROM t1", SQL_NTS);
    if (retcode != SQL_SUCCESS)
    {
        ShowError(SQL_HANDLE_STMT, hstmt);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Fetch all result sets
    do
    {
        // Process current result set here...

        // Move to the next result set, if any
        retcode = SQLMoreResults(hstmt);
        if (retcode == SQL_ERROR)
        {
            ShowError(SQL_HANDLE_STMT, hstmt);
            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
            SQLDisconnect(hdbc);
            SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
            SQLFreeHandle(SQL_HANDLE_ENV, henv);
            return 1;
        }
        else
        {
            printf("SQLMoreResults is executed successfully\n");
        }
    } while (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO);

    // Cleanup
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
