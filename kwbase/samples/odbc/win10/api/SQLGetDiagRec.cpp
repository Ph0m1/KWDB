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

void HandleDiagnosticRecord(SQLHANDLE hHandle, SQLSMALLINT hType, RETCODE RetCode)
{
    SQLWCHAR SqlState[6], Msg[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER NativeError;
    SQLSMALLINT i, MsgLen;

    i = 1;
    while (SQLGetDiagRecW(hType, hHandle, i, SqlState, &NativeError, Msg,
                          sizeof(Msg) / sizeof(SQLWCHAR), &MsgLen) != SQL_NO_DATA)
    {
        wprintf(L"SQL Error #%ld(%s): %s\n", NativeError, SqlState, Msg);
        i++;
    }
}

int main()
{
    SQLHENV hEnv;
    SQLHDBC hDbc;
    SQLRETURN retcode;

    // Allocate environment handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        fprintf(stderr, "Error allocating environment handle\n");
        return -1;
    }

    // Set the ODBC version environment attribute
    retcode = SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        fprintf(stderr, "Error setting environment attribute\n");
        return -1;
    }

    // Allocate connection handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        fprintf(stderr, "Error allocating connection handle\n");
        return -1;
    }

    // Connect to the data source
    SQLWCHAR connStr[] = L"DSN=YourDataSource;UID=YourUsername;PWD=YourPassword;";
    retcode = SQLDriverConnectW(hDbc, NULL, connStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        fprintf(stderr, "Error connecting to data source\n");
        HandleDiagnosticRecord(hDbc, SQL_HANDLE_DBC, retcode);
        return -1;
    }

    // Perform a query (example)
    SQLHSTMT hStmt;
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        fprintf(stderr, "Error allocating statement handle\n");
        HandleDiagnosticRecord(hDbc, SQL_HANDLE_DBC, retcode);
        return -1;
    }
    SQLWCHAR query[] = L"DSN=YourDataSource;UID=YourUsername;PWD=YourPassword;";
    retcode = SQLExecDirectW(hStmt, query, SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        fprintf(stderr, "Error executing SQL statement\n");
        HandleDiagnosticRecord(hStmt, SQL_HANDLE_STMT, retcode);
        return -1;
    }

    // Cleanup
    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    SQLDisconnect(hDbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
    SQLFreeHandle(SQL_HANDLE_ENV, hEnv);

    return 0;
}
