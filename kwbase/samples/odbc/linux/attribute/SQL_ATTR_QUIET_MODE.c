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

#define BUFSIZE 1024

void CheckRC(SQLRETURN rc, SQLSMALLINT HandleType, SQLHANDLE Handle)
{
    SQLCHAR SqlState[6];
    SQLINTEGER NativeError;
    SQLSMALLINT i, MsgLen;
    SQLCHAR Msg[SQL_MAX_MESSAGE_LENGTH];

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
    {
        i = 1;
        while (SQLGetDiagRec(HandleType, Handle, i, SqlState, &NativeError, Msg, sizeof(Msg), &MsgLen) == SQL_SUCCESS)
        {
            printf("ERROR %d: %s\n", NativeError, Msg);
            i++;
        }
        exit(1);
    }
}

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLRETURN ret;
    SQLINTEGER quietMode = SQL_TRUE;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    CheckRC(ret, SQL_HANDLE_ENV, henv);

    // set odbc version 3.x
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    CheckRC(ret, SQL_HANDLE_ENV, henv);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    CheckRC(ret, SQL_HANDLE_DBC, hdbc);

    // set SQL_TRUE
    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_QUIET_MODE, (SQLPOINTER)&quietMode, SQL_IS_INTEGER);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
    {
        printf("SQL_ATTR_QUIET_MODE set successfully to %s\n", quietMode ? "SQL_TRUE" : "SQL_FALSE");
        char dsn[BUFSIZE] = "kwdb";
        char uid[BUFSIZE] = "root";
        char pwd[BUFSIZE] = "123456";
        ret = SQLConnect(hdbc, (SQLCHAR *)dsn, SQL_NTS, (SQLCHAR *)uid, SQL_NTS, (SQLCHAR *)pwd, SQL_NTS);
        if (ret != SQL_SUCCESS)
        {
            printf("SQLConnect failed\n");
            SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
            SQLFreeHandle(SQL_HANDLE_ENV, henv);
            return 1;
        }
    }
    else
    {
        printf("Failed to set SQL_ATTR_QUIET_MODE\n");
    }

    // clear
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
