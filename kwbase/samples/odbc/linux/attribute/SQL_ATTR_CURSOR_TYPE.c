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
    SQLHSTMT hstmt;
    SQLRETURN ret;
    SQLSMALLINT cursorType = SQL_CURSOR_STATIC;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    CheckRC(ret, SQL_HANDLE_ENV, henv);

    // set odbc version 3.x
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    CheckRC(ret, SQL_HANDLE_ENV, henv);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    CheckRC(ret, SQL_HANDLE_DBC, hdbc);

    // connect database
    ret = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    CheckRC(ret, SQL_HANDLE_DBC, hdbc);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    CheckRC(ret, SQL_HANDLE_STMT, hstmt);

    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_TYPE, SQL_CURSOR_FORWARD_ONLY, SQL_IS_SMALLINT);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
    {
        printf("SQL_ATTR_CURSOR_TYPE set successfully to %s\n", (cursorType == SQL_CURSOR_STATIC) ? "SQL_CURSOR_STATIC" : "other");

        ret = SQLExecDirect(hstmt, (SQLCHAR *)"SELECT * FROM d1.t1", SQL_NTS);
        CheckRC(ret, SQL_HANDLE_STMT, hstmt);
    }
    else
    {
        printf("Failed to set SQL_ATTR_CURSOR_TYPE\n");
        CheckRC(ret, SQL_HANDLE_STMT, hstmt);
    }
    // clear
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

    // disconnect
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
