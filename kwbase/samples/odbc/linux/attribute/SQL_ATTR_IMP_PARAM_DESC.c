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

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    CheckRC(ret, SQL_HANDLE_ENV, henv);

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

    // ret = SQLPrepare(hstmt, (SQLCHAR*)"INSERT INTO test.t1 (id) VALUES (?)", SQL_NTS);
    // CheckRC(ret, SQL_HANDLE_STMT, hstmt);

    // Set statement attribute to enable implicit row descriptor
    SQLULEN imp_row_desc = SQL_TRUE; // Enable implicit row descriptor
    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_IMP_ROW_DESC, (SQLPOINTER)imp_row_desc, SQL_IS_INTEGER);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        printf("Error setting statement attribute for implicit row descriptor\n");
        // show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // clear
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
