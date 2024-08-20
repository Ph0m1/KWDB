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


    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret == SQL_SUCCESS)
    {
        printf("set successfully\n");
    }


    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret == SQL_SUCCESS)
    {
        printf("set successfully\n");
    }


    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret == SQL_SUCCESS)
    {
        printf("set successfully\n");
    }

    ret = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    CheckRC(ret, SQL_HANDLE_DBC, hdbc);


    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret == SQL_SUCCESS)
    {
        printf("set successfully\n");
    }

    // set SQL_ATTR_PARAMSET_SIZE
    SQLULEN paramSetSize = 10;
    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)paramSetSize, 0);
    if (ret == SQL_SUCCESS)
    {
        printf("set successfully\n");
    }

    // get SQL_ATTR_PARAMSET_SIZE
    SQLULEN retrievedParamSetSize = 0;
    ret = SQLGetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, &retrievedParamSetSize, 0, NULL);
    if (ret == SQL_SUCCESS)
    {
        printf("Retrieved parameter set size: %lu\n", retrievedParamSetSize);
    }
    else
    {
        printf("Failed to retrieve parameter set size\n");
    }

    return 0;
}
