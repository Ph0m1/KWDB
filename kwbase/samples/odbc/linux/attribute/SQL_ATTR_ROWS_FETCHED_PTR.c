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
#define MAX_MSG 512

void HandleErrors(SQLHANDLE handle, SQLSMALLINT handleType)
{
    SQLINTEGER i = 1;
    SQLINTEGER nativeError;
    SQLCHAR Sqlstate[6];
    SQLCHAR message[MAX_MSG];
    SQLSMALLINT textLength;

    printf("Error occurred\n");

    while (SQLGetDiagRec(handleType, handle, i, &Sqlstate, &nativeError, message, sizeof(message), &textLength) == SQL_SUCCESS)
    {
        printf("SQLSTATE: %s\n", Sqlstate);
        printf("Native error: %d\n", (int)nativeError);
        printf("Error message: %s\n", message);
        i++;
    }
}

void CheckRC(SQLRETURN rc, SQLHANDLE h, SQLSMALLINT ht)
{
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
    {
        SQLCHAR sqlstate[6];
        SQLINTEGER nativeerror;
        SQLSMALLINT msglen;
        SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH];

        SQLGetDiagRec(ht, h, 1, sqlstate, &nativeerror, msg, sizeof(msg), &msglen);
        printf("Error: %s\n", msg);
        exit(1);
    }
}

int main()
{
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;
    SQLRETURN ret;
    SQLINTEGER rows_fetched;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    CheckRC(ret, SQL_NULL_HANDLE, SQL_HANDLE_ENV);

    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    CheckRC(ret, SQL_HANDLE_ENV, NULL);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    CheckRC(ret, SQL_NULL_HANDLE, SQL_HANDLE_DBC);

    ret = SQLConnect(dbc, (SQLCHAR *)"kwdb", SQL_NTS, NULL, 0, NULL, 0);
    CheckRC(ret, SQL_HANDLE_STMT, NULL);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    CheckRC(ret, dbc, SQL_HANDLE_DBC);

    SQLCHAR sql[] = "SELECT * FROM d1.t1";
    ret = SQLPrepare(stmt, sql, SQL_NTS);
    CheckRC(ret, stmt, SQL_HANDLE_STMT);

    ret = SQLExecute(stmt);
    CheckRC(ret, stmt, SQL_HANDLE_STMT);

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);
    CheckRC(ret, stmt, SQL_HANDLE_STMT);

    while ((ret = SQLFetch(stmt)) != SQL_NO_DATA_FOUND)
    {
        // HandleErrors(dbc, SQL_HANDLE_DBC);
        printf("Fetched %d rows.\n", rows_fetched);
    }

    // clear
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);

    // SQLFreeHandle(SQL_HANDLE_ENV, env);

    return 0;
}
