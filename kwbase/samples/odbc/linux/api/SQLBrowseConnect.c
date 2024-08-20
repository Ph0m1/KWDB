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
#include <string.h>
#include <sql.h>
#include <sqlext.h>

#define INITIAL_CONN_STR "DRIVER={kwdb};"
#define DSN_BASED_CONN_STR "DSN=kwdb;UID=root;PWD=123456;"
#define MAX_CONN_STR_LEN 1024

int main()
{
    SQLHENV env;
    SQLHDBC dbc;
    SQLRETURN ret;
    SQLCHAR connStrIn[MAX_CONN_STR_LEN];
    SQLCHAR connStrOut[MAX_CONN_STR_LEN];
    SQLSMALLINT connStrOutLen;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLAllocHandle(env) failed\n");
        exit(1);
    }

    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLSetEnvAttr failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        exit(1);
    }

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    if (ret != SQL_SUCCESS)
    {
        printf("SQLAllocHandle(dbc) failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        exit(1);
    }

    strcpy((char *)connStrIn, DSN_BASED_CONN_STR);
    // strcpy((char*)connStrIn, INITIAL_CONN_STR);

    do
    {
        memset(connStrOut, 0, sizeof(connStrOut));

        ret = SQLBrowseConnect(dbc, connStrIn, SQL_NTS, connStrOut, MAX_CONN_STR_LEN, &connStrOutLen);
        if (ret == SQL_NEED_DATA)
        {
        }
        else if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
        {
            printf("Final connection string: %s\n", (char *)connStrOut);
        }
        else
        {
            // handle error
            printf("SQLBrowseConnect failed\n");
            break;
        }
    } while (ret == SQL_NEED_DATA);

    // clear
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);

    return 0;
}
