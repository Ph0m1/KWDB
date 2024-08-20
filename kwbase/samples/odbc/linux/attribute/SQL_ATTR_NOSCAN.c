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
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
    {
        fprintf(stderr, "Error %d in %s\n", (int)rc,
                (HandleType == SQL_HANDLE_DBC) ? "DBC Function" : (HandleType == SQL_HANDLE_STMT) ? "STMT Function"
                                                                                                  : "Unknown Function");
        exit(1);
    }
}

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;
    SQLCHAR SQLStmt[1024];

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    CheckRC(ret, SQL_HANDLE_ENV, NULL);

    // set odbc version 3.x
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    CheckRC(ret, SQL_HANDLE_ENV, NULL);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    CheckRC(ret, SQL_HANDLE_DBC, NULL);

    // connect database
    ret = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    CheckRC(ret, SQL_HANDLE_DBC, hdbc);

    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_NOSCAN, (SQLPOINTER)SQL_TRUE, 0);
    CheckRC(ret, SQL_HANDLE_DBC, hdbc);
    printf("SQL_ATTR_NOSCAN set to SQL_TRUE\n");

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    CheckRC(ret, SQL_HANDLE_STMT, NULL);

    strcpy((char *)SQLStmt, "SELECT * FROM d1.t1 WHERE id = 1");
    ret = SQLPrepare(hstmt, SQLStmt, SQL_NTS);
    CheckRC(ret, SQL_HANDLE_STMT, hstmt);

    ret = SQLExecute(hstmt);
    CheckRC(ret, SQL_HANDLE_STMT, hstmt);


    // clear
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    // SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
