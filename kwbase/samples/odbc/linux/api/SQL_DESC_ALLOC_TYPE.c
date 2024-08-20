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

void handleOdbcError(SQLRETURN ret, SQLSMALLINT handleType, SQLHANDLE handle)
{

}

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

    SQLRETURN ret;
    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)1, 0);
    if (!SQL_SUCCEEDED(ret))
    {
        printf("SQLSetStmtAttr failed\n");
        handleOdbcError(ret, SQL_HANDLE_STMT, hstmt);
        return 1;
    }

    ret = SQLSetDescField(hstmt, SQL_NO_ROW_NUMBER, SQL_DESC_ALLOC_TYPE, (SQLPOINTER)SQL_DESC_ALLOC_AUTO, 0);
    if (!SQL_SUCCEEDED(ret))
    {
        handleOdbcError(ret, SQL_HANDLE_STMT, hstmt);
        return 1;
    }

    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
