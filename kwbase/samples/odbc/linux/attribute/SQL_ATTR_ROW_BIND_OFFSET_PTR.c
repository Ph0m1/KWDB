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

  // connect database...
  ret = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
  if (ret == SQL_SUCCESS)
  {
    printf("set successfully\n");
  }

  ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
  if (ret == SQL_SUCCESS)
  {
    printf("set successfully\n");
  }

  // set SQL_ATTR_ROW_BIND_OFFSET_PTR
  SQLLEN rowBindOffset = 0;
  ret = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_OFFSET_PTR, &rowBindOffset, 0);
  if (ret == SQL_SUCCESS)
  {
    printf("set successfully\n");
  }

  // SQLExecute
  SQLCHAR SQLStmt[1024];
  strcpy((char *)SQLStmt, "SELECT * FROM d1.t1 WHERE id = 1");
  ret = SQLPrepare(hstmt, SQLStmt, SQL_NTS);
  if (ret == SQL_SUCCESS)
  {
    printf("set successfully\n");
  }
  ret = SQLExecute(hstmt);
  if (ret == SQL_SUCCESS)
  {
    printf("set successfully\n");
  }
  // check SQL_ATTR_ROW_BIND_OFFSET_PTR
  SQLLEN *retrievedRowBindOffsetPtr = NULL;
  ret = SQLGetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_OFFSET_PTR, &retrievedRowBindOffsetPtr, 0, NULL);
  if (ret == SQL_SUCCESS)
  {
    printf("Retrieved row bind offset pointer: %p\n", retrievedRowBindOffsetPtr);
  }
  else
  {
    printf("Failed to retrieve row bind offset pointer\n");
  }

  return 0;
}
