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

  SQLCHAR SQLStmt[1024];
  strcpy((char *)SQLStmt, "insert into d1.t1 values (1)");

  ret = SQLExecute(SQLStmt);
  if (ret == SQL_SUCCESS)
  {
    printf("set successfully\n");
  }

  // get SQL_ATTR_PARAMS_PROCESSED_PTR
  SQLULEN paramsProcessed = 0;
  ret = SQLGetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &paramsProcessed, 0, NULL);
  if (ret == SQL_SUCCESS)
  {
    printf("Number of parameters processed: %lu\n", paramsProcessed);
  }
  else
  {
    printf("Failed to retrieve number of parameters processed\n");
  }

  return 0;
}
