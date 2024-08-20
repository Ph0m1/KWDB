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

#define NAME_LEN 50
#define PHONE_LEN 10

SQLHENV henv = SQL_NULL_HENV;
SQLHDBC hdbc1 = SQL_NULL_HDBC;
// SQLHSTMT hstmt = SQL_NULL_HSTMT;
SQLHSTMT hstmt1 = SQL_NULL_HSTMT;

void Cleanup()
{
   if (hstmt1 != SQL_NULL_HSTMT)
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt1);

   if (hdbc1 != SQL_NULL_HDBC)
   {
      SQLDisconnect(hdbc1);
      SQLFreeHandle(SQL_HANDLE_DBC, hdbc1);
   }

   if (henv != SQL_NULL_HENV)
      SQLFreeHandle(SQL_HANDLE_ENV, henv);
}

int main()
{
#define NAME_LEN 50
#define PHONE_LEN 10

   SQLHSTMT hstmtCreate;
   SQLHSTMT hstmtSelect;
   SQLHSTMT hstmtUpdate;
   SQLRETURN retcode;
   // SQLHDBC      hdbc;
   SQLCHAR szName[NAME_LEN], szPhone[PHONE_LEN];
   SQLINTEGER cbName, cbPhone;

   // Allocate the ODBC environment and save handle.
   retcode = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv);
   if ((retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS))
   {
      printf("SQLAllocHandle(Env) Failed\n\n");
      Cleanup();
   }

   // Notify ODBC that this is an ODBC 3.0 app.
   retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER);
   if ((retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS))
   {
      printf("SQLSetEnvAttr(ODBC version) Failed\n\n");
      Cleanup();
   }

   // Allocate ODBC connection handle and connect.
   retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc1);
   if ((retcode != SQL_SUCCESS_WITH_INFO) && (retcode != SQL_SUCCESS))
   {
      printf("SQLAllocHandle(hdbc1) Failed\n\n");
      Cleanup();
   }
   // Sample uses Integrated Security, create SQL Server DSN using Windows NT authentication.
   retcode = SQLConnect(hdbc1, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
   if ((retcode != SQL_SUCCESS) && (retcode != SQL_SUCCESS_WITH_INFO))
   {
      printf("SQLConnect() Failed\n\n");
      Cleanup();
   }

   SQLAllocHandle(SQL_HANDLE_STMT, hdbc1, &hstmtCreate);
   // exec sql
   retcode = SQLExecDirect(hstmtCreate, "CREATE TABLE if not exists d1.CUSTOMERS (NAME VARCHAR(50), PHONE VARCHAR(20))", SQL_NTS);

   if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
   {
      printf("Table created successfully.\n");
   }
   else
   {
      printf("Error creating table.\n");
   }

   /* Allocate the statements and set the cursor name. */

   SQLAllocHandle(SQL_HANDLE_STMT, hdbc1, &hstmtSelect);
   SQLAllocHandle(SQL_HANDLE_STMT, hdbc1, &hstmtUpdate);
   retcode = SQLSetCursorName(hstmtSelect, "C1", SQL_NTS);
   if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
   {
      printf("SetCursorName executed successfully.\n");
   }
   else
   {
      printf("SetCursorName executed failed.\n");
   }

   /* SELECT the result set and bind its columns to local buffers. */

   SQLExecDirect(hstmtSelect,
                 "SELECT NAME, PHONE FROM d1.CUSTOMERS",
                 SQL_NTS);
   SQLBindCol(hstmtSelect, 1, SQL_C_CHAR, szName, NAME_LEN, &cbName);
   SQLBindCol(hstmtSelect, 2, SQL_C_CHAR, szPhone, PHONE_LEN, &cbPhone);

   /* Read through the result set until the cursor is */
   /* positioned on the row for John Smith. */

   do
      retcode = SQLFetch(hstmtSelect);
   while ((retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) &&
          (strcmp(szName, "Smith, John") != 0));

   /* Perform a positioned update of John Smith's name. */

   if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
   {
      SQLExecDirect(hstmtUpdate,
                    "UPDATE EMPLOYEE SET PHONE=\"2064890154\" WHERE CURRENT OF C1",
                    SQL_NTS);
   }
}
