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

// SQLPutData.cpp
#include <stdio.h>
#include <sqlext.h>

#define TEXTSIZE 12000
#define MAXBUFLEN 256

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
   RETCODE retcode;

   // SQLBindParameter variables.
   SQLLEN cbTextSize, lbytes;

   // SQLParamData variable.
   PTR pParmID;

   // SQLPutData variables.
   UCHAR Data[] =
       "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
       "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
       "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
       "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
       "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
       "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
       "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
       "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
       "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
       "abcdefghijklmnopqrstuvwxyz";

   SDWORD cbBatch = (SDWORD)sizeof(Data) - 1;

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


   // Allocate statement handle.
   retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc1, &hstmt1);
   if ((retcode != SQL_SUCCESS) && (retcode != SQL_SUCCESS_WITH_INFO))
   {
      printf("SQLAllocHandle(hstmt1) Failed\n\n");
      Cleanup();
   }

   printf("SQLAllocHandle(hstmt1) succeeded\n\n");

   // Set parameters based on total data to send.
   lbytes = (SDWORD)TEXTSIZE;
   cbTextSize = SQL_LEN_DATA_AT_EXEC(lbytes);

   // Bind the parameter marker.
   retcode = SQLBindParameter(hstmt1,          // hstmt
                              1,               // ipar
                              SQL_PARAM_INPUT, // fParamType
                              SQL_C_CHAR,      // fCType
                              SQL_LONGVARCHAR, // FSqlType
                              lbytes,          // cbColDef
                              0,               // ibScale
                              (void *)1,       // rgbValue
                              0,               // cbValueMax
                              &cbTextSize);    // pcbValue

   if ((retcode != SQL_SUCCESS) && (retcode != SQL_SUCCESS_WITH_INFO))
   {
      printf("SQLBindParameter Failed\n\n");
      Cleanup();
   }
   printf("SQLBindParameter succeeded\n\n");

   // Execute the command.
   retcode =
       SQLExecDirect(hstmt1, (UCHAR *)"INSERT INTO d1.emp4 VALUES('Paul Borm', 46, ?)", SQL_NTS);
   if ((retcode != SQL_SUCCESS) && (retcode != SQL_NEED_DATA) && (retcode != SQL_SUCCESS_WITH_INFO))
   {
      printf("SQLExecDirect Failed\n\n");
      Cleanup();
   }
   printf("SQLExecDirect succeeded\n\n");

   // Check to see if NEED_DATA; if yes, use SQLPutData.
   retcode = SQLParamData(hstmt1, &pParmID);
   if (retcode == SQL_NEED_DATA)
   {
      while (lbytes > cbBatch)
      {
         SQLPutData(hstmt1, Data, cbBatch);
         lbytes -= cbBatch;
      }
      // Put final batch.
      retcode = SQLPutData(hstmt1, Data, lbytes);
   }

   if ((retcode != SQL_SUCCESS) && (retcode != SQL_SUCCESS_WITH_INFO))
   {
      printf("SQLParamData Failed\n\n");
      Cleanup();
   }
   printf("SQLParamData succeeded\n\n");

   // Make final SQLParamData call.
   retcode = SQLParamData(hstmt1, &pParmID);
   if ((retcode != SQL_SUCCESS) && (retcode != SQL_SUCCESS_WITH_INFO))
   {
      printf("Final SQLParamData Failed\n\n");
      Cleanup();
   }
   printf("Final SQLParamData succeeded\n\n");

   // Clean up.
   SQLFreeHandle(SQL_HANDLE_STMT, hstmt1);
   SQLDisconnect(hdbc1);
   SQLFreeHandle(SQL_HANDLE_DBC, hdbc1);
   SQLFreeHandle(SQL_HANDLE_ENV, henv);
}
