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

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>

#define SQL_RESULT_LEN 240
#define SQL_RETURN_CODE_LEN 1000

#define TEXTSIZE 12000
int main()
{
    // Declare variables
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;

    INT Data[] = {1};
    SDWORD cbBatch = (SDWORD)sizeof(Data) - 1;

    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
    }

    // Set the ODBC version environment attribute
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
    }

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
    }

    // Connect to the DSN
    SQLCHAR *dsn = (SQLCHAR *)"PostgreSQL35W";
    SQLCHAR *user = (SQLCHAR *)"root";
    SQLCHAR *pass = (SQLCHAR *)"123456";
    ret = SQLConnectA(hdbc, dsn, SQL_NTS, user, SQL_NTS, pass, SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
    }

    // Allocate statement handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
    }

    // Prepare a parameterized query
    SQLWCHAR *query = (SQLWCHAR *)L"INSERT INTO t1 VALUES (?)";
    ret = SQLPrepareW(hstmt, query, SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
    }
    SQLLEN cbTextSize, lbytes;
    // Set parameters based on total data to send.
    lbytes = (SDWORD)TEXTSIZE;
    cbTextSize = SQL_LEN_DATA_AT_EXEC(lbytes);
    // Bind parameter
    SQLINTEGER param;
    ret = SQLBindParameter(hstmt,           // hstmt
                           1,               // ipar
                           SQL_PARAM_INPUT, // fParamType
                           SQL_C_CHAR,      // fCType
                           SQL_LONGVARCHAR, // FSqlType
                           lbytes,          // cbColDef
                           0,               // ibScale
                           (void *)1,       // rgbValue
                           0,               // cbValueMax
                           &cbTextSize);    // pcbValue

    // Set parameter value
    param = 123; // Example parameter value

    // Execute the statement
    ret = SQLExecute(hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
    }

    // Check if there are more parameters to send
    ret = SQLParamData(hstmt, NULL);
    if (ret == SQL_NEED_DATA)
    {
        // More parameters to send
        printf("More parameters to send...\n");
        while (lbytes > cbBatch)
        {
            SQLPutData(hstmt, Data, cbBatch);
            lbytes -= cbBatch;
        }
        // Put final batch.
        ret = SQLPutData(hstmt, Data, lbytes);
        if ((ret != SQL_SUCCESS) && (ret != SQL_SUCCESS_WITH_INFO))
        {
            printf("SQLPutData executed failed\n\n");
        }
        printf("SQLPutData executed SUCCEED!\n");
    }
    else if (ret == SQL_SUCCESS)
    {
        // All parameters sent successfully
        printf("All parameters sent successfully.\n");
    }
    else
    {
        printf("All parameters sent FAIL. %d\n", ret);
    }

    // Cleanup: Free handles
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
