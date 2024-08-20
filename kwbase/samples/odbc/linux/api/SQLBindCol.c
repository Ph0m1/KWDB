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

struct DataBinding
{
    SQLSMALLINT TargetType;
    SQLPOINTER TargetValuePtr;
    SQLINTEGER BufferLength;
    SQLLEN StrLen_or_Ind;
};

void printStatementResult(SQLHSTMT hstmt)
{
    int bufferSize = 1024, i;
    SQLRETURN retCode;
    SQLSMALLINT numColumn = 0, bufferLenUsed;

    retCode = SQLNumResultCols(hstmt, &numColumn);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLNumResultCols Failed\n\n");
    }

    SQLPOINTER *columnLabels = (SQLPOINTER *)malloc(numColumn * sizeof(*columnLabels));
    struct DataBinding *columnData = (struct DataBinding *)malloc(numColumn * sizeof(struct DataBinding));

    printf("Columns from that table:\n");
    for (i = 0; i < numColumn; i++)
    {
        columnLabels[i] = (SQLPOINTER)malloc(bufferSize * sizeof(char));

        retCode = SQLColAttribute(hstmt, (SQLUSMALLINT)i + 1, SQL_DESC_LABEL, columnLabels[i], (SQLSMALLINT)bufferSize, &bufferLenUsed, NULL);
        if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
        {
            printf("SQLColAttribute Failed\n\n");
        }
        printf("SQLColAttribute succeeded\n\n");
        wprintf(L"Column %d: %s\n", i, (wchar_t *)columnLabels[i]);
    }

    // allocate memory for the binding
    for (i = 0; i < numColumn; i++)
    {
        columnData[i].TargetType = SQL_C_CHAR;
        columnData[i].BufferLength = (bufferSize + 1);
        columnData[i].TargetValuePtr = malloc(sizeof(unsigned char) * columnData[i].BufferLength);
    }

    // setup the binding
    for (i = 0; i < numColumn; i++)
    {
        retCode = SQLBindCol(hstmt, (SQLUSMALLINT)i + 1, columnData[i].TargetType,
                             columnData[i].TargetValuePtr, columnData[i].BufferLength, &(columnData[i].StrLen_or_Ind));
        if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
        {
            printf("SQLColAttribute Failed\n\n");
        }
    }
    free(columnLabels);
}

int main()
{
    int bufferSize = 1024, i, count = 1, numCols = 5;
    SQLCHAR firstTableName[1024], dbName[1024], userName[1024];
    SQLRETURN retCode;

    SQLHENV henv = NULL;
    SQLHDBC hdbc = NULL;
    SQLHSTMT hstmt = NULL;

    struct DataBinding *catalogResult = (struct DataBinding *)malloc(numCols * sizeof(struct DataBinding));
    SQLCHAR *selectAllQuery = (SQLCHAR *)malloc(sizeof(SQLCHAR) * bufferSize);

    // connect to database
    retCode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLConnect() Failed\n\n");
        // Cleanup();
    }
    retCode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLConnect() Failed\n\n");
        // Cleanup();
    }
    retCode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLConnect() Failed\n\n");
    }

    // Connect to the database
    retCode = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLConnect() Failed\n\n");
        // Cleanup();
    }

    retCode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLConnect() Failed\n\n");
        // Cleanup();
    }
    // display the database information
    SQLSMALLINT bufferLen;
    retCode = SQLGetInfo(hdbc, SQL_DATABASE_NAME, dbName, sizeof(dbName), &bufferLen);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf(" Failed\n\n");
    }
    retCode = SQLGetInfo(hdbc, SQL_USER_NAME, userName, sizeof(userName), &bufferLen);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf(" Failed\n\n");
    }

    for (i = 0; i < numCols; i++)
    {
        catalogResult[i].TargetType = SQL_C_CHAR;
        catalogResult[i].BufferLength = bufferSize + 1;
        catalogResult[i].TargetValuePtr = malloc(sizeof(SQLCHAR) * catalogResult[i].BufferLength);
    }

    // Set up the binding
    for (i = 0; i < numCols; i++)
        retCode = SQLBindCol(hstmt, i + 1, catalogResult[i].TargetType, catalogResult[i].TargetValuePtr, catalogResult[i].BufferLength, &(catalogResult[i].StrLen_or_Ind));
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLBindCol Failed\n\n");
    }

    retCode = SQLTables(hstmt, (SQLCHAR *)SQL_ALL_CATALOGS, SQL_NTS, (SQLCHAR *)"", SQL_NTS, (SQLCHAR *)"", SQL_NTS, (SQLCHAR *)"", SQL_NTS);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLTables Failed\n\n");
    }
    retCode = SQLFreeStmt(hstmt, SQL_CLOSE);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLFreeStmt Failed\n\n");
    }

    retCode = SQLTables(hstmt, dbName, SQL_NTS, userName, SQL_NTS, (SQLCHAR *)"%", SQL_NTS, (SQLCHAR *)"TABLE", SQL_NTS);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLTables Failed\n\n");
    }

    for (retCode = SQLFetch(hstmt); retCode == SQL_SUCCESS || retCode == SQL_SUCCESS_WITH_INFO; retCode = SQLFetch(hstmt), ++count)
        if (count == 1)
            snprintf(firstTableName, sizeof(firstTableName), "%s", catalogResult[2].TargetValuePtr);

    retCode = SQLFreeStmt(hstmt, SQL_CLOSE);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLFreeStmt Failed\n\n");
    }

    printf("Select all data from the first table (%s)\n", firstTableName);
    snprintf(selectAllQuery, bufferSize, "SELECT * FROM %s", firstTableName);

    retCode = SQLExecDirect(hstmt, selectAllQuery, SQL_NTS);
    if ((retCode != SQL_SUCCESS) && (retCode != SQL_SUCCESS_WITH_INFO))
    {
        printf("SQLExecDirect Failed\n\n");
    }

    // Add code to handle and print statement result
    printStatementResult(hstmt);

    return 0;
}
