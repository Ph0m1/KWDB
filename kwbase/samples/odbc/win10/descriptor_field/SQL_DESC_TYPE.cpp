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
#include <iostream>

#define MAX_COL_NAME_LEN 256

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;
    SQLCHAR colName[256];
    SQLSMALLINT colNameLen;
    SQLLEN dataType, colSize;
    SQLSMALLINT numCols;

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);


    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    // alloc handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    ret = SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)30, 0);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLSetConnectAttr\n");
    }
    else
    {
        printf("Connected to the SQLSetConnectAttr\n");
    }

    // connect database
    // Construct the connection string
    SQLWCHAR connStrIn[] = L"DSN=PostgreSQL35W;UID=root;PWD=123456";

    // Connect to the database
    ret = SQLDriverConnect(hdbc, NULL, connStrIn, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the database\n");
    }
    else
    {
        printf("Connected to the database\n");
    }

    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS)
    {
        // Handle error
    }
    SQLWCHAR query[] = L"select * from t2;";
    ret = SQLExecDirect(hstmt, query, SQL_NTS);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLExecDirect\n");
    }
    else
    {
        printf("Successed to the SQLExecDirect\n");
    }
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_NAME, colName, sizeof(colName), &colNameLen, NULL);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLColAttribute\n");
    }
    else
    {
        printf("Successed to the SQLColAttribute\n");
    }
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_TYPE, NULL, 0, NULL, &dataType);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLColAttribute\n");
    }
    else
    {
        printf("Successed to the SQLColAttribute\n");
    }
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_LENGTH, NULL, 0, NULL, &colSize);
    if (ret != SQL_SUCCESS)
    {
        printf("Failed to the SQLColAttribute\n");
    }
    else
    {
        printf("Successed to the SQLColAttribute\n");
    }
    SQLWCHAR columnName[256];
    SQLSMALLINT columnNameLen;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_TYPE_NAME, columnName, sizeof(columnName), &columnNameLen, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        std::wcout << L"Column Type Name: " << columnName << std::endl;
    }
    SQLSMALLINT literalPrefixLen;
    SQLWCHAR literalPrefix[256];
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_LITERAL_PREFIX, literalPrefix, sizeof(literalPrefix), &literalPrefixLen, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Literal Prefix: " << literalPrefix << std::endl;
    }
    SQLSMALLINT literalSuffixLen;
    SQLWCHAR literalSuffix[256];
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_LITERAL_SUFFIX, literalSuffix, sizeof(literalSuffix), &literalSuffixLen, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        std::wcout << L"Column Literal Suffix: " << literalSuffix << std::endl;
    }
    SQLSMALLINT localTypeNameLen;
    SQLWCHAR localTypeName[256];
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_LOCAL_TYPE_NAME, localTypeName, sizeof(localTypeName), &localTypeNameLen, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Local Type Name: " << localTypeName << std::endl;
    }
    SQLLEN nullable;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_NULLABLE, NULL, 0, NULL, &nullable);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Nullable: " << (nullable == SQL_NULLABLE ? L"Yes" : L"No") << std::endl;
    }
    SQLLEN unnamed;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_UNNAMED, NULL, 0, NULL, &unnamed);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Unnamed: " << (unnamed == SQL_UNNAMED ? L"Yes" : L"No") << std::endl;
    }

    SQLLEN unsignedAttr;
    // Get column information
    ret = SQLColAttributeW(hstmt, 1, SQL_DESC_UNSIGNED, NULL, 0, NULL, &unsignedAttr);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Unsigned: " << (unsignedAttr == SQL_TRUE ? L"Yes" : L"No") << std::endl;
    }

    SQLLEN updatableAttr;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_UPDATABLE, NULL, 0, NULL, &updatableAttr);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Updatable: " << (updatableAttr == SQL_ATTR_READONLY ? L"No" : L"Yes") << std::endl;
    }

    SQLLEN octetLength;
    // Get column information
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &octetLength);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Octet Length: " << octetLength << std::endl;
    }

    SQLLEN precision;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_PRECISION, NULL, 0, NULL, &precision);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Precision: " << precision << std::endl;
    }

    SQLLEN scale;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_SCALE, NULL, 0, NULL, &scale);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Scale: " << scale << std::endl;
    }

    SQLLEN searchable;
    // Get column information
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_SEARCHABLE, NULL, 0, NULL, &searchable);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Column Searchable: " << searchable << std::endl;
    }
    SQLWCHAR baseColumnName[256];
    ret = SQLColAttributeW(hstmt, 1, SQL_DESC_BASE_COLUMN_NAME, baseColumnName, sizeof(baseColumnName), &columnNameLen, NULL);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        return 1;
    }
    else
    {
        // Output column information
        std::wcout << L"Base Column Name: " << baseColumnName << std::endl;
    }

    // Print column attributes
    printf("Column 1: Name=%s, DataType=%d, Size=%d\n", colName, dataType, colSize);

    ret = SQLNumResultCols(hstmt, &numCols);

    // Print the number of result columns
    printf("Number of result columns: %d\n", numCols);

    // free
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
