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
    // Declare variables
    SQLHDESC hdesc;
    SQLRETURN ret;
    SQLSMALLINT recNumber = 1; // Record number to retrieve information for
    SQLCHAR name[SQL_MAX_OPTION_STRING_LENGTH];
    SQLSMALLINT nameLength;
    SQLSMALLINT dataType;
    SQLULEN columnSize;
    SQLSMALLINT decimalDigits;
    SQLSMALLINT nullable;
    SQLSMALLINT unused; // Placeholder for reserved parameter
    SQLLEN unused2;     // Placeholder for reserved parameter

    // Allocate descriptor handle
    ret = SQLAllocHandle(SQL_HANDLE_DESC, SQL_NULL_HANDLE, &hdesc);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        // Error handling
    }

    // Retrieve descriptor record information
    ret = SQLGetDescRec(hdesc, recNumber, name, sizeof(name), &nameLength,
                        &dataType, &columnSize, &decimalDigits, &nullable,
                        &unused, &unused2);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO && ret != SQL_NO_DATA)
    {
        // Error handling
        printf("SQLGetDescRec failed\n");
    }
    else
    {
        // Display retrieved information
        printf("Field Name: %s\n", name);
        printf("Data Type: %d\n", dataType);
        printf("Column Size: %lu\n", columnSize);
        printf("Decimal Digits: %d\n", decimalDigits);
        printf("Nullable: %d\n", nullable);
    }

    // Cleanup: Free descriptor handle
    SQLFreeHandle(SQL_HANDLE_DESC, hdesc);

    return 0;
}
