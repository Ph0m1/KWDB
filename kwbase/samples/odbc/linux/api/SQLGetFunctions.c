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

void handle_error(SQLHANDLE handle, SQLRETURN ret, SQLSMALLINT handle_type)
{
    SQLCHAR sql_state[6];
    SQLINTEGER native_error;
    SQLCHAR message_text[256];
    SQLSMALLINT text_length;

    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_error, message_text, sizeof(message_text), &text_length);
        printf("Error %s: %s\n", sql_state, message_text);
        exit(1);
    }
}

int main()
{
    SQLHENV env;
    SQLRETURN ret;

    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    handle_error(env, ret, SQL_HANDLE_ENV);

    // Set the ODBC version to be used
    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    handle_error(env, ret, SQL_HANDLE_ENV);

    SQLUSMALLINT TablesExists, ColumnsExists, StatisticsExists;
    RETCODE retcodeTables, retcodeColumns, retcodeStatistics;
    retcodeTables = SQLGetFunctions(env, SQL_API_SQLTABLES, &TablesExists);
    handle_error(env, ret, SQL_HANDLE_ENV);
    retcodeColumns = SQLGetFunctions(env, SQL_API_SQLCOLUMNS, &ColumnsExists);
    handle_error(env, ret, SQL_HANDLE_ENV);
    retcodeStatistics = SQLGetFunctions(env, SQL_API_SQLSTATISTICS, &StatisticsExists);
    handle_error(env, ret, SQL_HANDLE_ENV);

    // SQLGetFunctions is completed successfully and SQLTables, SQLColumns, and SQLStatistics are supported by the driver.
    if (retcodeTables == SQL_SUCCESS && TablesExists == SQL_TRUE &&
        retcodeColumns == SQL_SUCCESS && ColumnsExists == SQL_TRUE &&
        retcodeStatistics == SQL_SUCCESS && StatisticsExists == SQL_TRUE)
    {
        // Continue with application
        printf("Supported ODBC Functions:\n");
    }

    SQLDisconnect(env);

    // Free the environment handle
    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    handle_error(env, ret, SQL_HANDLE_ENV);

    return 0;
}
