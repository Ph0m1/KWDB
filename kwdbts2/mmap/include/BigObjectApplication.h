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


#ifndef BIGOBJECTAPPLICATION_H_
#define BIGOBJECTAPPLICATION_H_

#include <fcntl.h>
#include "objcntl.h"
#include "DataType.h"
#include "DataOperation.h"
#include "BigTable.h"
#include "BigObjectError.h"

/**
 * @brief	release an object.
 *
 * @param 	obj		Pointer to a big object.
 * @return	none.
 */
void releaseObject(TSObject *object, bool is_force = false);

/**
 * @brief	Create a temporary table.
 *
 * @param db_path    Temporary table
 * @param	schema			Temporary table schema.
 * @param encoding    Temporary table encoding
 * @return	Pointer to the created temporary table. It returns NULL if fails.
 */
BigTable *CreateTempTable(const vector<AttributeInfo> &schema,
    const std::string &db_path,
    int encoding,
    ErrorInfo &err_info);

void DropTempTable(BigTable* bt, ErrorInfo &err_info);

#endif /* BIGOBJECTAPPLICATION_H_ */
