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


#ifndef BIGOBJECTEXTENSION_H_
#define BIGOBJECTEXTENSION_H_

#include "action/ExtensionAction.h"

extern "C" {
// Function boext_init() will be called when extension is loaded.
// It shall initialize necessary data and register extension actions.
void boext_init();
typedef void (*BOEXT_Init)();

// Return pointer of new derived BOAction class
typedef void * (*ExtActionFactory)();

// Register extension action factory
// @param act_name    name of action
// @param fact        extension action factory
void registerActionFactory(const char *act_name, ExtActionFactory fact);
}

#endif /* BIGOBJECTEXTENSION_H_ */

