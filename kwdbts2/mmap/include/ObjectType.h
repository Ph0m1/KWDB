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


#ifndef INCLUDE_OBJECTTYPE_H_
#define INCLUDE_OBJECTTYPE_H_


enum ObjectType {
  OBJ_BT,
  OBJ_BG,
  OBJ_BO,
  OBJ_ASSOC,
  OBJ_BIT,            // bitmap index names
  OBJ_NS,
  OBJ_BASEBT,
  OBJ_NULL,           // failed get object, useful for replication
  OBJ_RESERVED,
};

#endif /* INCLUDE_OBJECTTYPE_H_ */
