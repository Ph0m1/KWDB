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



#ifndef BIGOBJECTTYPE_H_
#define BIGOBJECTTYPE_H_

#include <inttypes.h>
#include "DataType.h"

#define ST_VTREE                0x00000001
#define ST_VTREE_LINK			0x00000002
#define ST_NS				    0x00000010
#define ST_NS_LINK			    0x00000020
#define ST_NS_EXT			    0x00000040			/// external name service
#define ST_DATA				    0x00000100
#define ST_DATA_LINK			0x00000200
#define ST_DATA_EXT             0x00000400
#define ST_SUPP                 0x00001000
#define ST_NODE_ARRAY			0x00010000
#define ST_SUB_BIGOBJECT		0x00020000
#define ST_HEAP_BIGOBJECT		0x00040000
#define ST_COLUMN_GROUP_TABLE   0x00100000
#define ST_COLUMN_TABLE         0x00200000
#define ST_VIRTUAL_BIGOBJECT    0x01000000
#define ST_TRANSIENT            0x20000000
#define ALL_TREE                0x10000000


enum MEASURE_MODE { ZERO_MEASURE, COPY_MEASURE, SHARE_MEASURE };


#define isSubBigObject(x)		((x & ST_SUB_BIGOBJECT) != 0)
#define isHeapBigObject(x)		((x & ST_HEAP_BIGOBJECT) != 0)
#define isPermanent(x)			((x & ST_TRANSIENT) == 0)
#define isTransient(x)			((x & ST_TRANSIENT) != 0)


#endif /* BIGOBJECTTYPE_H_ */
