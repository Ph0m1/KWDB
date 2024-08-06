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


#ifndef ATTRIBUTEINFOINDEX_H_
#define ATTRIBUTEINFOINDEX_H_

#include "DataType.h"
#include "InfoIndex.h"


struct InfoLessCompare {
	bool operator() (const AttributeInfo &l, const AttributeInfo &r)
	{
		return (l.name.compare(r.name) < 0);
	}
};

struct InfoStringCompare {
	int operator() (const AttributeInfo &l, const string &r)
	{
    auto res = l.name.compare(r);
    if ((res ==0) && l.isFlag(AINFO_DROPPED)) {
      return -1;
    }
    return res;
	}
};

typedef InfoIndex<AttributeInfo, InfoLessCompare, InfoStringCompare> AttributeInfoIndex;


#endif /* ATTRIBUTEINFOINDEX_H_ */
