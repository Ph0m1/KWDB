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

#include <data_model.h>
#include <strings.h>
#include "utils/big_table_utils.h"
#include "mmap/mmap_object.h"
#include "utils/string_utils.h"

timestamp64 DataModel::ts = 0;

DataModel::DataModel() {}

DataModel::~DataModel() {}

string DataModel::nameServiceURL() const { return kwdbts::s_emptyString; }

int DataModel::encoding() const { return 0; } // DICTIONARY

int DataModel::flags() const { return 0; }

// IOT functions
timestamp64 & DataModel::minTimestamp() { return ts; }

timestamp64 & DataModel::maxTimestamp() { return ts; }