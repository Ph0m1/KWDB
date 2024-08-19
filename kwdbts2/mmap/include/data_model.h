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

#pragma once

#include <ctime>
#include <memory>
#include <string>
#include <vector>
#include "ts_object.h"
#include "data_type.h"
#include "mmap/mmap_string_file.h"

using namespace std;

struct VarStringObject {
  TSObject *obj;
  MMapStringFile *sf;
  int col;                  // column # in table.

  VarStringObject() { obj = nullptr; sf = nullptr; }
};

///
// Data model class defining basic operations of a data object.
// A data object can be represented via URL
//
class DataModel: public TSObject {
private:
  static timestamp64 ts;
public:
  DataModel();

  virtual ~DataModel();

  virtual string nameServiceURL() const;

  virtual int encoding() const;

  virtual int flags() const;

  // IOT functions
  virtual timestamp64 &minTimestamp();
  virtual timestamp64 &maxTimestamp();
};

