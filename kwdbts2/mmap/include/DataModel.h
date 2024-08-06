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


#ifndef DATAMODEL_H_
#define DATAMODEL_H_

#include <ctime>
#include <memory>
#include <string>
#include <vector>
#include "TSObject.h"
#include "DataType.h"
#include "mmap/MMapStringFile.h"

using namespace std;

using namespace bigobject;

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
  static size_t dis_row;
public:
  DataModel();

  virtual ~DataModel();

  virtual const AttributeInfo * getColumnInfo(int col_num) const;

  // get column information returned in attr.
  virtual int getColumnInfo(int col_num, AttributeInfo &attr) const;

  virtual int getColumnInfo(const string &name, AttributeInfo &ainfo) const;

  // get column number searching from column 0 to N-1
  virtual int getColumnNumber(const string &attribute) const;

  // get extended column number based on alias column name
  virtual int getExtendedColumnNumber(const string &alias) const;

  virtual string nameServiceURL() const;

  virtual VarStringObject getVarStringObject(int col) const;

  virtual VarStringObject getVarStringObject(const string &attr_name) const;

  virtual MMapStringFile * getVarstringFile(int col) const;

  // @return  create time of an object
  virtual time_t createTime() const;

  virtual uint64_t dataLength() const;

  virtual uint64_t indexLength() const;

  virtual uint64_t updateVersion() const;

  // return source URL
  virtual string sourceURL() const;

  virtual int encoding() const;

  virtual int flags() const;

  // IOT functions
  virtual uint32_t getSyncVersion();
  virtual void setSyncVersion(uint32_t sync_ver);
  virtual timestamp64 &minSrcTimestamp();
  virtual timestamp64 &maxSrcTimestamp();
  virtual timestamp64 &minTimestamp();
  virtual timestamp64 &maxTimestamp();
  // check if table is in iot partition
  bool isIotTable() { return ((maxTimestamp() != 0)
    || (maxSrcTimestamp() != 0)); }
};

typedef std::unique_ptr<DataModel> DataModelPtr;

#endif /* DATAMODEL_H_ */
