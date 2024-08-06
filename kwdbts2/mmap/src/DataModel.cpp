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


#include <DataModel.h>
#include <strings.h>
#include "BigObjectUtils.h"
#include "mmap/MMapObject.h"
#include "BigObjectConst.h"
#include "string/mmapstring.h"

timestamp64 DataModel::ts = 0;
size_t DataModel::dis_row = 0;

DataModel::DataModel() {}

DataModel::~DataModel() {}

const AttributeInfo * DataModel::getColumnInfo(int col_num) const
{ return nullptr; }

int DataModel::getColumnInfo(int col_num, AttributeInfo &attr) const
{ return -1; }

int DataModel::getColumnInfo(const string &name, AttributeInfo &ainfo) const
{ return -1; }

int DataModel::getColumnNumber(const string &attribute) const { return -1; }

// get extended column number based on alias column name
int DataModel::getExtendedColumnNumber(const string &alias) const { return -1; }

string DataModel::nameServiceURL() const { return s_emptyString(); }

VarStringObject DataModel::getVarStringObject(int col) const {
  VarStringObject vso;
  return vso;
}

VarStringObject DataModel::getVarStringObject(const string &attr_name) const {
  VarStringObject vso;
  return vso;
}

MMapStringFile * DataModel::getVarstringFile(int col) const { return nullptr; }

time_t DataModel::createTime() const { return 0; }

uint64_t DataModel::dataLength() const { return 0; }

uint64_t DataModel::indexLength() const { return 0; }

uint64_t DataModel::updateVersion() const { return 0; }

string DataModel::sourceURL() const { return s_emptyString(); }

int DataModel::encoding() const { return 0; } // DICTIONARY

int DataModel::flags() const { return 0; }

// IOT functions
uint32_t DataModel::getSyncVersion() { return 0; }

void DataModel::setSyncVersion(uint32_t sync_ver) {}

timestamp64 & DataModel::minSrcTimestamp() { return ts; }

timestamp64 & DataModel::maxSrcTimestamp() { return ts; }

timestamp64 & DataModel::minTimestamp() { return ts; }

timestamp64 & DataModel::maxTimestamp() { return ts; }