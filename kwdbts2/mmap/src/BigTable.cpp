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


#include <cstring>
#include <map>
#include <limits>
#include <algorithm>
#include <iostream>
#include "BigTable.h"
#include "BigObjectUtils.h"
#include "BigObjectApplication.h"
#include "DateTime.h"

//#define __PROFILING__	1


BigTable::BigTable() : rw_latch_(RWLATCH_ID_BIGTABLE_RWLOCK) { is_len_changed_ = false; is_empty_rst_table_ = false; }

BigTable::~BigTable() {}

impl_latch_virtual_func(BigTable, &rw_latch_)

string BigTable::name() const { return bigobject::s_emptyString(); }


int BigTable::avgColumnStrLen(int col) { return 0; }

int BigTable::getDeletableColumn() const { return -1; }

int BigTable::firstDataColumn() const { return 0; }

const vector<AttributeInfo> & BigTable::getSchema() const {
    return getDummySchema();
}

const vector<AttributeInfo> & BigTable::getSchemaInfo() const
{ return getSchema(); }

int BigTable::structVersion() const { return 0; }

int BigTable::create(const vector<AttributeInfo> &schema,
  const vector<string> &key, const string &key_order, const string &ns_ur,
  const string &description, const string &tbl_sub_path, const string &source_url,
  int encoding, ErrorInfo &err_info) {
  return -1;
}

#if defined(COLUMN_GROUP)
int BigTable::create(const vector<ColumnGroup> &schema,
    const vector<string> &key,
    const string &ns_ur,
    const string &description,
    const string &tbl_sub_path,
    const string &source_url,
    int encoding, ErrorInfo &err_info)
{
    return -1;
}
#endif

int BigTable::reserveBase(size_t size) { return -1; }
int BigTable::reserve(size_t size) { return -1; }

int BigTable::resize(size_t size)
{
    return -1;
}

vector<string> BigTable::rank() const
{
    return vector<string> ();
}

string BigTable::description() const { return s_emptyString(); }

size_t BigTable::size() const { return 0; }

size_t BigTable::actualSize() const {
    return size();
}

size_t BigTable::correctActualSize() { return actualSize(); }

size_t BigTable::capacity() const {
    return endRowid();
}

size_t BigTable::bound() const
{ return size(); }

size_t BigTable::endIndex() const  { return 1; }

size_t BigTable::startIndex() const  { return size() + 1; }

size_t BigTable::lastIndex() const  { return size(); }

size_t BigTable::reservedSize() const  { return size() + 1; }

size_t BigTable:: endRowid() const {
    return size() + 1;
}

int BigTable::numColumn() const { return 0; }

DataHelper * BigTable::getRecordHelper() { return nullptr; }

int64_t BigTable::push_back(const void *rec) {
  mutexLock();
  int64_t err_code = push_back_nolock(rec);
  mutexUnlock();
  return err_code;
}

int64_t BigTable::push_back_nolock(const void *rec) { return (int64_t)-1; }

void stringToData(DataHelper &helper, const vector<string> &str, int num_col,
  void *data, size_t data_size) {

  memset(data, 0, data_size);
  for (int i = 0; i < (int)str.size() && i < num_col; ++i) {
    helper.stringToColumn(i, (char *)str[i].c_str(), data);
  }
}

int BigTable::push_back(const vector<string> &rec_str) {
  size_t rec_sz = recordSize();
  char *rec = new char[recordSize()];
  DataHelper *helper = getRecordHelper();
  int first_data_col = firstDataColumn();

  memset(rec, 0, rec_sz);
  ::getDeletableColumn(this, helper, rec);
  for (int i = 0;
    (i < (int)rec_str.size()) && (i + first_data_col < numColumn()); ++i) {
    helper->stringToColumn(first_data_col + i, (char *)rec_str[i].c_str(), rec);
  }
  int err_code = push_back(rec);
  delete[] rec;
  return err_code;
}

bool BigTable::isValidRow(size_t row) { return true; }

int BigTable::erase(size_t row) { return -1; }

void * BigTable::record(size_t n) const { return nullptr; }

void * BigTable::header(size_t n) const { return nullptr; }

uint64_t BigTable::recordSize() const { return 0; }

bool BigTable::isNull(size_t row, size_t col) { return false; }

int BigTable::getColumnValue(size_t row, size_t column, void *data) const { return 0; }

void BigTable::setColumnValue(size_t row, size_t column, void *data) {}

void BigTable::setColumnValue(size_t row, size_t column, const string &str) {}

void * BigTable::getColumnAddr(size_t row, size_t column) const
{ return nullptr; }

string BigTable::columnToString(size_t row, size_t column) const
{ return s_emptyString(); }

int BigTable::columnToString(size_t row, size_t column, char *str) const
{ return 0; }

size_t BigTable::find(const vector<string> &key) { return -1; }

size_t BigTable::find(const vector<string> &key, vector<size_t> &locs)
{ return -1;}

size_t BigTable::find(void* key) {
    return 0;
}

size_t BigTable::find(const KeyMaker &km) {
    return 0;
}

size_t BigTable::find(const KeyPair &kp) {
    return 0;
}


ostream & BigTable::printRecord(std::ostream &os, size_t row) {
  os << '[' << row << "] ";
  for (int i = 0; i < numColumn(); ++i) {
    if (isNull(row, i))
      os << cstr_null << ' ';
    else
      os << columnToString(row, i) << ' ';
  }
  return os;
}

size_t BigTable::count(const string &key_name) { return 0; }

size_t BigTable::trim(size_t count, int trim_type, ErrorInfo &err_info)
{ return 0; }

int BigTable::setColumnFlag(int col, int flag) { return -1; }
