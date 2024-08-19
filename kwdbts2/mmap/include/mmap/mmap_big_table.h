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

#include "date_time_util.h"
#include "big_table.h"
#include "mmap_object.h"
#include "mmap_string_file.h"

using namespace std;
using namespace kwdbts;

// V7: table with delete marker.
#define BT_STRUCT_FORMAT_V7             0x00000007

class MMapBigTable;

// big table use vtree to store row entry & data section to store index
class MMapBigTable: public BigTable, public MMapObject {
 private:
  KRWLatch rw_latch_;
  string db_path_;
  string tbl_sub_path_;

 protected:
  vector<AttributeInfo> ext_schema_;          // schema reported.
  vector<AttributeInfo> key_info_;
  vector<int> key_idx_;
  DataHelper rec_helper_;
  DataHelper key_helper_;
  vector<VarStringObject> vso_;
  string name_;
  int deleted_col_;
  int ts_col_;
  int header_sz_;       // 1 + size of [null bitmap]
  int header_offset_;   // [data][header]
  vector<unsigned char *> dummy_header_;    // for legacy table

  inline void * header_(size_t n) const
  { return offsetAddr(record_(n), header_offset_); }

  void initRow0();

  int open_(int char_code, const string &url, const std::string &db_path, const string &tbl_sub_path, int flags,
    ErrorInfo &err_info);

  int init(const vector<AttributeInfo> &schema, const vector<string> &key,
    const string &key_order, const string &ns_url, const string &description,
    const string &source_url, int encoding, ErrorInfo &err_info);

  int opening(ErrorInfo &err_info);

  int close();

  int setDataHelper(int flags, ErrorInfo &err_info);

  inline bool isValidRow_(size_t row) const {
    int8_t v ;
    getColumnValue(row, deleted_col_, &v);
    return (v != 0);
  }

  void removeStringFile();

  // internal use by circular table without mutex lock
  int erase_(size_t row);

  void setDataHelperVarString();
public:
  MMapBigTable();

  virtual ~MMapBigTable();

  int rdLock() override;
  int wrLock() override;
  int unLock() override;

  inline void * columnAddr_(size_t row, size_t col) const
  { return offsetAddr(record_(row), col_info_[col].offset); }

  virtual int type() const;

  virtual string name() const;

  virtual int getDeletableColumn() const;

  virtual int firstDataColumn() const;

  virtual const vector<AttributeInfo> & getSchema() const;

  int create(BigTable *sbt, const vector<AttributeInfo> &schema,
    const vector<string> &key,
    const vector<VarStringObject> &vs_cols,
    int encoding, ErrorInfo &err_info);

  int create(const string &source_url);

  int magic() { return *reinterpret_cast<const int *>("MMBT"); }

  /**
   * @brief	open a big object.
   *
   * @param 	url			big object URL to be opened.
   * @param 	flag		option to open a file; O_CREAT to create new file.
   * @return	0 succeed, otherwise -1.
   */
  virtual int open(const string &url, const std::string &db_path, const string &tbl_sub_path, int flags,
    ErrorInfo &err_info);

  virtual void sync(int flags);

  virtual int structVersion() const;

  /*--------------------------------------------------------------------
   * TSObject functions
   *--------------------------------------------------------------------
   */

  virtual int version() const;

  virtual int permission() const;

  virtual string URL() const;

  virtual const string & tbl_sub_path() const;

  virtual int remove();

  virtual int startRead(ErrorInfo &err_info);

  virtual int stopRead();

  virtual bool isTemporary() const;
  /*--------------------------------------------------------------------
   * data model functions
   *--------------------------------------------------------------------
   */
  virtual string nameServiceURL() const;

  /*--------------------------------------------------------------------
   * big table functions
   *--------------------------------------------------------------------
   */
  virtual int create(const vector<AttributeInfo> &schema,
    const vector<string> &key, const string &key_order,
    const string &ns_url = defaultNameServiceURL(),
    const string &description = kwdbts::s_emptyString,
    const string &tbl_sub_path = kwdbts::s_emptyString,
    const string &source_url = kwdbts::s_emptyString,
    int encoding = DICTIONARY,
    ErrorInfo &err_info = getDummyErrorInfo());

  virtual int reserveBase(size_t size);
  virtual int reserve(size_t size);

  virtual int resize(size_t size);

  virtual int avgColumnStrLen(int col);

  virtual const vector<AttributeInfo> & getSchemaInfo() const;

  virtual size_t size() const;

  virtual size_t actualSize() const;

  virtual size_t correctActualSize();

  virtual size_t capacity() const;

  virtual size_t bound() const;

  virtual size_t reservedSize() const;

  virtual int numColumn() const;

  // @return row_number: success; < 0: error
  virtual int64_t push_back_nolock(const void *rec);

  virtual bool isValidRow(size_t row);

  virtual int erase(size_t row);

  virtual void * record(size_t n) const;

  virtual void * header(size_t n) const;

  virtual uint64_t recordSize() const;

  virtual bool isNull(size_t row, size_t col);

  virtual int getColumnValue(size_t row, size_t column, void *data) const;

  virtual void setColumnValue(size_t row, size_t column, void *data);

  virtual void setColumnValue(size_t row, size_t column, const string &str);

  virtual void *getColumnAddr(size_t row, size_t column) const;

  virtual string columnToString(size_t row, size_t column) const;

  virtual int columnToString(size_t row, size_t col, char *str) const;

  virtual size_t find(const vector<string> &key);

  virtual size_t find(const vector<string> &key, vector<size_t> &locs);

  virtual size_t find(void* key);

  virtual size_t find(const KeyMaker &km);

  virtual size_t find(const KeyPair &kp);

  virtual int encoding() const;

  virtual int flags() const;

  virtual DataHelper * getRecordHelper();

  virtual int setColumnFlag(int col, int flag);

  //IOT functions
  virtual timestamp64 & minTimestamp();
  virtual timestamp64 & maxTimestamp();
};

