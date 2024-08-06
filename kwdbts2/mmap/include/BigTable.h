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

#ifndef BIGTABLE_H_
#define BIGTABLE_H_


#include <inttypes.h>
#include <memory>
#include <vector>
#include <iostream>
#include <string>
#include <map>
#include "DataModel.h"
#include "default.h"
#include "TSObject.h"
#include "DataHelper.h"
#include "BigObjectConst.h"

// the highest two bytes  0xXX------ is reserved
enum TableType {
  ROW_TABLE =             0x00000000,
  COL_TABLE =             0x00000001,
  NO_DEFAULT_TABLE =      0x00000002,     // auxiliary table without default
  GRAPH_TABLE =           0x00000004,     // Graph as table.
  TAG_TABLE =             0x00000008,
  DELETABLE_TABLE =       0x00000010,
  INDEX_TABLE =           0x00000020,     // allow duplicate key in index
  LAST_INDEX_TABLE =      0x00000040,     // keep only the last key in index
  ORDERED_TABLE =         0x00000100,
  TAG_WAL_TABLE =         0x00000200,
  TIMESTAMP_TABLE =       0x00001000,
  CIRCULAR_TABLE =        0x00002000,
  EXTENDED_TABLE =        0x00010000,
  JOINED_TABLE =          0x00020000,
  SELECTED_TABLE =        0x00100000,
  VARSTR_TO_CHAR_TABLE =  0x00400000,

  NULLBITMAP_TABLE =      0x00800000,
  ENTITY_TABLE     =      0x00200000,
  UNKNOWN_TABLE    =      0xff000000,
};

#define is_col_table(x)     ((x & COL_TABLE) != 0 || (x & ENTITY_TABLE) != 0 || (x & TAG_TABLE) != 0 || (x & TAG_WAL_TABLE) != 0)
#define is_entity_table(x)  ((x & ENTITY_TABLE) != 0)
#define is_deletable(x)     ((x & DELETABLE_TABLE) != 0)
#define is_timestamp(x)     ((x & TIMESTAMP_TABLE) != 0)
#define is_circular(x)      ((x & CIRCULAR_TABLE) != 0)
#define is_varstr_to_char(x)    ((x & VARSTR_TO_CHAR_TABLE) != 0)

enum TableActionFlag {
  PREPARED_STMT =       0x00000001, // = BOSQL_PREPARED prepared statement.
  SOURCE_ATTACHED =     0x00000100,
};

namespace bigobject {
class KeyMaker;
class KeyPair;
class BitmapIndex;
};

struct TableDef {
  string table_name;
  vector<AttributeInfo> schema;     // table schema
  vector<string> key;
  string key_order;
  vector<InputString> col_default;
  vector<InputString> col_comment;
  int32_t flags;
  int32_t encoding;
  uint64_t time_bound;
  timestamp64 min_ts;
  timestamp64 max_ts;
  uint32_t life_cycle;              // life cycle
};

using namespace bigobject;

class BigTable: public DataModel {
 private:
  KRWLatch rw_latch_;

protected:
  bool is_len_changed_;           // is table length changed?
  bool is_empty_rst_table_;

public:
  BigTable();

  BigTable(const string &url);

  virtual ~BigTable();

  int rdLock() override;
  int wrLock() override;
  int unLock() override;

  // name of table
  virtual string name() const;

  virtual bool isDeletable() const { return is_deletable(type()); }

  virtual int getDeletableColumn() const;

  virtual int firstDataColumn() const;

  /**
   * @brief	Obtain the attribute schema (external view without '_cts' and '_d').
   *
   * @return	Attribute information for each column.
   */
  virtual const vector<AttributeInfo> & getSchema() const;

  // Actual schema (internal view)
  virtual const vector<AttributeInfo> & getSchemaInfo() const;

#if defined(COLUMN_GROUP)
  virtual vector<AttributeInfo> getColumnGroupInfo() const;

  virtual vector<AttributeInfo> getColumnGroupSchema(int grp_num) const;
#endif

  virtual int structVersion() const;

  /*--------------------------------------------------------------------
   * big table functions
   *--------------------------------------------------------------------
   */

  /**
   * @brief	Create a table object.
   *
   * @param	schema		      Schema.
   * @param	key		          Key.
   * @param key_order_idx   Order column index for Last Index, -1 if not used.
   * @param	description	    Description string.
   * @param	alias		        Alias for the table object.
   * @param	tbl_sub_path		sub directory.
   * @param	source_url	    Data source URL.
   * @param	dimension     	Dimension Information.
   * @param	ns_url		      Name service URL.
   * @param encoding        Name encoding scheme & deletable table.
   * @return	0 if succeed; -1 otherwise.
   */
  virtual int create(const vector<AttributeInfo> &schema,
    const vector<string> &key, const string &key_order,
    const string &description = s_emptyString(),
    const string &ns_url = defaultNameServiceURL(),
    const string &tbl_sub_path = s_emptyString(),
    const string &source_url = s_emptyString(),
    int encoding = DICTIONARY,
    ErrorInfo &err_info = getDummyErrorInfo());

#if defined(COLUMN_GROUP)
  virtual int create(const vector<ColumnGroup> &schema,
    const vector<string> &key, const string &description = s_emptyString(),
    const string &ns_url = defaultNameServiceURL(),
    const string &tbl_sub_path = s_emptyString(),
    const string &source_url = s_emptyString(),
    int encoding = DICTIONARY,
    ErrorInfo &err_info = getDummyErrorInfo());
#endif

  virtual int reserveBase(size_t size);
  virtual int reserve(size_t size);

  virtual int resize(size_t size);

  // average string length of column col; used for VARSTIRNG
  virtual int avgColumnStrLen(int col);

  virtual vector<string> rank() const;

  virtual string description() const;

  virtual size_t size() const;

  // actual size of valid data (for default and sliding table)
  virtual size_t actualSize() const;

  virtual size_t correctActualSize();

  virtual size_t capacity() const;

  // return time bound or size bound
  virtual size_t bound() const;

  virtual size_t endIndex() const;

  virtual size_t startIndex() const;

  // get the last data row
  virtual size_t lastIndex() const;

  virtual size_t reservedSize() const;

  virtual size_t endRowid() const;

  virtual int numColumn() const;

  virtual DataHelper * getRecordHelper();

  // it doesn't need to write lock table for push_back
  // Insert data into table. It may expand table.
  // @return row_number: success; < 0: error
  virtual int64_t push_back(const void *rec);

  // no lock push_back;
  // when called directly, table should have reserved space for data
  // (mainly for INSERT INTO SELECT on the same table)
  // therefore, table extension should not happen
  // @return row_number: success; < 0: error
  virtual int64_t push_back_nolock(const void *rec);

  int push_back(const vector<string> &rec);

  // only apply on (default)deletable table.
  virtual bool isValidRow(size_t row);

  virtual int erase(size_t row);

  virtual void * record(size_t n) const;

  virtual void * header(size_t n) const;

  virtual uint64_t recordSize() const;

  virtual bool isNull(size_t row, size_t col);

  // return < 0 if NULL
  virtual int getColumnValue(size_t row, size_t column, void *data) const;

  virtual void setColumnValue(size_t row, size_t column, void *data);

  virtual void setColumnValue(size_t row, size_t column, const string &str);

  virtual void * getColumnAddr(size_t row, size_t column) const;

  virtual string columnToString(size_t row, size_t column) const;

  // @param   str     external string buffer
  // @return  length of string
  virtual int columnToString(size_t row, size_t column, char *str) const;

  // Returns the the first matched row for table with key/index;
  // 0 if not found.
  virtual size_t find(const vector<string> &key);

  // Returns matched row(s) in locs from table with key/index
  virtual size_t find(const vector<string> &key, vector<size_t> &locs);

  // Returns the matched row for table with numeric key
  // used in group by postAction for STD etc.
  virtual size_t find(void* key);

  // find row based on a key maker
  // used in table join and table itself
  virtual size_t find(const KeyMaker &km);

  virtual size_t find(const KeyPair &kp);

  virtual size_t count(const string &key_name);

  virtual size_t trim(size_t count, int trim_type, ErrorInfo &err_info);

  virtual int setColumnFlag(int col, int flag);

  virtual ostream & printRecord(std::ostream &os, size_t row);
};

#endif /* BIGTABLE_H_ */
