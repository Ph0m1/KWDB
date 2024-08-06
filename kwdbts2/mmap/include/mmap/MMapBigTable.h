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


#ifndef MMAPBIGTABLE_H_
#define MMAPBIGTABLE_H_

#include "DateTime.h"
#include "BigTable.h"
#include "MMapObject.h"
#include "MMapStringFile.h"

using namespace std;
using namespace bigobject;


// V3: (deprecated) integrate key info into schema in meta data
// V4: (deprecated) actual size for default table in meta data
// V5: (deprecated) replace hash with t1ha
// V6: new boclients schema.
// V7: table with delete marker.
// V8: null-bitmap table.

#define BT_STRUCT_FORMAT_V3             0x00000003
#define BT_STRUCT_FORMAT_V4             0x00000004
#define BT_STRUCT_FORMAT_V5             0x00000005
#define BT_STRUCT_FORMAT_V6             0x00000006
#define BT_STRUCT_FORMAT_V7             0x00000007

#define BT_STRUCT_FORMAT_V8             0x00000008

// null-bitmap header: [record flags][null-bitmap]
// [record flags]: 1 byte [null-bitmap]: (#column + 7) / 8
// record flags:  xxxxxxxd    x: reserved
//       -                d: deleted flag
//
#define REC_FLAG_DELETE                 0x01

// indicate range change for circular table:
// ... endIndex() ---> lastIndex()  -->  NORMAL_BAND
// 1 --> lastIndex() ...  endIndex() --> (reservedSize() - 1)
// LEFT_BAND                    RIGHT_BAND
enum UPDATE_BAND { NORMAL_BAND, RIGHT_BAND, LEFT_BAND };

struct KeyRange {
  int64_t max;
  int64_t min;
};

class MMapBigTable;

typedef void * (MMapBigTable::*RecordDataHandler)(size_t n) const;

// big table use vtree to store row entry & data section to store index
class MMapBigTable: public BigTable, public MMapObject {
 private:
  KRWLatch rw_latch_;

protected:
  HASHTYPE current_record_;
  vector<AttributeInfo> ext_schema_;          // schema reported.
  vector<AttributeInfo> key_info_;
  vector<int> key_idx_;
  AttributeInfoIndex key_index_;
  DataHelper rec_helper_;
  DataHelper key_helper_;
  vector<VarStringObject> vso_;
//  map<string, BitmapIndex *> bitmap_index_;
  string name_;
  int key_order_idx_;
  int deleted_col_;
  int ts_col_;
  int header_sz_;       // 1 + size of [null bitmap]
  int header_offset_;   // [data][header]
  vector<unsigned char *> dummy_header_;    // for legacy table
  bool unique_key_;
  bool index_key_;
  bool has_vs_;
  uint32_t vs_cols_;

  inline void * header_(size_t n) const
  { return offsetAddr(record_(n), header_offset_); }

  void initRow0();

  int open_(int char_code, const string &url, const std::string &db_path, const string &tbl_sub_path, int flags,
    ErrorInfo &err_info);

  int init(const vector<AttributeInfo> &schema, const vector<string> &key,
    const string &key_order, const string &ns_url, const string &description,
    const string &source_url, int encoding, ErrorInfo &err_info);

  void setKeyType();

  int opening(ErrorInfo &err_info);

  int close();

  int initVarStringFile(const AttributeInfo &cinfo, int col, int flags);

  int setDataHelper(int flags, ErrorInfo &err_info);

  int setStructType(int file_type);

  int numKeyColumn_() const { return meta_data_->num_key_info; }

  inline bool isValidRow_(size_t row) const {
    if (meta_data_->struct_version  >= BT_STRUCT_FORMAT_V8) {
      unsigned char *h = (unsigned char *)header_(row);
      return !(*h & REC_FLAG_DELETE);
    } else {
      int8_t v ;
      getColumnValue(row, deleted_col_, &v);
      return (v != 0);
    }
  }

  string VARSTRINGFileName(int i, bool initial);

  int reserveStringFile(size_t n);

  void removeStringFile();

  int rename_(const string &new_name, const string &new_db,
    ErrorInfo &err_info);

  inline timestamp _getTimeStamp(size_t row, int col) const {
    return *((timestamp *)getColumnAddr(row, col));
  }

  // internal use by circular table without mutex lock
  int erase_(size_t row);

  void setDataHelperVarString();

  void eraseHash(size_t start, size_t end);

  enum {
    TRIM_DIR_NONE,  // either forward or backward
    TRIM_DIR_FWD,   // foward search for the last row
    TRIM_DIR_BWD,   // backward search for the last row
    };

  size_t getValidRow(size_t low_r, size_t high_r);

public:
  MMapBigTable();

  virtual ~MMapBigTable();

  int rdLock() override;
  int wrLock() override;
  int unLock() override;

//  inline void * record_(size_t n) const
//  { return offsetAddr(mem_vtree_, n * meta_data_->record_size); }

  inline void * columnAddr_(size_t row, size_t col) const
  { return offsetAddr(record_(row), hrchy_info_[col].offset); }

  inline int getBitmapOffset() const { return header_offset_ + 1; }

  virtual int type() const;

  virtual string name() const;

  virtual int getDeletableColumn() const;

  virtual int firstDataColumn() const;

  virtual int swap_nolock(TSObject *other, ErrorInfo &err_info);

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

  virtual int openInit(ErrorInfo &err_info);

  virtual void sync(int flags);

  virtual int structVersion() const;
  virtual int setStructVersion(int version);  

  virtual bool hasUniqueKey() {return false;}

  /*--------------------------------------------------------------------
   * TSObject functions
   *--------------------------------------------------------------------
   */

  virtual int version() const;
  virtual void incVersion();

  virtual int permission() const;
  virtual void setPermission(int perms);

  virtual string URL() const;

  virtual const string & tbl_sub_path() const;

  virtual string link() const;

  virtual int remove();

  virtual int repair();

  virtual int rename_nolock(const string &new_name, const string &new_db,
    ErrorInfo &err_info);

  virtual int startRead(ErrorInfo &err_info);

  virtual int stopRead();

  virtual bool isTemporary() const;

  virtual bool isMaterialization() const;
  /*--------------------------------------------------------------------
   * data model functions
   *--------------------------------------------------------------------
   */
  virtual string nameServiceURL() const;

  virtual time_t createTime() const;

  virtual uint64_t dataLength() const;

  virtual string sourceURL() const;

  /*--------------------------------------------------------------------
   * big table functions
   *--------------------------------------------------------------------
   */
  virtual int create(const vector<AttributeInfo> &schema,
    const vector<string> &key, const string &key_order,
    const string &ns_url = defaultNameServiceURL(),
    const string &description = s_emptyString(),
    const string &tbl_sub_path = s_emptyString(),
    const string &source_url = s_emptyString(),
    int encoding = DICTIONARY,
    ErrorInfo &err_info = getDummyErrorInfo());

  virtual int reserveBase(size_t size);
  virtual int reserve(size_t size);

  virtual int resize(size_t size);

  virtual int avgColumnStrLen(int col);

  virtual int reserveColumn(size_t n, int col, int str_len);

  virtual vector<string> rank() const;

  virtual const vector<AttributeInfo> & getSchemaInfo() const;

  virtual string description() const;

  virtual size_t size() const;

  virtual size_t actualSize() const;

  virtual size_t correctActualSize();

  virtual size_t capacity() const;

  virtual bool isDeleted() const;

  virtual void setDeleted(bool is_deleted);

  virtual size_t bound() const;

  virtual size_t endIndex() const;

  virtual size_t startIndex() const;

  virtual size_t reservedSize() const;

  virtual int numColumn() const;

  virtual int numVarStrColumn() const;

  virtual int numKeyColumn() const;

  virtual void setTimeBound(size_t time_bound);

  virtual uint64_t getTimeBound() const;

  virtual void setCapacity(size_t capacity);

  // @return row_number: success; < 0: error
  virtual int64_t push_back_nolock(const void *rec);

  virtual bool isValidRow(size_t row);

  virtual int erase(size_t row);

  virtual void * record(size_t n) const;

  virtual void * header(size_t n) const;

  virtual uint64_t recordSize() const;

  virtual int32_t headerSize() const;

  virtual bool isNullable() const;

  virtual bool isNull(size_t row, size_t col);

  virtual void setNull(size_t row, size_t col);

  virtual void setNotNull(size_t row, size_t col);

  virtual uint64_t measureSize() const;

  virtual int getColumnValue(size_t row, size_t column, void *data) const;

  virtual void setColumnValue(size_t row, size_t column, void *data);

  virtual void setColumnValue(size_t row, size_t column, const string &str);

  virtual void *getColumnAddr(size_t row, size_t column) const;

  // Address columns will be read-locked.
  // Need to call unlockColumn() if data is not used.
  virtual void * dataAddrWithRdLock(size_t row, size_t col);

  virtual void unlockColumn(size_t row);

  virtual string columnToString(size_t row, size_t column) const;

  virtual int columnToString(size_t row, size_t col, char *str) const;

  virtual size_t find(const vector<string> &key);

  virtual size_t find(const vector<string> &key, vector<size_t> &locs);

  virtual size_t find(void* key);

  virtual size_t find(const KeyMaker &km);

  virtual size_t find(const KeyPair &kp);

  virtual KeyMaker* cloneKeyMaker() const;

  virtual int changeColumnName(const string &prev_name, const string &new_name);

  virtual const AttributeInfo * getColumnInfo(int col_num) const;

  virtual int getColumnInfo(int col_num, AttributeInfo &ainfo) const;

  virtual int getColumnInfo(const string &name, AttributeInfo &ainfo) const;

  virtual int getColumnNumber(const string &attribute) const;

  virtual int encoding() const;

  virtual int flags() const;

  virtual DataHelper * getRecordHelper();

  virtual VarStringObject getVarStringObject(int col) const;

  virtual VarStringObject getVarStringObject(const string &attr_name) const;

  virtual MMapStringFile * getVarstringFile(int col) const;


  virtual void updateColumnType(int col, int type);
  virtual void updateColumnMaxLength(int col, int new_len);

  virtual int setColumnFlag(int col, int flag);

  //IOT functions
  virtual uint32_t getSyncVersion();
  virtual void setSyncVersion(uint32_t sync_ver);
  virtual timestamp64 & minSrcTimestamp();
  virtual timestamp64 & maxSrcTimestamp();
  virtual timestamp64 & minTimestamp();
  virtual timestamp64 & maxTimestamp();
};

#endif
