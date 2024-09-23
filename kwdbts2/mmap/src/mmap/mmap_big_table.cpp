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

#include <chrono>
#include <cmath>
#include <cstdio>
#include <algorithm>
#include <cstring>
#include <thread>
#include "mmap/mmap_big_table.h"
#include "utils/big_table_utils.h"
#include "utils/date_time_util.h"
#include "lt_rw_latch.h"


// #define DEBUG_LOCK

MMapBigTable::MMapBigTable() : rw_latch_(RWLATCH_ID_MMAP_BIGTABLE_RWLOCK) {
  deleted_col_ = ts_col_ = -1;
}

MMapBigTable::~MMapBigTable() {
  if (meta_data_ == nullptr)
    return;
#if !defined(__x86_64__) && !defined(_M_X64)
  if (mem_ != 0) {
#endif

    if (vso_.size() > 0) {
      for (size_t i = 0; i < col_info_.size(); ++i) {
        if (col_info_[i].isFlag(AINFO_EXTERNAL)) {
          TSObject *obj = vso_[i].obj;
          if (obj) {
            releaseObject(obj);
          }
        } else {
          delete vso_[i].str_col_;      // internal VARSTRING
        }
      }
    }

#if !defined(__x86_64__) && !defined(_M_X64)
  }
#endif
}

impl_latch_virtual_func(MMapBigTable, &rw_latch_)

int MMapBigTable::avgColumnStrLen(int col) {
  MMapStringColumn *sf = vso_[col].str_col_;
  if (sf) {
    if (col_info_[col].isFlag(AINFO_EXTERNAL)) {
      return reinterpret_cast<BigTable *>(vso_[col].obj)->avgColumnStrLen(vso_[col].col);
    }
    size_t n = MMapObject::numRows();
    if (n != 0)
      return (int)(sf->size() / meta_data_->num_rows) + 1;
  }
  return 1; // simply return 1 when table is empty instead of max_len.
}

int MMapBigTable::type() const {
  return (readOnly()) ? obj_type_ | READONLY : obj_type_;
}

string MMapBigTable::name() const { return name_; }

int MMapBigTable::getDeletableColumn() const { return deleted_col_; }

int MMapBigTable::firstDataColumn() const
{ return std::max(deleted_col_, ts_col_) + 1; }

const vector<AttributeInfo> & MMapBigTable::getSchema() const {
    return ext_schema_;
}

int MMapBigTable::setDataHelper(int flags, ErrorInfo &err_info) {
  setDataHelperVarString();
  return  err_info.errcode;
}

vector<AttributeInfo> getExternalSchema(const vector<AttributeInfo> &schema) {
    vector<AttributeInfo> ext_schema;
    for (size_t i = 0; i < schema.size(); ++i) {
        if (!schema[i].isFlag(AINFO_INTERNAL) ||
            std::string(schema[i].name) != kwdbts::s_deletable)
            ext_schema.push_back(schema[i]);
    }
    return ext_schema;
}

int MMapBigTable::init(const vector<AttributeInfo> &schema,
                       const vector<string> &key, const string &key_order, const string &ns_path,
                       const string &description, const string &source_path, int encoding,
                       ErrorInfo &err_info) {
  err_info.errcode = initMetaData();
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  time(&meta_data_->create_time);
  meta_data_->struct_version = BT_STRUCT_FORMAT_V7;

  header_sz_ = 1 + ((schema.size() + 7) / 8); // 1 + [null bit map]

  // dummy header for legacy table without null-bitmap
  dummy_header_.resize(header_sz_, 0);

  name_ = getTsObjectName(path());

  _endIndex() = _startIndex() = 1;
  for (size_t i = 0; i < schema.size(); ++i) {
    col_info_.push_back(schema[i]);
  }
  obj_type_ = meta_data_->type;

  meta_data_->encoding = encoding;
  if ((meta_data_->record_size = setAttributeInfo(col_info_)) < 0) {
    err_info.errcode = KWEPERM;
  }

  header_offset_ = meta_data_->record_size;
  meta_data_->record_size += header_sz_;

  meta_data_->level = col_info_.size();
  meta_data_->depth = meta_data_->level;
  meta_data_->struct_type = (ST_VTREE | ST_NS_EXT);

  err_info.errcode = writeAttributePath(source_path, ns_path, description);
  if (err_info.errcode < 0) {
    return err_info.errcode;
  }

  ext_schema_ = getExternalSchema(col_info_);

  rec_helper_.setHelper(this, col_info_);
  key_helper_.setHelper(this, key_info_);

  if (setDataHelper(flags_, err_info) < 0) {
    return err_info.errcode;
  }

  return err_info.errcode;
}

void MMapBigTable::initRow0(){
  if (!meta_data_->has_data) {
    return;
  }
  // mainly for STRING type data since encoding of "" is not 0.
  for (size_t c = 0; c < col_info_.size(); ++c) {
    // skip external VARSTRING (which may be read only)
    if (col_info_[c].isFlag(AINFO_EXTERNAL) || col_info_[c].isFlag(AINFO_DROPPED))
      continue;
    // properly set value of column to 0 or ""
    setColumnValue(0, c, kwdbts::s_emptyString);
  }
  if (deleted_col_ >= 0) {  // row #0 is not deleted!!
    *((int8_t *)getColumnAddr(0, deleted_col_)) = 1;
  }
}

int MMapBigTable::create(const vector<AttributeInfo> &schema,
  const vector<string> &key, const string &key_order,
  const string &ns_path,
  const string &description,
  const string &tbl_sub_path,
  const string &source_path,
  int encoding,
  ErrorInfo &err_info){

  if (status_== OBJ_INVALID) {
    err_info.errcode = KWENOOBJ;
    return err_info.errcode;
  }

  if (init(schema, key, key_order, ns_path, description, source_path, encoding,
           err_info) < 0)
    return err_info.errcode;

  meta_data_->magic = magic();
  meta_data_->vtree_offset = memLen() = getPageOffset(memLen());

  // reserve three spaces.
  // row 0: dummy row
  // allocate at least two spaces for easier handling circular table.
  setObjectReady();

  int reserved_row = 1024;
  const string file_path = realFilePath();
  err_info.errcode = reserve(reserved_row);  // reserve space for row#0

  if (err_info.errcode >= 0)
    initRow0();

  return  err_info.errcode;
}

int MMapBigTable::create(BigTable *sbt, const vector<AttributeInfo> &schema,
  const vector<string> &key, const vector<VarStringObject> &vs_cols,
  int encoding, ErrorInfo &err_info) {
  // size of schema = size of vs_cols
  vso_.resize(schema.size());
  for (size_t i = 0; i < vs_cols.size(); ++i) {
    if (vs_cols[i].str_col_) {
      BigTable *vsbt = (BigTable *)vs_cols[i].obj;
      if (vsbt)
        vsbt->incRefCount();
      vso_[i].obj = (TSObject *)vsbt;
      vso_[i].str_col_ = vs_cols[i].str_col_;
      vso_[i].col = vs_cols[i].col;
    }
    else {
      vso_[i].obj = nullptr;
      vso_[i].str_col_ = nullptr;
    }
  }
  MMapBigTable::create(schema, key, kwdbts::s_emptyString, kwdbts::s_emptyString,
      kwdbts::s_emptyString, kwdbts::s_emptyString, sbt->name(), encoding, err_info);
  return err_info.errcode;
}

int MMapBigTable::reserveBase(size_t n) {
  size_t diff_n;
  off_t diff_size;
  size_t tn = n + 1;  // include row #0
  if (meta_data_ == nullptr)
    goto reserve_noobj_error;
  if (tn < _reservedSize())
    return 0;

  diff_n = tn - _reservedSize();
  diff_size = diff_n * meta_data_->record_size;
  int err_code;

  if (TSObject::startWrite() < 0) {
reserve_noobj_error:
    err_code = KWENOOBJ;
    goto reserve_nostop_exit;
  }

  err_code = memExtend(diff_size);

  if (err_code < 0) {
    goto reserve_exit;
  }

  _reservedSize() = n;

reserve_exit:
  this->TSObject::stopWrite();

reserve_nostop_exit:
  return err_code;
}

int MMapBigTable::reserve(size_t n) {
  int err_code;
  if (meta_data_ == nullptr) {
    err_code = KWENOOBJ;
    return err_code;
  }
  if (n < _reservedSize())
    return 0;

  err_code = reserveBase(n);

  return err_code;
}

int MMapBigTable::resize(size_t size) {
  if (_reservedSize() >= size + 1)
    meta_data_->num_rows = size;
  return 0;
}

int MMapBigTable::create(const string& link_path) {
  int err_code = memExtend(sizeof(MMapMetaData), 512);
  if (err_code < 0)
    return err_code;
  initSection();
  meta_data_->meta_data_length = sizeof(MMapMetaData);
  meta_data_->struct_type = (ST_TRANSIENT);
  pathCopy(&(meta_data_->link_path[0]), link_path);
  meta_data_->magic = magic();
  return 0;
}

int MMapBigTable::opening(ErrorInfo &err_info) {
  obj_type_ = meta_data_->type; // set type() first
  if (isTransient(meta_data_->struct_type))
    return 0;

  header_sz_ = 1 + ((col_info_.size() + 7) / 8); // 1 + [null bit map]
  header_offset_ = meta_data_->record_size - header_sz_;

  // dummy header for legacy table without null-bitmap
  dummy_header_.resize(header_sz_, 0);
  ext_schema_ = col_info_;

  key_idx_.clear();
  key_info_.clear();
  for (size_t i = 0; i < col_info_.size(); ++i) {
    if (col_info_[i].isFlag(AINFO_DROPPED)) {
      continue;
    }
    if (col_info_[i].isFlag(AINFO_KEY)) {
      key_idx_.push_back(i);
      key_info_.push_back(col_info_[i]);
    }
  }

  setAttributeInfo(key_info_);

  rec_helper_.setHelper(this, col_info_, false);
  key_helper_.setHelper(this, key_info_, false);

  if (setDataHelper(flags_, err_info) < 0)
    return err_info.errcode;

  return err_info.errcode;
}

int MMapBigTable::open_(int char_code, const string &table_path, const std::string &db_path,
                        const string &tbl_sub_path, int flags, ErrorInfo &err_info) {
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  string file_path = getTsFilePath(table_path);
  if ((err_info.errcode = MMapObject::open(file_path, db_path, tbl_sub_path,
    char_code, flags)) < 0) {
    return err_info.errcode;
  }
  name_ = getTsObjectName(path());
  if (memLen() >= (off_t) sizeof(MMapMetaData)) {
    if (opening(err_info) < 0)
      return err_info.errcode;

    //-----------------------------------------------------------
    // CAUTION: careful with changing num_node & reservedSize()
    // adjust size of table when the actual file size is different
    if (!readOnly()) {
      if ((_reservedSize() == 0)
        || ((recordSize() * (_reservedSize() + 1)) + metaSize()
          > (uint64_t) file_length_)) {
        _reservedSize() =
          (recordSize()) ?
            ((((uint64_t) file_length_ - metaSize()) / recordSize()) - 1) :
            file_length_ - metaSize();
      }
      if ((size() > _reservedSize() && _reservedSize() != 0)) {
        meta_data_->num_rows = _reservedSize();
      }
      memLen() = recordSize() * (_reservedSize() + 1) + metaSize();
      int64_t actual_size = (int64_t) actualSize();

      if ((size_t) actual_size > size() || actual_size < 0)
        correctActualSize();
      // use deprecated hash for old names service & key
//      if (meta_data_->struct_version < BT_STRUCT_FORMAT_V5)
//        hash_.setHashFunc(0);
    }
//    setObjectReady();
    if ((meta_data_->encoding & NO_DEFAULT_TABLE))
      setObjectReady();
    else
      status_ = OBJ_TO_SET_DEFUALT;
  } else {
    if (!(flags_ & O_CREAT)) {
      err_info.errcode = KWECORR;
    }
  }

  return err_info.errcode;
}

int MMapBigTable::open(const string &table_path, const std::string &db_path, const string &tbl_sub_path, int flags,
                       ErrorInfo &err_info) {
  return open_(magic(), table_path, db_path, tbl_sub_path, flags, err_info);
}

void MMapBigTable::sync(int flags) {
  if (isValid()) {
    MMapFile::sync(flags);

    for (size_t i = 0; i < vso_.size(); ++i) {
      if (vso_[i].str_col_)
        vso_[i].str_col_->sync(flags);
    }
  }
}

int MMapBigTable::structVersion() const { return MMapObject::structVersion(); }

void MMapBigTable::removeStringFile() {
    if (meta_data_) {
        for (size_t i = 0; i < vso_.size(); ++i) {
            if (vso_[i].str_col_) {
                string var_str_path = vso_[i].str_col_->realFilePath();
                delete vso_[i].str_col_;
                ::remove(var_str_path.c_str());
                vso_[i].obj = nullptr;
                vso_[i].str_col_ = nullptr;
            }
        }
    }
}

int MMapBigTable::close() {
  MMapObject::close();
  key_idx_.clear();
  return 0;
}

int MMapBigTable::remove() {
    int err_code = 0;
    string link_file;
    status_ = OBJ_INVALID;
    removeStringFile();

    close();
    ::remove(realFilePath().c_str());
    return err_code;
}

int MMapBigTable::startRead(ErrorInfo& err_info) {
  int res = TSObject::startRead(err_info);
  if (res >= 0) {
    size_t i = 0;
    for (; i < vso_.size(); ++i) {
      TSObject* obj = vso_[i].obj;
      if (obj && obj != this) {
        if (obj->startRead(err_info) < 0) {
          TSObject::stopRead();
          for (size_t j = 0; j < i; ++j) {
            obj = vso_[j].obj;
            if (obj && obj != this)
              obj->stopRead();
          }
          return err_info.errcode;
        }
      }
    }
  }
  return err_info.errcode;
}

int MMapBigTable::stopRead() {
  for (size_t i = 0; i < vso_.size(); ++i) {
    TSObject* obj = vso_[i].obj;
    if (obj && obj != this)
      obj->stopRead();
  }
  return TSObject::stopRead();
}

bool MMapBigTable::isTemporary() const {
  return (flags_ & O_ANONYMOUS);
}

int64_t MMapBigTable::push_back_nolock(const void *rec) {
  size_t num_node;
  int64_t err_code = 0;

  if (!isValid()) {
    err_code = (uint64_t)KWENOOBJ;
    goto push_back_exit;
  }

  if (size() + 1 >= _reservedSize()) {
    err_code = reserve(_reservedSize() * 2);
    if (err_code < 0) {
      goto push_back_exit;
    }
  }

  num_node = meta_data_->num_rows + 1;
  memcpy(record(num_node), rec, meta_data_->record_size);

  meta_data_->num_rows++;
  meta_data_->actul_size++;
  err_code = meta_data_->num_rows;
push_back_exit:
    return err_code;
}

bool MMapBigTable::isValidRow(size_t row){
  return (deleted_col_ >= 0) ? isValidRow_(row) : true;
}

int MMapBigTable::erase_(size_t row) {
  // need to erase key before clear data.
  int v = 0;
  setColumnValue(row, deleted_col_, &v);
  return 0;
}

int MMapBigTable::erase(size_t row) {
  if (row >= 0) {
    erase_(row);
    meta_data_->actul_size--;
  }
  return 0;
}

void * MMapBigTable::record(size_t n) const {
  return record_(n);
}

void * MMapBigTable::header(size_t n) const {
  return (void *)dummy_header_.data();
}

uint64_t MMapBigTable::recordSize() const
{ return MMapObject::recordSize(); }

bool MMapBigTable::isNull(size_t row, size_t col) {
  return rec_helper_.isNull(col, columnAddr_(row, col));
}

int MMapBigTable::getColumnValue(size_t row, size_t col, void *data) const {
  const AttributeInfo &info = col_info_[col];
  memcpy(data, columnAddr_(row, col), info.size);
  return 0;
}

void MMapBigTable::setColumnValue(size_t row, size_t column, void* data) {
  AttributeInfo& info = col_info_[column];
  memcpy(columnAddr_(row, column), data, info.size);
}

void MMapBigTable::setColumnValue(size_t row, size_t column, const string &str)
{ rec_helper_.stringToColumn(column, (char *)str.c_str(), record_(row)); }

void *MMapBigTable::getColumnAddr(size_t row, size_t column) const
{ return columnAddr_(row, column); }

string MMapBigTable::columnToString(size_t row, size_t col) const {
  return rec_helper_.columnToString(col, columnAddr_(row, col));
}

int MMapBigTable::columnToString(size_t row, size_t col, char *str) const {
  return rec_helper_.columnToString(col, columnAddr_(row, col), str);
}

size_t MMapBigTable::find(const vector<string> &key) {
  return 0;
}

size_t MMapBigTable::find(const vector<string> &key, vector<size_t> &locs) {
  return 0;
}

size_t MMapBigTable::find(void* key) {
  return 0;
}

size_t MMapBigTable::find(const KeyMaker &km) {
  return 0;
}

size_t MMapBigTable::find(const KeyPair &kp) {
  return 0;
}

size_t MMapBigTable::size() const { return MMapObject::numRows(); }

size_t MMapBigTable::actualSize() const { return _actualSize(); }

size_t MMapBigTable::correctActualSize() {
  if (deleted_col_ < 0) {
    _actualSize() = MMapObject::numRows();
  } else {
    table_partition tp;
    tp.push_back(std::move(make_pair(1, size() + 1)));

    size_t act_sz = 0;
    for (size_t p = 0; p < tp.size(); ++p) {
      for (size_t r = tp[p].first; r < tp[p].second; ++r) {
        if (*((int8_t *) getColumnAddr(r, deleted_col_)) != 0)
          act_sz++;
      }
    }
    _actualSize() = act_sz;
  }

  return _actualSize();
}

void MMapBigTable::setDataHelperVarString() {
  // set VARSTRING files in DataHelper
  for (size_t i = 0; i < vso_.size(); ++i) {
    if (vso_[i].str_col_) {
      rec_helper_.setStringFile(i, (void *)(vso_[i].str_col_));
    }
  }
  for (size_t i = 0; i < key_idx_.size(); ++i) {
    size_t key_idx = key_idx_[i];
    if (vso_[key_idx].str_col_)
      key_helper_.setStringFile(i, (void *)(vso_[key_idx].str_col_));
  }
}

size_t MMapBigTable::capacity() const { return MMapObject::_reservedSize(); }

size_t MMapBigTable::bound() const { return MMapObject::timeBound(); }

size_t MMapBigTable::reservedSize() const  { return _reservedSize(); }

int MMapBigTable::numColumn() const { return meta_data_->level; }

const string & MMapBigTable::tbl_sub_path() const { return tbl_sub_path_; }

int MMapBigTable::version() const { return meta_data_->version; }

int MMapBigTable::permission() const { return meta_data_->permission; }

string MMapBigTable::path() const
{ return filePath(); }

string MMapBigTable::nameServicePath() const
{ return MMapObject::nameServicePath(); }

const vector<AttributeInfo> & MMapBigTable::getSchemaInfo() const
{ return col_info_; }

int MMapBigTable::encoding() const { return MMapObject::encoding(); }

int MMapBigTable::flags() const { return MMapObject::flags(); }

DataHelper * MMapBigTable::getRecordHelper()
{ return &rec_helper_; }

int MMapBigTable::setColumnFlag(int col, int flag) {
  col_info_[col].setFlag(flag);
  setColumnInfo(col, col_info_[col]);
  return 0;
}

timestamp64 & MMapBigTable::minTimestamp()
{ return MMapObject::minTimestamp(); }

timestamp64 & MMapBigTable::maxTimestamp()
{ return MMapObject::maxTimestamp(); }

