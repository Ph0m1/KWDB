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
#include "objcntl.h"
#include "mmap/MMapBigTable.h"
#include "BigObjectUtils.h"
#include "BigObjectApplication.h"
#include "DateTime.h"
#include "lt_rw_latch.h"


// #define DEBUG_LOCK

MMapBigTable::MMapBigTable() : rw_latch_(RWLATCH_ID_MMAP_BIGTABLE_RWLOCK) {
  deleted_col_ = ts_col_ = -1;
  has_vs_ = false;
  index_key_ = false;
  vs_cols_ = 0;
}

MMapBigTable::~MMapBigTable() {
  if (meta_data_ == 0)
    return;
#if !defined(__x86_64__) && !defined(_M_X64)
  if (mem_ != 0) {
#endif

    if (vso_.size() > 0) {
      for (size_t i = 0; i < hrchy_info_.size(); ++i) {
        if (hrchy_info_[i].isFlag(AINFO_EXTERNAL)) {
          TSObject *obj = vso_[i].obj;
          if (obj) {
            releaseObject(obj);
          }
        } else {
          delete vso_[i].sf;      // internal VARSTRING
        }
      }
    }

#if !defined(__x86_64__) && !defined(_M_X64)
  }
#endif
}

impl_latch_virtual_func(MMapBigTable, &rw_latch_)

int MMapBigTable::avgColumnStrLen(int col) {
  MMapStringFile *sf = vso_[col].sf;
  if (sf) {
    if (hrchy_info_[col].isFlag(AINFO_EXTERNAL)) {
      return reinterpret_cast<BigTable *>(vso_[col].obj)->avgColumnStrLen(vso_[col].col);
    }
    size_t n = MMapObject::numNode();
    if (n != 0)
      return (int)(sf->size() / meta_data_->num_node) + 1;
  }
  return 1; // simply return 1 when table is empty instead of max_len.
}

int MMapBigTable::reserveColumn(size_t n, int col, int str_len) {
  MMapStringFile *sf = vso_[col].sf;
  str_len = std::min(str_len, hrchy_info_[col].max_len); // protection
  return (sf) ? sf->reserve(n,  str_len) : 0;
}

int MMapBigTable::type() const {
//  return (readOnly()) ? meta_data_->type | READONLY : meta_data_->type;
  return (readOnly()) ? obj_type_ | READONLY : obj_type_;
}

string MMapBigTable::name() const { return name_; }

int MMapBigTable::getDeletableColumn() const { return deleted_col_; }

int MMapBigTable::firstDataColumn() const
{ return std::max(deleted_col_, ts_col_) + 1; }

void updateVSObject(vector<VarStringObject> &vso, TSObject *old_obj,
    TSObject *new_obj) {
    for (size_t i = 0; i < vso.size(); ++i) {
        if (vso[i].obj == old_obj)
            vso[i].obj = new_obj;
    }
}

int MMapBigTable::swap_nolock(TSObject *other, ErrorInfo &err_info) {
    MMapBigTable *rhs = dynamic_cast<MMapBigTable *>(other);
    if (!rhs)
        return -1;
    if (status_ > OBJ_READY_TO_COMPACT)
        return -1;
//    TSObject::swapObject(other);
//    BO_DEBUG(cout, &hrchy_info_ << " " << hrchy_info_.data());
    MMapObject::swap(*rhs);
//    BO_DEBUG(cout, &hrchy_info_ << " " << hrchy_info_.data());
    std::swap(current_record_, rhs->current_record_);
    std::swap(ext_schema_, rhs->ext_schema_);
    std::swap(key_info_, rhs->key_info_);
    std::swap(key_idx_, rhs->key_idx_);
    std::swap(key_index_, rhs->key_index_);

//    BO_DEBUG(cout, hash_.memAddr() << " " << hash_.filePath());

    std::swap(vso_, rhs->vso_);
    TSObject *self = (TSObject *)this;
    TSObject *oth = (TSObject *)rhs;
    updateVSObject(vso_, oth, self);
    updateVSObject(rhs->vso_, self, oth);
    std::swap(name_, rhs->name_);
    std::swap(key_order_idx_, rhs->key_order_idx_);
    std::swap(deleted_col_, rhs->deleted_col_);
    std::swap(ts_col_, rhs->ts_col_);
    std::swap(unique_key_, rhs->unique_key_);
    std::swap(index_key_, rhs->index_key_);


    // the Attribute Info in rec_helper points to hrchy_info itself
    rec_helper_.swap(rhs->rec_helper_);
    key_helper_.swap(rhs->key_helper_);

    setDataHelperVarString();
    rhs->setDataHelperVarString();

    // it is easier to reset key maker in hash instead of writing swap()
    return 0;
}

const vector<AttributeInfo> & MMapBigTable::getSchema() const {
    return ext_schema_;
}


int MMapBigTable::initVarStringFile(const AttributeInfo &cinfo, int col,
  int flags) {
  int err_code = 0;
  int type = cinfo.type;
  if (isStringFileNeeded(type) && !cinfo.isFlag(AINFO_DROPPED)) {
    if (!cinfo.isFlag(AINFO_EXTERNAL)) {
//      MMapStringFile *sfile = getMMapStringFile(type);
      MMapStringFile *sfile = new MMapStringFile(LATCH_ID_TEMP_TABLE_STRING_FILE_MUTEX,
                                                RWLATCH_ID_TEMP_TABLE_STRING_FILE_RWLOCK);
      string col_name = name_ + '.' + intToString(col) + ".s";
      err_code = sfile->open(col_name, db_path_, tbl_sub_path_, flags);
      vso_[col].obj = (TSObject *) this;
      vso_[col].sf = sfile;
      vso_[col].col = col;
      has_vs_ = true;
      if (err_code == BOECORR) {
        BO_DEBUG(cout, "error");
      }
      vs_cols_++;
    }
  } else {
    vso_[col].obj = nullptr;
    vso_[col].sf = nullptr;
  }

  return err_code;
}

int MMapBigTable::setDataHelper(int flags, ErrorInfo &err_info) {
  if (is_deletable(type()) && meta_data_->struct_version < BT_STRUCT_FORMAT_V8) {
    deleted_col_ = getColumnNumber(bigobject::s_deletable());
  }

  if (is_timestamp(type()))
    ts_col_ = getColumnNumber(bigobject::s_cts());

//  if (vso_.size() < hrchy_info_.size())
//    vso_.resize(hrchy_info_.size());
//  for (size_t i = 0; i < hrchy_info_.size(); ++i) {
//    if ((err_info.errcode = initVarStringFile(hrchy_info_[i], i, flags)) < 0)
//      return err_info.errcode;
//
//  }
  setDataHelperVarString();
  return  err_info.errcode;
}

vector<AttributeInfo> getExternalSchema(const vector<AttributeInfo> &schema) {
    vector<AttributeInfo> ext_schema;
    for (size_t i = 0; i < schema.size(); ++i) {
        if (!schema[i].isFlag(AINFO_INTERNAL) ||
            schema[i].name != s_deletable())
            ext_schema.push_back(schema[i]);
    }
    return ext_schema;
}

int MMapBigTable::init(const vector<AttributeInfo> &schema,
  const vector<string> &key, const string &key_order, const string &ns_url,
  const string &description, const string &source_url, int encoding,
  ErrorInfo &err_info) {
  err_info.errcode = initMetaData();
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  time(&meta_data_->create_time);

#if defined(BT_STRUCT_FORMAT_V8)
  if (encoding & NULLBITMAP_TABLE) {
    meta_data_->struct_version = BT_STRUCT_FORMAT_V8;
    meta_data_->type |= NULLBITMAP_TABLE;
  } else
#endif
  {
    meta_data_->struct_version = BT_STRUCT_FORMAT_V7;
    if (is_deletable(encoding)) {
      meta_data_->type |= DELETABLE_TABLE;
      AttributeInfo a_info;
      a_info.name = bigobject::s_deletable();
      a_info.type = INT8;
      a_info.setFlag(AINFO_INTERNAL);
      hrchy_info_.push_back(a_info);
    }
  }

  if (is_deletable(encoding)) {
    meta_data_->type |= DELETABLE_TABLE;
  }

  header_sz_ = 1 + ((schema.size() + 7) / 8); // 1 + [null bit map]

  // dummy header for legacy table without null-bitmap
  if (meta_data_->struct_version < BT_STRUCT_FORMAT_V8)
    dummy_header_.resize(header_sz_, 0);

  name_ = getURLObjectName(URL());


  if (is_timestamp(encoding)) {
    meta_data_->type |= TIMESTAMP_TABLE;
    AttributeInfo a_info;
    a_info.name = bigobject::s_cts();
    a_info.type = TIMESTAMP;
    a_info.setFlag(AINFO_INTERNAL);
    hrchy_info_.push_back(a_info);
  }
  _endIndex() = _startIndex() = 1;
  if (is_circular(encoding))
    meta_data_->type |= CIRCULAR_TABLE;
  for (size_t i = 0; i < schema.size(); ++i) {
    hrchy_info_.push_back(schema[i]);
    if (is_varstr_to_char(encoding) &&
      (hrchy_info_.back().type == VARSTRING)) {
      hrchy_info_.back().type = CHAR;
    }
    if (isWKBType(schema[i].type)) {
      switch(schema[i].type) {
        case WKB_POINT: hrchy_info_.back().type = POINT; break;
        case WKB_LINESTRING: hrchy_info_.back().type = LINESTRING; break;
        case WKB_POLYGON: hrchy_info_.back().type = POLYGON; break;
        case WKB_MULTIPOLYGON: hrchy_info_.back().type = MULTIPOLYGON; break;
        case WKB_GEOMCOLLECT: hrchy_info_.back().type = GEOMCOLLECT; break;
        case WKB_GEOMETRY: hrchy_info_.back().type = GEOMETRY; break;
      }
    }
    //if (hrchy_info_.back().name.size() > MAX_COLUMNATTR_LEN) {
    //  hrchy_info_.back().name = hrchy_info_.back().name.substr(0,
    //    rectifyUtf8(hrchy_info_.back().name.c_str(), MAX_COLUMNATTR_LEN));
    //}
  }
  hrchy_index_.clear();
  hrchy_index_.setInfo(hrchy_info_);
  obj_type_ = meta_data_->type;

//    meta_data_->encoding = encoding & 0x00000001;
  meta_data_->encoding = encoding;
  if ((meta_data_->record_size = setAttributeInfo(hrchy_info_, encoding)) < 0)
    err_info.errcode = BOEPERM;

//  if (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8) {
    header_offset_ = meta_data_->record_size;
    meta_data_->record_size += header_sz_;
//  }
  // key_order_idx_ = key_order_idx; set in setKeyInfo()
  //key_order_idx_ = getIndex(hrchy_index_, key_order);
  //if (setKeyInfo(key, key_order_idx_, err_info) < 0)
  //  return err_info.errcode;

  meta_data_->level = hrchy_info_.size();
  meta_data_->depth = meta_data_->level;
  meta_data_->struct_type = (ST_VTREE | ST_NS_EXT);

  err_info.errcode = writeAttributeURL(source_url, ns_url, description);
  if (err_info.errcode < 0) {
    return err_info.errcode;
  }

  ext_schema_ = getExternalSchema(hrchy_info_);


  rec_helper_.setHelper(this, hrchy_info_);
  key_helper_.setHelper(this, key_info_);

  if (setDataHelper(flags_, err_info) < 0) {
    return err_info.errcode;
  }
  meta_data_->num_varstr_cols = this->vs_cols_;
  //openKey(err_info);

//  setDefaultValueAction(this, tbl_sub_path_, err_info);

  return err_info.errcode;
}

void MMapBigTable::initRow0(){
  if (!meta_data_->has_data) {
    return;
  }
  // mainly for STRING type data since encoding of "" is not 0.
  for (size_t c = 0; c < hrchy_info_.size(); ++c) {
    // skip external VARSTRING (which may be read only)
    if (hrchy_info_[c].isFlag(AINFO_EXTERNAL) || hrchy_info_[c].isFlag(AINFO_DROPPED))
      continue;
    // properly set value of column to 0 or ""
    setColumnValue(0, c, bigobject::s_emptyString());
  }
  if (deleted_col_ >= 0 && meta_data_->struct_version < BT_STRUCT_FORMAT_V8) {  // row #0 is not deleted!!
    *((int8_t *)getColumnAddr(0, deleted_col_)) = 1;
  }
}

int MMapBigTable::create(const vector<AttributeInfo> &schema,
  const vector<string> &key, const string &key_order,
  const string &ns_url,
  const string &description,
  const string &tbl_sub_path,
  const string &source_url,
  int encoding,
  ErrorInfo &err_info){

//    obj_mutex_.lock();
  if (status_== OBJ_INVALID) {
    err_info.errcode = BOENOOBJ;
    return err_info.errcode;
  }

  if (init(schema, key, key_order, ns_url, description, source_url, encoding,
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
  //if (file_path.length() >= 2 && file_path.compare(file_path.length() - 2, 2, ".l") == 0) {
  //  reserved_row = L_FILE_ROWS + 1;   // .l file has fixed rows
  //}
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
    if (vs_cols[i].sf) {
      BigTable *vsbt = (BigTable *)vs_cols[i].obj;
      if (vsbt)
        vsbt->incRefCount();
      vso_[i].obj = (TSObject *)vsbt;
      vso_[i].sf = vs_cols[i].sf;
      vso_[i].col = vs_cols[i].col;
    }
    else {
      vso_[i].obj = nullptr;
      vso_[i].sf = nullptr;
    }
  }
  MMapBigTable::create(schema, key, s_emptyString(), sbt->nameServiceURL(),
    s_emptyString(), s_emptyString(), sbt->name(), encoding, err_info);
  return err_info.errcode;
}

string MMapBigTable::VARSTRINGFileName(int i, bool initial) {
    string varstr_fn = (initial) ? "_" : "";
    return varstr_fn + name_ + '.' + intToString(i) + ".s";
}

int MMapBigTable::reserveStringFile(size_t n) {
  int err_code = TSObject::startWrite();
  if (err_code >= 0) {
    for (size_t i = 0; i < hrchy_info_.size(); ++i) {
//      if (hrchy_info_[i].type == VARSTRING &&
      if (isStringFileNeeded(hrchy_info_[i].type) &&
        !hrchy_info_[i].isFlag(AINFO_EXTERNAL) &&
        !hrchy_info_[i].isFlag(AINFO_DROPPED)) {
        if ((err_code = vso_[i].sf->reserve(size(), n, avgColumnStrLen(i))) < 0)
          break;
      }
    }
    TSObject::stopWrite();
  }
  return err_code;
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
#if defined(DEBUG_LOCK)
    BO_DEBUG(cout, "[W Lock] Fail: " << URL());
#endif
reserve_noobj_error:
    err_code = BOENOOBJ;
    goto reserve_nostop_exit;
  }
#if defined(DEBUG_LOCK)
  BO_DEBUG(cout, "[W Lock  ] " << URL() << " sz: " << n);
#endif

  err_code = memExtend(diff_size);

//  BO_DEBUG(cout, "new reserved size: " << n
//    << " reserved size: " << reservedSize()
//    << " diff_size: " << diff_size
//    << " record size: " << recordSize()
//    << " vtree_offset: " << meta_data_->vtree_offset
//    << " mem_length: " << memLen()
//    << " file length: " << file_length_ );

  if (err_code < 0) {
    goto reserve_exit;
  }

  if (err_code < 0)
    goto reserve_exit;

//    BO_DEBUG(cout, "BT " << name() << " reserve from " <<
//        reservedSize() << " -> " << n);
  _reservedSize() = n;

//  BO_DEBUG(cout, "reserved size() = " << _reservedSize() << " diff n = "
//     << diff_n << " meta_size " << metaSize() << " size()" << size()
//     << " file_legth " << file_length_);
//  if (((recordSize() * (_reservedSize() + 1)) + metaSize()> (uint64_t) file_length_)) {
//    BO_DEBUG(cout,  "table meta is worng");
//  }

  is_len_changed_ = true;

reserve_exit:
  this->TSObject::stopWrite();
#if defined(DEBUG_LOCK)
  BO_DEBUG(cout, "[W Unlock] " << URL() << " " << reservedSize());
#endif

reserve_nostop_exit:
  return err_code;
}

int MMapBigTable::reserve(size_t n) {
  int err_code;
  if (meta_data_ == nullptr) {
    err_code = BOENOOBJ;
    return err_code;
  }
  if (n < _reservedSize())
    return 0;

  err_code = reserveBase(n);

  return err_code;
}

int MMapBigTable::resize(size_t size) {
  if (_reservedSize() >= size + 1)
    meta_data_->num_node = size;
  return 0;
}

int MMapBigTable::create(const string &link_url)
{
    int err_code = memExtend(sizeof(MMapMetaData), 512);
    if (err_code < 0)
        return err_code;
    initSection();
    meta_data_->meta_data_length = sizeof(MMapMetaData);
    meta_data_->struct_type = (ST_TRANSIENT);
    urlCopy(&(meta_data_->link_url[0]), link_url);
    //   assign(meta_data_->source_url, urlCopy(source_url, 0, 512));
    meta_data_->magic = magic();
    return 0;
}

void MMapBigTable::setKeyType() {
  //unique_key_ = (hash_.type() == 0);
  //index_key_ = ((hash_.type() & INDEX_TABLE) != 0);
}

int MMapBigTable::opening(ErrorInfo &err_info) {
  obj_type_ = meta_data_->type; // set type() first
  if (isTransient(meta_data_->struct_type))
    return 0;

  header_sz_ = 1 + ((hrchy_info_.size() + 7) / 8); // 1 + [null bit map]
  header_offset_ = meta_data_->record_size - header_sz_;

  // dummy header for legacy table without null-bitmap
  if (meta_data_->struct_version < BT_STRUCT_FORMAT_V8)
    dummy_header_.resize(header_sz_, 0);

  if (isDeletable()) {
    ext_schema_ = getExternalSchema(hrchy_info_);
  } else
    ext_schema_ = hrchy_info_;

  key_order_idx_ = -1;
  key_idx_.clear();
  key_info_.clear();
  for (size_t i = 0; i < hrchy_info_.size(); ++i) {
    if (hrchy_info_[i].isFlag(AINFO_DROPPED)) {
      continue;
    }
    if (hrchy_info_[i].isFlag(AINFO_KEY)) {
      key_idx_.push_back(i);
      key_info_.push_back(hrchy_info_[i]);
    }
  }

  setAttributeInfo(key_info_, encoding());
  key_index_.clear();
  key_index_.setInfo(key_info_);

  rec_helper_.setHelper(this, hrchy_info_, false);
  key_helper_.setHelper(this, key_info_, false);

  if (setDataHelper(flags_, err_info) < 0)
    return err_info.errcode;


  return err_info.errcode;
}

int MMapBigTable::open_(int char_code, const string &url,const std::string &db_path,
                        const string &tbl_sub_path, int flags, ErrorInfo &err_info) {
  string file_path = getURLFilePath(url);
  if ((err_info.errcode = MMapObject::open(file_path, db_path, tbl_sub_path,
    char_code, flags)) < 0) {
    return err_info.errcode;
  }
  name_ = getURLObjectName(URL());
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
        meta_data_->num_node = _reservedSize();
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
      err_info.errcode = BOECORR;
    }
  }

  current_record_ = 0;
//    setObjectReady();
  return err_info.errcode;
}

int MMapBigTable::open(const string &url, const std::string &db_path, const string &tbl_sub_path, int flags,
  ErrorInfo &err_info) {
  return open_(magic(), url, db_path, tbl_sub_path, flags, err_info);
}

int MMapBigTable::openInit(ErrorInfo &err_info) {
  // otherwise deadlock
  int err_code = 0;
  if (status_ == OBJ_TO_SET_DEFUALT) {
    mutexLock();
    setObjectReady();
    pthread_cond_broadcast(&obj_mtx_cv_); // wake up all
    mutexUnlock();
    return err_code;
  }
  return 0;

}

void MMapBigTable::sync(int flags) {
  if (isValid()) {
    MMapFile::sync(flags);

    for (size_t i = 0; i < vso_.size(); ++i) {
      if (vso_[i].sf)
        vso_[i].sf->sync(flags);
    }
  }
}

int MMapBigTable::structVersion() const { return MMapObject::structVersion(); }

int MMapBigTable::setStructVersion(int version) { 
  MMapObject::setStructVersion(version);
  return 0;
}

void MMapBigTable::removeStringFile() {
    if (meta_data_) {
        for (size_t i = 0; i < vso_.size(); ++i) {
            if (vso_[i].sf) {
                string var_str_url = vso_[i].sf->realFilePath();
                delete vso_[i].sf;
                ::remove(var_str_url.c_str());
                vso_[i].obj = nullptr;
                vso_[i].sf = nullptr;
            }
        }
    }
}

int MMapBigTable::close() {
  MMapObject::close();
  key_idx_.clear();
  return 0;
}


// #define DEBUG_REMOVE_LOCK

int MMapBigTable::remove() {
    int err_code = 0;
    string link_file;
#if defined(DEBUG_REMOVE_LOCK)
    BO_DEBUG(cout, " trying to remove: " << URL());
#endif

//    obj_mutex_.lock();
//    if (startWrite() < 0) {
//        err_code = BOENOOBJ;
//        goto remove_exit;
//    }
    status_ = OBJ_INVALID;
#if defined(DEBUG_REMOVE_LOCK)
    BO_DEBUG(cout, " remove <Write Lock>[" << ref_count_ << "]: " << URL() <<
        " status: " << status_);
#endif
    removeStringFile();

    close();
    ::remove(realFilePath().c_str());
    // try to remove link table
//    link_file = BigObjectConfig::home() + tbl_sub_path_ + "_" + file_path_;
//    ::remove(link_file.c_str());

//    stopWrite();
#if defined(DEBUG_REMOVE_LOCK)
    BO_DEBUG(cout, " remove <Write Unlock>: " << URL());
#endif
remove_exit:
//    obj_mutex_.unlock();
    return err_code;
}

int MMapBigTable::repair() {
  correctActualSize();
  size_t sz = size();
  for (size_t i = 0; i < hrchy_info_.size(); ++i) {
    if (!(hrchy_info_[i].isFlag(AINFO_EXTERNAL)) || !(hrchy_info_[i].isFlag(AINFO_DROPPED))) {
      MMapStringFile *sf = vso_[i].sf;
      if (sf) {
        size_t loc;
        getColumnValue(sz, i, &loc);
        sf->adjustSize(loc);
      }
    }
  }
  return 0;
}

int MMapBigTable::rename_(const string &new_name, const string &new_db,
  ErrorInfo &err_info) {
  string link_file;
  string new_folder = fileFolder(new_db);
  string new_fhead = new_folder + new_name;
  string new_fp = new_fhead + s_bt();
  string new_hash_fp = new_fp + s_ht();
  name_ = new_name;
  tbl_sub_path_ = new_db;
  file_path_ = new_name + s_bt();
  if (isTemporary()) {    // simply update name for temporary table
    goto rename_exit;
  }
  close();
//  setObjectNotReady();
  if ((err_info.errcode = MMapFile::rename(new_fp)) < 0) {
    return err_info.errcode;
  }

  for (size_t i = 0; i < vso_.size(); ++i) {
    if (vso_[i].sf) {
      MMapStringFile *sf = vso_[i].sf;
      new_fp = new_fhead + '.' + intToString(i) + s_s();
      sf->rename(new_fp);
    }
  }
  vso_.clear();

rename_exit:

  return err_info.errcode;
}

int MMapBigTable::rename_nolock(const string &new_name, const string &new_db,
  ErrorInfo &err_info) {
  if (rename_(new_name, new_db, err_info) >= 0)
    open(URL(), db_path_, tbl_sub_path_, flags_, err_info);
  return err_info.errcode;
}



int MMapBigTable::startRead(ErrorInfo &err_info)
{
    int res = TSObject::startRead(err_info);
    if (res >= 0) {
        size_t i = 0;
        for (; i < vso_.size(); ++i) {
            TSObject *obj = vso_[i].obj;
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



int MMapBigTable::stopRead()
{
    for (size_t i = 0; i < vso_.size(); ++i) {
        TSObject *obj = vso_[i].obj;
        if (obj && obj != this)
            obj->stopRead();
    }
    return TSObject::stopRead();
}



bool MMapBigTable::isTemporary() const
{
    return (flags_ & O_ANONYMOUS);
}

bool MMapBigTable::isMaterialization() const {
    return (flags_ & O_MATERIALIZATION);
}

int MMapBigTable::setStructType(int struct_type)
{
    if (struct_type & ST_DATA_EXT) {
	meta_data_->struct_type = (meta_data_->struct_type & 0xFFFFF0FF)
	    | struct_type;
    }
    return meta_data_->struct_type;
}

void MMapBigTable::setTimeBound(size_t time_bound) {
  if (meta_data_)
    meta_data_->time_bound = time_bound;
}

uint64_t MMapBigTable::getTimeBound() const {
  if (meta_data_)
    return meta_data_->time_bound;
  return 0;
}

void MMapBigTable::setCapacity(size_t capacity) {
    if (meta_data_)
        meta_data_->capacity = capacity;
}

int64_t MMapBigTable::push_back_nolock(const void *rec) {
  size_t hash_loc;
  size_t num_node;
  int64_t err_code = 0;

  if (!isValid()) {
    err_code = (uint64_t)BOENOOBJ;
    goto push_back_exit;
  }

  if (size() + 1 >= _reservedSize()) {
    err_code = reserve(_reservedSize() * 2);
    if (err_code < 0) {
      goto push_back_exit;
    }
  }

  if (is_timestamp(type())) {
    timestamp now_ts = now();
    rec_helper_.toColumn(ts_col_, &now_ts, (void *) rec);
  }

  num_node = meta_data_->num_node + 1;
  memcpy(record(num_node), rec, meta_data_->record_size);

no_update_hash:

    meta_data_->num_node++;
    meta_data_->actul_size++;
    err_code = meta_data_->num_node;
push_back_exit:
    return err_code;
}

bool MMapBigTable::isValidRow(size_t row){
  if (meta_data_->struct_version  >= BT_STRUCT_FORMAT_V8) {
    unsigned char *h = (unsigned char *)header_(row);
    return !(*h & REC_FLAG_DELETE);
  }
  return (deleted_col_ >= 0) ? isValidRow_(row) : true;
}

int MMapBigTable::erase_(size_t row) {
  // need to erase key before clear data.

  if (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8) {
    unsigned char *h = (unsigned char *)header_(row);
    *h |=  REC_FLAG_DELETE;
    return 0;
  }
  int v = 0;
  setColumnValue(row, deleted_col_, &v);
  return 0;
}

int MMapBigTable::erase(size_t row) {
  // if (deleted_col_ >= 0 && row > 0) {
  //   erase_(row);
  //   meta_data_->actul_size--;
  // }
  // return 0;

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
  if (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8)
    return header_(n);
  return (void *)dummy_header_.data();
}

uint64_t MMapBigTable::recordSize() const
{ return MMapObject::recordSize(); }

int32_t MMapBigTable::headerSize() const { return header_sz_; }

bool MMapBigTable::isNullable() const
{ return (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8); }

bool MMapBigTable::isNull(size_t row, size_t col) {
  if (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8)
    return get_null_bitmap((unsigned char *)header_(row) + 1, col);

  return rec_helper_.isNull(col, columnAddr_(row, col));
}

void MMapBigTable::setNull(size_t row, size_t col) {
  if (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8)
    set_null_bitmap((unsigned char *)header_(row) + 1, col);
}

void MMapBigTable::setNotNull(size_t row, size_t col) {
  if (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8)
    unset_null_bitmap((unsigned char *)header_(row) + 1, col);
}

uint64_t MMapBigTable::measureSize() const
{ return MMapObject::measureSize(); }

int MMapBigTable::getColumnValue(size_t row, size_t col, void *data) const {
  const AttributeInfo &info = hrchy_info_[col];
  memcpy(data, columnAddr_(row, col), info.size);
  if (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8)
    return (get_null_bitmap((unsigned char *)header_(row) + 1, col)) ? -1 : 0;
  return 0;
}

void MMapBigTable::setColumnValue(size_t row, size_t column, void *data)
{
    AttributeInfo &info = hrchy_info_[column];
    memcpy(columnAddr_(row, column), data, info.size);
}

void MMapBigTable::setColumnValue(size_t row, size_t column, const string &str)
{ rec_helper_.stringToColumn(column, (char *)str.c_str(), record_(row)); }

void *MMapBigTable::getColumnAddr(size_t row, size_t column) const
{ return columnAddr_(row, column); }

void * MMapBigTable::dataAddrWithRdLock(size_t row, size_t col)
{ return rec_helper_.dataAddrWithRdLock(col, columnAddr_(row, col)); }

void MMapBigTable::unlockColumn(size_t col)
{ rec_helper_.unlockColumn(col); }

string MMapBigTable::columnToString(size_t row, size_t col) const {
   if (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8) {
    if (get_null_bitmap((unsigned char*) header_(row) + 1, col)) {
      return "NULL";
    }
  }
  return rec_helper_.columnToString(col, columnAddr_(row, col));
}

int MMapBigTable::columnToString(size_t row, size_t col, char *str) const {
  if (meta_data_->struct_version >= BT_STRUCT_FORMAT_V8) {
    if (get_null_bitmap((unsigned char*) header_(row) + 1, col))
      return -1;
  }
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
//    HASHTYPE value = hash_.get(kp, hash_loc);
//    return (value == NameService::bad_id) ? (size_t) -1 : value;
}

KeyMaker* MMapBigTable::cloneKeyMaker() const {
    return nullptr;
}

void MMapBigTable::eraseHash(size_t start, size_t end) {
  return;
}

// return valid rows between [low_r   -----  high_r) excluding high_r
size_t MMapBigTable::getValidRow(size_t low_r, size_t high_r) {
  if (isDeletable()) {
    size_t sz = 0;
    for (size_t r = low_r; r < high_r; ++r) {
      if (isValidRow_(r))
        sz++;
    }
    return sz;
  }
  return high_r - low_r;
}

int MMapBigTable::changeColumnName(const string &prev_name,
    const string &new_name) {
    int prev_idx = hrchy_index_.getIndex(prev_name);
    if (prev_idx < 0)
        return BOEBTNOCOL;
    int idx = hrchy_index_.getIndex(new_name);
    if (idx >= 0)
        return BOEBTCOLEXIST;
    hrchy_info_[prev_idx].name = new_name;
    hrchy_index_.clear();
    hrchy_index_.setInfo(hrchy_info_);
    MMapObject::changeColumnName(prev_idx, new_name);
    ext_schema_ = getExternalSchema(hrchy_info_);

    return 0;
}

string MMapBigTable::description() const { return MMapObject::description(); }

size_t MMapBigTable::size() const { return MMapObject::numNode(); }

size_t MMapBigTable::actualSize() const { return _actualSize(); }

size_t MMapBigTable::correctActualSize() {
  if (deleted_col_ < 0) {
    _actualSize() = MMapObject::numNode();
  } else {
    table_partition tp;
    if (is_circular(type())) {
      size_t start = _startIndex();
      size_t end = _endIndex();
      // ... endIndex() ---> startIndex()...
      if (start > end) {
        tp.push_back(std::move(make_pair(end, start)));
      } else {
        // 1 --> startIndex() ... endIndex() --> (reservedSize() - 1)
        tp.push_back(std::move(make_pair(1, start)));
        tp.push_back(std::move(make_pair(end, _reservedSize())));
      }
    } else {
      tp.push_back(std::move(make_pair(1, size() + 1)));
    }
    size_t act_sz = 0;
    for (size_t p = 0; p < tp.size(); ++p) {
      for (size_t r = tp[p].first; r < tp[p].second; ++r) {
        if (*((int8_t *) getColumnAddr(r, deleted_col_)) != 0)
          act_sz++;
//        else
//          BO_DEBUG(cout, r);
      }
    }
    _actualSize() = act_sz;
  }

  return _actualSize();
}



void MMapBigTable::setDataHelperVarString() {
  // set VARSTRING files in DataHelper
  for (size_t i = 0; i < vso_.size(); ++i) {
    if (vso_[i].sf) {
      rec_helper_.setStringFile(i, (void *)(vso_[i].sf));
    }
  }
  for (size_t i = 0; i < key_idx_.size(); ++i) {
    size_t key_idx = key_idx_[i];
    if (vso_[key_idx].sf)
      key_helper_.setStringFile(i, (void *)(vso_[key_idx].sf));
  }
}



size_t MMapBigTable::capacity() const { return MMapObject::_reservedSize(); }

bool MMapBigTable::isDeleted() const { return meta_data_->is_deleted; }

void MMapBigTable::setDeleted(bool is_deleted)
{ meta_data_->is_deleted = is_deleted; }

size_t MMapBigTable::bound() const { return MMapObject::timeBound(); }

size_t MMapBigTable::endIndex() const  { return MMapObject::_endIndex(); }

size_t MMapBigTable::startIndex() const  { return MMapObject::_startIndex(); }

size_t MMapBigTable::reservedSize() const  { return _reservedSize(); }

int MMapBigTable::numColumn() const { return meta_data_->level; }

int MMapBigTable::numVarStrColumn() const {return meta_data_->num_varstr_cols;}

int MMapBigTable::numKeyColumn() const { return meta_data_->num_key_info; }

const string & MMapBigTable::tbl_sub_path() const { return tbl_sub_path_; }

int MMapBigTable::version() const { return meta_data_->version; }

void MMapBigTable::incVersion() { meta_data_->version++; }

int MMapBigTable::permission() const { return meta_data_->permission; }

void MMapBigTable::setPermission(int perms) { meta_data_->permission = perms; }

string MMapBigTable::URL() const
{ return filePath(); }

string MMapBigTable::link() const
{ return MMapObject::linkURL(); }

string MMapBigTable::nameServiceURL() const
{ return MMapObject::nameServiceURL(); }

vector<string> MMapBigTable::rank() const
{ return MMapObject::hierarchyAttribute(); }

const vector<AttributeInfo> & MMapBigTable::getSchemaInfo() const
{ return hrchy_info_; }

time_t MMapBigTable::createTime() const { return MMapObject::createTime(); }

uint64_t MMapBigTable::dataLength() const { return (uint64_t)fileLen(); }

string MMapBigTable::sourceURL() const
{ return MMapObject::sourceURL(); }


const AttributeInfo * MMapBigTable::getColumnInfo(int col_num) const {
  if (col_num >= 0 && col_num < (int) hrchy_info_.size()) {
    return &(hrchy_info_[col_num]);
  }
  return nullptr;
}

int MMapBigTable::getColumnInfo(int col_num, AttributeInfo &ainfo) const {
  if (col_num >= 0 && col_num < (int) hrchy_info_.size()) {
    ainfo = (hrchy_info_[col_num]);
    return col_num;
  }
  return -1;
}

int MMapBigTable::getColumnInfo(const string &name,
    AttributeInfo &ainfo) const
{
    int idx = getColumnNumber(name);
    if (idx >= 0) {
        ainfo = hrchy_info_[idx];
        return idx;
    }
    return -1;
}

int MMapBigTable::getColumnNumber(const string &attribute) const
{ return getIndex(hrchy_index_, attribute); }
//{
//    int col = hrchy_index_.getIndex(attribute);
//    if (col < 0) {
//        string dim = getDimension(attribute);
//        string attr = getAttribute(attribute);
//        if (dim == obj_name_) {
//            col = hrchy_index_.getIndex(attr);
//        }
//    }
//    return col;
//}

//int64_t MMapBigTable::getDistinctOfAttribute(int level) const
//{
//    return MMapObject::getDistinctOfAttribute(level);
//}


int MMapBigTable::encoding() const { return MMapObject::encoding(); }

int MMapBigTable::flags() const { return MMapObject::flags(); }

DataHelper * MMapBigTable::getRecordHelper()
{ return &rec_helper_; }

VarStringObject MMapBigTable::getVarStringObject(int col) const {
  if (col < 0) {
    VarStringObject vso;
    return vso;
  }
  return vso_[col];
}

VarStringObject MMapBigTable::getVarStringObject(const string &col_name) const {
  int idx = getColumnNumber(col_name);
  return getVarStringObject(idx);
}

MMapStringFile * MMapBigTable::getVarstringFile(int col) const
{ return vso_[col].sf; }

void MMapBigTable::updateColumnType(int col, int type) {
    MMapObject::updateAttributeInfoType(col, type);
    rec_helper_.updateType(col);
    ext_schema_[col + firstDataColumn()].type = type;
}

void MMapBigTable::updateColumnMaxLength(int col, int new_len) {
  MMapObject::updateAttributeInfoMaxLen(col, new_len);
  if (hrchy_info_[col].max_len < new_len) {
    hrchy_info_[col].max_len = new_len;
    rec_helper_.updateType(col);
  }

  if (ext_schema_[col + firstDataColumn()].max_len < new_len) {
    ext_schema_[col + firstDataColumn()].max_len = new_len;
  }
}

int MMapBigTable::setColumnFlag(int col, int flag) {
  hrchy_info_[col].setFlag(flag);
  setColumnInfo(col, hrchy_info_[col]);
  return 0;
}

uint32_t MMapBigTable::getSyncVersion()
{ return MMapObject::getSyncVersion(); }

void MMapBigTable::setSyncVersion(uint32_t sync_ver)
{ MMapObject::setSyncVersion(sync_ver); }

timestamp64 & MMapBigTable::minSrcTimestamp()
{ return MMapObject::minSrcTimestamp(); }

timestamp64 & MMapBigTable::maxSrcTimestamp()
{ return MMapObject::maxSrcTimestamp(); }

timestamp64 & MMapBigTable::minTimestamp()
{ return MMapObject::minTimestamp(); }

timestamp64 & MMapBigTable::maxTimestamp()
{ return MMapObject::maxTimestamp(); }

