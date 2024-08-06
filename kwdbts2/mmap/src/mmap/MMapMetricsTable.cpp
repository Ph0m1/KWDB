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


#include <cstdio>
#include <algorithm>
#include <cstring>
#include <atomic>
#include <sys/mman.h>
#include "cm_func.h"
#include "dirent.h"
#include "objcntl.h"
#include "mmap/MMapMetricsTable.h"
#include "BigObjectUtils.h"
#include "BigObjectApplication.h"
#include "DateTime.h"
#include "engine.h"
#include "utils/ObjectUtils.h"

extern void markDeleted(char* delete_flags, size_t row_index);

MMapMetricsTable::~MMapMetricsTable() {
  if (entity_meta_) {
    delete entity_meta_;
  }
}

impl_latch_virtual_func(MMapMetricsTable, &rw_latch_)

int MMapMetricsTable::open(const string& url, const std::string& db_path, const string& tbl_sub_path,
                           int flags, ErrorInfo& err_info) {
  string file_path = getURLFilePath(url);
  if ((err_info.errcode = TsTableMMapObject::open(file_path, db_path, tbl_sub_path,
                                                  magic(), flags)) < 0) {
    err_info.setError(err_info.errcode, tbl_sub_path + getURLFilePath(url));
    return err_info.errcode;
  }
  name_ = getURLObjectName(URL());
  if (memLen() < (off_t) sizeof(TSTableFileMetadata)) {
    if (!(flags_ & O_CREAT)) {
      err_info.setError(BOECORR, tbl_sub_path + getURLFilePath(url));
    }
    return err_info.errcode;
  }
  if (meta_data_->has_data) {
    // load EntityMeta
    if ((err_info.errcode = openInitEntityMeta(flags)) < 0) {
      err_info.setError(err_info.errcode, tbl_sub_path + getURLFilePath(url));
      return err_info.errcode;
    }
  }
  setObjectReady();
  return err_info.errcode;
}

int MMapMetricsTable::create(const vector<AttributeInfo>& schema,
                             const vector<string>& key, const string& key_order,
                             const string& ns_url,
                             const string& description,
                             const string& tbl_sub_path,
                             const string& source_url,
                             uint64_t partition_interval,
                             int encoding, ErrorInfo& err_info,
                             bool init_data) {
  if (init(schema, encoding, init_data, err_info) < 0)
    return err_info.errcode;

  meta_data_->magic = magic();
  meta_data_->struct_type |= (ST_COLUMN_TABLE);
  meta_data_->partition_interval = partition_interval;

  meta_data_->has_data = init_data;
  if (meta_data_->has_data) {
    if (err_info.errcode = openInitEntityMeta(MMAP_CREAT_EXCL) < 0) {
      err_info.setError(err_info.errcode, db_path_ + tbl_sub_path);
      return err_info.errcode;
    }
  }
  setObjectReady();

  return 0;
}

int MMapMetricsTable::init(const vector<AttributeInfo>& schema, int encoding, bool init_data, ErrorInfo& err_info) {
  err_info.errcode = initMetaData();
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  time(&meta_data_->create_time);

  name_ = getURLObjectName(URL());

  for (size_t i = 0 ; i < schema.size() ; ++i) {
    hrchy_info_.push_back(schema[i]);
    if (is_varstr_to_char(encoding) &&
        (hrchy_info_.back().type == VARSTRING)) {
      hrchy_info_.back().type = CHAR;
    }
    if (isWKBType(schema[i].type)) {
      switch (schema[i].type) {
        case WKB_POINT:
          hrchy_info_.back().type = POINT;
          break;
        case WKB_LINESTRING:
          hrchy_info_.back().type = LINESTRING;
          break;
        case WKB_POLYGON:
          hrchy_info_.back().type = POLYGON;
          break;
        case WKB_MULTIPOLYGON:
          hrchy_info_.back().type = MULTIPOLYGON;
          break;
        case WKB_GEOMCOLLECT:
          hrchy_info_.back().type = GEOMCOLLECT;
          break;
        case WKB_GEOMETRY:
          hrchy_info_.back().type = GEOMETRY;
          break;
      }
    }
  }
  hrchy_index_.clear();
  hrchy_index_.setInfo(hrchy_info_);
  meta_data_->encoding = encoding;
  if ((meta_data_->record_size = setAttributeInfo(hrchy_info_, encoding, err_info)) < 0) {
    return err_info.errcode;
  }
  off_t col_off = 0;
  col_off = writeColumnInfo(hrchy_info_, err_info.errcode);
  if (err_info.errcode < 0) {
    return err_info.errcode;
  }
  assign(meta_data_->attribute_offset, col_off);

  meta_data_->level = hrchy_info_.size();
  meta_data_->depth = meta_data_->level;
  meta_data_->struct_type = (ST_VTREE | ST_NS_EXT);

  for (int i = 0; i < hrchy_info_.size(); ++i) {
    if(!hrchy_info_[i].isFlag(AINFO_DROPPED)) {
      actual_hrchy_info_.emplace_back(hrchy_info_[i]);
      actual_cols_.emplace_back(i);
    }
  }

  return err_info.errcode;
}

int MMapMetricsTable::openInitEntityMeta(const int flags) {
  if (entity_meta_ == nullptr) {
    entity_meta_ = new MMapEntityMeta();
  }
  string meta_url = name_ + ".meta";
  int ret = entity_meta_->open(meta_url, db_path_, tbl_sub_path_, flags, true, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

string MMapMetricsTable::URL() const {
  return filePath();
}

int MMapMetricsTable::remove() {
  int error_code = 0;
  if (entity_meta_ != nullptr) {
    error_code = entity_meta_->remove();
    if (error_code < 0) {
      return error_code;
    }
  }
  delete entity_meta_;
  entity_meta_ = nullptr;
  return TsTableMMapObject::remove();
}

void MMapMetricsTable::sync(int flags) {
  if (entity_meta_ != nullptr) {
    entity_meta_->sync(flags);
  }
  MMapFile::sync(flags);
}


int MMapMetricsTable::Flush(kwdbts::TS_LSN check_lsn, ErrorInfo& err_info) {
  sync(MS_SYNC);
  if (entity_meta_ != nullptr) {
    entity_meta_->sync(MS_SYNC);
  }
  return 0;
}

int MMapMetricsTable::Flush(kwdbts::TS_LSN check_lsn, map<uint32_t, uint64_t>& rows,
                            ErrorInfo& err_info) {
  if (entity_meta_ != nullptr) {
    entity_meta_->sync(MS_SYNC);
  }
  return 0;
}

int MMapMetricsTable::UndoPut(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t hint_row, kwdbts::Payload* payload,
               ErrorInfo& err_info) {
  return 0;
}

int MMapMetricsTable::UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  return 0;
}

int MMapMetricsTable::DropColumn(const AttributeInfo& attr_info, ErrorInfo &err_info) {
  wrLock();
  Defer defer{[&]() {
    unLock();
  }};

  int col_no = -1;
  // Traversal and find the attribute information which we need and not set dropped.
  for (int i = 0; i < hrchy_info_.size(); ++i) {
    if ((hrchy_info_[i].id == attr_info.id) && (!hrchy_info_[i].isFlag(AINFO_DROPPED))) {
      col_no = i;
      break;
    }
  }
  if (col_no >= 0) {
    auto &col_info = hrchy_info_[col_no];
    // set column dropped
    col_info.setFlag(AINFO_DROPPED);
    setColumnInfo(col_no, col_info);
    actual_hrchy_info_.clear();
    actual_cols_.clear();
    // re-construct attributes information
    for (int i = 0; i < hrchy_info_.size(); ++i) {
      if (!hrchy_info_[i].isFlag(AINFO_DROPPED)) {
        actual_hrchy_info_.emplace_back(hrchy_info_[i]);
        actual_cols_.emplace_back(i);
      }
    }
  } else {
    // column not found
    err_info.setError(BOENOATTR);
  }

  return 0;
}

int MMapMetricsTable::AlterColumnType(int col_index, const AttributeInfo& new_attr) {
  wrLock();
  Defer defer{[&]() {
    unLock();
  }};

  auto& col_info = hrchy_info_[col_index];
  if(col_info.id != new_attr.id) {
    return -1;
  }
  // Reset hrchy_info_ and actual_hrchy_info_ using new attribute information
  col_info.type = new_attr.type;
  col_info.size = new_attr.size;
  col_info.length = new_attr.length;
  col_info.max_len = new_attr.max_len;
  setColumnInfo(col_index, col_info);

  for (auto& attr : actual_hrchy_info_) {
    if (attr.id == new_attr.id) {
      attr.type = new_attr.type;
      attr.size = new_attr.size;
      attr.length = new_attr.length;
      attr.max_len = new_attr.max_len;
      break;
    }
  }
  return 0;
}

int MMapMetricsTable::rename(const string& new_fp, const string& file_path) {
  int err_code = 0;
  if (!real_file_path_.empty()) {
    err_code = ::rename(real_file_path_.c_str(), new_fp.c_str());
    if (err_code != 0) {
      err_code = errnoToErrorCode();
      return err_code;
    }
    real_file_path_ = new_fp;
    file_path_ = file_path;
    name_ = getURLObjectName(file_path_);
  }
  return err_code;
}

int MMapMetricsTable::UndoAddColumn(const uint32_t& col_id, ErrorInfo &err_info) {
  wrLock();
  Defer defer{[&]() {
    unLock();
  }};

  // Get column attribute infromation from hrchy_info_ and delete it
  for (auto iter = hrchy_info_.begin(); iter != hrchy_info_.end(); iter++) {
    if (iter->id == col_id) {
      hrchy_info_.erase(iter);
      break;
    }
  }

  // Rebuild index
  hrchy_index_.clear();
  hrchy_index_.setInfo(hrchy_info_);

  // Recalculate record size
  if ((meta_data_->record_size = setAttributeInfo(hrchy_info_, meta_data_->encoding, err_info)) < 0) {
    return err_info.errcode;
  }
  off_t col_off = 0;
  // Write to disk
  col_off = writeColumnInfo(hrchy_info_, err_info.errcode);
  if (err_info.errcode < 0) {
    return err_info.errcode;
  }
  assign(meta_data_->attribute_offset, col_off);

  // Re-construct meta data
  meta_data_->level = hrchy_info_.size();
  meta_data_->depth = meta_data_->level;
  meta_data_->struct_type = (ST_VTREE | ST_NS_EXT);

  actual_hrchy_info_.clear();
  actual_cols_.clear();
  // Re-construct actual hrchy information
  for (int i = 0; i < hrchy_info_.size(); ++i) {
    if(!hrchy_info_[i].isFlag(AINFO_DROPPED)) {
      actual_hrchy_info_.emplace_back(hrchy_info_[i]);
      actual_cols_.emplace_back(i);
    }
  }
  return err_info.errcode;
}

int MMapMetricsTable::UndoDropColumn(const uint32_t& col_id, ErrorInfo &err_info) {
  wrLock();
  Defer defer{[&]() {
    unLock();
  }};

  // Get column and unset drop flag
  for (auto& attr_info : hrchy_info_) {
    if ((attr_info.id == col_id) && (attr_info.isFlag(AINFO_DROPPED))) {
      attr_info.unsetFlag(AINFO_DROPPED);
      break;
    }
  }
  actual_hrchy_info_.clear();
  actual_cols_.clear();
  // Re-construct actual hrchy information
  for (int i = 0; i < hrchy_info_.size(); ++i) {
    if (!hrchy_info_[i].isFlag(AINFO_DROPPED)) {
      actual_hrchy_info_.emplace_back(hrchy_info_[i]);
      actual_cols_.emplace_back(i);
    }
  }
  return 0;
}
