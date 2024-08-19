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
#include "mmap/mmap_metrics_table.h"
#include "utils/big_table_utils.h"
#include "date_time_util.h"
#include "engine.h"
#include "utils/compress_utils.h"

extern void markDeleted(char* delete_flags, size_t row_index);

MMapMetricsTable::~MMapMetricsTable() {
  if (entity_block_idx_) {
    delete entity_block_idx_;
  }
}

impl_latch_virtual_func(MMapMetricsTable, &rw_latch_)

int MMapMetricsTable::open(const string& url, const std::string& db_path, const string& tbl_sub_path,
                           int flags, ErrorInfo& err_info) {
  string file_path = getURLFilePath(url);
  if ((err_info.errcode = TsTableObject::open(file_path, db_path, tbl_sub_path,
                                              magic(), flags)) < 0) {
    err_info.setError(err_info.errcode, tbl_sub_path + getURLFilePath(url));
    return err_info.errcode;
  }
  name_ = getURLObjectName(URL());
  if (metaDataLen() < (off_t) sizeof(TSTableFileMetadata)) {
    if (!(bt_file_.flags() & O_CREAT)) {
      err_info.setError(KWECORR, tbl_sub_path + getURLFilePath(url));
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
  if (init(schema, err_info) < 0)
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

int MMapMetricsTable::init(const vector<AttributeInfo>& schema, ErrorInfo& err_info) {
  err_info.errcode = initMetaData();
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  time(&meta_data_->create_time);

  name_ = getURLObjectName(URL());

  for (size_t i = 0 ; i < schema.size() ; ++i) {
    cols_info_with_hidden_.push_back(schema[i]);
  }

  if ((meta_data_->record_size = setAttributeInfo(cols_info_with_hidden_)) < 0) {
    return err_info.errcode;
  }
  off_t col_off = 0;
  col_off = addColumnInfo(cols_info_with_hidden_, err_info.errcode);
  if (err_info.errcode < 0) {
    return err_info.errcode;
  }
  assign(meta_data_->attribute_offset, col_off);

  meta_data_->cols_num = cols_info_with_hidden_.size();
  meta_data_->struct_type = (ST_VTREE | ST_NS_EXT);

  for (int i = 0; i < cols_info_with_hidden_.size(); ++i) {
    if(!cols_info_with_hidden_[i].isFlag(AINFO_DROPPED)) {
      cols_info_without_hidden_.emplace_back(cols_info_with_hidden_[i]);
      cols_idx_for_hidden_.emplace_back(i);
    }
  }

  return err_info.errcode;
}

int MMapMetricsTable::openInitEntityMeta(const int flags) {
  if (entity_block_idx_ == nullptr) {
    entity_block_idx_ = new MMapEntityIdx();
  }
  string meta_url = name_ + ".meta";
  int ret = entity_block_idx_->init(meta_url, db_path_, tbl_sub_path_, flags, true,
                                    CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
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
  if (entity_block_idx_ != nullptr) {
    error_code = entity_block_idx_->remove();
    if (error_code < 0) {
      return error_code;
    }
  }
  delete entity_block_idx_;
  entity_block_idx_ = nullptr;
  return bt_file_.remove();
}

void MMapMetricsTable::sync(int flags) {
  if (entity_block_idx_ != nullptr) {
    entity_block_idx_->sync(flags);
  }
  bt_file_.sync(flags);
}


int MMapMetricsTable::Sync(kwdbts::TS_LSN check_lsn, ErrorInfo& err_info) {
  sync(MS_SYNC);
  if (entity_block_idx_ != nullptr) {
    entity_block_idx_->sync(MS_SYNC);
  }
  return 0;
}

int MMapMetricsTable::Sync(kwdbts::TS_LSN check_lsn, map<uint32_t, uint64_t>& rows,
                           ErrorInfo& err_info) {
  if (entity_block_idx_ != nullptr) {
    entity_block_idx_->sync(MS_SYNC);
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
  for (int i = 0; i < cols_info_with_hidden_.size(); ++i) {
    if ((cols_info_with_hidden_[i].id == attr_info.id) && (!cols_info_with_hidden_[i].isFlag(AINFO_DROPPED))) {
      col_no = i;
      break;
    }
  }
  if (col_no >= 0) {
    auto &col_info = cols_info_with_hidden_[col_no];
    // set column dropped
    col_info.setFlag(AINFO_DROPPED);
    setColumnInfo(col_no, col_info);
    cols_info_without_hidden_.clear();
    cols_idx_for_hidden_.clear();
    // re-construct attributes information
    for (int i = 0; i < cols_info_with_hidden_.size(); ++i) {
      if (!cols_info_with_hidden_[i].isFlag(AINFO_DROPPED)) {
        cols_info_without_hidden_.emplace_back(cols_info_with_hidden_[i]);
        cols_idx_for_hidden_.emplace_back(i);
      }
    }
  } else {
    // column not found
    err_info.setError(KWENOATTR);
  }

  return 0;
}

int MMapMetricsTable::AlterColumnType(int col_index, const AttributeInfo& new_attr) {
  wrLock();
  Defer defer{[&]() {
    unLock();
  }};

  auto& col_info = cols_info_with_hidden_[col_index];
  if(col_info.id != new_attr.id) {
    return -1;
  }
  // Reset hrchy_info_ and actual_hrchy_info_ using new attribute information
  col_info.type = new_attr.type;
  col_info.size = new_attr.size;
  col_info.length = new_attr.length;
  col_info.max_len = new_attr.max_len;
  setColumnInfo(col_index, col_info);

  for (auto& attr : cols_info_without_hidden_) {
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
  if (!realFilePath().empty()) {
    err_code = ::rename(bt_file_.realFilePath().c_str(), new_fp.c_str());
    if (err_code != 0) {
      err_code = errnoToErrorCode();
      return err_code;
    }
    bt_file_.realFilePath() = new_fp;
    bt_file_.filePath() = file_path;
    name_ = getURLObjectName(bt_file_.filePath());
  }
  return err_code;
}

int MMapMetricsTable::UndoAddColumn(const uint32_t& col_id, ErrorInfo &err_info) {
  wrLock();
  Defer defer{[&]() {
    unLock();
  }};

  // Get column attribute infromation from hrchy_info_ and delete it
  for (auto iter = cols_info_with_hidden_.begin(); iter != cols_info_with_hidden_.end(); iter++) {
    if (iter->id == col_id) {
      cols_info_with_hidden_.erase(iter);
      break;
    }
  }

  // Recalculate record size
  if ((meta_data_->record_size = setAttributeInfo(cols_info_with_hidden_)) < 0) {
    return err_info.errcode;
  }
  off_t col_off = 0;
  // Write to disk
  col_off = addColumnInfo(cols_info_with_hidden_, err_info.errcode);
  if (err_info.errcode < 0) {
    return err_info.errcode;
  }
  assign(meta_data_->attribute_offset, col_off);

  // Re-construct meta data
  meta_data_->cols_num = cols_info_with_hidden_.size();
  meta_data_->struct_type = (ST_VTREE | ST_NS_EXT);

  cols_info_without_hidden_.clear();
  cols_idx_for_hidden_.clear();
  // Re-construct actual hrchy information
  for (int i = 0; i < cols_info_with_hidden_.size(); ++i) {
    if(!cols_info_with_hidden_[i].isFlag(AINFO_DROPPED)) {
      cols_info_without_hidden_.emplace_back(cols_info_with_hidden_[i]);
      cols_idx_for_hidden_.emplace_back(i);
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
  for (auto& attr_info : cols_info_with_hidden_) {
    if ((attr_info.id == col_id) && (attr_info.isFlag(AINFO_DROPPED))) {
      attr_info.unsetFlag(AINFO_DROPPED);
      break;
    }
  }
  cols_info_without_hidden_.clear();
  cols_idx_for_hidden_.clear();
  // Re-construct actual hrchy information
  for (int i = 0; i < cols_info_with_hidden_.size(); ++i) {
    if (!cols_info_with_hidden_[i].isFlag(AINFO_DROPPED)) {
      cols_info_without_hidden_.emplace_back(cols_info_with_hidden_[i]);
      cols_idx_for_hidden_.emplace_back(i);
    }
  }
  return 0;
}
