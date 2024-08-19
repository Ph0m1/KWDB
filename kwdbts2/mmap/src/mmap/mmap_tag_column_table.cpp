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

#include <sys/types.h>
#include <sys/mman.h>
#include <dirent.h>
#include <errno.h>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <assert.h>
#include "mmap/mmap_tag_column_table.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "utils/big_table_utils.h"
#include "date_time_util.h"
#include "utils/string_utils.h"
#include "sys_utils.h"
#include "data_value_handler.h"
#include "lt_rw_latch.h"
#include "cm_func.h"

uint32_t k_entity_group_id_size = 8;
uint32_t k_per_null_bitmap_size = 1;

TagColumn::TagColumn(int32_t idx, const TagInfo& attr) {
  m_attr_ = attr;
  m_idx_ = idx;
  m_str_file_ = nullptr;
  m_is_primary_tag_ = false;
  m_store_offset_ = 0;
  m_store_size_ = m_attr_.m_size;
  if (m_idx_ != -1) {
     m_store_size_ += k_per_null_bitmap_size;
  }
}
TagColumn::~TagColumn() {
  if (m_str_file_) {
    delete m_str_file_;
    m_str_file_ = nullptr;
  }
  munmap();
}
int TagColumn::open(const std::string& col_file_name, const std::string &db_path,
                    const std::string &dbname, int flags) {
  int error_code;
  if ((error_code = MMapFile::open(col_file_name, db_path + dbname + col_file_name, flags)) < 0) {
    LOG_ERROR("failed to open the tag file %s%s%s, errcode: %d",
      db_path.c_str(), dbname.c_str(), col_file_name.c_str(), error_code);
    return error_code;
  }
  if (m_attr_.m_data_type == VARSTRING || m_attr_.m_data_type == VARBINARY) {
    m_str_file_ = new MMapStringFile(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
    std::string col_str_file_name = col_file_name + ".s";
    if ((error_code = m_str_file_->open(col_str_file_name, db_path + dbname + col_str_file_name, flags)) < 0) {
      LOG_ERROR("failed to open the tag string file %s%s%s, errcode: %d",
        db_path.c_str(), dbname.c_str(), col_str_file_name.c_str(), error_code);
      return error_code;
    }
  }
  m_db_path_ = db_path;
  m_db_name_ = dbname;
  return 0;
}
uint32_t  TagColumn::avgeStringColumnLength(size_t n) {
  if (n != 0) {
    return static_cast<int>(m_str_file_->size() / n) + 1;
  }
  return m_attr_.m_size;
}

int TagColumn::extend(size_t old_record_count, size_t new_record_count) {
  int err_code = 0;
  if (m_is_primary_tag_) {
    return 0;
  }
  if ((err_code = mremap(sizeof(TagColumnMetaData) + new_record_count*m_store_size_)) < 0) {
    LOG_ERROR("failed to extend the tag file %s, new size is: %lu",
        file_path_.c_str(), new_record_count*m_store_size_);
    return err_code;
  }
  if (m_str_file_) {
    err_code = m_str_file_->reserve(old_record_count, new_record_count, avgeStringColumnLength(old_record_count));
  }
  return err_code;
}

int TagColumn::remove() {
  if (!mem_) {
    return 0;
  }

  setDrop();
  if (m_str_file_) {
    m_str_file_->remove();
    delete m_str_file_;
    m_str_file_ = nullptr;
  }
  MMapFile::remove();
  return 0;
}

int TagColumn::writeValue(size_t row, const char* data, uint32_t len) {
  if (m_str_file_) {
    // var tag
    size_t loc_str;
    if (m_attr_.m_data_type == DATATYPE::VARSTRING) {
      loc_str = m_str_file_->push_back_nolock(data, len);
    } else {
      // varbinary
      loc_str = m_str_file_->push_back_binary(data, len);
    }
    if (loc_str == 0) {
      LOG_ERROR("failed to write the tag file %s", file_path_.c_str());
      return -1;
    }
    memcpy(rowAddrNoNullBitmap(row), &loc_str, sizeof(uint64_t));
  } else {
    memcpy(rowAddrNoNullBitmap(row), data, m_attr_.m_size);
  }
  return 0;
}

int TagColumn::getColumnValue(size_t row,  void *data) const {
  if (m_str_file_) {
    size_t loc = *reinterpret_cast<size_t*>(rowAddrNoNullBitmap(row));
    char *rec_ptr = m_str_file_->getStringAddr(loc);
    uint16_t len = *reinterpret_cast<uint16_t*>(rec_ptr);
    memcpy(data, rec_ptr + MMapStringFile::kStringLenLen, len);
  } else {
    memcpy(data, rowAddrNoNullBitmap(row), m_attr_.m_size);
  }
  return 0;
}

int TagColumn::rename(std::string& new_col_file_name) {
  int err_code = 0;
  if (m_str_file_) {
     std::string new_str_file_name = new_col_file_name + ".s";
     size_t lastSep = new_col_file_name.find_last_of('.');
     if (lastSep != new_col_file_name.npos) {
       std::string suffix = new_col_file_name.substr(lastSep + 1);
       if (suffix == "bak") {
         new_str_file_name = new_col_file_name.substr(0, lastSep) + ".s.bak";
       }
     }

    std::string new_str_file_path = m_db_path_ + m_db_name_ + new_str_file_name;
    if ((m_str_file_->rename(new_str_file_path)) < 0) {
      LOG_ERROR("failed to rename the tag string file %s to %s",
        m_str_file_->realFilePath().c_str(), new_str_file_path.c_str());
      return -1;
    }
    if ((m_str_file_->open(new_str_file_name, m_db_path_ + m_db_name_ + new_str_file_name, MMAP_OPEN)) < 0) {
      LOG_ERROR("failed to open the tag string file %s after renaming",
        m_str_file_->realFilePath().c_str());
      return -1;
    }
  }

  std::string new_file_path = m_db_path_ + m_db_name_ + new_col_file_name;
  if ((err_code = MMapFile::rename(new_file_path)) < 0) {
    LOG_ERROR("failed to rename the tag file %s to %s",
      realFilePath().c_str(), new_file_path.c_str());
    return -1;
  }
  this->setFlags(MMAP_OPEN);
  this->file_path_ = new_col_file_name;
  if ((MMapFile::open()) < 0) {
    LOG_ERROR("failed to open the tag file %s after renaming",
      realFilePath().c_str());
    return -1;
  }
  return err_code;
}

int TagColumn::sync(int flags) {
  int err_code = 0;
  if (mem_) {
    if ((err_code = MMapFile::sync(flags)) < 0) {
      return err_code;
    }
  }
  if (m_str_file_) {
    if ((err_code = m_str_file_->sync(flags)) < 0) {
      return err_code;
    }
  }
  return err_code;
}

void TagColumn::writeNullVarOffset(size_t row) {
  if (m_str_file_) {
    // var tag
    m_str_file_->mutexLock();
    size_t loc_str = m_str_file_->size();
    m_str_file_->mutexUnlock();
    memcpy(rowAddrNoNullBitmap(row), &loc_str, sizeof(uint64_t));
  }
  return;
}

MMapTagColumnTable::MMapTagColumnTable() {
  enableWal_ = false;
  m_tag_table_mutex_ = new KLatch(LATCH_ID_TAG_TABLE_MUTEX);
  m_tag_table_rw_lock_ = new KRWLatch(RWLATCH_ID_TAG_TABLE_RWLOCK);
  m_ref_cnt_mtx_ = new TagTableCntMutex(LATCH_ID_TAG_REF_COUNT_MUTEX);
  m_ref_cnt_cv_  = new TagTableCondVal(COND_ID_TAG_REF_COUNT_COND);
}
MMapTagColumnTable::~MMapTagColumnTable() {
  delete m_tag_table_rw_lock_;
  delete m_tag_table_mutex_;
  delete m_ref_cnt_mtx_;
  delete m_ref_cnt_cv_;
  m_ref_cnt_cv_ = nullptr;
  m_ref_cnt_mtx_ = nullptr;
  m_tag_table_mutex_ = nullptr;
  m_tag_table_rw_lock_ = nullptr;
  if (!m_ptag_file_ || m_ptag_file_->memAddr() == nullptr) {
    return;
  }

  delete  m_bitmap_file_;
  delete  m_index_;
  delete  m_meta_file_;
  m_bitmap_file_ = nullptr;
  m_index_ = nullptr;
  m_meta_file_ = nullptr;
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (m_cols_[i] != nullptr) {
      delete m_cols_[i];
      m_cols_[i] = nullptr;
    }
  }
  delete m_ptag_file_;
  m_ptag_file_ = nullptr;
}

int MMapTagColumnTable::initMetaData(ErrorInfo &err_info) {
  // check if need read backup file
  std::string file_name = m_name_ + ".mt";

  // Only do normal open, recovery do something to bring into alignment.
#if 0
  std::string real_file_path = db_path_ + tbl_sub_path_ + file_name + ".bak";
  bool is_need_recovery = (access(real_file_path.c_str(), F_OK) == 0) ? true : false;
  if (is_need_recovery) {
    file_name = m_name_ + ".mt" + ".bak";
  }
#endif

  TagInfo ainfo = {0x00};
  ainfo.m_offset = 0;
  ainfo.m_size = kwdbts::EngineOptions::pageSize();
  m_meta_file_ = new TagColumn(-1, ainfo);
  err_info.errcode = m_meta_file_->open(file_name, m_db_path_, m_tbl_sub_path_, m_flags_);
  if (err_info.errcode < 0) {
    err_info.setError(err_info.errcode);
    LOG_ERROR("failed to open the tag meta file %s%s, error: %s",
      m_db_name_.c_str(), file_name.c_str(), err_info.errmsg.c_str());
    delete m_meta_file_;
    m_meta_file_ = nullptr;
    return -1;
  }
  if (!m_meta_file_->isInited()) {
    if ((err_info.errcode = m_meta_file_->extend(0, 1)) < 0) {
      err_info.setError(err_info.errcode);
      LOG_ERROR("failed to extend the meta file %s%s, error: %s",
        m_db_name_.c_str(), file_name.c_str(), err_info.errmsg.c_str());
      delete m_meta_file_;
      return -1;
    }
  }
  setMetaData();

#if 0
  if (is_need_recovery) {
    std::string meta_file_name = m_name_ + ".mt";
    std::string real_file_path = db_path_ + tbl_sub_path_ + meta_file_name;
    ::remove(real_file_path.c_str());
    m_meta_file_->rename(meta_file_name);
  }
#endif

  return err_info.errcode;
}

int MMapTagColumnTable::open_(const string &url, const std::string &db_path,
                              const string &tbl_sub_path, int flags, ErrorInfo &err_info) {
  string file_path = getURLFilePath(url);
  m_ptag_file_ = new MMapFile();
  int error_code = m_ptag_file_->open(file_path, db_path + tbl_sub_path + file_path, flags);
  if (error_code < 0) {
    err_info.setError(error_code);
    LOG_ERROR("failed to open the tag file %s%s, error: %s",
              tbl_sub_path.c_str(), file_path.c_str(), err_info.errmsg.c_str());
    return error_code;
  }
  // set members
  m_db_path_ = db_path;
  m_flags_ = flags;
  m_tbl_sub_path_ = tbl_sub_path;
  m_name_ =  getURLObjectName(url);
  // open init
  if ((error_code = initMetaData(err_info)) < 0) {
    return error_code;
  }
  uint32_t check_code = *reinterpret_cast<const uint32_t*>("MMTT");
  if (m_meta_data_->m_magic != check_code) {
    err_info.errcode = -1;
    err_info.errmsg = "tag magic check failed.";
    LOG_ERROR("failed to check megic for the tag table %s%s, expect %u, "
      "but %u in fact",
              tbl_sub_path.c_str(), m_name_.c_str(), check_code, m_meta_data_->m_magic);
    return -1;
  }
  if (m_ptag_file_->fileLen() > metaDataSize()) {
    if ((error_code = readColumnInfo(err_info)) < 0) {
      err_info.setError(error_code);
      return error_code;
    }
  }
  setObjectReady();
  LOG_DEBUG("open the tag table %s%s successfully", tbl_sub_path.c_str(), m_name_.c_str());
  return 0;
}

int MMapTagColumnTable::create_mmap_file(const string& url, const std::string& db_path,
                                         const string& tbl_sub_path, int flags, ErrorInfo& err_info) {
  // create file + mremap
  std::string file_name = getURLFilePath(url);
  m_name_ = getURLObjectName(url);
  m_ptag_file_ = new MMapFile();
  int error_code = m_ptag_file_->open(file_name, db_path + tbl_sub_path + file_name, flags, metaDataSize(), err_info);
  if (error_code < 0) {
    err_info.setError(error_code);
    delete m_ptag_file_;
    m_ptag_file_ = nullptr;
    LOG_ERROR("failed to create the tag file %s%s, error: %s",
              tbl_sub_path.c_str(), m_name_.c_str(), err_info.errmsg.c_str());
    return error_code;
  }
  // set members
  m_db_path_ = db_path;
  m_flags_ = flags;
  m_tbl_sub_path_ = tbl_sub_path;
  if ((error_code = initMetaData(err_info)) < 0) {
    m_ptag_file_->remove();
    delete m_ptag_file_;
    m_ptag_file_ = nullptr;
    err_info.setError(error_code);
    return error_code;
  }

  return err_info.errcode;
}

int MMapTagColumnTable::initBitMapColumn(ErrorInfo& err_info) {
  std::string bitmap_file_name = m_name_ + ".header";
  TagInfo ainfo = {0x00};
  ainfo.m_offset = 0;
  // ainfo.m_size =  1 + (m_cols_.size() + 7)/8;
  ainfo.m_size = 1;  // delete mark
  m_bitmap_file_ = new TagColumn(-1, ainfo);
  err_info.errcode = m_bitmap_file_->open(bitmap_file_name, m_db_path_, m_tbl_sub_path_, m_flags_);
  if (err_info.errcode < 0) {
    delete m_bitmap_file_;
    m_bitmap_file_ = nullptr;
    err_info.errmsg = "initBitMapColumn failed.";
    LOG_ERROR("failed to open the bitmap file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), bitmap_file_name.c_str(), err_info.errmsg.c_str());
  }
  return err_info.errcode;
}

int MMapTagColumnTable::initIndex(ErrorInfo& err_info) {
  string index_name = m_name_ + ".ht";
  m_index_ = new MMapHashIndex();
  err_info.errcode = m_index_->open(index_name, m_db_path_, m_tbl_sub_path_, m_flags_, err_info);
  if (err_info.errcode < 0) {
    delete m_index_;
    m_index_ = nullptr;
    err_info.errmsg = "initIndex failed.";
    LOG_ERROR("failed to open the tag hash index file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), index_name.c_str(), err_info.errmsg.c_str())
    return err_info.errcode;
  }
  m_index_->init(m_meta_data_->m_primary_tag_size);
  return err_info.errcode;
}

int MMapTagColumnTable::readColumnInfo(ErrorInfo& err_info) {
  TagColumn* tag_col = nullptr;
  uint32_t store_offset = 0;
  TagInfo* cols = reinterpret_cast<TagInfo*>(static_cast<uint8_t*>(m_meta_file_->startAddr()) +
                                             m_meta_data_->m_column_info_offset);
  for (int idx = 0; idx < m_meta_data_->m_column_count; ++idx) {
    TagInfo ainfo;
    memcpy(&ainfo, &(cols[idx]), sizeof(TagInfo));
    tag_col = new TagColumn(idx, ainfo);
    if (!tag_col) {
      LOG_ERROR("failed to new TagColumn object for the tag table %s%s",
        m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
    if (tag_col->attributeInfo().m_tag_type == GENERAL_TAG) {
      if (tag_col->open(m_name_ + "." + std::to_string(ainfo.m_id),
                                      m_db_path_, m_db_name_, m_flags_) < 0) {
	    LOG_WARN("faild to open the tag %s%s%s(%u), retry open it after recovery",
               m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str(), ainfo.m_id);
	  delete tag_col;
	  continue;
      }
    } else if (tag_col->attributeInfo().m_tag_type == PRIMARY_TAG) {
      tag_col->setPrimaryTag(true);
      tag_col->setStoreOffset(store_offset);
      store_offset += tag_col->attributeInfo().m_size;
    }
    m_cols_.push_back(std::move(tag_col));
  }
  err_info.errcode = initBitMapColumn(err_info);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  err_info.errcode = initIndex(err_info);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  return 0;
}

int MMapTagColumnTable::initColumn(const std::vector<TagInfo>& schema, ErrorInfo& err_info) {
  uint32_t rec_size = 0;
  m_cols_.reserve(schema.size());
  TagColumn* tag_col = nullptr;
  int error_code;
  m_meta_data_->m_primary_tag_store_size = rec_size = k_entity_group_id_size;
  uint32_t col_offset = 0;
  uint32_t store_offset = 0;
  for (size_t idx = 0; idx < schema.size(); ++idx) {
    if (schema[idx].m_tag_type != GENERAL_TAG && schema[idx].m_tag_type != PRIMARY_TAG) {
      LOG_ERROR("invalid tag type, expect %d or %d, but %d in fact about the "
        " tag table %s%s",
        GENERAL_TAG, PRIMARY_TAG, schema[idx].m_tag_type, m_db_name_.c_str(),
        m_name_.c_str());
      // TODO(zhuderun): add errorcode
      err_info.errmsg = "tag type is invalid";
      return -1;
    }
    tag_col = new TagColumn(idx, schema[idx]);
    if (!tag_col) {
      LOG_ERROR("faild to new TagColumn object");
      return -1;
    }
    // tag_col->attributeInfo().m_size = getDataTypeSize(tag_col->attributeInfo().m_data_type);
    if (tag_col->attributeInfo().m_data_type == VARSTRING ||
        tag_col->attributeInfo().m_data_type == DATATYPE::VARBINARY) {
      tag_col->attributeInfo().m_size = sizeof(intptr_t);
    }

    if (tag_col->attributeInfo().m_tag_type == PRIMARY_TAG) {
      tag_col->setPrimaryTag(true);
      tag_col->setStoreOffset(store_offset);
      if (tag_col->attributeInfo().m_data_type == VARSTRING) {
        tag_col->attributeInfo().m_size = tag_col->attributeInfo().m_length;
      }
      store_offset += tag_col->attributeInfo().m_size;
      m_meta_data_->m_primary_tag_size += tag_col->attributeInfo().m_size;
      m_meta_data_->m_primary_tag_store_size += tag_col->attributeInfo().m_size;
    } else if (tag_col->attributeInfo().m_tag_type == GENERAL_TAG) {
      if ((error_code = tag_col->open(m_name_ + "." + std::to_string(schema[idx].m_id),
                                      m_db_path_, m_db_name_, m_flags_)) < 0) {
        LOG_ERROR("failed to open the tag file %s, error_code: %d",
          tag_col->filePath().c_str(), error_code);
        return error_code;
      }
    }
    tag_col->attributeInfo().m_offset = col_offset;
    rec_size += tag_col->attributeInfo().m_size;
    col_offset += tag_col->attributeInfo().m_size;
    m_cols_.push_back(std::move(tag_col));
  }
  m_meta_data_->m_record_store_size = rec_size;
  m_meta_data_->m_record_size = (rec_size - k_entity_group_id_size);
  return 0;
}


int MMapTagColumnTable::writeColumnInfo(uint64_t start_offset, const std::vector<TagColumn*>& tag_schemas) {
  uint64_t len = tag_schemas.size() * sizeof(TagInfo);
  uint64_t new_mem_len = start_offset + len;
  if (new_mem_len > kwdbts::EngineOptions::pageSize()) {
    //TODO(zhuderun): error
    LOG_ERROR("failed to write tag info for the tag table %s%s, new memory "
      "length %lu > pageSize(%lu bytes), [new_mem_len=start_offset: %lu + tags: "
      "%lu * sizeof(Taginfo): %lu ]",
      m_db_name_.c_str(), m_name_.c_str(),
      new_mem_len, kwdbts::EngineOptions::pageSize(),
      start_offset, tag_schemas.size(), sizeof(TagInfo));
    return -1;
  }
  if (m_meta_file_->fileLen() < new_mem_len) {
    size_t old_row_count = m_meta_file_->fileLen() / kwdbts::EngineOptions::pageSize();
    size_t new_row_count = (getPageOffset(new_mem_len)) / kwdbts::EngineOptions::pageSize();
    int err_code = m_meta_file_->extend(old_row_count, new_row_count);
    if (err_code < 0) {
      LOG_ERROR("failed to extend the meta file %s",
        m_meta_file_->filePath().c_str());
      return err_code;
    }
    setMetaData();
  }
  TagInfo* col_attr = reinterpret_cast<TagInfo*>(static_cast<uint8_t*>(m_meta_file_->startAddr()) + start_offset);
  for (size_t i = 0; i < tag_schemas.size(); ++i) {
    memcpy(&(col_attr[i]), &(tag_schemas[i]->attributeInfo()), sizeof(TagInfo));
  }
  return 0;
}

int MMapTagColumnTable::init(const vector<TagInfo>& schema, ErrorInfo& err_info) {
  // err_info.errcode = initMetaData();
  // if (err_info.errcode < 0) {
  //  return err_info.setError(err_info.errcode);
  // }
  // initColumn
  err_info.errcode = initColumn(schema, err_info);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  m_meta_data_->m_header_size = 1 + ((m_cols_.size() + 7) / 8);  //  1 + [null bit map]
  m_meta_data_->m_bitmap_size = ((m_cols_.size() + 7) / 8);
  // initBitmap
  err_info.errcode = initBitMapColumn(err_info);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  // initIndex
  err_info.errcode = initIndex(err_info);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }

  m_meta_data_->m_record_store_size += m_meta_data_->m_header_size;
  m_meta_data_->m_record_size += m_meta_data_->m_bitmap_size;
  m_meta_data_->m_column_count = m_cols_.size();

  m_meta_data_->m_column_info_offset = metaDataSize();  // sizeof(TagTableMeatData);

  err_info.errcode = writeColumnInfo(m_meta_data_->m_column_info_offset, m_cols_);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }

  return err_info.errcode;
}

int MMapTagColumnTable::create(const vector<TagInfo>& schema, int32_t entity_group_id, ErrorInfo& err_info) {
  // 1. create mmap file
  if (init(schema, err_info) < 0) {
    LOG_ERROR("failed to init the tag table %s%s, error: %s",
      m_db_name_.c_str(), m_name_.c_str(), err_info.errmsg.c_str());
    return err_info.errcode;
  }

  m_meta_data_->m_magic = *reinterpret_cast<const uint32_t*>("MMTT");
  m_meta_data_->m_record_start_offset = m_ptag_file_->fileLen();  // one page size
  m_meta_data_->m_entitygroup_id = entity_group_id;
  m_meta_data_->m_ts_version = 1;
  setObjectReady();
  err_info.errcode = reserve(1024, err_info);  // reserve space for row#0

  LOG_INFO("create the tag table %s%s successfully",
    m_db_name_.c_str(), m_name_.c_str());
  return err_info.errcode;
}

int MMapTagColumnTable::open(const string& url, const std::string& db_path,
                             const string& tbl_sub_path, int flags, ErrorInfo& err_info) {
  m_db_name_ = tbl_sub_path;

  if (flags & O_CREAT) {
    return create_mmap_file(url, db_path, tbl_sub_path, flags, err_info);
  }
  return open_(url, db_path, tbl_sub_path, flags, err_info);
}

int MMapTagColumnTable::remove() {
  int err_code = 0;
  
  if (!m_ptag_file_ || !m_ptag_file_->memAddr()) {
    return 0;
  }

  if (m_ptag_file_->memAddr()) {
    (reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr()))->m_droped = true;
  }

  for (size_t i = 0; i < m_cols_.size(); ++i) {
    m_cols_[i]->remove();
    delete m_cols_[i];
    m_cols_[i] = nullptr;
  }
  if (m_bitmap_file_) {
    m_bitmap_file_->remove();
    delete m_bitmap_file_;
    m_bitmap_file_ = nullptr;
  }
  if (m_index_) {
    m_index_->setDrop();
    m_index_->remove();
    delete m_index_;
    m_index_ = nullptr;
  }
  if (m_meta_file_) {
    m_meta_file_->remove();
    delete m_meta_file_;
    m_meta_file_ = nullptr;
  }
  // remove primary tags
  m_ptag_file_->remove();
  delete m_ptag_file_;
  m_ptag_file_ = nullptr;
  m_meta_data_ = nullptr;
  return err_code;
}

int MMapTagColumnTable::extend(size_t new_record_count, ErrorInfo& err_info) {
  int err_code = 0;
  off_t new_mem_len = new_record_count * m_meta_data_->m_primary_tag_store_size;
  if (m_ptag_file_->fileLen() < (m_meta_data_->m_record_start_offset + new_mem_len)) {
    err_code = m_ptag_file_->mremap(m_meta_data_->m_record_start_offset + new_mem_len);
    if (err_code < 0) {
      return err_code;
    }
  }
  return err_code;
}

TagColumn* MMapTagColumnTable::cloneMetaData(ErrorInfo& err_info) {
  std::string meta_file_name = m_name_ + ".mt" + ".bak";  // tempory file name
  TagInfo ainfo = {0x00};
  ainfo.m_offset = 0;
  ainfo.m_size = kwdbts::EngineOptions::pageSize();
  TagColumn* tmp_meta_file = new TagColumn(-1, ainfo);
  err_info.errcode = tmp_meta_file->open(meta_file_name, m_db_path_, m_tbl_sub_path_, m_flags_ | MMAP_CREATTRUNC);
  if (err_info.errcode < 0) {
    err_info.errmsg = "initMetaData failed.";
    LOG_ERROR("failed to open the tag meta file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), meta_file_name.c_str(), err_info.errmsg.c_str());
    return nullptr;
  }

  // extend backup file
  size_t new_row_count = m_meta_file_->fileLen() / kwdbts::EngineOptions::pageSize();
  if ((err_info.errcode = tmp_meta_file->extend(0, new_row_count)) < 0) {
    err_info.errmsg = "extend MetaData failed.";
    LOG_ERROR("failed to extend the meta file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), meta_file_name.c_str(), err_info.errmsg.c_str());
    return nullptr;
  }

  // copy data
  memmove(tmp_meta_file->memAddr(), m_meta_file_->memAddr(), m_meta_file_->fileLen());

  return tmp_meta_file;
}

void MMapTagColumnTable::push_back_primary(size_t r, const char* data) {
  // null bitmap
  for (int i = 0; i < m_cols_.size(); ++i) {
    if (m_cols_[i]->isPrimaryTag()) {
      continue;
    }
    set_null_bitmap(reinterpret_cast<unsigned char*>(header_(r)) + 1, i);
  }
  // primary
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]->isPrimaryTag()) {
      break;
    }
    memcpy(columnValueAddr(r, i),
           offsetAddr(data, m_cols_[i]->attributeInfo().m_offset),
           m_cols_[i]->attributeInfo().m_size);
  }
}

int MMapTagColumnTable::reserve(size_t n, ErrorInfo& err_info) {
  int err_code = 0;
  if (m_meta_data_ == nullptr) {
    LOG_ERROR("failed to get meta data for the tag table %s%s",
      m_db_name_.c_str(), m_name_.c_str());
    return -1;
  }

  setObjectStatus(OBJ_READY);
  startWrite();
  if (n < m_meta_data_->m_reserve_row_count) {
    stopWrite();
    return 0;
  }
  LOG_DEBUG("the tag table %s%s reserve new row_size: %lu ",
    m_db_name_.c_str(), m_name_.c_str(), n);
  // bitmap mremap
  err_code = m_bitmap_file_->extend(m_meta_data_->m_row_count, n);
  if (err_code < 0) {
    LOG_ERROR("failed to extend bitmap file for the tag table %s%s",
      m_db_name_.c_str(), m_name_.c_str());
    stopWrite();
    return err_code;
  }
  // hash index reserved
  err_code = m_index_->reserve(n);
  if (err_code < 0) {
    LOG_ERROR("failed to extend index file for the tag table %s%s",
      m_db_name_.c_str(), m_name_.c_str());
    stopWrite();
    return err_code;
  }
  // tagcolumn extend
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    err_code = m_cols_[i]->extend(m_meta_data_->m_row_count, n);
    if (err_code < 0) {
      LOG_ERROR("failed to extend column file for the tag table %s%s(%u)",
        m_db_name_.c_str(), m_name_.c_str(), m_cols_[i]->attributeInfo().m_id);
      stopWrite();
      return err_code;
    }
  }

  // primary tags extend
  err_code = this->extend(n, err_info);
  if (err_code < 0) {
      LOG_ERROR("failed to extend primary tags for the tag table %s%s",
        m_db_name_.c_str(), m_name_.c_str());
    stopWrite();
    return err_code;
  }

  m_meta_data_->m_reserve_row_count = n;
  err_info.errcode = err_code;
  stopWrite();
  return err_code;
}

int MMapTagColumnTable::insert(uint32_t entity_id, uint32_t subgroup_id, const char* rec) {
  size_t num_node;
  int err_code = 0;
  ErrorInfo err_info;

  if (!isValid()) {
    return KWENOOBJ;
  }
  mutexLock();
  // reserve 0 + last rows
  if (size() + 2 >= reserveRowCount()) {
    // if (ins_ref_cnt_ != 0)
    //   pthread_cond_wait(&obj_mtx_cv_, &obj_mutex_);
    if (ins_ref_cnt_ == 0) {
      err_code = reserve(reserveRowCount() * 2, err_info);
      if (err_code < 0) {
        mutexUnlock();
        return err_code;
      }
    } else {
      LOG_DEBUG("already resized");
    }
  }

  // ins_ref_cnt_++;
  ++m_meta_data_->m_row_count;
  num_node = m_meta_data_->m_row_count;

  startRead();
  mutexUnlock();

  // if (resize_ref_cnt == 1) {
  //   pthread_cond_broadcast(&obj_mtx_cv_);
  // }

  // put entity id
  push_back_entityid(num_node, entity_id, subgroup_id);

  // put tag table record
  if ((push_back(num_node, rec)) < 0) {
    stopRead();
    return -1;
  }

  // put index record
  err_code = m_index_->put(reinterpret_cast<char*>(record(num_node)),
                           m_meta_data_->m_primary_tag_store_size - k_entity_group_id_size,
                           num_node);
  if (err_code < 0) {
    stopRead();
    return err_code;
  }
  stopRead();
  mutexLock();
  ++m_meta_data_->m_valid_row_count;
  mutexUnlock();
  return err_code;
}

int MMapTagColumnTable::UpdateTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id, ErrorInfo& err_info) {
  TagTableRowID row = 0;
  // 1. delete
  TSSlice tmp_primary_tag = payload.GetPrimaryTag();
  row = m_index_->delete_data(tmp_primary_tag.data, tmp_primary_tag.len);
  if (row == 0) {
    // not found
    err_info.errmsg = "delete data not found";
    err_info.errcode = -1;
    return -1;
  }
  mutexLock();
  setDeleteMark(row);
  mutexUnlock();

  // 2. insert
  if ((err_info.errcode = InsertTagRecord(payload, sub_group_id, entity_id)) < 0 ) {
    err_info.errmsg = "insert tag data fail";
    return err_info.errcode;
  }
  return 0;
}

int MMapTagColumnTable::push_back(size_t r, const char* data) {
  // 1. direct write into bitmap
  // memcpy(header_(r) + 1, data, m_meta_data_->m_bitmap_size);  // skip del_mark

  // 2. write data
  const char* rec = (data + m_meta_data_->m_bitmap_size);
  int err_code = 0;
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    //if (IsNull(r, i)) {
    if (get_null_bitmap(reinterpret_cast<uchar*>((intptr_t) data), i)) {
      // col is NULL
      if (m_cols_[i]->isPrimaryTag()) {
        LOG_ERROR("the value to be written is null for primary tag of the tag "
          "table %s%s",
          m_db_name_.c_str(), m_name_.c_str());
        return -1;
      }
      m_cols_[i]->setNull(r);
      if (!m_cols_[i]->isPrimaryTag() && m_cols_[i]->isVarTag()) {
        m_cols_[i]->writeNullVarOffset(r);
      }
      continue;
    }
    if (!m_cols_[i]->isPrimaryTag()) {
      m_cols_[i]->setNotNull(r);
    }
    if (!m_cols_[i]->isPrimaryTag() && m_cols_[i]->isVarTag()) {
      // parse var column, relative to bitmap offset
      uint64_t tag_value_offset = *reinterpret_cast<uint64_t*>(offsetAddr(rec, m_cols_[i]->attributeInfo().m_offset));
      // get var len
      uint16_t tag_value_len = *reinterpret_cast<const uint16_t*>(data + tag_value_offset);

      err_code = m_cols_[i]->writeValue(r, (data + tag_value_offset + sizeof(tag_value_len)), tag_value_len);
      if (err_code < 0) {
        LOG_ERROR("failed to write the tag table %s%s(%u)",
          m_db_name_.c_str(), m_name_.c_str(), m_cols_[i]->attributeInfo().m_id);
        return -1;
      }
    } else {
      // fixed-len tag column
      memcpy(columnValueAddr(r, i),
             offsetAddr(rec, m_cols_[i]->attributeInfo().m_offset),
             m_cols_[i]->attributeInfo().m_size);
    }
  }  // end for
  return 0;
}

int MMapTagColumnTable::getColumnValue(size_t row, size_t col,
                                       void* data) const {
  if (m_cols_[col]->isPrimaryTag()) {
    memcpy(data, columnValueAddr(row, col), m_cols_[col]->attributeInfo().m_size);
  } else {
    m_cols_[col]->getColumnValue(row, data);
  }
  return 0;
}

void MMapTagColumnTable::setColumnValue(size_t row, size_t col, char* data) {
  memcpy(columnValueAddr(row, col), data, m_cols_[col]->attributeInfo().m_size);
}

int MMapTagColumnTable::getEntityIdGroupId(const char* primary_tag_val, int len,
                                           uint32_t& entity_id, uint32_t& group_id) {
  TagTableRowID row;
  // HashIndexData h_val;
  row = m_index_->get(primary_tag_val, len);
  if (row == 0) {
    // not found
    return -1;
  }
  startRead();
  char* rec_ptr = entityIdStoreAddr(row);
  memcpy(&entity_id, rec_ptr, sizeof(uint32_t));
  memcpy(&group_id, rec_ptr + sizeof(entity_id), sizeof(uint32_t));
  stopRead();
  return 0;
}

int MMapTagColumnTable::GetEntityIdList(const std::vector<void*>& primary_tags,
                                        const std::vector<uint32_t>& scan_tags,
                                        std::vector<kwdbts::EntityResultIndex>* entityIdList,
                                        kwdbts::ResultSet* res, uint32_t* count) {
  if (primary_tags.empty()) {
    // tag table scan
    return fullReadEntityId(scan_tags, entityIdList, res, count);
  } else {
    // primary tag list
    TagTableRowID row;
    uint32_t fetch_count = 0;
    startRead();
    for (int idx = 0; idx < primary_tags.size(); idx++) {
      // HashIndexData h_val;
      row = m_index_->get(reinterpret_cast<char*>(primary_tags[idx]), m_meta_data_->m_primary_tag_size);
      if (row == 0) {
        // not found
        continue;
      }
      getEntityIdByRownum(row, entityIdList);
      // tag column
      int err_code = 0;
      if ((err_code = getColumnsByRownum(row, scan_tags, res)) < 0) {
        stopRead();
        return err_code;
      }
      fetch_count++;
    }  // end for primary_tags
    stopRead();
    *count = fetch_count;
  }
  return 0;
}

int MMapTagColumnTable::getEntityIdByRownum(size_t row, std::vector<kwdbts::EntityResultIndex>* entityIdList) {
  uint32_t entity_id;
  uint32_t subgroup_id;
  char* record_ptr;
  record_ptr = entityIdStoreAddr(row);
  memcpy(&entity_id, record_ptr, sizeof(uint32_t));
  memcpy(&subgroup_id, record_ptr + sizeof(entity_id), sizeof(uint32_t));
  LOG_DEBUG("entityid: %u, groupid: %u", entity_id, subgroup_id);
  entityIdList->emplace_back(std::move(kwdbts::EntityResultIndex{m_meta_data_->m_entitygroup_id,
                                                                 entity_id,
                                                                 subgroup_id,
                                                                 record_ptr + k_entity_group_id_size}));
  return 0;
}

int MMapTagColumnTable::getColumnsByRownum(size_t row, const std::vector<uint32_t>& scan_tags, kwdbts::ResultSet* res) {
  if (res == nullptr) {
    return 0;
  }
  ErrorInfo err_info;
  res->setColumnNum(scan_tags.size());//TODO: make sure the scan_tags size
  for (int idx = 0; idx < scan_tags.size(); idx++) {
    // kwdbts::Batch* batch = new kwdbts::Batch(this->GetColumnAddr(row, tagid), 1, this->getBitmapAddr(row, tagid));
    kwdbts::Batch* batch = GenTagBatchRecord(this, row, row + 1, scan_tags[idx], err_info);
    if (!batch) {
      LOG_ERROR("getColumnsByRownum out of memory for the tag table %s%s",
        m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
    res->push_back(idx, batch);
  }  // end fetch scan_tags
  return 0;
}

int MMapTagColumnTable::fullReadEntityId(const std::vector<uint32_t>& scan_tags,
                                         std::vector<kwdbts::EntityResultIndex>* entityIdList,
                                         kwdbts::ResultSet* res, uint32_t* count) {
  uint32_t fetch_count = 0;
  mutexLock();
  uint64_t full_count = actual_size();
  mutexUnlock();
  startRead();
  for (size_t row = 1; row <= full_count; row++) {
    if (!isValidRow(row)) {
      // printRecord(row, row);
      continue;
    }
    getEntityIdByRownum(row, entityIdList);
    getColumnsByRownum(row, scan_tags, res);
    fetch_count++;
  }
  stopRead();
  *count = fetch_count;
  return 0;
}

int MMapTagColumnTable::DeleteTagRecord(const char* primary, int len, ErrorInfo& err_info) {
  TagTableRowID row = 0;
  row = m_index_->delete_data(primary, len);
  if (row == 0) {
    // not found
    return -1;
  }
  mutexLock();
  setDeleteMark(row);
  mutexUnlock();
  return 0;
}

TagColumn* MMapTagColumnTable::addNewColumn(TagInfo& tag_schema, bool need_ext, ErrorInfo& err_info) {
  // 1. add new tag column
  TagColumn* tag_column;
  int err_code;
  tag_column = new TagColumn(tag_schema.m_id, tag_schema);
  if (!tag_column) {
    LOG_ERROR("failed to new TagColumn object");
    return nullptr;
  }
  if (tag_column->attributeInfo().m_data_type == VARSTRING ||
      tag_column->attributeInfo().m_data_type == VARBINARY) {
    tag_column->attributeInfo().m_size = sizeof(intptr_t);
  }
  std::string col_file_name = m_name_ + "." + std::to_string(tag_schema.m_id);
  if (need_ext) {
    col_file_name += ".0000001";
  }
  if ((err_code = tag_column->open(col_file_name,
                                   m_db_path_, m_db_name_, MMAP_CREAT_EXCL)) < 0) {
    LOG_ERROR("failed to open the tag file %s, error_code: %d",
      tag_column->filePath().c_str(), err_code);
    return nullptr;
  }

  // 2. extend tag column
  if ((err_code = tag_column->extend(0, reserveRowCount())) < 0) {
    LOG_ERROR("failed to extend the tag file %s",
      tag_column->filePath().c_str());
    tag_column->remove();
    delete tag_column;
    return nullptr;
  }

  // 3. set null flag
  /*if (!need_ext) {
    size_t row_count = size();
    for (size_t row = 1; row <= row_count; ++row) {
      tag_column->setNull(row);
      if (tag_column->isVarTag()) {
        tag_column->writeNullVarOffset(row);
      }
    }
  }*/
  tag_column->setFlags(MMAP_OPEN);

  return tag_column;
}

int MMapTagColumnTable::AddTagColumn(TagInfo& tag_schema, ErrorInfo& err_info) {
  if (!isValid()) {
    LOG_ERROR("adding new tag to an invalid tag table %s%s is not allowed",
      m_db_name_.c_str(), m_name_.c_str());
    err_info.errcode = KWENOOBJ;
    return KWENOOBJ;
  }
  int err_code = 0;
  mutexLock();    // mutex lock
  startWrite();
  // 1. add new tag
  TagColumn* tag_col = addNewColumn(tag_schema, false, err_info);
  if (!tag_col) {
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "add new column file failed.";
    return -1;
  }
  // 2. backup meta data
  TagColumn* backup_meta_file = cloneMetaData(err_info);
  if (backup_meta_file == nullptr) {
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "backup meta data file failed.";
    return -1;
  }

  // 3. set meta
  tag_col->attributeInfo().m_offset = m_cols_.back()->attributeInfo().m_offset + m_cols_.back()->attributeInfo().m_size;
  uint64_t start_offset = m_meta_data_->m_column_info_offset + m_meta_data_->m_column_count * sizeof(TagInfo);
  std::vector<TagColumn*> tag_schemas = {tag_col};
  if ((err_code = writeColumnInfo(start_offset, tag_schemas)) < 0) {
    LOG_ERROR("faild to write tag info for the tag table %s%s, no space",
      m_db_name_.c_str(), m_name_.c_str());
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "write tag info has no space";
    return err_code;
  }
  m_meta_data_->m_record_store_size += tag_col->attributeInfo().m_size;
  m_meta_data_->m_record_size += tag_col->attributeInfo().m_size;
  m_meta_data_->m_column_count++;
  m_cols_.push_back(std::move(tag_col));
  uint16_t new_bitmap_size = (7 + m_cols_.size()) / 8;
  if (new_bitmap_size != m_meta_data_->m_bitmap_size) {
    m_meta_data_->m_bitmap_size += 1;
    m_meta_data_->m_record_size += 1;
  }
  // 4. sync meta data
  m_meta_file_->sync(MS_SYNC);

  // 5. remove backup meta data file
// After the wal is complete, switch to setDrop mode.
#if 1
  backup_meta_file->setDrop();
#else
  backup_meta_file->remove();
#endif
  delete backup_meta_file;
  stopWrite();
  mutexUnlock();
  return 0;
}

int MMapTagColumnTable::DropTagColumn(TagInfo& tag_schema, ErrorInfo& err_info) {
  if (!isValid()) {
    LOG_ERROR("dropping tag to an invalid tag table %s%s is not allowed",
      m_db_name_.c_str(), m_name_.c_str());
    err_info.errcode = KWENOOBJ;
    return KWENOOBJ;
  }
  int err_code = 0;
  mutexLock();    // mutex lock
  startWrite();
  // 1. find tag col & change offset
  TagColumn* tag_col = nullptr;
  std::vector<TagColumn*> new_cols;
  uint32_t col_offset = 0;
  uint32_t rec_size = 0;
  for (std::vector<TagColumn*>::iterator it = m_cols_.begin(); it != m_cols_.end(); ++it) {
    if ((*it)->attributeInfo().m_id == tag_schema.m_id) {
      // found
      tag_col = *it;
      continue;
    }
    (*it)->attributeInfo().m_offset = col_offset;
    rec_size += (*it)->attributeInfo().m_size;
    col_offset += (*it)->attributeInfo().m_size;
    new_cols.emplace_back(std::move(*it));
  }
  if (!tag_col) {
    LOG_ERROR("failed to get tag %u in the tag table %s%s",
      tag_schema.m_id, m_db_name_.c_str(), m_name_.c_str());
    m_cols_.clear();
    m_cols_ = new_cols;
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "tag schema is not exist";
    return -1;
  }

  // 2. write column info
  // 2.1 backup meta data
  TagColumn* backup_meta_file = cloneMetaData(err_info);
  if (nullptr == backup_meta_file) {
    m_cols_.clear();
    m_cols_ = new_cols;
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "backup meta data failed.";
    return -1;
  }
  // 2.2 write data
  uint64_t start_offset = m_meta_data_->m_column_info_offset;
  if ((err_code = writeColumnInfo(start_offset, new_cols)) < 0) {
    LOG_ERROR("faild to write tag info for the tag table %s%s, no space",
      m_db_name_.c_str(), m_name_.c_str());
    m_cols_.clear();
    m_cols_ = new_cols;
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "write tag info has no space.";
    return err_code;
  }

  // 3. change metadata
  m_cols_.clear();
  m_cols_ = new_cols;
  m_meta_data_->m_record_store_size -= tag_col->attributeInfo().m_size;
  m_meta_data_->m_record_size -= tag_col->attributeInfo().m_size;
  m_meta_data_->m_column_count--;
  uint16_t new_bitmap_size = (7 + m_cols_.size()) / 8;
  if (new_bitmap_size != m_meta_data_->m_bitmap_size) {
    m_meta_data_->m_bitmap_size -= 1;
    m_meta_data_->m_record_size -= 1;
  }
  m_meta_file_->sync(MS_SYNC);

  // 4. rename tag file
  std::string new_file_name = tag_col->filePath() + ".bak";
  if ((err_code = tag_col->rename(new_file_name)) < 0) {
    LOG_ERROR("failed to rename tag file %s in the tag table %s%s",
      new_file_name.c_str(), m_db_name_.c_str(), m_name_.c_str());
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "tag file rename " +  new_file_name + " failed.";
    return -1;
  }

  // 5. remove tag file
// After the wal is complete, switch to setDrop mode.
#if 1
  tag_col->setDrop();
  backup_meta_file->setDrop();
#else
  if ((err_code = tag_col->remove()) < 0) {
    LOG_ERROR("failed to remote tag file %s in the tag table %s%s",
      tag_col->realFilePath().c_str(), m_db_name_.c_str(), m_name_.c_str());
  }
  if ((err_code = backup_meta_file->remove()) < 0) {
    LOG_ERROR("failed to remove tag meta file %s in the tag table %s%s",
      backup_meta_file->realFilePath().c_str(),
      m_db_name_.c_str(), m_name_.c_str());
  }
#endif

  delete tag_col;
  delete backup_meta_file;
  stopWrite();
  mutexUnlock();
  return err_code;
}

int MMapTagColumnTable::convertData(int32_t col, TagColumn* new_tag_col, CONVERT_DATA_FUNC convert_data,
                                    bool is_digit_data, ErrorInfo& err_info) {
  size_t row_count = size();
  TagColumn* old_tag_col = m_cols_[col];
  char dst_data[new_tag_col->attributeInfo().m_length + 1] = {0x00};
  char* rec_ptr = nullptr;
  int data_len;
  int err_code = 0;
  for (size_t row = 1; row <= row_count; ++row) {
    if (!isValidRow(row)) {
      // deleted tag
      continue;
    }
    if (old_tag_col->isNull(row)) {
      // null tag data
      new_tag_col->setNull(row);
      if (new_tag_col->isVarTag()) {
        new_tag_col->writeNullVarOffset(row);
      }
      continue;
    }
    new_tag_col->setNotNull(row);  // set not null
    if (is_digit_data) {
      data_len = err_code = convert_data(old_tag_col->rowAddrNoNullBitmap(row), dst_data, new_tag_col->attributeInfo().m_length);
      if (err_code < 0) {
        LOG_ERROR("failed to convert the tag column %u to new type %s "
          "at row %lu in the tag table %s%s, error_code: %d, out of range",
          new_tag_col->attributeInfo().m_id,
          getDataTypeName(new_tag_col->attributeInfo().m_data_type).c_str(),
          row, m_db_name_.c_str(), m_name_.c_str(), err_code);
        err_info.errmsg = "Out of range value length, at row:" + std::to_string(row);
        return err_code;
      }
      rec_ptr = dst_data;
      // data_len = new_tag_col->attributeInfo().m_length;
      // new_tag_col->writeValue(row, dst_data, new_tag_col->attributeInfo().m_length);
    } else {
      // string data,avoid copy
      if (old_tag_col->isVarTag()) {
        rec_ptr = old_tag_col->getVarValueAddr(row);
        data_len = *reinterpret_cast<uint16_t*>(rec_ptr);
        rec_ptr += MMapStringFile::kStringLenLen;
        // varchar convert to fix data
        if (old_tag_col->attributeInfo().m_data_type == DATATYPE::VARSTRING &&
            convert_data != nullptr) {
          err_code = convert_data(rec_ptr, dst_data, data_len - 1);
          if (err_code < 0) {
            LOG_ERROR("failed to convert the tag column %u to new type %s "
              "at row %lu in the tag table %s%s, error_code: %d, over defined length",
              new_tag_col->attributeInfo().m_id,
              getDataTypeName(new_tag_col->attributeInfo().m_data_type).c_str(),
              row, m_db_name_.c_str(), m_name_.c_str(), err_code);
            err_info.errmsg = "string convert to fixdata failed, at row: " + std::to_string(row);
            return err_code;
          }
          rec_ptr = dst_data;
          data_len = new_tag_col->attributeInfo().m_length;
        }
      } else {
        rec_ptr = old_tag_col->rowAddrNoNullBitmap(row);
        data_len = old_tag_col->attributeInfo().m_size;
        // char convert to varchar use real size
        if (old_tag_col->attributeInfo().m_data_type == DATATYPE::CHAR) {
          data_len = std::min(old_tag_col->attributeInfo().m_size, static_cast<uint32_t>(strlen(rec_ptr)));
        }
      }
    }
    new_tag_col->writeValue(row, rec_ptr, data_len);
    memset(dst_data, 0x00, new_tag_col->attributeInfo().m_length + 1);
  }
  return 0;
}

int MMapTagColumnTable::AlterTagType(TagInfo& old_tag_schema, TagInfo& new_tag_schema, ErrorInfo& err_info) {
  if (!isValid()) {
    LOG_ERROR("alter tag to an invalid tag table %s%s is not allowed",
      m_db_name_.c_str(), m_name_.c_str());
    err_info.errcode = KWENOOBJ;
    return KWENOOBJ;
  }
  int err_code = 0;
  // 1. check type
  bool is_digit_data;
  CONVERT_DATA_FUNC convert_data = getConvertFunc(old_tag_schema.m_data_type, new_tag_schema.m_data_type,
                                                 new_tag_schema.m_length, is_digit_data, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("conversion from type %s, length %u to type %s, length %u "
      "is not supported in the tag table %s%s",
      getDataTypeName(old_tag_schema.m_data_type).c_str(), old_tag_schema.m_length,
      getDataTypeName(new_tag_schema.m_data_type).c_str(), new_tag_schema.m_length,
      m_db_name_.c_str(), m_name_.c_str());
    return -1;
  }
  mutexLock();    // mutex lock
  startWrite();
  // 2. found && open new tag file
  TagColumn* old_tag_col = nullptr;
  int32_t col_idx = 0;
  for (const auto it : m_cols_) {
    if (it->attributeInfo().m_id == old_tag_schema.m_id &&
        it->attributeInfo().m_data_type == old_tag_schema.m_data_type) {
      old_tag_col = it;
      break;
    }
    ++col_idx;
  }
  if (!old_tag_col) {
    LOG_ERROR("failed to get tag %u in the tag table %s%s",
      old_tag_schema.m_id, m_db_name_.c_str(), m_name_.c_str());
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "tag id is invalid";
    return -1;
  }
  TagColumn* new_tag_col = addNewColumn(new_tag_schema, true, err_info);
  if (!new_tag_col) {
    LOG_ERROR("failed to add new column to the tag table %s%s",
      m_db_name_.c_str(), m_name_.c_str());
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "add new column failed";
    return -1;
  }

  // 3. convert data && write data
  if ((err_code = this->convertData(col_idx, new_tag_col, convert_data, is_digit_data, err_info)) < 0) {
    LOG_ERROR("failed to convert data in the tag table %s%s",
      m_db_name_.c_str(), m_name_.c_str());
    if (new_tag_col->remove() < 0) {
      LOG_WARN("failed to remove the tag file %s, please remove it manually",
        new_tag_col->realFilePath().c_str());
    }
    stopWrite();
    mutexUnlock();
    delete new_tag_col;
    return -1;
  }

  /*
    NOTE: previous operation failures do not need to do undo.
    Please refer AlterAlterRU
  */

  // 4. change metadata
  // 1) backup meta data file
  TagColumn* backup_meta_file = cloneMetaData(err_info);
  if (nullptr == backup_meta_file) {
    LOG_ERROR("failed to backup meta data of the tag table %s%s",
      m_db_name_.c_str(), m_name_.c_str());
    if (new_tag_col->remove() < 0) {
      LOG_WARN("failed to remove the the tag file %s, please remove it manually",
        new_tag_col->realFilePath().c_str());
    }
    delete new_tag_col;
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "backup meta data failed";
    return -1;
  }

  // 5. rename file old -> old.lsn , new->old
  std::string old_tag_file_name = old_tag_col->filePath();
  std::string old_tag_new_name = old_tag_file_name + ".bak";
  if ((err_code = old_tag_col->rename(old_tag_new_name)) < 0) {
    LOG_ERROR("failed to rename the tag file %s to %s ",
      old_tag_file_name.c_str(), old_tag_new_name.c_str());
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "rename " + old_tag_file_name + " to " + old_tag_new_name + " failed.";
    return -1;
  }
  std::string new_tag_file_name = new_tag_col->filePath();
  if ((err_code = new_tag_col->rename(old_tag_file_name)) < 0) {
    LOG_ERROR("failed to rename the tag file %s to %s",
      new_tag_file_name.c_str(), old_tag_file_name.c_str());
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "rename " + new_tag_file_name + " to " + old_tag_file_name + " failed.";
    return -1;
  }

  // 2) change meta data
  m_cols_[col_idx] = new_tag_col;
  uint32_t col_offset = 0;
  uint32_t rec_size = 0;
  for (auto it : m_cols_) {
    it->attributeInfo().m_offset = col_offset;
    col_offset += it->attributeInfo().m_size;
    rec_size += it->attributeInfo().m_size;
  }
  m_meta_data_->m_record_size += (new_tag_col->attributeInfo().m_size - old_tag_col->attributeInfo().m_size);
  m_meta_data_->m_record_store_size += (new_tag_col->attributeInfo().m_size - old_tag_col->attributeInfo().m_size);

  // 6. write column info
  uint64_t start_offset = m_meta_data_->m_column_info_offset;
  if ((err_code = writeColumnInfo(start_offset, m_cols_)) < 0) {
    stopWrite();
    mutexUnlock();
    err_info.errmsg = "write tag info has no space";
    return err_code;
  }
  m_meta_file_->sync(MS_SYNC);

  // 7. drop backup file
// After the wal is complete, switch to setDrop mode.
#if 1
  old_tag_col->setDrop();
  backup_meta_file->setDrop();
#else
  if ((err_code = old_tag_col->remove()) < 0) {
    LOG_WARN("failed to remove the the tag file %s, please remove it manually",
      old_tag_col->realFilePath().c_str());
  }
  if ((err_code = backup_meta_file->remove()) < 0) {
    LOG_WARN("failed to remove the tag meta file %s, please remove it manually",
      backup_meta_file->realFilePath().c_str());
  }
#endif

  delete old_tag_col;
  delete backup_meta_file;
  stopWrite();
  mutexUnlock();
  return err_code;
}

void toHexString(const char* rec_ptr, uint32_t len, std::string& ret_str) {
  for (uint32_t idx = 0; idx < len; idx++) {
    ret_str += std::to_string(rec_ptr[idx]);
    ret_str += " ";
  }
}

string MMapTagColumnTable::printRecord(size_t lhs, size_t rhs, bool with_header, bool sort_by_primary) {
  if (lhs > size()) {
    return "error: row(" + std::to_string(lhs) + ") out of range.";
  }
  // max width of output
  vector<size_t> max_width(m_cols_.size());
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    max_width[i] = 2;
  }

  vector<vector<string>> all;
  for (auto row = lhs; row <= std::min(rhs, size()); ++row) {
    if (!isValidRow(row)) {
      continue;
    }
    vector<std::string> record;
    for (size_t col = 0; col < m_cols_.size(); ++col) {
      // check null
      auto null = this->isNull(row, col);
      if (null) {
        record.emplace_back("NULL");
        if (max_width[col] < 6) {
          max_width[col] = 6;
        }
        continue;
      }

      // printable
      switch (m_cols_[col]->attributeInfo().m_data_type) {
        case DATATYPE::BOOL:
          record.emplace_back(std::to_string(*reinterpret_cast<bool*>(columnValueAddr(row, col))));
          break;
        case DATATYPE::BYTE:
        case DATATYPE::BINARY: {
          std::string tmp_str;
          toHexString(columnValueAddr(row, col), m_cols_[col]->attributeInfo().m_length, tmp_str);
          record.emplace_back(tmp_str);
        }
          break;
        case DATATYPE::INT8:
          record.emplace_back(std::to_string(*reinterpret_cast<uint8_t*>(columnValueAddr(row, col))));
          break;
        case DATATYPE::INT16:
          record.emplace_back(std::to_string(*reinterpret_cast<uint16_t*>(columnValueAddr(row, col))));
          break;
        case DATATYPE::INT32:
          record.emplace_back(std::to_string(*reinterpret_cast<uint32_t*>(columnValueAddr(row, col))));
          break;
        case DATATYPE::INT64:
          record.emplace_back(std::to_string(*reinterpret_cast<uint64_t*>(columnValueAddr(row, col))));
          break;
        case DATATYPE::FLOAT:
          record.emplace_back(std::to_string(*reinterpret_cast<float*>(columnValueAddr(row, col))));
          break;
        case DATATYPE::DOUBLE:
          record.emplace_back(std::to_string(*reinterpret_cast<double*>(columnValueAddr(row, col))));
          break;
        case DATATYPE::CHAR:
          record.emplace_back(reinterpret_cast<const char*>(columnValueAddr(row, col)));
          break;
        case DATATYPE::VARBINARY: {
          std::string tmp_str;
          char* var_ptr = m_cols_[col]->getVarValueAddr(row);
          int16_t var_len = *reinterpret_cast<int16_t*>(var_ptr);
          var_ptr += MMapStringFile::kStringLenLen;
          toHexString(var_ptr, var_len, tmp_str);
          record.emplace_back(tmp_str);
        }
          break;
        case DATATYPE::VARSTRING:
          if (m_cols_[col]->isPrimaryTag()) {
            auto str = reinterpret_cast<const char*>(columnValueAddr(row, col));
            record.emplace_back(str,
                                std::min(strlen(str), static_cast<size_t>(m_cols_[col]->attributeInfo().m_length)));
          } else {
            char* str = m_cols_[col]->getVarValueAddr(row);
            str += MMapStringFile::kStringLenLen;
            record.emplace_back(str);
          }
          break;
        case DATATYPE::TIMESTAMP:
          record.emplace_back(std::to_string(*reinterpret_cast<uint32_t*>(columnValueAddr(row, col))));
          break;
        case DATATYPE::TIMESTAMP64:
          record.emplace_back(std::to_string(*reinterpret_cast<uint64_t*>(columnValueAddr(row, col))));
          break;
        default:
          record.emplace_back("unsupport type");
      }
      if (max_width[col] < record[col].length() + 2) {
        max_width[col] = record[col].length() + 2;
      }
    }
    all.emplace_back(std::move(record));
  }

  // sort by primary
  if (sort_by_primary) {
    vector<size_t> pri_idx;
    for (size_t i = 0; i < m_cols_.size(); ++i) {
      if (m_cols_[i]->isPrimaryTag()) {
        pri_idx.emplace_back(i);
      }
    }
    std::sort(all.begin(), all.end(), [&pri_idx, this](const vector<string>& lhs, const vector<string>& rhs) {
      // find first element not equal
      size_t idx = 0;
      for (size_t i : pri_idx) {
        idx = i;
        if (lhs[i] != rhs[i]) {
          break;
        } else {
          continue;
        }
      }
      // compare with type
      switch (this->m_cols_[idx]->attributeInfo().m_data_type) {
        case DATATYPE::INT8:
        case DATATYPE::INT16:
        case DATATYPE::INT32:
        case DATATYPE::INT64:
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64: {
          char* p_end = nullptr;
          auto l = strtoll(lhs[idx].c_str(), &p_end, 10);
          if (p_end == lhs[idx].c_str()) break;
          auto r = strtoll(rhs[idx].c_str(), &p_end, 10);
          if (p_end == rhs[idx].c_str()) break;
          return l < r;
        }
        case DATATYPE::FLOAT:
        case DATATYPE::DOUBLE: {
          char* p_end = nullptr;
          auto l = strtod(lhs[idx].c_str(), &p_end);
          if (p_end == lhs[idx].c_str()) break;
          auto r = strtod(rhs[idx].c_str(), &p_end);
          if (p_end == rhs[idx].c_str()) break;
          return l < r;
        }
      }
      return lhs[idx] < rhs[idx];
    });
  }

  // format result
  string result("\n");

  // data result
  for (size_t i = 0; i < all.size(); ++i) {
    for (size_t j = 0; j < all[i].size(); ++j) {
      auto sz = max_width[j] - 1;
      result.append("| ");
      result.append(all[i][j]);
      sz -= all[i][j].length();
      for (; sz > 0; --sz) {
        result.append(" ");
      }
    }
    result.append("|\n");
  }
  return result;
}

MMapTagColumnTable* OpenTagTable(const std::string& db_path, const std::string& dir_path,
                                 uint64_t table_id, ErrorInfo& err_info) {
  // open tag table
  MMapTagColumnTable* tmp_bt = new MMapTagColumnTable();
  string table_name = nameToTagBigTableURL(std::to_string(table_id) + ".tag");
  int err_code = tmp_bt->open(table_name, db_path, dir_path, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode == KWENOOBJ) {
    // table not exists.
    LOG_WARN("the tag table %s%s does not exist", dir_path.c_str(), table_name.c_str());
    err_info.clear();
    delete tmp_bt;
    return nullptr;
  }
  if (err_code < 0) {
    // other errors
    LOG_ERROR("failed to open the tag table %s%s, error: %s",
      dir_path.c_str(), table_name.c_str(), err_info.errmsg.c_str());
    delete tmp_bt;
    return nullptr;
  }
  return tmp_bt;
}

MMapTagColumnTable* CreateTagTable(const std::vector<TagInfo>& tag_schema, const std::string& db_path,
                                   const std::string& dir_path, uint64_t table_id,
                                   int32_t entity_group_id, int flags, ErrorInfo& err_info) {
  // create tag table
  MMapTagColumnTable* tmp_bt = new MMapTagColumnTable();
  string table_name = nameToTagBigTableURL(std::to_string(table_id) + ".tag");
  if (tmp_bt->open(table_name, db_path, dir_path, MMAP_CREAT_EXCL, err_info) >= 0 ||
      err_info.errcode == KWECORR) {
    tmp_bt->create(tag_schema, entity_group_id, err_info);
  }
  if (err_info.errcode < 0) {
    LOG_ERROR("failed to create the tag table %s%s, error: %s",
        dir_path.c_str(), table_name.c_str(), err_info.errmsg.c_str());
    tmp_bt->setObjectReady();
    tmp_bt->remove();
    delete tmp_bt;
    tmp_bt = nullptr;
  }
  return tmp_bt;
}

int DropTagTable(MMapTagColumnTable* bt, ErrorInfo& err_info) {
  if (bt == nullptr) {
    err_info.setError(KWENOOBJ);
    return err_info.errcode;
  }
  bt->mutexLock();
  if ((reinterpret_cast<TSObject*>(bt))->getObjectStatus() == OBJ_READY) {
    // from now, user cannot query this obj, so lock can be released
    bt->setObjectStatus(OBJ_READY_TO_CHANGE);
    bt->mutexUnlock();
    // wait if object is used by other threads.
    MUTEX_LOCK(bt->m_ref_cnt_mtx_);
    while (bt->isUsed()) {
      KW_COND_WAIT(bt->m_ref_cnt_cv_, bt->m_ref_cnt_mtx_);
    }
    MUTEX_UNLOCK(bt->m_ref_cnt_mtx_);
    err_info.errcode = bt->remove();
  } else {
    bt->mutexUnlock();
  }
  return err_info.errcode;
}

kwdbts::Batch* GenTagBatchRecord(MMapTagColumnTable* bt, size_t start_row,
                                 size_t end_row, size_t col, ErrorInfo& err_info) {
  kwdbts::Batch* batch = nullptr;
  assert(end_row > start_row);
  // check tag index is valid
  if (UNLIKELY(col >= bt->getSchemaInfo().size())) {
    LOG_ERROR("failed to get tag(%lu) in the tag table %s%s, "
      "but the max tag index no is %lu",
      col, bt->sandbox().c_str(), bt->name().c_str(), bt->getSchemaInfo().size());
    err_info.errcode = -1;
    return nullptr;
  }
  size_t total_size = bt->getColumnSize(col) * (end_row - start_row);
  void* data = std::malloc(total_size);
  void* var_data = nullptr;
  if (UNLIKELY(data == nullptr)) {
    LOG_ERROR("failed to allocate memory for fetching tag(%lu) from the tag "
      "table %s%s, it takes about %lu bytes for %lu rows starting from %lu",
      col, bt->sandbox().c_str(), bt->name().c_str(), total_size,
      (end_row - start_row), start_row);
    err_info.errcode = -4;
    return nullptr;
  }
  std::memcpy(data, bt->getColumnAddr(start_row, col), total_size);
  if (bt->isVarTag(col)) {
    size_t var_start_offset = bt->getVarOffset(start_row, col);
    if (UNLIKELY(var_start_offset < MMapStringFile::startLoc())) {
      if (bt->isNull(start_row, col)) {
        var_start_offset = MMapStringFile::startLoc();
      } else {
        LOG_ERROR("invalid start offset for tag(%lu) in the tag tagle %s%s, "
          "start_offset: %lu, start_row: %lu, end_row: %lu, row_count: %lu, "
          "actual_size: %lu",
          col, bt->sandbox().c_str(), bt->name().c_str(), var_start_offset,
          start_row, end_row, bt->size(), bt->actual_size());
        goto error_exit;
      }
    }
    size_t var_end_offset = bt->getVarOffset(end_row, col);
    if (UNLIKELY(var_end_offset < var_start_offset)) {
      //  multi-process  var_end_offset maybe 0, not correct.
      if (var_end_offset == 0 && bt->isNull(end_row, col)) {
        var_end_offset = var_start_offset;
      } else {
        LOG_ERROR("invalid end offset for tag(%lu) in the tag tagle %s%s, "
          "start_offset: %lu, end_offset: %lu, start_row: %lu, end_row:%lu, "
          "row_count: %lu actual_size: %lu ",
          col, bt->sandbox().c_str(), bt->name().c_str(), var_start_offset,
          var_end_offset, start_row, end_row, bt->size(), bt->actual_size());
        goto error_exit;
      }
    }
    size_t var_len = (var_end_offset - var_start_offset);
    var_data = std::malloc(var_len + 1);  // avoid var_len == 0
    if (UNLIKELY(var_data == nullptr)) {
      LOG_ERROR("failed to allocate memory for fetching tag(%lu) from the tag "
        "table %s%s, it takes about %lu bytes for %lu rows starting from %lu",
        col, bt->sandbox().c_str(), bt->name().c_str(),
        (var_end_offset - var_start_offset), (end_row - start_row), start_row);
      goto error_exit;
    }
    // std::memcpy(var_data, bt->getColumnValueAddr(start_row, col), var_len);
    std::memcpy(var_data, bt->getColumnVarValueAddrByOffset(col, var_start_offset), var_len);
    batch = new(std::nothrow) kwdbts::TagBatch(data, (end_row - start_row), nullptr, var_start_offset, var_data);
  } else {
    batch = new(std::nothrow) kwdbts::TagBatch(data, (end_row - start_row), nullptr);
  }
  if (UNLIKELY(batch == nullptr)) {
    LOG_ERROR("failed to new TagBatch for fetching tag(%lu) from the tag table "
      "%s%s, start_row: %lu end_row: %lu, row_count: %lu, actual_size: %lu",
      col, bt->sandbox().c_str(), bt->name().c_str(), start_row, end_row,
      bt->size(), bt->actual_size());
      goto error_exit;
  }
  return batch;
error_exit:
  if (data != nullptr) {
    free(data);
  }
  if (var_data != nullptr) {
    free(var_data);
  }
  err_info.errcode = -4;
  return nullptr;
}
