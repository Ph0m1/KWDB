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


#include "mmap/TSTableMMapObject.h"
#include "mmap/MMapEntityMeta.h"
#include "BigObjectUtils.h"
#include "BigObjectApplication.h"

TsTableMMapObject::TsTableMMapObject() {
  mem_length_ = 0;
  mem_vtree_ = mem_ns_ = mem_data_ = mem_data_ext_ = nullptr;
  meta_data_ = nullptr;
  is_ns_needed_ = false;
}

TsTableMMapObject::~TsTableMMapObject() {
}


void RecordHelper::setHelper(const vector<AttributeInfo> &attr_info, bool is_internal_data,
                             const string & time_format) {
  time_format_ = time_format;
  attr_info_ = &attr_info;

  to_str_handler_.clear();
  to_data_handler_.clear();
  data_size_ = 0;
  for (size_t i = 0; i < (*attr_info_).size(); ++i) {
    to_str_handler_.push_back(
        std::move(getDataToStringHandler((*attr_info_)[i], DICTIONARY)));

    to_data_handler_.push_back(
        std::move(getStringToDataHandler(nullptr, i, (*attr_info_)[i],
                                         (*attr_info_)[i].encoding, time_format_)));

    if (to_data_handler_.back()->isLatest()) {
      to_data_handler_[i]->setColumn(i);
    }

    data_size_ += (*attr_info_)[i].size;
  }
  if (is_internal_data) {
    internal_data_.reserve(data_size_);
    internal_data_.resize(data_size_);
    data_ = &(internal_data_[0]);
  }
}

int TsTableMMapObject::swap(TsTableMMapObject& rhs) {
  MMapFile::swap(rhs);
  std::swap(mem_length_, rhs.mem_length_);
  std::swap(meta_data_, rhs.meta_data_);
  std::swap(mem_vtree_, rhs.mem_vtree_);
  std::swap(mem_ns_, rhs.mem_ns_);
  std::swap(mem_data_, rhs.mem_data_);
  std::swap(mem_data_ext_, rhs.mem_data_ext_);
  std::swap(obj_name_, rhs.obj_name_);
  std::swap(hrchy_info_, rhs.hrchy_info_);
  std::swap(hrchy_index_, rhs.hrchy_index_);
  std::swap(is_ns_needed_, rhs.is_ns_needed_);
  return 0;
}


int TsTableMMapObject::open(const string& file_path, const std::string& db_path, const string& tbl_sub_path, int cc,
                            int flags) {
  int error_code = MMapFile::open(file_path, db_path, tbl_sub_path, flags);
  if (error_code < 0) {
    return error_code;
  }
  if (file_length_ >= (off_t) sizeof(TSTableFileMetadata)) {
    hrchy_info_.clear();
    // initialize all pointers first
    initSection();
    if (meta_data_->magic != cc) {
      return BOENOOBJ;
    }

    error_code = readColumnInfo(meta_data_->attribute_offset,
                                meta_data_->level, hrchy_info_);
    if (error_code < 0) {
      return error_code;
    }

    hrchy_index_.clear();
    hrchy_index_.setInfo(hrchy_info_);

    actual_hrchy_info_.clear();
    actual_cols_.clear();
    for (int i = 0; i < hrchy_info_.size(); ++i) {
      if(!hrchy_info_[i].isFlag(AINFO_DROPPED)) {
        actual_hrchy_info_.emplace_back(hrchy_info_[i]);
        actual_cols_.emplace_back(i);
      }
    }

    ErrorInfo err_info;

    mem_length_ = meta_data_->length;
  }
  if (mem_length_ == 0)
    mem_length_ = file_length_;

  obj_name_ = getURLObjectName(file_path_);

  return 0;
}

int TsTableMMapObject::close() {
  munmap();
  mem_length_ = 0;
  mem_vtree_ = mem_ns_ = mem_data_ = mem_data_ext_ = nullptr;
  meta_data_ = nullptr;
  return 0;
}

int TsTableMMapObject::initMetaData() {
  int err_code = 0;
  if (file_length_ < (off_t) sizeof(TSTableFileMetadata))
    err_code = memExtend(sizeof(TSTableFileMetadata));
  if (err_code < 0)
    return err_code;
  memset(meta_data_, 0, sizeof(TSTableFileMetadata));
  meta_data_->meta_data_length = sizeof(TSTableFileMetadata);
  meta_data_->schema_version = 1; // default: 1
  return 0;
}

void TsTableMMapObject::initSection() {
  meta_data_ = reinterpret_cast<TSTableFileMetadata*>(mem_);

//  if (meta_data_->struct_type & ST_VTREE) {
//    mem_vtree_ = (void*) addr(meta_data_->vtree_offset);
//  }

  if (meta_data_->struct_type & ST_NS) {
    mem_ns_ = reinterpret_cast<void*>(addr(meta_data_->ns_offset));
  }
//  if (meta_data_->struct_type & ST_DATA) {
//    mem_data_ = (void*) addr(meta_data_->data_offset);
//  }
//  if (meta_data_->struct_type & ST_SUPP) {
//    mem_supp_ = (void*) addr(meta_data_->supp_offset);
//  }
}

int TsTableMMapObject::memExtend(off_t offset, size_t ps) {
  off_t new_mem_len = mem_length_ + offset;
  if (file_length_ < new_mem_len) {
    int err_code = mremap(getPageOffset(new_mem_len, ps));
    if (err_code < 0)
      return err_code;
    initSection();
  } else {
    /// trim table
  }
  mem_length_ = new_mem_len;
  return 0;
}

off_t TsTableMMapObject::urlCopy(const string& source_url, off_t offset, size_t ps) {
  off_t len = source_url.size() + 1 + offset;
  off_t prev_len;
  off_t limit;
  int err_code = 0;

  limit = mem_length_;
//  if (meta_data_->struct_type & ST_VTREE)
//    limit = min(limit, meta_data_->vtree_offset);
  if (meta_data_->struct_type & ST_NS)
    limit = min(limit, meta_data_->ns_offset);
//  if (meta_data_->struct_type & ST_DATA)
//    limit = min(limit, meta_data_->data_offset);

  if (len + meta_data_->meta_data_length <= limit) {
    prev_len = meta_data_->meta_data_length;
    meta_data_->meta_data_length += len;
  } else {
    prev_len = mem_length_;
    err_code = memExtend(len, ps);
    if (err_code < 0) {
      memLen() = fileLen();
      return prev_len;
    }
    meta_data_->meta_data_length = mem_length_;
  }

  if (err_code >= 0)
    snprintf(reinterpret_cast<char*>(((intptr_t) addr(prev_len) + offset)), ps,
           "%s", source_url.c_str());

  return prev_len;
}

int TsTableMMapObject::readColumnInfo(off_t offset, int size, vector<AttributeInfo>& attr_info) {
  TsColumnInfo* cols = reinterpret_cast<TsColumnInfo*>(addr(offset));
  for (int i = 0 ; i < size ; ++i) {
    AttributeInfo ainfo;
    ainfo.id = cols[i].id;
    ainfo.name = string(cols[i].name);
    memcpy(&(ainfo.type), &(cols[i].type), AttrInfoTailSize);
    if (ainfo.type == STRING)
      is_ns_needed_ = true;
    attr_info.push_back(ainfo);
  }
  return 0;
}

void TsTableMMapObject::setColumnInfo(TsColumnInfo* col_attr, const AttributeInfo& a_info) {
  col_attr->id = a_info.id;
  strncpy(col_attr->name, a_info.name.c_str(), TSCOLUMNATTR_LEN);
  memcpy(&(col_attr->type), &(a_info.type), AttrInfoTailSize);
}

int TsTableMMapObject::getIndex(const AttributeInfoIndex& idx,
                                const string& col_name) const {
  int col = idx.getIndex(col_name);
  if (col < 0) {
    // col_name = obj.name
    string dim = getDimension(col_name);
    string attr = getAttribute(col_name);
    if (dim == obj_name_) {
      col = idx.getIndex(attr);
    }
  }
  return col;
}

void TsTableMMapObject::setColumnInfo(int i, const AttributeInfo& a_info) {
  TsColumnInfo* col_attr = reinterpret_cast<TsColumnInfo*>(addr(meta_data_->attribute_offset));
  setColumnInfo(&(col_attr[i]), a_info);
}

off_t TsTableMMapObject::writeColumnInfo(const vector<AttributeInfo>& attr_info,
                                         int& err_code) {
  off_t start_addr = memLen();
  off_t len = attr_info.size() * sizeof(TsColumnInfo);
  err_code = memExtend(len);
  if (err_code < 0) {
    memLen() = fileLen();
    return err_code;
  }
  meta_data_->meta_data_length += len;

  TsColumnInfo* col_attr = reinterpret_cast<TsColumnInfo*>(addr(start_addr));
  for (size_t i = 0 ; i < attr_info.size() ; ++i) {
    setColumnInfo(&(col_attr[i]), attr_info[i]);
  }

  return start_addr;
}

vector<string> TsTableMMapObject::rank() const {
  return AttributeInfoToString(hrchy_info_);
}

const vector<AttributeInfo>& TsTableMMapObject::hierarchyInfo() const {
  return hrchy_info_;
}

int TsTableMMapObject::getColumnIndex(const AttributeInfo& attr_info) {
  int col_no = -1;
  for (int i = 0; i < hrchy_info_.size(); ++i) {
    if ((hrchy_info_[i].id == attr_info.id) && (!hrchy_info_[i].isFlag(AINFO_DROPPED))) {
      col_no = i;
      break;
    }
  }
  return col_no;
}

int TsTableMMapObject::getColumnIndex(const uint32_t& col_id) {
  int col_no = -1;
  for (int i = 0; i < hrchy_info_.size(); ++i) {
    if (hrchy_info_[i].id == col_id) {
      col_no = i;
      break;
    }
  }
  return col_no;
}
