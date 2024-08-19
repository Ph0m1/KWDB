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

#include "mmap/mmap_object.h"
#include "utils/big_table_utils.h"

MMapObject::MMapObject() {
  mem_length_ = 0;
  mem_vtree_ = mem_data_ = nullptr;
  meta_data_ = nullptr;
  obj_type_ = 0;
}

MMapObject::~MMapObject()
{
}


int  MMapObject::openNameService(ErrorInfo &err_info, int flags) {

  return 0;
}

int MMapObject::open(const string &file_path, const std::string &db_path, const string &tbl_sub_path, int cc,
  int flags) {
  int error_code = MMapFile::open(file_path, db_path + tbl_sub_path + file_path, flags);
  if (error_code < 0) {
    return error_code;
  }
  if (file_length_ >= (off_t) sizeof(MMapMetaData)) {
    col_info_.clear();
    // initialize all pointers first
    initSection();
    if (meta_data_->magic != cc) {
      return KWENOOBJ;
    }

    error_code = readColumnInfo(meta_data_->attribute_offset,
                                meta_data_->level, col_info_);
    if (error_code < 0) {
      return error_code;
    }
    error_code = readColumnInfo(meta_data_->measure_info,
      meta_data_->num_measure_info, measure_info_);
    if (error_code < 0) {
      return error_code;
    }

    ErrorInfo err_info;
    if (openNameService(err_info, flags_) < 0) {
        return err_info.errcode;
    }

    mem_length_ = meta_data_->length;
  }

  if (mem_length_ == 0) {
      mem_length_ = file_length_;
  }

  obj_name_ = getURLObjectName(file_path_);

  return 0;
}

int MMapObject::close() {
  munmap();
  mem_length_ = 0;
  mem_vtree_ = mem_data_ = nullptr;
  meta_data_ = nullptr;
  return 0;
}

int MMapObject::initMetaData()
{
    int err_code = 0;
    if (file_length_ < (off_t) sizeof(MMapMetaData))
        err_code = memExtend(sizeof(MMapMetaData));
    if (err_code < 0)
        return err_code;
    memset(meta_data_, 0, sizeof(MMapMetaData));
    meta_data_->meta_data_length = sizeof(MMapMetaData);
    return 0;
}

void MMapObject::initSection() {
  meta_data_ = (MMapMetaData*) mem_;

  if (meta_data_->struct_type & ST_VTREE) {
    mem_vtree_ = (void*) addr(meta_data_->vtree_offset);
  }

  if (meta_data_->struct_type & ST_DATA) {
    mem_data_ = (void*) addr(meta_data_->data_offset);
  }
}

int MMapObject::memExtend(off_t offset, size_t ps)
{
    off_t new_mem_len = mem_length_ + offset;
    if (file_length_ < new_mem_len) {
        int err_code = mremap(getPageOffset(new_mem_len, ps));
        if (err_code < 0)
            return err_code;
        initSection();
    } else {
        ///TODO: trim table
    }
    mem_length_ = new_mem_len;
    return 0;
}

off_t MMapObject::urlCopy(const string &source_url, off_t offset,
    size_t ps) {
    off_t len = source_url.size() + 1 + offset;
    off_t prev_len;
    off_t limit;
    int err_code = 0;

    limit = mem_length_;
    if (meta_data_->struct_type & ST_VTREE)
        limit = min(limit, meta_data_->vtree_offset);
    if (meta_data_->struct_type & ST_NS)
        limit = min(limit, meta_data_->ns_offset);
    if (meta_data_->struct_type & ST_DATA)
        limit = min(limit, meta_data_->data_offset);

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
        strcpy((char *) ((intptr_t) addr(prev_len) + offset),
            source_url.c_str());

    return prev_len;
}

int MMapObject::readColumnInfo(off_t offset, int size,
    vector<AttributeInfo> &attr_info) {
  MMapColumnInfo *cols = (MMapColumnInfo *) addr(offset);
  for (int i = 0; i < size; ++i) {
    AttributeInfo ainfo;
    memcpy(&ainfo, &cols[i], sizeof(AttributeInfo));
    attr_info.push_back(ainfo);
  }
  return 0;
}

void MMapObject::setColumnInfo(MMapColumnInfo *col_attr, const AttributeInfo &a_info) {
    strncpy(col_attr->name, a_info.name, COLUMNATTR_LEN);
    memcpy(&(col_attr->type), &(a_info.type), TsColumnTailSize);
}

void MMapObject::setColumnInfo(int i, const AttributeInfo &a_info) {
    MMapColumnInfo *col_attr = (MMapColumnInfo *) addr(meta_data_->attribute_offset);
    setColumnInfo(&(col_attr[i]), a_info);
}

off_t MMapObject::writeColumnInfo(const vector<AttributeInfo> &attr_info,
    int &err_code)
{
    off_t start_addr = memLen();
    off_t len = attr_info.size() * sizeof(MMapColumnInfo);
    err_code = memExtend(len);
    if (err_code < 0) {
        memLen() = fileLen();
        return err_code;
    }
    meta_data_->meta_data_length += len;

    MMapColumnInfo *col_attr = (MMapColumnInfo *) addr(start_addr);
    for (size_t i = 0; i < attr_info.size(); ++i) {
        setColumnInfo(&(col_attr[i]), attr_info[i]);
    }

    return start_addr;
}

int MMapObject::writeAttributeURL(
    const string &source_url,
    const string &ns_url,
    const string &description)
{
    urlCopy(&(meta_data_->source_url[0]), source_url.c_str());
    urlCopy(&(meta_data_->ns_url[0]), ns_url.c_str());
    assign(meta_data_->description, urlCopy(description));

    int err_code = 0;
    meta_data_->num_measure_info = measure_info_.size();
#if defined(__x86_64__) || defined(_M_X64)
    off_t col_off = writeColumnInfo(measure_info_, err_code);
    if (err_code < 0) {
      return err_code;
    }
    assign(meta_data_->measure_info, col_off);
#else
    off_t tmp = writeColumnInfo(measure_info_, err_code);
    assign(meta_data_->measure_info, tmp);
#endif

    meta_data_->level = col_info_.size();
#if defined(__x86_64__) || defined(_M_X64)
    col_off = writeColumnInfo(col_info_, err_code);
    if (err_code < 0) {
      return err_code;
    }
    assign(meta_data_->attribute_offset, col_off);
#else
    tmp = writeColumnInfo(hrchy_info_, err_code);
    assign(meta_data_->attribute_offset, tmp);
#endif

    return err_code;
}
