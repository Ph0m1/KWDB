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

#include <mutex>
#include <shared_mutex>
#include <DataModel.h>
#include <pthread.h>
#include "TSObject.h"
#include "BigObjectType.h"
#include "MMapFile.h"
#include "BigObjectConst.h"
#include "BigObjectConfig.h"
#include "BigObjectUtils.h"
#include "lib/AttributeInfoIndex.h"

using namespace bigobject;

#define TSCOLUMNATTR_LEN      64  // MAX_COLUMNATTR_LEN + 2

typedef char col_a[TSCOLUMNATTR_LEN];

/**
 * @brief segment status, segment is unit of compress.
 *        compress to sqfs file, and mount sqfs file to os
*/
enum SegmentStatus {
  ActiveSegment = 0,        // can write
  ActiveInWriteSegment = 1, // segment is writing
  InActiveSegment = 2,      // cannot write, and no compressed
  ImmuWithRawSegment = 3,   // cannot write, compressed but not delete original dir
  ImmuSegment = 4,          // cannot write, compressed and original dir deleted.
  ImmuWithMountSegment = 5, // cannot write, sqfs file mounted.
};

struct TsColumnInfo {
  uint32_t id;
  col_a name;
  int32_t type;         ///< Attribute type.
  int32_t offset;       ///< Offset.
  int32_t size;         ///< Size.
  int32_t length;       ///< Length.
  int32_t encoding;
  int32_t flag;
  int32_t max_len;
  uint32_t version;
  AttrType attr_type;
  LASTAGGFLAG lastFlag;
};

/**
 * meta data information in a TSTable object
 * DO NOT ALTER variable order within the structure!!!
 */
struct TSTableFileMetadata {
  int magic;                ///< Magic number for a big object.
  int struct_version;       ///< object structure version number
  int struct_type;          ///< structure type
  int schema_version;       ///< data version number
  uint64_t life_time ;          /// unit: second, over time data will be deleted.
  uint64_t partition_interval;  /// unit: second
  int level;                    ///< level of attributes.
  int depth;                    ///< depth of whole (distributed) tree;
  ///< shift size of name service.
  time_t create_time;       ///< TSTable object (vtree) create time.
  off_t meta_data_length;   ///< length of meta data section.
  uint32_t  block_num_of_segment;
  col_a source_url;        ///< Source URL.
  col_a ns_url;             ///< offset to the name service.
  ///< encryption vector.
  off_t attribute_offset;   ///< offset to tree attributes.
  off_t record_size;        ///< size of record (in bytes).
  off_t description;        ///< Description.
  int encoding;             ///< Encoding scheme.
  int32_t is_deleted;
  bool is_dropped;
  char reserved[12];
  // Updatable data, start from 512 bytes for recoverability.
  size_t num_node;          ///< total number of nodes.
  off_t length;             ///< file data length.
  off_t ns_offset;          ///< offset to the name service.
  int status;               ///< status flag.
  int permission;           ///< object permission.
  size_t num_leaf_node;     ///< total number of leaf nodes;
  col_a link_url;           ///< link URL.
  col_a nameservice_dim;    ///< Dimension Name for Name Service.
  size_t actul_size;        ///< Actual table size.
  // Possibly depreciated
  size_t checksum;          ///< Weak checksum.
  size_t life_cycle;
  char reserved1[60];
  timestamp64 min_ts;       // minimum timestamp partition
  timestamp64 max_ts;       // maximum timestamp partition
  // has valid row
  bool has_data;
  // entity hash range
  uint64_t begin_hash;
  uint64_t end_hash;

  char user_defined[123]; ///< reserved for user-defined meta data information.
};

class MMapEntityMeta;

class TsTableMMapObject : public MMapFile {
 protected:
  off_t mem_length_;
  TSTableFileMetadata* meta_data_;
  void* mem_vtree_;       ///< VTree section starting address.
  void* mem_ns_;          ///< name service starting address.
  void* mem_data_;        ///< data section starting address.
  void* mem_data_ext_;    ///< external data section starting address.
  string obj_name_;

  vector<AttributeInfo> hrchy_info_;
  AttributeInfoIndex hrchy_index_;
  vector<AttributeInfo> actual_hrchy_info_;
  vector<uint32_t> actual_cols_;
  bool is_ns_needed_;

  int getIndex(const AttributeInfoIndex& idx, const string& col_name) const;

  inline size_t& _reservedSize() const { return meta_data_->num_leaf_node; }

  inline size_t& size_() const { return meta_data_->num_node; }

  void setColumnInfo(TsColumnInfo* col_attr, const AttributeInfo& a_info);

  void setColumnInfo(int idx, const AttributeInfo& a_info);

  int readColumnInfo(off_t offset, int size, vector<AttributeInfo>& attr_info);

  off_t writeColumnInfo(const vector<AttributeInfo>& attr_info, int& err_code);

  int swap(TsTableMMapObject& rhs);

 public:
  TsTableMMapObject();

  virtual ~TsTableMMapObject();
  off_t& memLen() { return mem_length_; }

  virtual void* memData() { return mem_data_; }

  /**
   * @brief	open a big object.
   *
   * @param 	url			big object URL to be opened.
   * @param 	flag		option to open a file; O_CREAT to create new file
   * @param	tbl_sub_path		sub path.
   * @return	>= 0 if succeed, otherwise -1.
   */
  int open(const string& url, const std::string& db_path, const string& tbl_sub_path, int cc, int flags);

  int close();

  int initMetaData();

  /**
   * @brief	obtain the physical address of based on the offset.
   *
   * @param 	offset	offset in the data section.
   * @return	memory address of the data
   */
  void* addr(off_t offset) const { return (void*) ((intptr_t) mem_ + offset); }

  void initSection();

  int memExtend(off_t offset = 0, size_t ps = BigObjectConfig::pageSize());

  /**
   * @brief	copy url string to meta_data section (if the space is enough) or to the end of file
   * 			the destination url format is:
   * 			| offset | url |
   *
   * @param 	source_url	source url char string
   * @param 	offset 		the space reserved between the destination and string
   * @return	the beginning location of destination url
   */
  off_t urlCopy(const string& source_url, off_t = 0, size_t ps =
  BigObjectConfig::pageSize());

  void urlCopy(char* d, const string& url) { strncpy(d, url.c_str(), TSCOLUMNATTR_LEN); }

  string smartPointerURL(off_t offset, off_t url_offset = 0) const {
    return string((const char*) ((intptr_t) mem_ + offset + url_offset));
  }

  std::string filePath() const { return file_path_; }

  /*
   * --------------------------------------------------------------
   * functions related to meta data information.
   * --------------------------------------------------------------
   */
  void setStatus(int status) { meta_data_->status = status; }

  void unsetStatus(int status) { meta_data_->status &= ~status; }

  int status() const { return meta_data_->status; }

  /**
   * @brief	obtain attributes hierarchy of a big boject.
   *
   * @return 	vector of string attributes.
   */
  vector<string> rank() const;

  /**
   * @brief	obtain the hierarchy attributes series.
   *
   * @param	attr		the hierarchy attribute.
   * @return	1 if succeeds; 0 otherwise.
   */
  const vector<AttributeInfo>& hierarchyInfo() const;

  /**
   * @brief	obtain version of big object structure.
   *
   * @return 	structure version.
   */
  int structVersion() const { return (mem_) ? meta_data_->struct_version : 0; }

  int setStructVersion(int version) {
    meta_data_->struct_version = version;
    return 0;
  }

  /**
   * @brief	obtain big object source URL string.
   *
   * @return 	source URL string.
   */
  string nameServiceURL() const { return (mem_) ? string(meta_data_->ns_url) : s_emptyString(); }

  /**
   * @brief	check if a big object's structure type.
   *
   * @return	structure type of a big object; available types are: ST_VTREE, ST_VTREE_LINK,
   * 					ST_DATA, ST_DATA_LINK, ST_SUPP, ST_SUPP_LINK
   */
  int structType() const { return (mem_) ? meta_data_->struct_type : 0; }

  /**
   * @brief	obtain version of the big object
   *
   * @return 	data object version.
   */
  int version() const { return (mem_) ? meta_data_->schema_version : 0; }

  uint64_t checkSum() const { return meta_data_->checksum; }

  /**
 * @brief	obtain level of attributes
 *
 * @return  total number of attributes level in a big object.
 */
  int level() const { return (mem_) ? meta_data_->level : 0; }

  /**
   * @brief	obtain depth of big object.
   *
   * @return 	depth of big object.
   */
  int depth() const { return (mem_) ? meta_data_->depth : 0; }

  int encoding() const { return meta_data_->encoding; }

  TSTableFileMetadata* metaData() { return meta_data_; }

  void setDropped() { meta_data_->is_dropped = true; }

  bool isDropped() { return meta_data_->is_dropped; }

  int getColumnIndex(const AttributeInfo& attr_info);

  int getColumnIndex(const uint32_t& col_id);
};

/**
 * @brief parse column value between data file and memory.
*/
class RecordHelper {
 protected:
  const vector<AttributeInfo> *attr_info_{nullptr};
  void *data_{nullptr};
  vector<char> internal_data_;
  int data_size_{0};
  vector<DataToStringPtr> to_str_handler_;
  vector<StringToDataPtr> to_data_handler_;
  std::string time_format_;

 public:
  RecordHelper(){  }

  virtual ~RecordHelper(){
    internal_data_.clear();
    to_str_handler_.clear();
    to_data_handler_.clear();
  };

  void setHelper(const vector<AttributeInfo> &attr_info, bool is_internal_data = true,
                 const std::string & time_format = BigObjectConfig::dateTimeFormat());

  void swap(DataHelper &rhs);

  inline void * columnAddr(int col, void *data) const
  { return (void *) ((intptr_t)data + (*attr_info_)[col].offset); }

  // Returns  a string for a column data
  string columnToString(int col, void *data) const
  { return to_str_handler_[col]->toString(data); }
};