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
#include <data_model.h>
#include <pthread.h>
#include "ts_object.h"
#include "mmap_file.h"
#include "utils/big_table_utils.h"

using namespace kwdbts;

#define COLUMNATTR_LEN      64  // MAX_COLUMNATTR_LEN + 2

typedef char col_a  [COLUMNATTR_LEN];

struct MMapColumnInfo {
  col_a name;
  int type;             ///< Attribute type.
  int offset;           ///< Offset.
  int size;				///< Size.
  int length;           ///< Length.
  int encoding;
  int flag;
  int max_len;
  int reserved_1;
  int reserved_2;
  int padding;
};

/**
 * meta data information in a big object
 * DO NOT ALTER variable order within the structure!!!
 */
struct MMapMetaData {
  int magic;                ///< Magic number for a big object.
  int struct_version;       ///< Big object structure version number.
  int struct_type;          ///< Big object structure type.
  int level;                ///< level of attributes.
  int depth;                ///< depth of whole (distributed) tree;
                            ///< shift size of name service.
  int data_version;         ///< data version number.
  int reserved_1;
  int type;                 ///< object type.

  time_t create_time; 		///< big object (vtree) create time.
  off_t meta_data_length;   ///< length of meta data section.

  col_a source_url;		    ///< Source URL.
  col_a reserved_2;
  col_a ns_url;             ///< offset to the name service.
  col_a reserved_3;
  col_a reserved_4;

  off_t vtree_offset;       ///< offset to the vtree section in the file.
  off_t data_offset;		///< offset to data section in file.
                            ///< encryption vector.
  off_t attribute_offset;   ///< offset to tree attributes.
  off_t record_size;        ///< size of record (in bytes).
  off_t measure_info; 		///< information for sereis_measure_info.
  off_t reserved_5;
  off_t reserved_6;
  off_t description;        ///< Description.

  int num_measure_info;
  int reserved_7;
  int reserved_8;
  int encoding;             ///< Encoding scheme .
  size_t time_bound;        ///< Time bound for circular table.
  size_t capacity;          ///< Circular table capacity.
  size_t start_index;
  size_t end_index;
  int32_t reserved_9;
  int32_t reserved_10;
  int32_t reserved_11;
  int32_t is_deleted;
  char reserved[12];

  // Updatable data, start from 512 bytes for recoverability.
  size_t num_rows;          ///< total number of rows.
  off_t length;             ///< file data length.
  off_t reserved_12;
  off_t ns_offset;          ///< offset to the name service.
  off_t reserved_13;
  off_t reserved_14;
  int status;               ///< status flag.
  int permission;           ///< object permission.
  size_t num_leaf_node;     ///< total number of leaf nodes;
  size_t reserved_15;
  off_t reserved_16;
  col_a link_url;           ///< link URL.
  col_a reserved_17;
  size_t actul_size;        ///< Actual table size.

  // Possibly depreciated
  size_t reserved_18;
  uint32_t version;         ///< version is incremented for update/delete.
  uint32_t sync_version;    // sync version
  time_t reserved_19;
  off_t reserved_20;
  size_t reserved_21;
  uint32_t reserved_22;
  uint32_t reserved_23;
  char reserved_24[60];

  // IOT
  timestamp64 src_min_ts;
  timestamp64 src_max_ts;

  timestamp64 min_ts;       // minimum timestamp partition
  timestamp64 max_ts;       // maximum timestamp partition

  // exists valid row value.
  bool has_data;

  // entity hash range
  uint64_t begin_hash;
  uint64_t end_hash;

  char user_defined[123]; ///< reserved for user-defined meta data information.
};

class MMapObject: public MMapFile {
protected:
  off_t mem_length_;
  MMapMetaData *meta_data_;
  void *mem_vtree_;       ///< VTree section starting address.
  void *mem_data_;        ///< data section starting address.
  string obj_name_;

  vector<AttributeInfo> col_info_;
  vector<AttributeInfo> measure_info_;
  int obj_type_;

  uint64_t metaSize() const { return meta_data_->vtree_offset; }

  uint64_t recordSize() const { return meta_data_->record_size; }

  inline size_t & _actualSize() const { return meta_data_->actul_size; }

  inline size_t timeBound() const { return meta_data_->time_bound; }

  inline size_t & _startIndex() const { return meta_data_->start_index; }

  inline size_t & _endIndex() const { return meta_data_->end_index; }

  inline size_t & _reservedSize() const { return meta_data_->num_leaf_node; }

  inline size_t & size_() const { return meta_data_->num_rows; }

  void setColumnInfo(MMapColumnInfo *col_attr, const AttributeInfo &a_info);

  void setColumnInfo(int idx, const AttributeInfo &a_info);

  int readColumnInfo(off_t offset, int size, vector<AttributeInfo> &attr_info);

  off_t writeColumnInfo(const vector<AttributeInfo> &attr_info, int &err_code);

  int openNameService(ErrorInfo &err_info, int flags);

public:
  MMapObject();

  virtual ~MMapObject();

  off_t & memLen() { return mem_length_; }

  /**
   * @brief	open a big object.
   *
   * @param 	url			big object URL to be opened.
   * @param 	flag		option to open a file; O_CREAT to create new file
   * @param	tbl_sub_path		sub path.
   * @return	>= 0 if succeed, otherwise -1.
   */
  int open(const string &url, const std::string &db_path, const string &tbl_sub_path, int cc, int flags);

  int close();

  int initMetaData();

  /**
   * @brief	obtain the physical address of based on the offset.
   *
   * @param 	offset	offset in the data section.
   * @return	memory address of the data
   */
  void* addr(off_t offset) const
  { return (void*) ((intptr_t) mem_ + offset); }

  // return row data address of a table object.
  inline void * record_(size_t n) const
  { return offsetAddr(mem_vtree_, n * meta_data_->record_size); }

  void initSection();

  int memExtend(off_t offset = 0, size_t ps = kwdbts::EngineOptions::pageSize());

  /**
   * @brief	copy url string to meta_data section (if the space is enough) or to the end of file
   * 			the destination url format is:
   * 			| offset | url |
   *
   * @param 	source_url	source url char string
   * @param 	offset 		the space reserved between the destination and string
   * @return	the beginning location of destination url
   */
  off_t urlCopy(const string &source_url, off_t = 0, size_t ps =
      kwdbts::EngineOptions::pageSize());

  void urlCopy (char *d, const string &url)
  { strncpy(d, url.c_str(), COLUMNATTR_LEN); }

  std::string filePath() const
  { return file_path_; }

  int writeAttributeURL(const string &source_url, const string &ns_url,
    const string &description);

  int status() const
  { return meta_data_->status; }

  /**
   * @brief	obtain version of big object structure.
   *
   * @return 	structure version.
   */
  int structVersion() const
  { return (mem_) ? meta_data_->struct_version : 0; }

  /**
   * @brief	obtain big object source URL string.
   *
   * @return 	source URL string.
   */
  string nameServiceURL() const
  { return (mem_) ? string(meta_data_->ns_url) : kwdbts::s_emptyString; }

  /**
   * @brief	obtain version of the big object
   *
   * @return 	data object version.
   */
  int version() const
  { return (mem_) ? meta_data_->data_version : 0; }

    /**
   * @brief	obtain level of attributes
   *
   * @return  total number of attributes level in a big object.
   */
  int level() const
  { return (mem_) ? meta_data_->level : 0; }

  /**
   * @brief	obtain depth of big object.
   *
   * @return 	depth of big object.
   */
  int depth() const
  { return (mem_) ? meta_data_->depth : 0; }

  /**
   * @brief	obtain time when a big object is created
   *
   * @return 	create time of a big object
   */
  time_t createTime() const
  { return (mem_) ? meta_data_->create_time : 0; }

  /**
   * @brief	obtain the number of rows of a big object.
   *
   * @return 	number of rows.
   */
  size_t numRows() const
  { return (mem_) ? meta_data_->num_rows : 0; }

  int encoding() const
  { return meta_data_->encoding; }

  //IOT functions
  uint32_t getSyncVersion() { return meta_data_->sync_version; }
  void setSyncVersion(uint32_t sync_ver)
  { meta_data_->sync_version = sync_ver; }
  timestamp64 & minSrcTimestamp() { return meta_data_->src_min_ts; }
  timestamp64 & maxSrcTimestamp() { return meta_data_->src_max_ts; }
  timestamp64 & minTimestamp() { return meta_data_->min_ts; }
  timestamp64 & maxTimestamp() { return meta_data_->max_ts; }
  size_t & rangeBegin() { return meta_data_->begin_hash; }
  size_t & rangeEnd() { return meta_data_->end_hash; }
};

