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



#ifndef MMAPOBJECT_H_
#define MMAPOBJECT_H_

#include <mutex>
#include <DataModel.h>
#include <pthread.h>
#include "TSObject.h"
#include "BigObjectType.h"
#include "MMapFile.h"
#include "lib/AttributeInfoIndex.h"
#include "BigObjectUtils.h"
#include "BigObjectConst.h"

using namespace bigobject;

#define COLUMNATTR_LEN      64  // MAX_COLUMNATTR_LEN + 2

typedef char col_a  [COLUMNATTR_LEN];

struct ColumnInfo {
  col_a name;
  int type;             ///< Attribute type.
  int offset;           ///< Offset.
  int size;				///< Size.
  int length;           ///< Length.
  int encoding;
  int flag;
  int max_len;
  int reserved;
  LASTAGGFLAG lastFlag;
  int padding;
};

#define ColumnInfoTrailSize	(sizeof(ColumnInfo) - sizeof(attr_str))


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
  int source_version;       ///< data source version number.
  int type;                 ///< object type.

  time_t create_time; 		///< big object (vtree) create time.
  off_t meta_data_length;   ///< length of meta data section.

  col_a source_url;		    ///< Source URL.
  col_a hash_url; 		    ///< hash file URL.
  col_a ns_url;             ///< offset to the name service.
  col_a index_url;          ///< Index file URL.
  col_a index_hash_url;     ///< Hash URL for index file.

  off_t vtree_offset;       ///< offset to the vtree section in the file.
  off_t data_offset;		///< offset to data section in file.
                            ///< encryption vector.
  off_t attribute_offset;   ///< offset to tree attributes.
  off_t record_size;        ///< size of record (in bytes).
  off_t measure_info; 		///< information for sereis_measure_info.
  off_t measure_size;       ///< size of measure data (in bytes).
  off_t key_info;           ///< information for key.
  off_t description;        ///< Description.

  int num_measure_info;
  int num_key_info;
  int key_length;           ///< Length of key in big table (in bytes).
  int encoding;             ///< Encoding scheme.
  size_t time_bound;        ///< Time bound for circular table.
  size_t capacity;          ///< Circular table capacity.
  size_t start_index;
  size_t end_index;
  int32_t srid;             ///< spatial system SRID
  int32_t reserved_srid;
  int32_t duplicate_key;    // # of duplicate keys.
  int32_t is_deleted;
  char reserved[12];

  // Updatable data, start from 512 bytes for recoverability.
  size_t num_node;          ///< total number of nodes.
  off_t length;             ///< file data length.
  off_t vtree_length;       ///< length of vtree section.
  off_t ns_offset;          ///< offset to the name service.
  off_t ns_length;          ///< length of name service section.
  off_t name_pool_length;   ///< length of all name string pool length.
  int status;               ///< status flag.
  int permission;           ///< object permission.
  size_t num_leaf_node;     ///< total number of leaf nodes;
  size_t num_index; 	    ///< Number of index.
  off_t class_fact_info;    ///< information for class variables.
  col_a link_url;           ///< link URL.
  col_a nameservice_dim;    ///< Dimension Name for Name Service.
  size_t actul_size;        ///< Actual table size.

  // Possibly depreciated
  size_t checksum;          ///< Weak checksum.
  uint32_t version;         ///< version is incremented for update/delete.
  uint32_t sync_version;    // sync version
  time_t update_time;       ///< last update time.
  off_t data_level_offset;  ///< offset to the data level info.
  size_t life_cycle;
//  col_a dimension;          ///< Dimension name.
  uint32_t non_def_value;     // = 1 if no need to set default value.
  uint32_t num_varstr_cols;   // number of var str column
  char reserved1[60];

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

//    off_t   root_xpath;       ///< Relative xpath of root.
//    col_a   alias;            ///< Alias.
//    int     num_dimension_info;
//    off_t   dimension_info;
//    uint64_t    ext_measure_size;   ///< size of external measure data (in bytes).
//    DataType  data_type;      ///< data type information.
//    off_t name_id_table_offset;   ///< offset to the name->id table.
//    off_t name_id_table_ordered_size; ///< size (entries) of ordered name->id table.
//    off_t name_id_table_size; ///< size (total entries) of name->id table.

  char user_defined[123]; ///< reserved for user-defined meta data information.
};

class MMapObject: public MMapFile {
protected:
  off_t mem_length_;
  MMapMetaData *meta_data_;
  void *mem_vtree_;       ///< VTree section starting address.
  void *mem_ns_;          ///< name service starting address.
  void *mem_data_;        ///< data section starting address.
  void *mem_data_ext_;    ///< external data section starting address.
  string obj_name_;

  vector<AttributeInfo> hrchy_info_;
  AttributeInfoIndex hrchy_index_;
  vector<AttributeInfo> measure_info_;
  AttributeInfoIndex measure_index_;
  int obj_type_;
  bool is_ns_needed_;

  int getIndex(const AttributeInfoIndex &idx, const string &col_name) const;

  uint64_t metaSize() const { return meta_data_->vtree_offset; }

  uint64_t recordSize() const { return meta_data_->record_size; }

  uint64_t measureSize() const { return meta_data_->measure_size; }

  inline size_t & _actualSize() const { return meta_data_->actul_size; }

  inline size_t timeBound() const { return meta_data_->time_bound; }

#if defined(IOT_MODE)
  inline size_t lifeCycle() const { return meta_data_->life_cycle; }
#endif

  inline size_t & _startIndex() const { return meta_data_->start_index; }

  inline size_t & _endIndex() const { return meta_data_->end_index; }

  inline size_t & _reservedSize() const { return meta_data_->num_leaf_node; }

  size_t _lastIndex() const
  { return (_startIndex() == 1) ? _reservedSize() - 1: (_startIndex() - 1); }

  inline size_t & size_() const { return meta_data_->num_node; }

  void setColumnInfo(ColumnInfo *col_attr, const AttributeInfo &a_info);

  void setColumnInfo(int idx, const AttributeInfo &a_info);

  void changeColumnName(int idx, const string &new_name);

  int readColumnInfo(off_t offset, int size, vector<AttributeInfo> &attr_info);

  off_t writeColumnInfo(const vector<AttributeInfo> &attr_info, int &err_code);

  bool isMetaExist() const
  { return (mem_length_ >= (off_t) sizeof(MMapMetaData)); }

  void updateChecksum(IDTYPE id, uint32_t nc)
  { fletcher64((uint64_t *)&(meta_data_->checksum), id, nc); }

  int openNameService(ErrorInfo &err_info, int flags);

  int swap(MMapObject &rhs);

public:
  MMapObject();

  virtual ~MMapObject();

  off_t & memLen() { return mem_length_; }

  virtual void *memData() { return mem_data_; }

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
  off_t urlCopy(const string &source_url, off_t = 0, size_t ps =
    BigObjectConfig::pageSize());

  void urlCopy (char *d, const string &url)
  { strncpy(d, url.c_str(), COLUMNATTR_LEN); }

  string smartPointerURL(off_t offset, off_t url_offset = 0) const
  { return string((const char*) ((intptr_t) mem_ + offset + url_offset)); }

  std::string filePath() const
  { return file_path_; }

  int writeAttributeURL(const string &source_url, const string &ns_url,
    const string &description);

  /*
   * --------------------------------------------------------------
   * functions related to meta data information.
   * --------------------------------------------------------------
   */
  void setStatus(int status)
  { meta_data_->status |= status; }

  void unsetStatus(int status)
  { meta_data_->status &= ~status; }

  int status() const
  { return meta_data_->status; }

//  int64_t getDistinctOfAttribute(int level) const
//  { return ((int64_t *) addr(meta_data_->distinct_attr))[level]; }

  string description() const
  { return (mem_) ? smartPointerURL(meta_data_->description) : string(""); }

  /**
   * @brief	obtain attributes hierarchy of a big boject.
   *
   * @return 	vector of string attributes.
   */
  vector<string> hierarchyAttribute() const;

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
  vector<AttributeInfo> hierarchyInfo() const;

  int getFactColumnInfo(int index, AttributeInfo &ainfo) const;

  int getFactColumnInfo(const string &name, AttributeInfo &ainfo) const;

  /**
   * @brief	obtain version of big object structure.
   *
   * @return 	structure version.
   */
  int structVersion() const
  { return (mem_) ? meta_data_->struct_version : 0; }

  int setStructVersion(int version) {
    meta_data_->struct_version = version;
    return 0;
  }

  /**
   * @brief	obtain big object source URL string.
   *
   * @return 	source URL string.
   */
  string nameServiceURL() const
  { return (mem_) ? string(meta_data_->ns_url) : s_emptyString(); }

    /**
   * @brief	obtain big object source URL string.
   *
   * @return 	source URL string.
   */
  string sourceURL() const
  { return (mem_) ? string(meta_data_->source_url) : s_emptyString(); }

  string linkURL() const
  { return (mem_) ? string(meta_data_->link_url) : s_emptyString(); }

  /**
   * @brief	check if a big object's structure type.
   *
   * @return	structure type of a big object; available types are: ST_VTREE, ST_VTREE_LINK,
   * 					ST_DATA, ST_DATA_LINK, ST_SUPP, ST_SUPP_LINK
   */
  int structType() const
  { return (mem_) ? meta_data_->struct_type : 0; }

  /**
   * @brief	obtain version of the big object
   *
   * @return 	data object version.
   */
  int version() const
  { return (mem_) ? meta_data_->data_version : 0; }

  uint64_t checkSum() const
  { return meta_data_->checksum; }

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
   * @brief	obtain the number of nodes of a big object.
   *
   * @return 	number of nodes.
   */
  size_t numNode() const
  { return (mem_) ? meta_data_->num_node : 0; }

  size_t numLeafNode() const
  { return (mem_) ? meta_data_->num_leaf_node : 0; }

  int encoding() const
  { return meta_data_->encoding; }

  void updateAttributeInfoType(int col, int type);
  void updateAttributeInfoMaxLen(int col, int len);

  int numFact() const { return measure_info_.size(); }

  // 0: key is unique; 1: key is not unique
  inline int32_t duplicateKey() { return meta_data_->duplicate_key; }

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

#endif /* MMAPOBJECT_H_ */
