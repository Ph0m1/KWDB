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
#include <string>
#include <vector>
#include "DateTime.h"
#include "BigTable.h"
#include "MMapObject.h"
#include "MMapHashIndex.h"
#include "ts_common.h"
#include "lg_api.h"
#include "payload.h"
#include "lt_rw_latch.h"
#include "lt_cond.h"
#include "cm_func.h"

extern uint32_t k_entity_group_id_size;
extern uint32_t k_per_null_bitmap_size;

class TagTuplePack;

enum TagType {
    UNKNOWN_TAG = -1,
    GENERAL_TAG = 1,
    PRIMARY_TAG,
};

struct TagInfo {
  uint32_t  m_id;        //  tag column id
  int32_t   m_data_type;  // data type
  uint32_t  m_length;   // data length
  uint32_t  m_offset;    // offset
  uint32_t  m_size;      // data size
  TagType   m_tag_type;  // tag type
};

struct TagColumnMetaData {
  uint64_t  m_lsn;
  uint8_t   m_droped;
};

// please keep lsn and drop together and relative order
constexpr int lsnOffsetInTag() {
  return offsetof(struct TagColumnMetaData, m_lsn);
}

constexpr int lsnOffsetInStr() {
  return offsetof(struct TagColumnMetaData, m_lsn);
}

class TagColumn : public MMapFile {
 protected:
  TagInfo m_attr_;
  int32_t      m_idx_;
  MMapStringFile* m_str_file_;
  bool          m_is_primary_tag_;
  uint32_t      m_store_size_;
  uint32_t      m_store_offset_;  // Only the primary tag column needs to be used
  std::string   m_db_path_;
  std::string   m_db_name_;
  uint32_t avgeStringColumnLength(size_t n);

  // inline int startLoc() const { return sizeof(TagColumnMetaData); }
  TagColumnMetaData& tagColumnMetaData() { return *(reinterpret_cast<TagColumnMetaData *>(mem_)); }
  TagColumnMetaData& strFileMetaData() { return *(reinterpret_cast<TagColumnMetaData *>(m_str_file_->memAddr())); }

 public:
  TagColumn() = delete;

  TagColumn(int32_t idx, const TagInfo& attr);

  int open(const std::string& col_file_name, const std::string &db_path, const std::string &dbname, int flags);

  TagInfo& attributeInfo() {return m_attr_;}

  inline bool isPrimaryTag() {return m_is_primary_tag_;}

  inline bool isVarTag() {return (m_str_file_ != nullptr) ? true : false;}

  inline void setPrimaryTag(bool is_primary_tag) {m_is_primary_tag_ = is_primary_tag;}

  inline void* startAddr() const {
    return reinterpret_cast<void *>((intptr_t)mem_ + sizeof(TagColumnMetaData));
  }

  int extend(size_t old_record_count, size_t new_record_count);

  int remove();

  int writeValue(size_t row, const char* data, uint32_t len);

  int getColumnValue(size_t row, void *data) const;

  inline char *getVarValueAddr(size_t r) {
    size_t offset = *reinterpret_cast<uint64_t *>((intptr_t)startAddr() + r * (m_attr_.m_size + k_per_null_bitmap_size) + k_per_null_bitmap_size);
    if (UNLIKELY(offset < MMapStringFile::startLoc())) {
      offset = MMapStringFile::startLoc();
    }
    m_str_file_->rdLock();
    char* rec_ptr = m_str_file_->getStringAddr(offset);
    m_str_file_->unLock();
    return rec_ptr;
  }

  inline char *getVarValueAddrByOffset(size_t offset) {
    m_str_file_->rdLock();
    char* rec_ptr = m_str_file_->getStringAddr(offset);
    m_str_file_->unLock();
    return rec_ptr;
  }

  inline char *rowAddrHasNullBitmap(size_t row) const  {
    return reinterpret_cast<char *>(offsetAddr(startAddr(), row * (m_attr_.m_size + k_per_null_bitmap_size)));
  }

  inline char *rowAddrNoNullBitmap(size_t row) const {
    return reinterpret_cast<char *>(offsetAddr(startAddr(), row * (m_attr_.m_size + k_per_null_bitmap_size) + k_per_null_bitmap_size));
  }

  inline size_t getVarFileSize() {
    return m_str_file_->size();
  }

  inline uint32_t getStoreOffset() {return m_store_offset_;}

  inline void setStoreOffset(uint32_t store_offset) {m_store_offset_ = store_offset;}

  int rename(std::string& new_col_file_name);

  inline const uint64_t getLSN() {
    return tagColumnMetaData().m_lsn;
  }

  inline void setLSN(uint64_t lsn) {
    tagColumnMetaData().m_lsn = lsn;
    if (m_str_file_) {
      strFileMetaData().m_lsn = lsn;
    }
  }

  inline void setDrop() {
    tagColumnMetaData().m_droped = true;
    if (m_str_file_) {
      strFileMetaData().m_droped = true;
    }
  }

  void unsetDrop() {
    tagColumnMetaData().m_droped = false;
    if (m_str_file_) {
      strFileMetaData().m_droped = false;
    }
    return;
  }

  const bool isDroped() {
    return tagColumnMetaData().m_droped;
  }

  inline bool isNull(size_t row) {
    return (reinterpret_cast<char *>(offsetAddr(startAddr(), row * (m_attr_.m_size + k_per_null_bitmap_size)))[0] != 0x01);
  }

  inline void setNull(size_t row) {
    reinterpret_cast<char *>(offsetAddr(startAddr(), row * (m_attr_.m_size + k_per_null_bitmap_size)))[0] = 0;
  }

  inline void setNotNull(size_t row) {
    reinterpret_cast<char *>(offsetAddr(startAddr(), row * (m_attr_.m_size + k_per_null_bitmap_size)))[0] = 1;
  }

  inline bool isInited() {
    return ((file_length_ > 0) ? true : false);
  }

  void writeNullVarOffset(size_t row);

  int sync(int flags);
  ~TagColumn() override;
};

  // primary tags struct
struct  TagTableMeatData {
  uint32_t    m_magic;
  uint32_t    m_record_size;       // record size bitmap+primarytags+tags
  uint32_t    m_record_store_size;  // storage record size delmark+bitmap+entity+group+primarytags+tags
  uint16_t    m_header_size;
  uint16_t    m_bitmap_size;
  uint32_t    m_primary_tag_size;  // primarytags size
  uint32_t    m_primary_tag_store_size;  // storage record primarytags size: entity+group+primarytags
  uint32_t    m_column_count;
  uint32_t    m_column_info_offset;
  uint32_t    m_record_start_offset;
  uint64_t    m_row_count;
  uint64_t    m_reserve_row_count;
  uint64_t    m_valid_row_count;  // valid row count
  uint64_t    m_mem_length;
  uint64_t    m_entitygroup_id;
  uint32_t    m_ts_version;
//  uint64_t    m_lsn;
//  uint8_t     m_droped;
};

constexpr int lsnOffsetInMeta() {
  return offsetof(struct TagColumnMetaData, m_lsn);
}

constexpr int lsnOffsetInPrimaryTag() {
  return 0;
}

using TagTableMutex = KLatch;
using TagTableRWLatch = KRWLatch;
using TagTableCntMutex = KLatch;
using TagTableCondVal  = KCond_t;

class MMapTagColumnTable: public TSObject, public MMapFile {
 public:
  TagTableCntMutex*   m_ref_cnt_mtx_;
  TagTableCondVal*    m_ref_cnt_cv_;
 protected:
  TagTableMeatData* m_meta_data_;
  std::vector<TagColumn*> m_cols_;
  TagColumn*             m_bitmap_file_;
  TagColumn*             m_meta_file_ = nullptr;
  MMapHashIndex*        m_index_;
  std::string m_name_;
  std::string m_db_name_;
  std::string m_db_path_;
  TagTableMutex*  m_tag_table_mutex_;
  TagTableRWLatch*  m_tag_table_rw_lock_;
  bool enableWal_;

  int open_(const string &url, const std::string &db_path, const string &tbl_sub_path, int flags,
    ErrorInfo &err_info);

  int create_mmap_file(const string &url, const std::string &db_path,
                       const string &tbl_sub_path, int flags, ErrorInfo &err_info);

  int init(const vector<TagInfo> &schema, ErrorInfo &err_info);

  int initMetaData(ErrorInfo &err_info);

  int32_t headerSize() const { return m_meta_data_->m_header_size; }

  int initColumn(const std::vector<TagInfo> &schema, ErrorInfo &err_info);

  int writeColumnInfo(uint64_t start_offset, const std::vector<TagColumn*>& tag_schemas);

  int readColumnInfo(ErrorInfo &err_info);

  int initBitMapColumn(ErrorInfo &err_info);

  int initIndex(ErrorInfo &err_info);

  int extend(size_t new_record_count, ErrorInfo &err_info);

  TagColumn* cloneMetaData(ErrorInfo& err_info);

  inline char * header_(size_t n) const
  { return reinterpret_cast<char *>((intptr_t)m_bitmap_file_->startAddr() + n); }

  // bitmap + primarytags + tags
  int push_back(size_t r, const char *data);

  void push_back_primary(size_t r, const char * data);

  inline void push_back_entityid(size_t r, uint32_t entity_id, uint32_t group_id) {
    char *rec_ptr = entityIdStoreAddr(r);
    memcpy(rec_ptr, &entity_id, sizeof(uint32_t));
    memcpy(rec_ptr + sizeof(entity_id), &group_id, sizeof(uint32_t));
  }

  inline void setNull(size_t row, size_t col) {
    if (m_cols_[col]->isPrimaryTag()) {
      return;
    } else {
      m_cols_[col]->setNull(row);
    }
    // return set_null_bitmap((unsigned char *)header_(row) + 1, col);
  }

  inline void setNotNull(size_t row, size_t col) {
    if (m_cols_[col]->isPrimaryTag()) {
      return;
    } else {
      m_cols_[col]->setNotNull(row);
    }
    //return unset_null_bitmap((unsigned char *)header_(row) + 1, col);
  }

  inline void setDeleteMark(size_t row) {
    reinterpret_cast<uint8_t*>(header_(row))[0] = 1;
  }

  size_t getRowNo(uint32_t groupid, uint32_t entityid, void *primaryTag);

  inline void unsetDeleteMark(size_t row) {
    reinterpret_cast<uint8_t *>(header_(row))[0] = 0;
  }

  inline char * columnValueAddr(size_t r, size_t c) const {
    if (m_cols_[c]->isPrimaryTag()) {
      return  reinterpret_cast<char *>((intptr_t) mem_ + m_meta_data_->m_record_start_offset
                                       + m_meta_data_->m_primary_tag_store_size * r + m_cols_[c]->getStoreOffset() + k_entity_group_id_size);
    } else {
      return m_cols_[c]->rowAddrNoNullBitmap(r);
    }
  }

  inline char * columnAddr(size_t r, size_t c) const {
    if (m_cols_[c]->isPrimaryTag()) {
      return  reinterpret_cast<char *>((intptr_t) mem_ + m_meta_data_->m_record_start_offset
                                       + m_meta_data_->m_primary_tag_store_size * r + m_cols_[c]->getStoreOffset() + k_entity_group_id_size);
    } else {
      return m_cols_[c]->rowAddrHasNullBitmap(r);
    }
  }

  inline char * primaryTagStoreAddr(size_t r) const {
      return  reinterpret_cast<char *>((intptr_t) mem_ + m_meta_data_->m_record_start_offset
                                       + m_meta_data_->m_primary_tag_store_size * r + k_entity_group_id_size);
  }

  inline char * entityIdStoreAddr(size_t r) const {
      return  reinterpret_cast<char *>((intptr_t) mem_ + m_meta_data_->m_record_start_offset
        + m_meta_data_->m_primary_tag_store_size * r);
  }

  int fullReadEntityId(const std::vector<uint32_t> &scan_tags,
                      std::vector<kwdbts::EntityResultIndex>* entityIdList,
                      kwdbts::ResultSet* res, uint32_t* count);

  inline void setMetaData() {
    // m_meta_data_ = reinterpret_cast<TagTableMeatData*>(mem_);
    m_meta_data_ = reinterpret_cast<TagTableMeatData*>(m_meta_file_->startAddr());
  }
  TagTableMeatData& metaData() { return *m_meta_data_; }
  inline size_t metaDataSize() {
    return sizeof(TagTableMeatData);
  }

  int reserve(size_t n, ErrorInfo &err_info);

  TagColumn* addNewColumn(TagInfo& tag_schema,  bool need_ext, ErrorInfo &err_info);

  int convertData(int32_t col, TagColumn* new_tag_col, CONVERT_DATA_FUNC convert_data, bool is_digit_data, ErrorInfo& err_info);

  int rdLock() override {
    return RW_LATCH_S_LOCK(m_tag_table_rw_lock_);
  }
  int wrLock() override {
    return RW_LATCH_X_LOCK(m_tag_table_rw_lock_);
  }
  int unLock() override {
    return RW_LATCH_UNLOCK(m_tag_table_rw_lock_);
  }

 public:
  MMapTagColumnTable();

  virtual ~MMapTagColumnTable();

  int create(const vector<TagInfo> &schema, int32_t entity_group_id, ErrorInfo &err_info);

  int open(const string &url, const std::string &db_path, const string &tbl_sub_path, int flags, ErrorInfo &err_info);

  int remove();

  int insert(uint32_t entity_id, uint32_t subgroup_id, const char *rec);

  int InsertTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id) {
    return insert(entity_id, sub_group_id, payload.GetTagAddr());
  }

  int UpdateTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id, ErrorInfo& err_info);

  inline const size_t recordSize() {return m_meta_data_->m_record_size;}

  inline const int numColumn() {return m_meta_data_->m_column_count;}

  inline bool isNull(size_t row, size_t col) {
    if (m_cols_[col]->isPrimaryTag()) {
      return false;
    } else {
      return m_cols_[col]->isNull(row);
    }
    //return get_null_bitmap((unsigned char *)header_(row) + 1, col);
  }

  // int insertPrimaryTag(uint32_t entity_id, uint32_t subgroup_id, const char *rec, int len);

  int getColumnValue(size_t row, size_t col,  void *data) const;

  int getColumnsByRownum(size_t row, const std::vector<uint32_t> &scan_tags, kwdbts::ResultSet* res);

  void setColumnValue(size_t row, size_t col, char *data);

  int getEntityIdGroupId(const char* primary_tag_val, int len, uint32_t& entity_id, uint32_t& group_id);

  const std::vector<TagColumn*>& getSchemaInfo() {return m_cols_;}

  inline size_t reserveRowCount() const { return m_meta_data_->m_reserve_row_count;}

  inline size_t size() const {return m_meta_data_->m_row_count;}

  inline size_t actual_size() const {return m_meta_data_->m_valid_row_count;}

  inline void *getColumnAddr(size_t row, size_t column) const {
    return (columnAddr(row, column));
  }

  void *getColumnValueAddr(size_t row, size_t column) {
    if (m_cols_[column]->isVarTag()) {
      return m_cols_[column]->getVarValueAddr(row);
    }

    return (columnAddr(row, column));
  }

  void *getColumnVarValueAddrByOffset(size_t column, size_t offset) {
    return m_cols_[column]->getVarValueAddrByOffset(offset);
  }

  inline bool isVarTag(size_t column) {
    return m_cols_[column]->isVarTag();
  }

  size_t getColumnSize(size_t column) {
    if (m_cols_[column]->isPrimaryTag()) {
      return m_meta_data_->m_primary_tag_store_size;
    }else {
      return (m_cols_[column]->attributeInfo().m_size + k_per_null_bitmap_size);
    }
  }

  size_t getVarOffset(size_t row, size_t column) {
    if (m_cols_[column]->isVarTag()) {
      if (row <= actual_size()) {
        return *(reinterpret_cast<size_t *>(m_cols_[column]->rowAddrNoNullBitmap(row)));
      } else {
        return m_cols_[column]->getVarFileSize();
      }
    }
    return 0;
  }

  void *record(size_t n) const {
    return primaryTagStoreAddr(n);
  }

  void *getBitmapAddr(size_t row, size_t column) {
    if (m_cols_[column]->isVarTag()) {
      return m_cols_[column]->rowAddrHasNullBitmap(row);
    }
    return (columnAddr(row, column));
  }
  
  inline size_t primaryTagSize() {return m_meta_data_->m_primary_tag_size;}

  int GetEntityIdList(const std::vector<void*>& primary_tags, const std::vector<uint32_t> &scan_tags,
                              std::vector<kwdbts::EntityResultIndex>* entity_id_list,
                              kwdbts::ResultSet* res, uint32_t* count);

  string printRecord(size_t lhs, size_t rhs, bool with_header = true, bool sort_by_primary = false);

  int getEntityIdByRownum(size_t row, std::vector<kwdbts::EntityResultIndex>* entityIdList);

  inline bool isValidRow(size_t row) {
    return (((unsigned char *)header_(row))[0] & 0x01) ? false : true;
    // return (((unsigned char *)header_(row))[0] == 0x00) ? true : false;
  }

  int startRead() {
    return RW_LATCH_S_LOCK(m_tag_table_rw_lock_);
  }

  int stopRead() {
    return RW_LATCH_UNLOCK(m_tag_table_rw_lock_);
  }

  int startWrite() {
    return RW_LATCH_X_LOCK(m_tag_table_rw_lock_);
  }

  int stopWrite() {
    return RW_LATCH_UNLOCK(m_tag_table_rw_lock_);
  }

  void mutexLock() override { MUTEX_LOCK(m_tag_table_mutex_);}

  void mutexUnlock() override {MUTEX_UNLOCK(m_tag_table_mutex_);}

  int refMutexLock() override{
    return MUTEX_LOCK(m_ref_cnt_mtx_);
  }
  int refMutexUnlock() override{
    return MUTEX_UNLOCK(m_ref_cnt_mtx_);
  }

  int DeleteTagRecord(const char *primary_tags, int len, ErrorInfo& err_info);

  int AddTagColumn(TagInfo& tag_schema, ErrorInfo& err_info);

  int DropTagColumn(TagInfo& tag_schema, ErrorInfo& err_info);

  int AlterTagType(TagInfo& old_tag_schema, TagInfo& new_tag_schema, ErrorInfo& err_info);

  string name() const override { return m_name_; }
  const string& sandbox() const { return m_db_name_; }

  void enableWal() { enableWal_ = true; }
  void setLSN(kwdbts::TS_LSN lsn);
  kwdbts::TS_LSN getLSN();

  void setDrop();
  bool isDroped();

  int flush(kwdbts::TS_LSN lsn);

  TagTuplePack GenTagPack(const char* primarytag, int len);
  int CreateTableForUndo(const std::string &url, std::string &tbl_sub_path,
			 std::vector<TagInfo>& attr_infos,
                         int encoding);
  int CreateTableForRedo(const std::string &url, std::string &tbl_sub_path,
			 std::vector<TagInfo>& attr_infos,
                         int encoding);
  int InsertForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag);
  int InsertForRedo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag, const TSSlice& tag);
  int DeleteForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag, const TSSlice& tag_pack);

  int DeleteForRedo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag);
  int UpdateForRedo(uint32_t group_id, uint32_t entity_id,
                    const TSSlice& primary_tag, const TSSlice& tag);
  int UpdateForUndo(uint32_t group_id, uint32_t entity_id, const TSSlice& primary_tag,
                    const TSSlice& new_tag, const TSSlice& old_tag);
  int DropTableForUndo();
  int DropTableForRedo();

  int AlterTableForUndo(uint32_t groupid, uint32_t entityid,
			TagInfo& oldInfo, TagInfo& newInfo, int opCode);
  int AlterTableForRedo(uint32_t groupid, uint32_t entityid,
			TagInfo& oldInfo, TagInfo& newInfo, int opCode);

private:
  TagInfo* getTagInfo(uint32_t colId, int &colIdx);
  TagColumn *getTagCol(uint32_t colId, int &colIdx);
  int findSerialNo(TagInfo& info);
  string getFileDir();
  int buildMetaName(bool isold, string &metaFileName);
  int buildFileName(const TagInfo& info, bool isOld, string &priFileName,
		    string &secFileName);
  bool isEqualTagInfo(const TagInfo& info1, const TagInfo& info2);
  bool isOldMetaFileExist();
  bool isOldFileExist(TagInfo& info);
  int flipOldFile(TagInfo& info, bool toOld);
  int flipOldMetaFile(bool toOld);
  void recalcOffset();
  void recalcRecordSize();
  void updateOffsetAndSize();
  TagColumn *openTagCol(int32_t idx, const TagInfo& info);
  void cleanTagCol(const TagInfo& info, bool ext=false);

  int AlterRenameRU(TagInfo& oldInfo, TagInfo& newInfo, bool isRedo);
  int AlterAlterRU(TagInfo& oldInfo, TagInfo& newInfo, bool isRedo);
  int AlterAddRU(TagInfo& newInfo, bool isRedo);
  int AlterDropRU(TagInfo& oldInfo, bool isRedo);
};

MMapTagColumnTable* OpenTagTable(const std::string& db_path, const std::string &dir_path,
                                uint64_t table_id, ErrorInfo &err_info);

MMapTagColumnTable* CreateTagTable(const std::vector<TagInfo> &tag_schema,
                                   const std::string& db_path, const std::string &dir_path,
                                   uint64_t table_id, int32_t entity_group_id,
                                   int flags, ErrorInfo &err_info);

int DropTagTable(MMapTagColumnTable* bt, ErrorInfo& err_info);


kwdbts::Batch* GenTagBatchRecord(MMapTagColumnTable* bt, size_t start_row,
                                size_t end_row, size_t col, ErrorInfo& err_info);
