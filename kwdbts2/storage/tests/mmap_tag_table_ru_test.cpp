// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <gtest/gtest.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <random>
#include <utility>
#include "test_util.h"
#include "mmap/mmap_tag_column_table.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "utils/big_table_utils.h"
#include "test_tag_util.h"
#include "data_type.h"
#include "payload.h"

const string TestBigTableInstance::kw_home_ = "./data_kw/";  // NOLINT
const string TestBigTableInstance::db_name_ = "testdb/";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestTagRuTable : public TestBigTableInstance {
 public:
  TestTagRuTable() : db_name_(TestBigTableInstance::db_name_), db_path_(TestBigTableInstance::kw_home_) { }

  class PayLoadBuilder {
  public:
    explicit PayLoadBuilder(vector<AttributeInfo> schema):
        schema_(std::move(schema)),
        flag_(0),
        buf_start_ptr_(nullptr),
        buf_end_ptr_(nullptr),
        max_size_(0),
        cur_operand_(0),
        cur_tag_operand_(0),
        primary_tag_start_ptr_(nullptr),
        primary_tag_end_ptr_(nullptr),
        tag_start_ptr_(nullptr),
        tag_end_ptr_(nullptr),
        tag_bit_map_start_ptr_(nullptr),
        tag_var_start_ptr_(nullptr),
        tag_var_end_ptr_(nullptr),
        data_start_ptr_(nullptr),
        data_end_ptr_(nullptr) {
      init0();
    }


    explicit PayLoadBuilder(vector<TagInfo> schema):
        schema_(),
        flag_(0),
        buf_start_ptr_(nullptr),
        buf_end_ptr_(nullptr),
        max_size_(0),
        cur_operand_(0),
        cur_tag_operand_(0),
        primary_tag_start_ptr_(nullptr),
        primary_tag_end_ptr_(nullptr),
        tag_start_ptr_(nullptr),
        tag_end_ptr_(nullptr),
        tag_bit_map_start_ptr_(nullptr),
        tag_var_start_ptr_(nullptr),
        tag_var_end_ptr_(nullptr),
        data_start_ptr_(nullptr),
        data_end_ptr_(nullptr) {
      for (int i = 0; i < schema.size(); i++) {
        AttributeInfo info;
        info.id = i;
        strcpy(info.name, "");
        info.type = schema[i].m_data_type;
        info.offset = 0;
        info.size = schema[i].m_size;
        info.length = schema[i].m_length;
        info.encoding = 0;
        info.flag = 0;
        info.max_len = 0;
        info.version = 1;
        info.col_flag = (schema[i].m_tag_type == PRIMARY_TAG) ? COL_PRIMARY_TAG : COL_GENERAL_TAG;
        schema_.push_back(std::move(info));
      }

      init0();
    }

    ~PayLoadBuilder() {
      delete[] buf_start_ptr_;
    }

    vector<AttributeInfo> GetSchema() {
      return schema_;
    }

    void SetFlag(int flag) {
      flag_ = flag;
      buf_start_ptr_[flag_pos_] = (char)flag;
    }

    void Fill(const char *val, int len) {
      if (cur_operand_ == 0) {
	memset(buf_start_ptr_, 0, max_size_);

        primary_tag_end_ptr_ = primary_tag_start_ptr_;
        primary_tag_end_ptr_ = primary_tag_end_ptr_ + 2; //skip primarylen
	cur_tag_operand_ = -1;

        tag_end_ptr_ = tag_start_ptr_;
        tag_var_end_ptr_ = tag_var_start_ptr_;

        data_end_ptr_ = data_start_ptr_;
      }

      while (cur_operand_ < schema_.size()) {
  AttributeInfo info = schema_[cur_operand_];
	if ((flag_ == 1) && (info.col_flag == COL_GENERAL_TAG)) {
	  cur_operand_++; //skip general tag
	} else if ((flag_ == 2) && (info.col_flag == COL_TS_DATA)) {
	  cur_operand_++; // skip data
	} else {
	  break;
	}
      }

      AttributeInfo info = schema_[cur_operand_++];
      if (info.col_flag == COL_PRIMARY_TAG) {
	  cur_tag_operand_++;
	  fillPrimary(val, len, info.length);
      } else if (info.col_flag == COL_GENERAL_TAG) {
	cur_tag_operand_++;
	if (info.type == DATATYPE::VARSTRING) {
	  fillTag(val, len, info.length, true);
	} else {
	  fillTag(val, len, info.length, false);
	}
      } else {
	if (info.type == DATATYPE::VARSTRING) {
	  fillData(val, len, info.length, true);
	} else {
	  fillData(val, len, info.length, false);
	}
      }

      if (cur_operand_ == schema_.size()) {
        cur_operand_ = 0;

	*(uint16_t *)(primary_tag_start_ptr_) = primary_tag_end_ptr_ - primary_tag_start_ptr_ - 2;
	if (flag_ == 0) {

	} else if (flag_ == 1) {

	} else if (flag_ == 2) {
	  *(int32_t *)(tag_bit_map_start_ptr_ - 4) = tag_var_end_ptr_ - tag_bit_map_start_ptr_;
	  *(int32_t *)(tag_var_end_ptr_) = 0;
    tag_var_end_ptr_ = tag_var_end_ptr_ + 4;
	  *(int32_t *)(tag_var_end_ptr_ ) = 0;
    tag_var_end_ptr_ = tag_var_end_ptr_ + 4;
	}
      }

      return;
    }

    char *GetBuf(size_t *size) {
      if (flag_ == 0) {

      } else if (flag_ == 1) {

      } else if (flag_ == 2) {
	if (tag_end_ptr_ != tag_var_start_ptr_) {
	  // there are null in tag
	  // ?????????????????
	}
	*size = tag_var_end_ptr_ - buf_start_ptr_;
	return buf_start_ptr_;
      }

      *size = 0;
      return nullptr;
    }

  private:
    void init0() {
      max_size_ = header_size_;

      size_t tag_col_num = 0;
      size_t pri_len = 0;
      size_t fix_len = 0;
      size_t var_len = 0;
      for (auto & i : schema_) {
        if (i.col_flag == COL_PRIMARY_TAG) {
          tag_col_num += 1;
          pri_len += i.length;
          fix_len += i.length;
        } else if (i.col_flag == COL_GENERAL_TAG) {
          tag_col_num += 1;
          if (i.type == DATATYPE::VARSTRING) {
            fix_len += 8;
            var_len += 2 + i.length;
          } else {
            fix_len += i.length;
          }
        } else {
          max_size_ += 8 + 1 + 2 + i.length; //treat all type as var type
        }
      }
      max_size_ += pri_len + fix_len + var_len;
      max_size_ += 2; //primaryTagLen
      max_size_ += (tag_col_num + 7) / 8; //parimayTagBitMap
      max_size_ += 4; //tagLen
      max_size_ += 4; //dataLen
      max_size_ += 4; //dataOffset

      buf_start_ptr_ = new char[max_size_];
      buf_end_ptr_ = buf_start_ptr_ + max_size_;

      primary_tag_start_ptr_ = buf_start_ptr_ + header_size_;
      tag_bit_map_start_ptr_ = primary_tag_start_ptr_ + 2 + pri_len + 4;
      tag_start_ptr_ = tag_bit_map_start_ptr_ + (tag_col_num + 7) / 8;
      tag_var_start_ptr_ = tag_start_ptr_ + fix_len;
      data_start_ptr_ = tag_var_start_ptr_ + var_len;

      init1();

      cur_operand_ = 0;
    }

    void init1() {
       *(int32_t *)(buf_start_ptr_ + row_num_pos_) = 1;
    }

    void setTagBitMap() {
      int byte_pos = cur_tag_operand_ / 8;
      int bit_pos = cur_tag_operand_ % 8;

      tag_bit_map_start_ptr_[byte_pos] =
          (unsigned char)(tag_bit_map_start_ptr_[byte_pos]) | (unsigned char)(1 << bit_pos);
    }

    void fillPrimary(const char *val, size_t len, size_t def_len) {
      size_t copy_len = (len < def_len) ? len : def_len;
      if (flag_ == 1) {
        memcpy(primary_tag_end_ptr_, val, copy_len);
        primary_tag_end_ptr_ += def_len;
      } else {
        memcpy(primary_tag_end_ptr_, val, copy_len);
        primary_tag_end_ptr_ += def_len;

        memcpy(tag_end_ptr_, val, copy_len);
        tag_end_ptr_ += def_len;
      }
    }

    void fillTag(const char *val, size_t len, size_t def_len, bool is_var) {
      size_t copy_len = (len < def_len) ? len : def_len;
      if (is_var) {
        if (val != nullptr) {
          uint64_t offset = tag_var_end_ptr_ - tag_bit_map_start_ptr_;
          memcpy(tag_end_ptr_, &offset, 8);
          tag_end_ptr_ += 8;

          uint16_t len0 = (uint16_t)copy_len;
          memcpy(tag_var_end_ptr_, &len0, 2);
          tag_var_end_ptr_ += 2;

          memcpy(tag_var_end_ptr_, val, copy_len);
          tag_var_end_ptr_ += copy_len;
        } else {
          tag_end_ptr_ += 8;
          setTagBitMap();
        }
      } else {
        if (val != nullptr) {
          memcpy(tag_end_ptr_, val, copy_len);
          tag_end_ptr_ += def_len;
        } else {
          tag_end_ptr_ += def_len;
          setTagBitMap();
        }
      }
    }

    void fillData(const char *val, size_t len, size_t def_len, bool is_var) {

    }
  private:
    static const size_t header_size_ = kwdbts::Payload::header_size_;
    static const size_t txn_pos_ = 0;
    static const size_t group_id_pos_ = 16;
    static const size_t version_pos_ = 18;
    static const size_t database_id_pos_ = 22;
    static const size_t table_id_pos_ = 26;
    static const size_t row_num_pos_ = 34;
    static const size_t flag_pos_ = 38;

    vector<AttributeInfo> schema_;
    int flag_;
    char *buf_start_ptr_;
    char *buf_end_ptr_;
    size_t max_size_;

    int cur_operand_;
    int cur_tag_operand_;
    char *primary_tag_start_ptr_;
    char *primary_tag_end_ptr_;
    char *tag_start_ptr_;
    char *tag_end_ptr_;
    char *tag_bit_map_start_ptr_;
    char *tag_var_start_ptr_;
    char *tag_var_end_ptr_;
    char *data_start_ptr_;
    char *data_end_ptr_;
  };

  std::string db_name_;
  std::string db_path_;

 protected:
  static void setUpTestCase() {}

  static void tearDownTestCase() {}
};

TEST_F(TestTagRuTable, createTableForUndo) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {5, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
  };

  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 110, 119, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  EXPECT_EQ(bt->CreateTableForUndo("t1", db_name_, schema, MMAP_OPEN_NORECURSIVE), 0);
  releaseObject(bt);
}

TEST_F(TestTagRuTable, insertForUndo) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {5, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
  };

  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 110, 119, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);


  PayLoadBuilder pb(schema);
  pb.SetFlag(2);
  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].m_data_type) {
    case DATATYPE::INT64: {
      int64_t val0 = 11;
      pb.Fill((char *)&val0, sizeof(val0));
      }
      break;
    case DATATYPE::VARSTRING: {
      const char *val1 = "v hello world";
      int len = strlen(val1);
      pb.Fill(val1, len);
      }
      break;
    case DATATYPE::CHAR: {
      const char *val2 = "c hello world";
      int len = strlen(val2);
      pb.Fill(val2, len);
      }
      break;
    }
  }

  size_t buf_len = 0;
  char *buf_ptr = pb.GetBuf(&buf_len);
  TSSlice slice{.data = buf_ptr, .len = buf_len};
  kwdbts::Payload tmp_pd(pb.GetSchema(), slice);

  uint32_t entity_id = 110;
  uint32_t group_id = 119;
  int rc = bt->insert(entity_id, group_id, tmp_pd.GetTagAddr());
  if (rc < 0) {
    // insert failed, do actually undo, if success, the row num should be 0
    rc = bt->InsertForUndo(group_id, entity_id, tmp_pd.GetPrimaryTag());
    if (rc >= 0) {
      EXPECT_EQ(bt->size(), 0);
    } else {
      EXPECT_TRUE(rc < 0) << "insertForUndo failed";
    }
  } else {
    // insert success, don't do undo, only do check, the row num should be 1
    uint32_t entity_id1;
    uint32_t group_id1;
    rc = bt->getEntityIdGroupId(tmp_pd.GetPrimaryTag().data,
                                tmp_pd.GetPrimaryTag().len,
                                entity_id1,
                                group_id1);
    EXPECT_GE(rc, 0);
    EXPECT_EQ(entity_id1, entity_id);
    EXPECT_EQ(group_id1, group_id);
    rc = bt->InsertForUndo(group_id, entity_id, tmp_pd.GetPrimaryTag());
    EXPECT_EQ(bt->size(), 1);
    EXPECT_EQ(rc, 0);
  }

  EXPECT_EQ(bt->remove(), 0);
  releaseObject(bt);
}

// when delete is implemented, supplement this section
TEST_F(TestTagRuTable, deleteForUndo) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {5, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
  };

  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 110, 119, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);


  PayLoadBuilder pb(schema);
  pb.SetFlag(2);
  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].m_data_type) {
    case DATATYPE::INT64: {
      int64_t val0 = 11;
      pb.Fill((char *)&val0, sizeof(val0));
      }
      break;
    case DATATYPE::VARSTRING: {
      const char *val1 = "v hello world";
      int len = strlen(val1);
      pb.Fill(val1, len);
      }
      break;
    case DATATYPE::CHAR: {
      const char *val2 = "c hello world";
      int len = strlen(val2);
      pb.Fill(val2, len);
      }
      break;
    }
  }

  size_t buf_len = 0;
  char *buf_ptr = pb.GetBuf(&buf_len);
  TSSlice slice{.data = buf_ptr, .len = buf_len};
  kwdbts::Payload tmp_pd(pb.GetSchema(), slice);

  uint32_t entityid = 110;
  uint32_t groupid = 119;
  int rc = bt->insert(entityid, groupid, tmp_pd.GetTagAddr());
  EXPECT_GE(rc, 0);
  EXPECT_EQ(bt->size(), 1);

  TagTuplePack ttlog = bt->GenTagPack(tmp_pd.GetPrimaryTag().data, tmp_pd.GetPrimaryTag().len);
  EXPECT_NE(ttlog.getData().data, nullptr);

  TSSlice slice0 = tmp_pd.GetPrimaryTag();
  rc = bt->DeleteTagRecord(tmp_pd.GetPrimaryTag().data, tmp_pd.GetPrimaryTag().len, err_info);
  if (rc < 0) {
    // delete failed, do actually undo, if success, the row num should be 1
    rc = bt->DeleteForUndo(groupid, entityid, slice0, ttlog.getData());
    if (rc >= 0) {
      EXPECT_EQ(bt->size(), 1);
    } else {
      EXPECT_TRUE(rc < 0);
    }
  } else {
    // delete is a marked delete, the row number don't change
    EXPECT_EQ(bt->size(), 1);
    rc = bt->DeleteForUndo(groupid, entityid, slice0, ttlog.getData());
    ASSERT_TRUE(rc >= 0);
    uint32_t entityid1;
    uint32_t groupid1;
    rc = bt->getEntityIdGroupId(tmp_pd.GetPrimaryTag().data,
				tmp_pd.GetPrimaryTag().len,
				entityid1,
				groupid1);
    EXPECT_GE(rc, 0);
    EXPECT_EQ(entityid1, entityid);
    EXPECT_EQ(groupid1, groupid);
  }

  EXPECT_EQ(bt->remove(), 0);
  releaseObject(bt);
}

TEST_F(TestTagRuTable, alterAddRU) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {5, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
  };

  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 110, 119, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);


  PayLoadBuilder pb(schema);
  pb.SetFlag(2);
  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].m_data_type) {
    case DATATYPE::INT64: {
      int64_t val0 = 11;
      pb.Fill((char *)&val0, sizeof(val0));
      }
      break;
    case DATATYPE::VARSTRING: {
      const char *val1 = "v hello world";
      int len = strlen(val1);
      pb.Fill(val1, len);
      }
      break;
    case DATATYPE::CHAR: {
      const char *val2 = "c hello world";
      int len = strlen(val2);
      pb.Fill(val2, len);
      }
      break;
    }
  }

  size_t buf_len = 0;
  char *buf_ptr = pb.GetBuf(&buf_len);
  TSSlice slice{.data = buf_ptr, .len = buf_len};
  kwdbts::Payload tmp_pd(pb.GetSchema(), slice);

  uint32_t entityid = 110;
  uint32_t groupid = 119;
  int rc = bt->insert(entityid, groupid, tmp_pd.GetTagAddr());
  EXPECT_GE(rc, 0);
  EXPECT_EQ(bt->size(), 1);


  TagInfo add_tag_info_new{6, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG};
  TagInfo add_tag_info_old{6, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG};
  rc = bt->AlterTableForUndo(groupid, entityid,
                             add_tag_info_old, add_tag_info_new, 3);
  EXPECT_GE(rc, 0);

  rc = bt->AddTagColumn(add_tag_info_new, err_info);
  EXPECT_GE(rc, 0);

  char col_valbuf[128]= {0};
  char *col_1_ptr = col_valbuf;
  char *col_6_ptr = col_valbuf + 8;
  rc = bt->getColumnValue(1, 0, col_1_ptr);
  EXPECT_GE(rc, 0);
  rc = bt->getColumnValue(1, 5, col_6_ptr);
  EXPECT_GE(rc, 0);

  rc = bt->AlterTableForUndo(groupid, entityid,
                             add_tag_info_old, add_tag_info_new, 3);
  EXPECT_GE(rc, 0);

  rc = bt->getColumnValue(1, 0, col_1_ptr);
  EXPECT_GE(rc, 0);

  EXPECT_EQ(bt->remove(), 0);
  releaseObject(bt);
}

TEST_F(TestTagRuTable, alterDropRU) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {5, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
  };

  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 110, 119, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);


  PayLoadBuilder pb(schema);
  pb.SetFlag(2);
  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].m_data_type) {
    case DATATYPE::INT64: {
      int64_t val0 = 11;
      pb.Fill((char *)&val0, sizeof(val0));
      }
      break;
    case DATATYPE::VARSTRING: {
      const char *val1 = "v hello world";
      int len = strlen(val1);
      pb.Fill(val1, len);
      }
      break;
    case DATATYPE::CHAR: {
      const char *val2 = "c hello world";
      int len = strlen(val2);
      pb.Fill(val2, len);
      }
      break;
    }
  }

  size_t buf_len = 0;
  char *buf_ptr = pb.GetBuf(&buf_len);
  TSSlice slice{.data = buf_ptr, .len = buf_len};
  kwdbts::Payload tmp_pd(pb.GetSchema(), slice);

  uint32_t entityid = 110;
  uint32_t groupid = 119;
  int rc = bt->insert(entityid, groupid, tmp_pd.GetTagAddr());
  EXPECT_GE(rc, 0);
  EXPECT_EQ(bt->size(), 1);

  TagInfo drop_tag_info_new{3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG};
  TagInfo drop_tag_info_old{3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG};

  rc = bt->AlterTableForUndo(groupid, entityid,
                             drop_tag_info_old, drop_tag_info_new, 4);
  // the column don't been deleted, so undo must success.
  EXPECT_GE(rc, 0);

  rc = bt->DropTagColumn(drop_tag_info_old, err_info);
  EXPECT_GE(rc, 0);

  char col_val_buf[128]= {0};
  char *col_1_ptr = col_val_buf;
  char *col_6_ptr = col_val_buf + 8;
  rc = bt->getColumnValue(1, 0, col_1_ptr);
  EXPECT_GE(rc, 0);
  rc = bt->getColumnValue(1, 3, col_6_ptr);
  EXPECT_GE(rc, 0);

  rc = bt->AlterTableForUndo(groupid, entityid,
                             drop_tag_info_old, drop_tag_info_new, 4);
  EXPECT_GE(rc, -1);

  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].m_data_type) {
    case DATATYPE::INT64: {
      int64_t val0 = 11;
      pb.Fill((char *)&val0, sizeof(val0));
      }
      break;
    case DATATYPE::VARSTRING: {
      const char *val1 = "v hello world";
      int len = strlen(val1);
      pb.Fill(val1, len);
      }
      break;
    case DATATYPE::CHAR: {
      const char *val2 = "c hello world";
      int len = strlen(val2);
      pb.Fill(val2, len);
      }
      break;
    }
  }
  buf_len = 0;
  buf_ptr = pb.GetBuf(&buf_len);
  TSSlice slice1{.data = buf_ptr, .len = buf_len};
  kwdbts::Payload tmp_pd1(pb.GetSchema(), slice1);

  entityid = 111;
  groupid = 120;
  rc = bt->insert(entityid, groupid, tmp_pd1.GetTagAddr());
  EXPECT_GE(rc, 0);
  EXPECT_EQ(bt->size(), 2);

  EXPECT_EQ(bt->remove(), 0);
  releaseObject(bt);
}

TEST_F(TestTagRuTable, alterAlterRU) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {5, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
  };

  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 110, 119, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);


  PayLoadBuilder pb(schema);
  pb.SetFlag(2);
  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].m_data_type) {
    case DATATYPE::INT64: {
      int64_t val0 = 11;
      pb.Fill((char *)&val0, sizeof(val0));
      }
      break;
    case DATATYPE::INT32: {
      int32_t val0 = 11;
      pb.Fill((char *)&val0, sizeof(val0));
      }
      break;
    case DATATYPE::VARSTRING: {
      const char *val1 = "v hello world";
      int len = strlen(val1);
      pb.Fill(val1, len);
      }
      break;
    case DATATYPE::CHAR: {
      const char *val2 = "c hello world";
      int len = strlen(val2);
      pb.Fill(val2, len);
      }
      break;
    }
  }

  size_t buf_len = 0;
  char *buf_ptr = pb.GetBuf(&buf_len);
  TSSlice slice{.data = buf_ptr, .len = buf_len};
  kwdbts::Payload tmp_pd(pb.GetSchema(), slice);

  uint32_t entityid = 110;
  uint32_t groupid = 119;
  int rc = bt->insert(entityid, groupid, tmp_pd.GetTagAddr());
  EXPECT_GE(rc, 0);
  EXPECT_EQ(bt->size(), 1);

  TagInfo alter_tag_info_new{1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG};
  TagInfo alter_tag_info_old{1, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG};

  rc = bt->AlterTableForUndo(groupid, entityid,
                             alter_tag_info_old, alter_tag_info_new, 2);
  // the column don't been deleted, so undo must success.
  EXPECT_GE(rc, 0);

  rc = bt->AlterTagType(alter_tag_info_old, alter_tag_info_new, err_info);
  EXPECT_GE(rc, 0);

  char col_val_buf[128]= {0};
  char *col_1_ptr = col_val_buf;
  char *col_6_ptr = col_val_buf + 8;
  rc = bt->getColumnValue(1, 0, col_1_ptr);
  EXPECT_GE(rc, 0);
  rc = bt->getColumnValue(1, 3, col_6_ptr);
  EXPECT_GE(rc, 0);

  rc = bt->AlterTableForUndo(groupid, entityid,
                             alter_tag_info_old, alter_tag_info_new, 2);
  EXPECT_GE(rc, 0);

  rc = bt->getColumnValue(1, 0, col_1_ptr);
  EXPECT_GE(rc, 0);

  EXPECT_EQ(bt->remove(), 0);
  releaseObject(bt);
}

//TestTagRuTable_cleanTagFiles_Test::TestBody
TEST_F(TestTagRuTable, cleanTagFiles) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT32, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {5, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
  };

  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 110, 119, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  bt->setDrop();
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 110, 101);

  string filename(db_path_ + db_name_ + "/110.tag.pt");
  errno = 0;
  int rc = open(filename.c_str(), O_RDWR);
  int err_num = errno;
  EXPECT_LT(rc, 0);
  EXPECT_EQ(err_num, ENOENT);

  filename = db_path_ + db_name_ + "/110.tag.header";
  errno = 0;
  rc = open(filename.c_str(), O_RDWR);
  err_num = errno;
  EXPECT_LT(rc, 0);
  EXPECT_EQ(err_num, ENOENT);

  filename = db_path_ + db_name_ + "/110.tag.ht";
  errno = 0;
  rc = open(filename.c_str(), O_RDWR);
  err_num = errno;
  EXPECT_LT(rc, 0);
  EXPECT_EQ(err_num, ENOENT);

  filename = db_path_ + db_name_ + "/110.tag.1";
  errno = 0;
  rc = open(filename.c_str(), O_RDWR);
  err_num = errno;
  EXPECT_LT(rc, 0);
  EXPECT_EQ(err_num, ENOENT);

  EXPECT_EQ(bt->remove(), 0);
  releaseObject(bt);

  bt = CreateTagTable(schema, db_path_, db_name_, 110, 119, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  TagInfo alter_tag_info_new{1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG};
  TagInfo alter_tag_info_old{1, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG};

  rc = bt->AlterTagType(alter_tag_info_old, alter_tag_info_new, err_info);
  EXPECT_GE(rc, 0);

  bt->setDrop();
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 110, 101);

  filename = db_path_ + db_name_ + "/110.tag.1.old";
  errno = 0;
  rc = open(filename.c_str(), O_RDWR);
  err_num = errno;
  EXPECT_LT(rc, 0);
  EXPECT_EQ(err_num, ENOENT);

  EXPECT_EQ(bt->remove(), 0);
  releaseObject(bt);
}

TEST_F(TestTagRuTable, genTagPack) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {5, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
  };

  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 110, 119, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);


  PayLoadBuilder pb(schema);
  pb.SetFlag(2);
  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].m_data_type) {
    case DATATYPE::INT64: {
      int64_t val0 = 11;
      pb.Fill((char *)&val0, sizeof(val0));
      }
      break;
    case DATATYPE::VARSTRING: {
      const char *val1 = "v hello world";
      int len = strlen(val1);
      pb.Fill(val1, len);
      }
      break;
    case DATATYPE::CHAR: {
      const char *val2 = "c hello world";
      int len = strlen(val2);
      pb.Fill(val2, len);
      }
      break;
    }
  }

  size_t buf_len = 0;
  char *buf_ptr = pb.GetBuf(&buf_len);
  TSSlice slice{.data = buf_ptr, .len = buf_len};
  kwdbts::Payload tmp_pd(pb.GetSchema(), slice);

  uint32_t entity_id = 110;
  uint32_t group_id = 119;
  int rc = bt->insert(entity_id, group_id, tmp_pd.GetTagAddr());
  EXPECT_GE(rc, 0);

  TagTuplePack ttp = bt->GenTagPack(tmp_pd.GetPrimaryTag().data,
				     tmp_pd.GetPrimaryTag().len);
  EXPECT_EQ(ttp.getEntityId(), entity_id);
  EXPECT_EQ(ttp.getSubgroupId(), group_id);

  TSSlice ttp_pt = ttp.getPrimaryTags();
  EXPECT_EQ(ttp_pt.len, tmp_pd.GetPrimaryTag().len);
  EXPECT_EQ(memcmp(ttp_pt.data, tmp_pd.GetPrimaryTag().data, ttp_pt.len), 0);

  TSSlice ttpt = ttp.getTags();
  EXPECT_EQ(ttpt.len, tmp_pd.GetTagLen());
  EXPECT_EQ(memcmp(ttpt.data, tmp_pd.GetTagAddr(), ttpt.len), 0);

  EXPECT_EQ(bt->remove(), 0);
  releaseObject(bt);
}
