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
#ifndef KWDBTS2_EXEC_TESTS_EE_TEST_UTIL_H_
#define KWDBTS2_EXEC_TESTS_EE_TEST_UTIL_H_

#include <dirent.h>
#include <gtest/gtest.h>
#include <linux/magic.h>
#include <string.h>
#include <sys/statfs.h>
#include <unistd.h>

#include <any>
#include <iostream>

#include "utils/big_table_utils.h"
#include "data_type.h"
#include "engine.h"
#include "payload.h"
#include "sys_utils.h"

#define Def_Column(col_var, pname, ptype, poffset, psize, plength, pencoding, \
                   pflag, pmax_len, pversion)                                 \
  struct AttributeInfo col_var;                                               \
  {                                                                           \
    col_var.name = pname;                                                     \
    col_var.type = ptype;                                                     \
    col_var.offset = poffset;                                                 \
    col_var.size = psize;                                                     \
  }                                                                           \
  {                                                                           \
    col_var.length = plength;                                                 \
    col_var.encoding = pencoding;                                             \
    col_var.flag = pflag;                                                     \
    col_var.max_len = pmax_len;                                               \
    col_var.version = pversion;                                               \
  }

#define STORE_HOME (BigObjectConfig::home())
#define DIR_SEP "/"


extern "C" {
// Tests are run in plain C++, we need a symbol for isCanceledCtx, normally
// implemented on the Go side.
bool __attribute__((weak)) isCanceledCtx(uint64_t goCtxPtr) { return false; }
}  // extern "C"


int IsDbNameValid(const string& db) {
  if (db.size() > MAX_DATABASE_NAME_LEN)  // can`t longer than 63
    return KWELENLIMIT;
  for (size_t i = 0; i < db.size(); ++i) {
    char c = db[i];
    if (c == '?' || c == '*' || c == ':' || c == '|' || c == '"' || c == '<' ||
        c == '>' || c == '.')
      return KWEINVALIDNAME;
  }
  return 0;
}

bool RemoveDirectory(const char* path) {
  DIR* dir = opendir(path);
  if (dir == nullptr) {
    // open dir failed
    return false;
  }

  dirent* entry;
  while ((entry = readdir(dir)) != nullptr) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      // skip . ..
      continue;
    }

    // Construct the full path to the file/folder
    std::string full_path = std::string(path) + "/" + entry->d_name;
    if (entry->d_type == DT_DIR) {
      // if dir，Recursive deletion
      if (!RemoveDirectory(full_path.c_str())) {
        closedir(dir);
        return false;
      }
    } else {
      // if file，delete
      if (remove(full_path.c_str()) != 0) {
        closedir(dir);
        return false;
      }
    }
  }

  // close
  closedir(dir);

  // rm dir
  if (rmdir(path) != 0) {
    return false;
  }

  return true;
}

class BtUtil {
 public:
  static int CreateDB(const std::string& db, size_t life_cycle,
                      ErrorInfo& err_info) {
    string db_path = normalizePath(db);
    string ws = worksapceToDatabase(db_path);
    if (ws == "") return err_info.setError(KWEINVALIDNAME, db);
    int err_code = IsDbNameValid(ws);
    if (err_code != 0) err_info.setError(err_code, db);
    string dir_path = makeDirectoryPath(kwdbts::EngineOptions::home() + ws);
    MakeDirectory(dir_path, err_info);
    return err_info.errcode;
  }

  static int CheckError(const char* msg, ErrorInfo err_info) {
    if (err_info.errcode != 0) {
      fprintf(stderr, "%s : %s\n", msg, err_info.errmsg.c_str());
    }
    return err_info.errcode;
  }

  static void PrintTable(BigTable* bt) {
    fprintf(stdout, "----------Print Table Data----------\n");
    for (int r = 1; r <= bt->size(); r++) {
      bt->printRecord(std::cout, r);
      fprintf(stdout, "\n");
    }
  }
};

struct ZTableColumnMeta {
  roachpb::DataType type;        // col type
  kwdbts::k_uint32 storage_len;  // max len
  kwdbts::k_uint32 actual_len;   // real len
  roachpb::VariableLengthType storage_type;
};

const kwdbts::k_uint32 g_testcase_col_count = 20;
void ConstructColumnMetas(std::vector<ZTableColumnMeta>* metas) {
  // construct all types
  metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8,
                    roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::SMALLINT, 2, 2,
                    roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::INT, 4, 4,
                    roachpb::VariableLengthType::ColStorageTypeTuple});
}
void ConstructTagMetas(std::vector<ZTableColumnMeta>* metas) {
  // construct all tag types
  metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8,
                    roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::INT, 4, 4,
                    roachpb::VariableLengthType::ColStorageTypeTuple});
}

void ConstructVarColumnMetas(std::vector<ZTableColumnMeta>* metas) {
  // construct all col types
  metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8,
                    roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::CHAR, 8, 8,
                    roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 8, 32,
                    roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::CHAR, 8, 8,
                    roachpb::VariableLengthType::ColStorageTypeTuple});
}

void constructRoachpbTable(roachpb::CreateTsTable* meta,
                           const kwdbts::KString& prefix_table_name,
                           kwdbts::KTableKey table_id,
                           uint64_t partition_interval = kwdbts::EngineOptions::iot_interval) {
  // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL |
  // BINARY(binary_len)
  roachpb::KWDBTsTable* table = KNEW roachpb::KWDBTsTable();
  table->set_ts_table_id(table_id);
  table->set_table_name(prefix_table_name + std::to_string(table_id));
  table->set_partition_interval(partition_interval);
  meta->set_allocated_ts_table(table);

  std::vector<ZTableColumnMeta> col_meta;
  ConstructColumnMetas(&col_meta);

  for (int i = 0; i < col_meta.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(col_meta[i].type));
    column->set_storage_len(col_meta[i].storage_len);
    column->set_column_id(i + 1);
    if (i == 0) {
      column->set_name("k_timestamp");  // first ts name: k_timestamp
    } else {
      column->set_name("column" + std::to_string(i + 1));
    }
  }
  // add tag
  std::vector<ZTableColumnMeta> tag_metas;
  ConstructTagMetas(&tag_metas);
  for (int i = 0; i < tag_metas.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(tag_metas[i].type));
    column->set_storage_len(tag_metas[i].storage_len);
    column->set_column_id(tag_metas.size() + 1 + i);
    if (i == 0) {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::
                               KWDBKTSColumn_ColumnType_TYPE_PTAG);
    } else {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::
                               KWDBKTSColumn_ColumnType_TYPE_TAG);
    }
    column->set_name("tag" + std::to_string(i + 1));
  }
}

void constructVarRoachpbTable(roachpb::CreateTsTable* meta,
                              const KString& prefix_table_name,
                              KTableKey table_id) {
  // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL |
  // BINARY(binary_len)
  roachpb::KWDBTsTable* table = KNEW roachpb::KWDBTsTable();
  table->set_ts_table_id(table_id);
  table->set_table_name(prefix_table_name + std::to_string(table_id));
  meta->set_allocated_ts_table(table);

  std::vector<ZTableColumnMeta> col_meta;
  ConstructVarColumnMetas(&col_meta);

  for (int i = 0; i < col_meta.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(col_meta[i].type));
    column->set_storage_len(col_meta[i].storage_len);
    column->set_column_id(i + 1);
    if (i == 0) {
      column->set_name("k_timestamp");  // first ts name: k_timestamp
    } else {
      column->set_name("column" + std::to_string(i + 1));
    }
  }
  // add tag
  std::vector<ZTableColumnMeta> tag_metas;
  ConstructTagMetas(&tag_metas);
  for (int i = 0; i < tag_metas.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(tag_metas[i].type));
    column->set_storage_len(tag_metas[i].storage_len);
    column->set_column_id(tag_metas.size() + 1 + i);
    if (i == 0) {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::
                               KWDBKTSColumn_ColumnType_TYPE_PTAG);
    } else {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::
                               KWDBKTSColumn_ColumnType_TYPE_TAG);
    }
    column->set_name("tag" + std::to_string(i + 1));
  }
}

class TestBigTableInstance : public ::testing::Test {
 public:
  static const string kw_home;  // The current directory is the storage
                                // directory of the big table
  static const string db_name;  // database name
  static const uint64_t iot_interval;

 protected:
  virtual void SetUp() {
    // The current directory is the storage directory of the big table
    setenv("KW_HOME", kw_home.c_str(), 1);
    setenv("KW_IOT_INTERVAL", std::to_string(iot_interval).c_str(), 1);
    EngineOptions::init();
    ErrorInfo err_info;
    // clean database data
    system(("rm -rf " + kw_home + "default*").c_str());
    system(("rm -rf " + kw_home + DIR_SEP + db_name + "*").c_str());

    err_info.clear();
  }

  virtual void TearDown() {}
};

// payload
const static int g_header_size = Payload::header_size_;

void genPayloadTagData(kwdbts::Payload& payload,
                        std::vector<AttributeInfo>& tag_schema,
                        kwdbts::KTimestamp start_ts,
                        bool fix_primary_tag = true) {
  if (fix_primary_tag) {
    start_ts = 100;
  }
  char* primary_start_ptr = payload.GetPrimaryTagAddr();
  char* tag_data_start_ptr = payload.GetTagAddr() + (tag_schema.size() + 7) / 8;
  char* bitmap_ptr = payload.GetTagAddr();
  char* var_data_ptr = tag_data_start_ptr + (tag_schema.back().offset + tag_schema.back().size);
  std::string var_str = std::to_string(start_ts);
  for (int i = 0; i < tag_schema.size(); i++) {
    // generate primary tag
    if (tag_schema[i].isAttrType(COL_PRIMARY_TAG)) {
      switch (tag_schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          KTimestamp(primary_start_ptr) = start_ts;
          primary_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT8:
          *(static_cast<kwdbts::k_int8*>(
              static_cast<void*>(primary_start_ptr))) = 10;
          primary_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT32:
          KInt32(primary_start_ptr) = start_ts;
          primary_start_ptr += tag_schema[i].size;
          break;
        default:
          break;
      }
    }
    // generate tag
    switch (tag_schema[i].type) {
      case DATATYPE::TIMESTAMP64:
        KTimestamp(tag_data_start_ptr) = start_ts;
        tag_data_start_ptr += tag_schema[i].size;
        break;
      case DATATYPE::INT8:
        *(static_cast<kwdbts::k_int8*>(
            static_cast<void*>(tag_data_start_ptr))) = 10;
        tag_data_start_ptr += tag_schema[i].size;
        break;
      case DATATYPE::INT32:
        KInt32(tag_data_start_ptr) = start_ts;
        tag_data_start_ptr += tag_schema[i].size;
        break;
      case DATATYPE::VARSTRING:
        KInt64(tag_data_start_ptr) = var_data_ptr - bitmap_ptr;
        tag_data_start_ptr += tag_schema[i].size;
        KInt16(var_data_ptr) = var_str.length();
        memcpy(var_data_ptr + 2, var_str.c_str(), var_str.length());
        var_data_ptr += 2 + var_str.length();
          break;
      default:
        break;
    }
  }
  return;
}

char* GenSomePayloadData(kwdbContext_p ctx, k_uint32 count,
                         k_uint32& payload_length,
                         KTimestamp start_ts,
                         roachpb::CreateTsTable* meta,
                         k_uint32 ms_interval = 10, int test_value = 0,
                         bool fix_entityid = true) {
  vector<AttributeInfo> schema;
  vector<AttributeInfo> tag_schema;
  kwdbts::k_int32 tag_value_len = 0;
  payload_length = 0;
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  kwdbts::k_int32 tag_offset = 0;

  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
    payload_length += col_var.size;
    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_value_len += col_var.size;
      col_var.offset = tag_offset;
      tag_offset += col_var.size;
      if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
        tag_value_len += (32 + 2);
      }
      tag_schema.emplace_back(std::move(col_var));
    } else {
      payload_length += col_var.size;
      if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
        payload_length += (test_str.size() + 2);
      }
      schema.push_back(std::move(col_var));
    }
  }

  kwdbts::k_uint32 header_len = g_header_size;
  kwdbts::k_int32 primary_tag_len = 8;
  kwdbts::k_int16 primary_len_len = 2;
  kwdbts::k_int32 tag_len_len = 4;
  kwdbts::k_int32 data_len_len = 4;
  kwdbts::k_int32 bitmap_len = (count + 7) / 8;
  tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
  kwdbts::k_int32 data_len =  payload_length * count + bitmap_len * schema.size();
  kwdbts::k_uint32 data_length = header_len + primary_len_len +
                                 primary_tag_len + tag_len_len + tag_value_len +
                                 data_len_len + data_len;
  char* value = new char[data_length];
  memset(value, 0, data_length);
  KInt32(value + Payload::row_num_offset_) = count;
  // set primary_len_len
  KInt16(value + g_header_size) = primary_tag_len;
  // set tag_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len) =
      tag_value_len;
  // set data_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len + tag_len_len +
         tag_value_len) = data_len;
  kwdbts::Payload p(schema, {value, data_length});
  int16_t len = 0;
  genPayloadTagData(p, tag_schema, start_ts, fix_entityid);
  uint64_t var_exist_len = 0;
  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].type) {
      case DATATYPE::TIMESTAMP64:
      case DATATYPE::TIMESTAMP64_LSN:
        for (int j = 0; j < count; j++) {
          KTimestamp(p.GetColumnAddr(j, i)) = start_ts;
          start_ts += ms_interval;
        }
        break;
      case DATATYPE::INT16:
        for (int j = 0; j < count; j++) {
          KInt16(p.GetColumnAddr(j, i)) = 11;
        }
        break;
      case DATATYPE::INT32:
        for (int j = 0; j < count; j++) {
          KInt32(p.GetColumnAddr(j, i)) = 2222;
        }
        break;
      case DATATYPE::CHAR:
        for (int j = 0; j < count; j++) {
          strncpy(p.GetColumnAddr(j, i), test_str.c_str(), schema[i].size);
        }
        break;
      case DATATYPE::VARSTRING:
      case DATATYPE::VARBINARY:
      {
        len = test_str.size();
        uint64_t var_type_offset = 0;
        for (int k = i; k < schema.size(); k++) {
          var_type_offset += (schema[k].size * count + bitmap_len);
        }
        for (int j = 0; j < count; j++) {
          // offset
          KInt64(p.GetColumnAddr(j, i)) = var_type_offset + var_exist_len;
          // len + value
          memcpy(p.GetVarColumnAddr(j, i), &len, 2);
          strncpy(p.GetVarColumnAddr(j, i) + 2, test_str.c_str(), test_str.size());
          var_exist_len += (test_str.size() + 2);
        }
      }
        break;
      default:
        break;
    }
  }
  return value;
}

char* GenPayloadDataWithNull(kwdbContext_p ctx, k_uint32 count,
                             k_uint32& payload_length,
                             KTimestamp start_ts,
                             roachpb::CreateTsTable* meta,
                             k_uint32 ms_interval = 10,
                             int test_value = 0, bool fix_entityid = true) {
  vector<AttributeInfo> schema;
  vector<AttributeInfo> tag_schema;
  kwdbts::k_int32 tag_value_len = 0;
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
    payload_length += col_var.size;
    if (col_var.isAttrType(COL_GENERAL_TAG) ||
        col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_value_len += col_var.size;
      tag_schema.push_back(std::move(col_var));
    } else {
      schema.push_back(std::move(col_var));
    }
  }

  kwdbts::k_uint32 header_len = g_header_size;
  kwdbts::k_int32 primary_tag_len = 8;
  kwdbts::k_int16 primary_len_len = 2;
  kwdbts::k_int32 tag_len_len = 4;
  kwdbts::k_int32 data_len_len = 4;
  kwdbts::k_int32 bitmap_len = (count + 7) / 8;
  tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
  kwdbts::k_int32 data_len = (payload_length + bitmap_len) * count;
  kwdbts::k_uint32 data_length = header_len + primary_len_len +
                                 primary_tag_len + tag_len_len + tag_value_len +
                                 data_len_len + data_len;
  char* value = new char[data_length];
  memset(value, 0, data_length);
  KInt32(value + Payload::row_num_offset_) = count;
  // set primary_len_len
  KInt16(value + g_header_size) = primary_tag_len;
  // set tag_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len) =
      tag_value_len;
  // set data_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len + tag_len_len +
         tag_value_len) = data_len;
  Payload p(schema, {value, data_length});

  genPayloadTagData(p, tag_schema, start_ts, fix_entityid);

  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].type) {
      case DATATYPE::TIMESTAMP64:
      case DATATYPE::TIMESTAMP64_LSN:
        for (int j = 0; j < count; j++) {
          KTimestamp(p.GetColumnAddr(j, i)) = start_ts;
          start_ts += ms_interval;
        }
        break;
      case DATATYPE::INT16:
        for (int j = 0; j < count; j++) {
          *(static_cast<kwdbts::k_int8*>(
              static_cast<void*>(p.GetColumnAddr(j, i)))) = 11;
        }
        break;
      case DATATYPE::INT32:
        for (int j = 0; j < count; j++) {
          int row_id = j;
          auto bitmap = (unsigned char*)(p.GetNullBitMapAddr(i));
          unsigned char bit_pos = (1 << (j & 7));
          row_id = row_id >> 3;
          bitmap[row_id] |= bit_pos;
        }
        break;
      default:
        break;
    }
  }
  return value;
}
#endif  // KWDBTS2_EXEC_TESTS_EE_TEST_UTIL_H_
