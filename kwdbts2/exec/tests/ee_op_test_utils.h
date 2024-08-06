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

#pragma once

#include <gtest/gtest.h>
#include <string>
#include <unistd.h>

#include "ts_table.h"

namespace kwdbts {

struct zTableColumnMeta {
  roachpb::DataType type;        // col type
  k_uint32 storage_len;  // length
  k_uint32 actual_len;   // the len of bytes stored
  roachpb::VariableLengthType storage_type;
};

class BaseSchema {
 public:
  static void constructColumnMetas(std::vector<zTableColumnMeta>& metas) {
    metas.push_back({roachpb::DataType::TIMESTAMP, 8, 8,
                     roachpb::VariableLengthType::ColStorageTypeTuple});
    metas.push_back({roachpb::DataType::SMALLINT, 2, 2,
                     roachpb::VariableLengthType::ColStorageTypeTuple});
    metas.push_back({roachpb::DataType::INT, 4, 4,
                     roachpb::VariableLengthType::ColStorageTypeTuple});
  }

  static void constructTagMetas(std::vector<zTableColumnMeta>& metas) {
    metas.push_back({roachpb::DataType::TIMESTAMP, 8, 8,
                     roachpb::VariableLengthType::ColStorageTypeTuple});
    metas.push_back({roachpb::DataType::INT, 4, 4,
                     roachpb::VariableLengthType::ColStorageTypeTuple});
  }

  static void constructVarColumnMetas(std::vector<zTableColumnMeta>& metas) {
    metas.push_back({roachpb::DataType::TIMESTAMP, 8, 8,
                     roachpb::VariableLengthType::ColStorageTypeTuple});
    metas.push_back({roachpb::DataType::CHAR, 8, 8,
                     roachpb::VariableLengthType::ColStorageTypeTuple});
    metas.push_back({roachpb::DataType::VARCHAR, 8, 32,
                     roachpb::VariableLengthType::ColStorageTypeTuple});
    metas.push_back({roachpb::DataType::CHAR, 8, 8,
                     roachpb::VariableLengthType::ColStorageTypeTuple});
  }

  static void constructTableMetadata(roachpb::CreateTsTable& meta,
                                     const KString& prefix_table_name,
                                     KTableKey table_id) {
    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL |
    // BINARY(binary_len)
    auto* table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id);
    table->set_table_name(prefix_table_name + std::to_string(table_id));
    meta.set_allocated_ts_table(table);

    std::vector<zTableColumnMeta> col_meta;
    constructColumnMetas(col_meta);

    for (int i = 0; i < col_meta.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType) (col_meta[i].type));
      column->set_storage_len(col_meta[i].storage_len);
      column->set_column_id(i + 1);
      if (i == 0) {
        column->set_name("k_timestamp");  // first timestmap name: k_timestamp
      } else {
        column->set_name("column" + std::to_string(i + 1));
      }
//      column->set_nullable(true);
    }
    // add tag attribute
    std::vector<zTableColumnMeta> tag_metas;
    constructTagMetas(tag_metas);
    for (int i = 0; i < tag_metas.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType) (tag_metas[i].type));
      column->set_storage_len(tag_metas[i].storage_len);
      column->set_column_id(tag_metas.size() + 1 + i);
      if (i == 0) {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::
                             KWDBKTSColumn_ColumnType_TYPE_PTAG);
//        column->set_nullable(true);
      } else {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::
                             KWDBKTSColumn_ColumnType_TYPE_TAG);
      }
      column->set_name("tag" + std::to_string(i + 1));
    }
  }

//  static void ConstructVarRoachpbTable(roachpb::CreateTsTable& meta,
//                                       const KString& prefix_table_name,
//                                       KTableKey table_id) {
//    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL |
//    // BINARY(binary_len)
//    auto* table = KNEW roachpb::KWDBTsTable();
//    table->set_ts_table_id(table_id);
//    table->set_table_name(prefix_table_name + std::to_string(table_id));
//    meta.set_allocated_ts_table(table);
//
//    std::vector<ZTableColumnMeta> col_meta;
//    ConstructVarColumnMetas(col_meta);
//
//    for (int i = 0; i < col_meta.size(); i++) {
//      roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
//      column->set_storage_type((roachpb::DataType) (col_meta[i].type));
//      column->set_storage_len(col_meta[i].storage_len);
//      column->set_column_id(i + 1);
//      if (i == 0) {
//        column->set_name("k_timestamp");  // first timestmap name: k_timestamp
//      } else {
//        column->set_name("column" + std::to_string(i + 1));
//      }
//    }
//    // add tag attribute
//    std::vector<ZTableColumnMeta> tag_metas;
//    ConstructTagMetas(tag_metas);
//    for (int i = 0; i < tag_metas.size(); i++) {
//      roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
//      column->set_storage_type((roachpb::DataType) (tag_metas[i].type));
//      column->set_storage_len(tag_metas[i].storage_len);
//      column->set_column_id(tag_metas.size() + 1 + i);
//      if (i == 0) {
//        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::
//                             KWDBKTSColumn_ColumnType_TYPE_PTAG);
//      } else {
//        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::
//                             KWDBKTSColumn_ColumnType_TYPE_TAG);
//      }
//      column->set_name("tag" + std::to_string(i + 1));
//    }
//  }

// payload
  const static int HEADER_SIZE = Payload::header_size_;

  static void genPayloadTagData(Payload& payload,
                                std::vector<AttributeInfo>& tag_schema,
                                KTimestamp start_ts,
                                bool fix_primary_tag = true) {
    if (fix_primary_tag) {
      start_ts = 100;
    }
    char* primary_start_ptr = payload.GetPrimaryTagAddr();
    char* tag_data_start_ptr = payload.GetTagAddr() + (tag_schema.size() + 7) / 8;
    char* bitmap_ptr = payload.GetTagAddr();
    char* var_data_ptr = tag_data_start_ptr + (tag_schema.back().offset + tag_schema.back().size);
    std::string var_str = std::to_string(start_ts);
    for (auto& info : tag_schema) {
      // Generate the Primaritag part
      if (info.isAttrType(ATTR_PRIMARY_TAG)) {
        switch (info.type) {
          case DATATYPE::TIMESTAMP64:
            KTimestamp(primary_start_ptr) = start_ts;
            primary_start_ptr += info.size;
            break;
          case DATATYPE::INT8:
            *(static_cast<k_int8*>(
                static_cast<void*>(primary_start_ptr))) = 10;
            primary_start_ptr += info.size;
            break;
          case DATATYPE::INT32:
            KInt32(primary_start_ptr) = start_ts;
            primary_start_ptr += info.size;
            break;
          default:
            break;
        }
      }
      // Generate the tag part
      switch (info.type) {
        case DATATYPE::TIMESTAMP64:
          KTimestamp(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += info.size;
          break;
        case DATATYPE::INT8:
          *(static_cast<k_int8*>(
              static_cast<void*>(tag_data_start_ptr))) = 10;
          tag_data_start_ptr += info.size;
          break;
        case DATATYPE::INT32:
          KInt32(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += info.size;
          break;
        case DATATYPE::VARSTRING:
          KInt64(tag_data_start_ptr) = var_data_ptr - bitmap_ptr;
          tag_data_start_ptr += info.size;
          KInt16(var_data_ptr) = var_str.length();
          memcpy(var_data_ptr + 2, var_str.c_str(), var_str.length());
          var_data_ptr += 2 + var_str.length();
          break;
        default:
          break;
      }
    }
  }

  static unique_ptr<char[]> genPayloadData(kwdbContext_p ctx, k_uint32 count,
                                           k_uint32& payload_length,
                                           KTimestamp start_ts,
                                           roachpb::CreateTsTable& meta,
                                           k_uint32 ms_interval = 10, int test_value = 0,
                                           bool fix_entityid = true) {
    vector<AttributeInfo> schema;
    vector<AttributeInfo> tag_schema;
    k_int32 tag_value_len = 0;
    payload_length = 0;
    string test_str = "abcdefghijklmnopqrstuvwxyz";
    k_int32 tag_offset = 0;

    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      if (i == 0) {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, true);
      } else {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, false);
      }
      payload_length += col_var.size;
      if (col_var.isAttrType(ATTR_GENERAL_TAG) || col_var.isAttrType(ATTR_PRIMARY_TAG)) {
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

    k_uint32 header_len = HEADER_SIZE;
    k_int32 primary_tag_len = 8;
    k_int16 primary_len_len = 2;
    k_int32 tag_len_len = 4;
    k_int32 data_len_len = 4;
    k_int32 bitmap_len = (count + 7) / 8;
    tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
    k_int32 data_len = payload_length * count + bitmap_len * schema.size();
    k_uint32 data_length = header_len + primary_len_len +
                           primary_tag_len + tag_len_len + tag_value_len +
                           data_len_len + data_len;
    auto value = make_unique<char[]>(data_length);
    memset(value.get(), 0, data_length);
    KInt32(value.get() + Payload::row_num_offset_) = count;
    // set primary_len_len
    KInt16(value.get() + HEADER_SIZE) = primary_tag_len;
    // set tag_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len) =
        tag_value_len;
    // set data_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len + tag_len_len +
           tag_value_len) = data_len;
    Payload p(schema, {value.get(), data_length});
    int16_t len = 0;
    genPayloadTagData(p, tag_schema, start_ts, fix_entityid);
    uint64_t var_exist_len = 0;
    for (int i = 0; i < schema.size(); i++) {
      switch (schema[i].type) {
        case DATATYPE::TIMESTAMP64:
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
        case DATATYPE::VARBINARY: {
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

  unique_ptr<char[]> genPayloadDataWithNull(kwdbContext_p ctx, k_uint32 count,
                                            k_uint32& payload_length,
                                            KTimestamp start_ts,
                                            roachpb::CreateTsTable& meta,
                                            k_uint32 ms_interval = 10,
                                            int test_value = 0, bool fix_entityid = true) {
    vector<AttributeInfo> schema;
    vector<AttributeInfo> tag_schema;
    k_int32 tag_value_len = 0;
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      if (i == 0) {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, true);
      } else {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, false);
      }
      payload_length += col_var.size;
      if (col_var.isAttrType(ATTR_GENERAL_TAG) ||
          col_var.isAttrType(ATTR_PRIMARY_TAG)) {
        tag_value_len += col_var.size;
        tag_schema.push_back(std::move(col_var));
      } else {
        schema.push_back(std::move(col_var));
      }
    }

    k_uint32 header_len = HEADER_SIZE;
    k_int32 primary_tag_len = 8;
    k_int16 primary_len_len = 2;
    k_int32 tag_len_len = 4;
    k_int32 data_len_len = 4;
    k_int32 bitmap_len = (count + 7) / 8;
    tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
    k_int32 data_len = (payload_length + bitmap_len) * count;
    k_uint32 data_length = header_len + primary_len_len +
                           primary_tag_len + tag_len_len + tag_value_len +
                           data_len_len + data_len;
    auto value = make_unique<char[]>(data_length);
    memset(value.get(), 0, data_length);
    KInt32(value.get() + Payload::row_num_offset_) = count;
    // set primary_len_len
    KInt16(value.get() + HEADER_SIZE) = primary_tag_len;
    // set tag_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len) =
        tag_value_len;
    // set data_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len + tag_len_len +
           tag_value_len) = data_len;
    Payload p(schema, {value.get(), data_length});

    genPayloadTagData(p, tag_schema, start_ts, fix_entityid);

    for (int i = 0; i < schema.size(); i++) {
      switch (schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          for (int j = 0; j < count; j++) {
            KTimestamp(p.GetColumnAddr(j, i)) = start_ts;
            start_ts += ms_interval;
          }
          break;
        case DATATYPE::INT16:
          for (int j = 0; j < count; j++) {
            *(static_cast<k_int8*>(
                static_cast<void*>(p.GetColumnAddr(j, i)))) = 11;
          }
          break;
        case DATATYPE::INT32:
          for (int j = 0; j < count; j++) {
            int row_id = j;
            auto bitmap = (unsigned char*) (p.GetNullBitMapAddr(i));
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
};


class TSBSSchema {
 public:
  static void constructColumnMetas(std::vector<zTableColumnMeta>* metas) {
    // construct all col type
    metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
    for (int i = 0; i < 10; i++) {
      metas->push_back({roachpb::DataType::BIGINT, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
    }
  }

  static void constructTagMetas(std::vector<zTableColumnMeta>* metas) {
    // construct all tag type
    for (int i = 0; i < 10; i++) {
      metas->push_back({roachpb::DataType::CHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
//      metas->push_back({roachpb::DataType::INT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple});
    }
  }

  static void constructTableMetadata(roachpb::CreateTsTable& meta, const KString& prefix_table_name,
                                     KTableKey table_id, uint64_t partition_interval = BigObjectConfig::iot_interval) {
    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
    roachpb::KWDBTsTable* table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id);
    table->set_table_name(prefix_table_name + std::to_string(table_id));
    table->set_partition_interval(partition_interval);
    meta.set_allocated_ts_table(table);

    std::vector<zTableColumnMeta> col_meta;
    constructColumnMetas(&col_meta);

    for (int i = 0; i < col_meta.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType) (col_meta[i].type));
      column->set_storage_len(col_meta[i].storage_len);
      column->set_column_id(i + 1);
      if (i == 0) {
        column->set_name("k_timestamp");  // first timestmap name: k_timestamp
      } else {
        column->set_name("column" + std::to_string(i + 1));
      }
    }
    // add tag
    std::vector<zTableColumnMeta> tag_metas;
    constructTagMetas(&tag_metas);
    for (int i = 0; i < tag_metas.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType) (tag_metas[i].type));
      column->set_storage_len(tag_metas[i].storage_len);
      column->set_column_id(tag_metas.size() + 1 + i);
      if (i == 0) {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
      } else {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
      }
      column->set_name("tag" + std::to_string(i + 1));
    }

  }

  const static int HEADER_SIZE = Payload::header_size_;  // NOLINT

  static void genPayloadTagData(Payload& payload, std::vector<AttributeInfo>& tag_schema,
                                KTimestamp start_ts, bool fix_primary_tag = true) {
    string test_str = "abcdefghijklmnopqrstuvwxyz";
    if (fix_primary_tag) {
      start_ts = 100;
    }
    char* primary_start_ptr = payload.GetPrimaryTagAddr();
    char* tag_data_start_ptr = payload.GetTagAddr() + (tag_schema.size() + 7) / 8;
    for (int i = 0; i < tag_schema.size(); i++) {
      // Generate the Primaritag part
      if (tag_schema[i].isAttrType(ATTR_PRIMARY_TAG)) {
        switch (tag_schema[i].type) {
          case DATATYPE::TIMESTAMP64:
            KTimestamp(primary_start_ptr) = start_ts;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::INT8:
            *(static_cast<k_int8*>(static_cast<void*>(primary_start_ptr))) = 10;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::INT32:
            KInt32(primary_start_ptr) = start_ts;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::CHAR:
            memset(primary_start_ptr, '1', tag_schema[i].size);
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::VARSTRING: {
            strncpy(primary_start_ptr, test_str.c_str(), test_str.length());
            primary_start_ptr += tag_schema[i].size;
          }
            break;
          default:
            break;
        }
      }
      // Generate the tag part
      switch (tag_schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          KTimestamp(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT8:
          *(static_cast<k_int8*>(static_cast<void*>(tag_data_start_ptr))) = 10;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT32:
          KInt32(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::CHAR:
          memset(tag_data_start_ptr, '1', tag_schema[i].size);
          memset(tag_data_start_ptr + tag_schema[i].size - 1, '\0', 1);
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::VARSTRING: {
          *reinterpret_cast<int16_t*>(tag_data_start_ptr) = ((int16_t) test_str.length());
          tag_data_start_ptr += sizeof(int16_t);
          strncpy(tag_data_start_ptr, test_str.c_str(), test_str.length());
          tag_data_start_ptr += test_str.length();
        }
          break;
        default:
          break;
      }
    }
  }

  static unique_ptr<char[]> genPayloadData(kwdbContext_p ctx, k_uint32 count, k_uint32& payload_length,
                                           KTimestamp start_ts,
                                           roachpb::CreateTsTable& meta,
                                           k_uint32 ms_interval = 10, int test_value = 0,
                                           bool fix_entityid = true) {
    vector<AttributeInfo> schema;
    vector<AttributeInfo> tag_schema;
    k_int32 tag_value_len = 0;
    payload_length = 0;
    string test_str = "abcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      if (i == 0) {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, true);
      } else {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, false);
      }
      if (col_var.isAttrType(ATTR_GENERAL_TAG) || col_var.isAttrType(ATTR_PRIMARY_TAG)) {
        tag_value_len += col_var.size;
        tag_schema.emplace_back(std::move(col_var));
      } else {
        payload_length += col_var.size;
        if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
          payload_length += (test_str.size() + 2);
        }
        schema.push_back(std::move(col_var));
      }
    }

    k_uint32 header_len = HEADER_SIZE;
    k_int32 primary_tag_len = 30;
    k_int16 primary_len_len = 2;
    k_int32 tag_len_len = 4;
    k_int32 data_len_len = 4;
    k_int32 bitmap_len = (count + 7) / 8;
    tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
    k_int32 data_len = payload_length * count + bitmap_len * schema.size();
    k_uint32 data_length =
        header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len + data_len_len + data_len;
    payload_length = data_length;
    auto value = make_unique<char[]>(data_length);
    memset(value.get(), 0, data_length);
    KInt32(value.get() + Payload::row_num_offset_) = count;
    // set primary_len_len
    KInt16(value.get() + HEADER_SIZE) = primary_tag_len;
    // set tag_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len) = tag_value_len;
    // set data_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len) = data_len;
    Payload p(schema, {value.get(), data_length});
    int16_t len = 0;
    genPayloadTagData(p, tag_schema, start_ts, fix_entityid);
    uint64_t var_exist_len = 0;
    for (int i = 0; i < schema.size(); i++) {
      switch (schema[i].type) {
        case DATATYPE::TIMESTAMP64:
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
        case DATATYPE::INT64:
          for (int j = 0; j < count; j++) {
            KInt32(p.GetColumnAddr(j, i)) = 33333;
          }
          break;
        case DATATYPE::CHAR:
          for (int j = 0; j < count; j++) {
            strncpy(p.GetColumnAddr(j, i), test_str.c_str(), schema[i].size);
          }
          break;
        case DATATYPE::VARSTRING:
        case DATATYPE::VARBINARY: {
          len = test_str.size();
          uint64_t var_type_offset = 0;
          for (int k = i; k < schema.size(); k++) {
            var_type_offset += (schema[k].size * count + bitmap_len);
          }
          for (int j = 0; j < count; j++) {
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
};
}