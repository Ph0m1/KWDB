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

#include "lg_api.h"
#include "ee_data_chunk.h"
#include "cm_func.h"
#include "ee_common.h"

namespace kwdbts {

DataChunk::DataChunk(vector<ColumnInfo>& col_info, k_uint32 capacity) :
    col_info_(col_info), capacity_(capacity) {}

DataChunk::~DataChunk() {
  col_info_.clear();
  col_offset_.clear();
  SafeDeleteArray(data_);
}

int DataChunk::Initialize() {
  // null bitmap
  col_num_ = col_info_.size();
  bitmap_size_ = (col_num_ + 7) / 8;
  // calculate row width and length
  k_uint32 data_size = 0;
  for (int i = 0; i < col_num_; ++i) {
    col_offset_.push_back(data_size);

    /**
     * Row size adjustment for string type column and decimal type column. Add
     * 2 byte for string length and 1 byte indicator for decimal type. Ideally
     * storage length of the field (FieldDecimal, FieldVarChar, FieldVarBlob,
     * etc.) should take extra bytes into account, and it needs to make
     * necessary changes on all derived Field classes. Now we temporarily make
     * row size adjustment in several places.
     */
    if (IsStringType(col_info_[i].storage_type)) {
      data_size += STRING_WIDE;
    } else if (col_info_[i].storage_type == roachpb::DataType::DECIMAL) {
      data_size += BOOL_WIDE;
    }
    data_size += col_info_[i].storage_len;
  }
  // In order to be consistent with the bigtable format, an additional byte of
  // delete is required
  data_size += 1;
  row_size_ = data_size + bitmap_size_;
  if (capacity_ == 0) {
    if (DataChunk::SIZE_LIMIT >= row_size_) {
      capacity_ = DataChunk::SIZE_LIMIT / row_size_;
    } else {
      // use 1 as the minimum capacity.
      capacity_ = 1;
    }
  }
  // alloc
  if (capacity_ * row_size_ > 0) {
    data_ = KNEW char[capacity_ * row_size_];
    // allocation failure
    if (data_ == nullptr) {
      LOG_ERROR("Allocate buffer in DataChunk failed.");
      return -1;
    }
    std::memset(data_, 0, capacity_ * row_size_);
  }
  return 0;
}

KStatus DataChunk::InsertData(k_uint32 row, k_uint32 col, DatumPtr value, k_uint16 len) {
  // record the length of all strings
  if (IsStringType(col_info_[col].storage_type)) {
    std::memcpy(data_ + row * row_size_ + col_offset_[col], &len, STRING_WIDE);
    std::memcpy(data_ + row * row_size_ + col_offset_[col] + STRING_WIDE, value, len);
  } else {
    std::memcpy(data_ + row * row_size_ + col_offset_[col], value, len);
  }
  SetNotNull(row, col);
  return SUCCESS;
}

KStatus DataChunk::InsertDecimal(k_uint32 row, k_uint32 col, DatumPtr value, k_bool is_double) {
  if (col_info_[col].storage_type != roachpb::DataType::DECIMAL) {
    return FAIL;
  }

  std::memcpy(data_ + row * row_size_ + col_offset_[col], &is_double, sizeof(k_bool));
  std::memcpy(data_ + row * row_size_ + col_offset_[col] + sizeof(k_bool), value, sizeof(k_double64));
  return SUCCESS;
}

KStatus DataChunk::InsertData(kwdbContext_p ctx, DatumPtr value, Field** renders) {
  EnterFunc();

  if (nullptr == renders) {
    if (value == nullptr) {
      Return(KStatus::FAIL);
    }
    memcpy(data_ + count_ * row_size_, value, row_size_);
    AddCount();
    Return(KStatus::SUCCESS);
  }

  for (k_uint32 col = 0; col < col_num_; ++col) {
    Field* field = renders[col];

    // dispose null
    if (field->is_nullable()) {
      SetNull(count_, col);
      continue;
    }

    k_uint32 len = field->get_storage_length();
    switch (field->get_storage_type()) {
      case roachpb::DataType::BOOL: {
        bool val = field->ValInt() > 0 ? 1 : 0;
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BIGINT: {
        k_int64 val = field->ValInt();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::INT: {
        k_int32 val = field->ValInt();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::SMALLINT: {
        k_int16 val = field->ValInt();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::FLOAT: {
        k_float32 val = field->ValReal();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::DOUBLE: {
        k_double64 val = field->ValReal();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::VARBINARY: {
        kwdbts::String val = field->ValStr();
        if (val.isNull()) {
          SetNull(count_, col);
          break;
        }
        char* mem = const_cast<char*>(val.c_str());
        InsertData(count_, col, mem, val.length());
        break;
      }
      case roachpb::DataType::DECIMAL: {
        if (field->get_field_type() == Field::Type::FIELD_AGG ||
            field->get_field_type() == Field::Type::FIELD_ITEM) {
          DatumPtr src = field->get_ptr();
          InsertData(count_, col, src, len + BOOL_WIDE);
        }
        break;
      }
      default: {
        break;
      }
    }
  }
  AddCount();

  Return(KStatus::SUCCESS);
}

DatumPtr DataChunk::GetData(k_uint32 row, k_uint32 col) {
  return data_ + row * row_size_ + col_offset_[col];
}

DatumPtr DataChunk::GetData(k_uint32 row, k_uint32 col, k_uint16& len) {
  DatumPtr ptr = data_ + row * row_size_ + col_offset_[col];
  std::memcpy(&len, ptr, sizeof(k_uint16));
  return data_ + row * row_size_ + col_offset_[col] + sizeof(k_uint16);
}

DatumPtr DataChunk::GetData(k_uint32 col) {
  return data_ + current_line_ * row_size_ + col_offset_[col];
}

DatumRowPtr DataChunk::GetRow(k_uint32 row) {
  return data_ + row * row_size_;
}

void DataChunk::CopyRow(k_uint32 row, void* src) {
  std::memcpy(data_ + row * row_size_, src, row_size_);
}

bool DataChunk::IsNull(k_uint32 row, k_uint32 col) {
  char* bitmap = reinterpret_cast<char*>(data_ + (row + 1) * row_size_ - bitmap_size_);
  if (bitmap == nullptr) {
    return true;
  }

  return (bitmap[col / 8] & ((1 << 7) >> (col % 8))) != 0;
}

bool DataChunk::IsNull(k_uint32 col) {
  char* bitmap = reinterpret_cast<char*>(
      data_ + (current_line_ + 1) * row_size_ - bitmap_size_);
  if (bitmap == nullptr) {
    return true;
  }
  // return ((bitmap[col >> 3]) & (1 << (col & 7))) != 0;
  return (bitmap[col / 8] & ((1 << 7) >> (col % 8))) != 0;
}

// 1 null，0 not null
void DataChunk::SetNull(k_uint32 row, k_uint32 col) {
  char* bitmap = reinterpret_cast<char*>(data_ + (row + 1) * row_size_ - bitmap_size_);
  if (bitmap == nullptr) {
    return;
  }

  bitmap[col / 8] |= (1 << 7) >> (col % 8);
}

void DataChunk::SetNotNull(k_uint32 row, k_uint32 col) {
  char* bitmap = reinterpret_cast<char*>(data_ + (row + 1) * row_size_ - bitmap_size_);
  if (bitmap == nullptr) {
    return;
  }

  k_uint32 index = col >> 3;     // col / 8
  unsigned int pos = 1 << 7;    // binary 1000 0000
  unsigned int mask = pos >> (col & 7);     // pos >> (col % 8)
  bitmap[index] &= ~mask;
}

void DataChunk::SetAllNull() {
  for (int row = 0; row < capacity_; row++) {
    char* bitmap = reinterpret_cast<char*>(data_ + (row + 1) * row_size_ - bitmap_size_);
    if (bitmap == nullptr) {
      return;
    }
    std::memset(bitmap, 0xff, bitmap_size_);
  }
}

KStatus DataChunk::Append(std::queue<DataChunkPtr>& buffer) {
  while (!buffer.empty()) {
    auto& buf = buffer.front();
    size_t batch_buf_length = buf->RowSize() * buf->Count();

    size_t offset = count_ * RowSize();
    memcpy(data_ + offset, buf->GetData(), batch_buf_length);
    count_ += buf->Count();
    buffer.pop();
  }
  return SUCCESS;
}

KStatus DataChunk::EncodingValue(kwdbContext_p ctx, k_uint32 row, k_uint32 col, const EE_StringInfo& info) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;

  // dispose null
  if (IsNull(row, col)) {
    k_int32 len = ValueEncoding::EncodeComputeLenNull(0);
    ret = ee_enlargeStringInfo(info, len);
    if (ret != SUCCESS) {
      Return(ret);
    }

    CKSlice slice{info->data + info->len, len};
    ValueEncoding::EncodeNullValue(&slice, 0);
    info->len = info->len + len;
    Return(ret);
  }

  switch (col_info_[col].return_type) {
    case KWDBTypeFamily::BoolFamily: {
      DatumPtr raw = GetData(row, col);
      k_bool val;
      std::memcpy(&val, raw, sizeof(k_bool));
      k_int32 len = ValueEncoding::EncodeComputeLenBool(0, val);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeBoolValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::BytesFamily:
    case KWDBTypeFamily::StringFamily: {
      k_uint16 val_len;
      DatumPtr raw = GetData(row, col, val_len);
      std::string val = std::string{static_cast<char*>(raw), val_len};
      k_int32 len = ValueEncoding::EncodeComputeLenString(0, val.size());
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeBytesValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::TimestampFamily:
    case KWDBTypeFamily::TimestampTZFamily: {
      DatumPtr raw = GetData(row, col);
      k_int64 val;
      std::memcpy(&val, raw, sizeof(k_int64));
      CKTime ck_time;
      if (val < 0) {
        ck_time.t_timespec.tv_sec = floor(val / 1000.0);
        ck_time.t_timespec.tv_nsec = ((val % 1000) ? (val % 1000) + 1000 : 0) * 1000000;
      } else {
        ck_time.t_timespec.tv_sec = (val / 1000);
        ck_time.t_timespec.tv_nsec = val % 1000 * 1000000;
      }
      ck_time.UpdateSecWithTZ(ctx->timezone);
      k_int32 len = ValueEncoding::EncodeComputeLenTime(0, ck_time);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeTimeValue(&slice, 0, ck_time);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::IntFamily: {
      DatumPtr raw = GetData(row, col);
      k_int64 val;
      switch (col_info_[col].storage_type) {
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::DATE:
          std::memcpy(&val, raw, sizeof(k_int64));
          break;
        case roachpb::DataType::SMALLINT:
          k_int16 val16;
          std::memcpy(&val16, raw, sizeof(k_int16));
          val = val16;
          break;
        default:
          k_int32 val32;
          std::memcpy(&val32, raw, sizeof(k_int32));
          val = val32;
          break;
      }
      k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeIntValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::FloatFamily: {
      DatumPtr raw = GetData(row, col);
      k_double64 val;
      if (col_info_[col].storage_type == roachpb::DataType::FLOAT) {
        k_float32 val32;
        std::memcpy(&val32, raw, sizeof(k_float32));
        val = val32;
      } else {
        std::memcpy(&val, raw, sizeof(k_double64));
      }

      k_int32 len = ValueEncoding::EncodeComputeLenFloat(0);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeFloatValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::DecimalFamily: {
      switch (col_info_[col].storage_type) {
        case roachpb::DataType::SMALLINT: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_int16>(ptr, info);
          break;
        }
        case roachpb::DataType::INT: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_int32>(ptr, info);
          break;
        }
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_int64>(ptr, info);
          break;
        }
        case roachpb::DataType::FLOAT: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_float32>(ptr, info);
          break;
        }
        case roachpb::DataType::DOUBLE: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_double64>(ptr, info);
          break;
        }
        case roachpb::DataType::DECIMAL: {
          DatumPtr ptr = GetData(row, col);
          k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
          if (is_double) {
            EncodeDecimal<k_double64>(ptr + sizeof(k_bool), info);
          } else {
            EncodeDecimal<k_int64>(ptr + sizeof(k_bool), info);
          }
          break;
        }
        default: {
          LOG_ERROR("Unsupported Decimal type for encoding: %d ", col_info_[col].storage_type)
          break;
        }
      }
      break;
    }
    case KWDBTypeFamily::IntervalFamily: {
      DatumPtr raw = GetData(row, col);
      k_int64 val;
      std::memcpy(&val, raw, sizeof(k_int64));

      struct KWDuration duration;
      duration.format(val);
      k_int32 len = ValueEncoding::EncodeComputeLenDuration(0, duration);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeDurationValue(&slice, 0, duration);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::DateFamily: {
      const int secondOfDay = 24 * 3600;
      DatumPtr raw = GetData(row, col);
      std::string date_str = std::string{static_cast<char*>(raw)};
      struct tm stm{
          0
      };
      int year, mon, day;
      k_int64 msec;
      std::memcpy(&msec, raw, sizeof(k_int64));
      k_int64 seconds = msec / 1000;
      time_t rawtime = (time_t)seconds;
      tm timeinfo;
      localtime_r(&rawtime, &timeinfo);

      stm.tm_year = timeinfo.tm_year;
      stm.tm_mon = timeinfo.tm_mon;
      stm.tm_mday = timeinfo.tm_mday;
      time_t val = timelocal(&stm);
      val += ctx->timezone * 60 * 60;
      val /= secondOfDay;
      if (val < 0 && val % secondOfDay) {
        val -= 1;
      }
      val += 1;
      k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeIntValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    default: {
      DatumPtr raw = GetData(row, col);
      k_int64 val;
      std::memcpy(&val, raw, sizeof(k_int64));
      k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeIntValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
  }
  Return(ret);
}

KStatus DataChunk::PgResultData(kwdbContext_p ctx, k_uint32 row, const EE_StringInfo& info) {
  EnterFunc();
  k_uint32 temp_len = info->len;
  char* temp_addr = nullptr;

  if (ee_appendBinaryStringInfo(info, "D0000", 5) != SUCCESS) {
    Return(FAIL);
  }

  // write column quantity
  if (ee_sendint(info, col_num_, 2) != SUCCESS) {
    Return(FAIL);
  }

  for (k_uint16 col = 0; col < col_num_; ++col) {
    if (IsNull(row, col)) {
      // write a negative value to indicate that the column is NULL
      if (ee_sendint(info, -1, 4) != SUCCESS) {
        Return(FAIL);
      }
      continue;
    }

    // get col value
    KWDBTypeFamily return_type = col_info_[col].return_type;
    switch (return_type) {
      case KWDBTypeFamily::BoolFamily: {
        // get col value
        DatumPtr raw = GetData(row, col);
        k_bool val;
        std::memcpy(&val, raw, sizeof(k_bool));
        // get col value
        std::string val_str;
        if (val == 0) {
          val_str = "f";
        } else {
          val_str = "t";
        }
        // write the length of col value
        if (ee_sendint(info, val_str.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string
        if (ee_appendBinaryStringInfo(info, val_str.data(), val_str.length()) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::StringFamily: {
        k_uint16 val_len;
        DatumPtr raw = GetData(row, col, val_len);
        std::string val_str = std::string{static_cast<char*>(raw), val_len};

        std::string val = std::string{static_cast<char*>(raw)};
        k_int32 len = ValueEncoding::EncodeComputeLenString(0, val.size());
        // write the length of col value
        if (ee_sendint(info, val.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string
        if (ee_appendBinaryStringInfo(info, val.c_str(), val.length()) !=
            SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::BytesFamily: {
        k_uint16 len;
        DatumPtr raw = GetData(row, col, len);
        std::string val_str = std::string{static_cast<char*>(raw), len};

        // use format of varbinary
        std::string bytes_f;
        bytes_f.append("\\x");
        char tmp[3] = {0};
        for (u_char c : val_str) {
          snprintf(tmp, sizeof(tmp), "%02x", c);
          bytes_f.append(tmp, 2);
        }
        if (ee_sendint(info, bytes_f.size(), 4) != SUCCESS) {
          Return(FAIL);
        }
        if (ee_appendBinaryStringInfo(info, bytes_f.c_str(), bytes_f.size()) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::TimestampFamily:
      case KWDBTypeFamily::TimestampTZFamily: {
        char ts_format_buf[32] = {0};
        // format timestamps as strings
        time_t ms;
        time_t sec;
        DatumPtr raw = GetData(row, col);
        std::memcpy(&ms, raw, sizeof(k_int64));
        if (ms < 0) {
          sec = floor(ms / 1000.0);
          ms = (ms % 1000) ? (ms % 1000) + 1000 : 0;
        } else {
          sec = ms / 1000;
          ms = ms % 1000;
        }

        if (return_type == KWDBTypeFamily::TimestampTZFamily) {
          sec += ctx->timezone * 3600;
        }
        tm ts{};
        gmtime_r(&sec, &ts);
        strftime(ts_format_buf, 32, "%F %T", &ts);
        k_uint8 format_len = strlen(ts_format_buf);
        if (ms != 0) {
          snprintf(&ts_format_buf[format_len], sizeof(char[5]), ".%03ld", ms);
        }
        format_len = strlen(ts_format_buf);
        // encode the time Zone Information
        if (return_type == KWDBTypeFamily::TimestampTZFamily) {
          const char* timezoneFormat;
          auto timezoneAbs = std::abs(ctx->timezone);
          if (ctx->timezone >= 0) {
            timezoneFormat = "+%02d:00";
            timezoneAbs = ctx->timezone;
          } else {
            timezoneFormat = "-%02d:00";
          }
          snprintf(&ts_format_buf[format_len], sizeof(char[7]), timezoneFormat, timezoneAbs);
        }
        format_len = strlen(ts_format_buf);
        // write the length of column value
        if (ee_sendint(info, format_len, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, ts_format_buf, format_len) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::FloatFamily: {
        k_char buf[30] = {0};
        k_int32 n = 0;

        DatumPtr raw = GetData(row, col);
        k_double64 d;
        if (col_info_[col].storage_type == roachpb::DataType::FLOAT) {
          k_float32 val32;
          std::memcpy(&val32, raw, sizeof(k_float32));
          d = (k_double64) val32;
          n = snprintf(buf, sizeof(buf), "%.6f", d);
        } else {
          std::memcpy(&d, raw, sizeof(k_double64));
          n = snprintf(buf, sizeof(buf), "%.17g", d);
        }

        // write the length of column value
        if (ee_sendint(info, n, 4) != SUCCESS) {
          Return(FAIL);
        }
        if (std::isnan(d)) {
          buf[0] = 'N';
          buf[1] = 'a';
          buf[2] = 'N';
          n = 3;
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, buf, n) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::DecimalFamily: {
        switch (col_info_[col].storage_type) {
          case roachpb::DataType::SMALLINT: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_int16>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::INT: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_int32>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_int64>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::FLOAT: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_float32>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::DOUBLE: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_double64>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::DECIMAL: {
            DatumPtr ptr = GetData(row, col);
            k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
            if (is_double) {
              PgEncodeDecimal<k_double64>(ptr + sizeof(k_bool), info);
            } else {
              PgEncodeDecimal<k_int64>(ptr + sizeof(k_bool), info);
            }
            break;
          }
          default: {
            LOG_ERROR("Unsupported Decimal type for encoding: %d ", col_info_[col].storage_type)
            break;
          }
        }
      }
        break;
      case KWDBTypeFamily::IntervalFamily: {
        time_t ms;
        DatumPtr raw = GetData(row, col);
        std::memcpy(&ms, raw, sizeof(k_int64));
        char buf[32] = {0};
        struct KWDuration duration;
        size_t n = duration.format_pg_result(ms, buf, 32);
        // write the length of column value
        if (ee_sendint(info, n, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, buf, n) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::DateFamily: {
        DatumPtr raw = GetData(row, col);
        std::string str = std::string{static_cast<char*>(raw)};
        // write the length of column value
        if (ee_sendint(info, str.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, str.c_str(), str.length()) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      case KWDBTypeFamily::IntFamily: {
        DatumPtr raw = GetData(row, col);
        k_int64 val;
        switch (col_info_[col].storage_type) {
          case roachpb::DataType::BIGINT:
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
            std::memcpy(&val, raw, sizeof(k_int64));
            break;
          case roachpb::DataType::SMALLINT:
            k_int16 val16;
            std::memcpy(&val16, raw, sizeof(k_int16));
            val = val16;
            break;
          default:
            k_int32 val32;
            std::memcpy(&val32, raw, sizeof(k_int32));
            val = val32;
            break;
        }

        char val_char[32];
        snprintf(val_char, sizeof(val_char), "%ld", val);
        if (ee_sendint(info, strlen(val_char), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, val_char, strlen(val_char)) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
      default: {
        // write the length of column value
        k_int64 val;
        DatumPtr raw = GetData(row, col);
        std::memcpy(&val, raw, sizeof(k_int64));
        char val_char[32];
        snprintf(val_char, sizeof(val_char), "%ld", val);
        if (ee_sendint(info, strlen(val_char), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, val_char, strlen(val_char)) != SUCCESS) {
          Return(FAIL);
        }
      }
        break;
    }
  }

  temp_addr = &info->data[temp_len + 1];
  k_uint32 n32 = be32toh(info->len - temp_len - 1);
  memcpy(temp_addr, &n32, 4);
  Return(SUCCESS);
}

KStatus DataChunk::AddRowBatchData(kwdbContext_p ctx, RowBatch* row_batch, Field** renders) {
  EnterFunc();
  if (row_batch == nullptr) {
    Return(KStatus::FAIL);
  }

  for (int row = 0; row < row_batch->Count(); ++row) {
    for (k_uint32 col = 0; col < col_num_; ++col) {
      Field* field = renders[col];

      // dispose null
      if (field->isNullable() && field->is_nullable()) {
        SetNull(row, col);
        continue;
      }

      k_uint32 len = field->get_storage_length();
      switch (field->get_storage_type()) {
        case roachpb::DataType::BOOL: {
          bool val = field->ValInt() > 0 ? 1 : 0;
          InsertData(row, col, reinterpret_cast<char*>(&val), len);
          break;
        }
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT: {
          k_int64 val = field->ValInt();
          InsertData(row, col, reinterpret_cast<char*>(&val), len);
          break;
        }
        case roachpb::DataType::INT: {
          k_int32 val = field->ValInt();
          InsertData(row, col, reinterpret_cast<char*>(&val), len);
          break;
        }
        case roachpb::DataType::SMALLINT: {
          k_int16 val = field->ValInt();
          InsertData(row, col, reinterpret_cast<char*>(&val), len);
          break;
        }
        case roachpb::DataType::FLOAT: {
          k_float32 val = field->ValReal();
          InsertData(row, col, reinterpret_cast<char*>(&val), len);
          break;
        }
        case roachpb::DataType::DOUBLE: {
          k_double64 val = field->ValReal();
          InsertData(row, col, reinterpret_cast<char*>(&val), len);
          break;
        }
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          kwdbts::String val = field->ValStr();
          if (val.isNull()) {
            SetNull(row, col);
            break;
          }
          char* mem = const_cast<char*>(val.c_str());
          InsertData(row, col, mem, val.length());
          break;
        }
        case roachpb::DataType::DECIMAL: {
          k_bool overflow = field->is_over_flow();
          if (field->get_sql_type() == roachpb::DataType::DOUBLE ||
              field->get_sql_type() == roachpb::DataType::FLOAT || overflow) {
            k_double64 val = field->ValReal();
            InsertDecimal(row, col, reinterpret_cast<char*>(&val), true);
          } else {
            k_int64 val = field->ValInt();
            InsertDecimal(row, col, reinterpret_cast<char*>(&val), false);
          }
          break;
        }
        default: {
          break;
        }
      }
    }
    AddCount();
    row_batch->NextLine();
  }

  Return(KStatus::SUCCESS);
}

/**
 * reset current_line_ to the first record
*/
void DataChunk::ResetLine() {
  current_line_ = -1;
}

/**
 * NextLine
*/
k_int32 DataChunk::NextLine() {
  if (current_line_ + 1 >= count_) {
    return -1;
  }
  ++current_line_;
  return current_line_;
}

/**
 * return the number of valid rows in the result set
 */
k_uint32 DataChunk::Count() {
  return count_;
}
}   // namespace kwdbts
