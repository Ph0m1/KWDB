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

#include "var_string.h"
#include "utils/string_utils.h"
#include "utils/big_table_utils.h"


StringToVARSTRING::StringToVARSTRING(BigTable *bt, int col, int max_len):
  bt_(bt), col_(col) {
  strfile_ = nullptr;
  max_len_ = (max_len <= 0) ? DEFAULT_VARSTRING_MAX_LEN : max_len;
}


StringToVARSTRING::~StringToVARSTRING() {}

int StringToVARSTRING::toData(char *str, void *data) {
  int ret_code = 0;
  int len = strlen(str);
  if (len > max_len_) {
    return -1;
  }
    

  if (len > 0) {
    unsigned char c = ((unsigned char*)str)[len - 1] & 0xC0;
    if (c == 0xC0) { // 0x1100---- | 0x1110----
      len--;
    } else if (c == 0x80) {
      if (len < 2)
        len = 0;
      else if ((((unsigned char*)str)[len - 2] & 0xE0) == 0xE0) {
        len -= 2;     // 0x1110---- 0x10------
      }
    }
  }

  {
    size_t loc = strfile_->push_back(str, len);
    if (loc == (size_t)(-1)) {
      loc = strfile_->startLoc();
      ret_code = -1;
    }
    *((size_t *)data) = loc;
  }
  return ret_code;
}

int StringToVARSTRING::toBinaryData(char *s, void *data) {
  int32_t len = *((int32_t *)s);
  size_t loc = strfile_->push_back_binary(s, len);
  int ret_code = 0;
  if (loc == (size_t)(-1)) {
    loc = strfile_->startLoc();
    ret_code = -1;
  }
  *((size_t *)data) = loc;
  return ret_code;
}

int StringToVARSTRING::updateData(char *s, void *data) {
  int len = strlen(s);
  int err_code = 0;
  if (len > max_len_)
        len = max_len_;
  {
    size_t convert_loc = ((size_t *)data)[0];
    if (convert_loc == (size_t)-1) {
      return -1;
    }
    strfile_->rdLock();
    if (strfile_->fileLen() < convert_loc) {
      strfile_->unLock();
      return -1;
    }
    char *vs = strfile_->getStringAddr(convert_loc);
    int vs_len = strlen(vs);
    if (vs_len >= len) {
      mmap_strlcpy(vs, s, vs_len);
      strfile_->unLock();
    } else {
      strfile_->unLock();
      size_t loc = strfile_->push_back(s, len);
      if (loc == (size_t)(-1)) {
        loc = strfile_->startLoc();
        err_code = -1;
      }
      *((size_t *)data) = loc;
      bt_->setColumnFlag(col_, AINFO_OOO_VARSTRING);
    }
  }

  return err_code;
}

int StringToVARSTRING::updateBinaryData(void *new_data, void *data) {
  int len = *((int32_t *)new_data);
  strfile_->rdLock();
  void *org_len_data = strfile_->getStringAddr(*(size_t *)data);
  int org_len = *((int32_t *)org_len_data);
  int err_code = 0;
  if (org_len >= len) {
    memcpy(org_len_data, new_data, len);
    strfile_->unLock();
  } else {
    strfile_->unLock();
    size_t loc = strfile_->push_back_binary(new_data, len);
    if (loc == (size_t)(-1)) {
      loc = strfile_->startLoc();
      err_code = -1;
    }
    *((size_t *)data) = loc;
    if (org_len != 0) // org_len = 0 if #row = 0
      bt_->setColumnFlag(col_, AINFO_OOO_VARSTRING);
  }
  return err_code;
}

void StringToVARSTRING::noPushToData(char *str, void *addr) {
  size_t loc = strfile_->stringToAddr(str);
  *((size_t *)addr) = loc;
}

void StringToVARSTRING::setStringFile(MMapStringFile *sf) {
  strfile_ = sf;
}

string VARSTRINGToString::toString(void *data) {
    strfile_->rdLock();
    string s = string(strfile_->getStringAddr(((size_t *)data)[0]));
    strfile_->unLock();
    return s;
}

int VARSTRINGToString::toString(void *data, char *str) {
  strfile_->rdLock();
  size_t size = mmap_strlcpy(str, strfile_->getStringAddr(((size_t*) data)[0]), max_len_);
  strfile_->unLock();
  return size;
}

void * VARSTRINGToString::dataAddrWithRdLock(void *data) {
  strfile_->rdLock();
  return strfile_->getStringAddr(*(size_t *)data);
}

void VARSTRINGToString::unlock() {
  strfile_->unLock();
}

StringToVARBINARY::StringToVARBINARY(BigTable *bt, int col, int max_len): StringToVARSTRING(bt,col,max_len){

}

StringToVARBINARY::~StringToVARBINARY() {};

int StringToVARBINARY::toData(char *str, void *data) {
  size_t loc = strfile_->push_back_hexbinary((void *)str, var_max_len_);
  if (loc != (size_t)(-1)) {
    *((size_t *)data) = loc;
    return 0;
  }
  return -1;
}

int StringToVARBINARY::toData2(char *str, void *data) {
  size_t loc = strfile_->push_back_hexbinary((void *)str, var_max_len_);
  if (loc != (size_t)(-1)) {
    *((size_t *)data) = loc;
    return 0;
  }
  return -1;
}

int StringToVARBINARY::updateData(char *s, void *data) {
  int len = unHex(s, vb_.data() + sizeof(int32_t), max_len_);
  if (len < 0)
    return -1;
  LD_setDataLen(vb_.data(), len);
  return updateBinaryData(vb_.data(), data);
}

void StringToVARBINARY::noPushToData(char *str, void *addr) {}

VARBINARYToString::VARBINARYToString(int max_len):
  VARSTRINGToString(max_len) {}
VARBINARYToString::~VARBINARYToString() {}

string VARBINARYToString::toString(void *data) {
  strfile_->rdLock();
  void *vs = strfile_->getStringAddr(((size_t *) data)[0]);
  int len = LD_DATALEN(vs);
  if (len <= 0) {
    strfile_->unLock();
    return {};
  }
  string tmp = string(reinterpret_cast<char*>(LD_GETDATA(vs)), len);
  strfile_->unLock();
  return tmp;
}

int VARBINARYToString::toString(void *data, char *str) {
  strfile_->rdLock();
  void *vs = strfile_->getStringAddr(((size_t *) data)[0]);
  int len = LD_DATALEN(vs);
  if (len <= 0) {
    strfile_->unLock();
    return 0;
  }
  memcpy(str, LD_GETDATA(vs), len);
  strfile_->unLock();
  return len;
}
