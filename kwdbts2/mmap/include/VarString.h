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

#ifndef INCLUDE_VARSTRING_H_
#define INCLUDE_VARSTRING_H_

#include "DataValueHandler.h"
#include "mmap/MMapStringFile.h"


class StringToVARSTRING: public StringToData {
protected:
  BigTable *bt_;
  MMapStringFile *strfile_;
  int col_;
  int max_len_;

public:
  StringToVARSTRING(BigTable *bt, int col, int max_len);

  virtual ~StringToVARSTRING();

  virtual int toData(char *s, void *data);

  virtual int toBinaryData(char *s, void *data);

  virtual int updateData(char *s, void *data);

  virtual int updateBinaryData(void *new_data, void *data);

  virtual void noPushToData(char *str, void *addr);

  void setStringFile(MMapStringFile *sf);
};

class VARSTRINGToString: public DataToString {
protected:
  MMapStringFile *strfile_;
public:
  VARSTRINGToString(int max_len):
    DataToString(max_len, sizeof(size_t)) {
    strfile_ = nullptr;
  }

  virtual ~VARSTRINGToString() {};

  virtual string toString(void *data);

  void setStringFile(MMapStringFile *sf) { strfile_ = sf; }

  virtual int toString(void *data, char *str);

  virtual void * dataAddrWithRdLock(void *data);

  virtual void unlock();
};

// [len, binary]
class StringToVARBINARY: public StringToVARSTRING {
protected:
  vector<unsigned char> vb_;
  int var_max_len_;
public:
  StringToVARBINARY(BigTable *bt, int col, int max_len);
  virtual ~StringToVARBINARY();
  virtual int toData(char *s, void *data);
  virtual int toData2(char *s, void *data);
  virtual int updateData(char *s, void *data);
  virtual void noPushToData(char *str, void *addr);
};


class VARBINARYToString: public VARSTRINGToString {
public:
  VARBINARYToString(int max_len);
  virtual ~VARBINARYToString();
  virtual string toString(void *data);
  virtual int toString(void *data, char *str);
};


#endif /* INCLUDE_VARSTRING_H_ */
