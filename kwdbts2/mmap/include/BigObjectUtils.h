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


#ifndef BIGOBJECTUTILS_H_
#define BIGOBJECTUTILS_H_


#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <algorithm>
#include <sys/resource.h>
#include "default.h"
#include "DataOperation.h"
#include "BigObjectError.h"
#include "BigObjectConfig.h"
#include "BigTable.h"


using namespace bigobject;

#define BIGOBJECT_LOG( header, os, msg ) \
  (os) << pthread_self() << header << __func__  << msg << std::endl


#if !defined(NDEBUG)
#define BIGOBJECT_DEBUG( header, os, msg ) \
  (os) << std::hex << pthread_self() << std::dec << header << '<' << __FILE__ \
  << ", " << __func__ << "():" << __LINE__ << "> " << msg << std::endl
#else
#define BIGOBJECT_DEBUG( header, os, msg )
#endif

#define BO_DEBUG(os, msg)  BIGOBJECT_DEBUG( " [KWDBG] ", os, msg )

// @return  "" if db = bigobject | bigobject/; db otherwise.
const string & getActualWorkspace(const string &db);

// trim trailing '/' from workspace, e.g., `abc/` ->  `abc`
const string & worksapceToDatabase(string &ws);

// get deletable column and set data to 1 (non-deleted) if it exists
int getDeletableColumn(BigTable *bt, DataHelper *helper, void *rec_data);

vector<AttributeInfo> & getDummySchema();

string & toParentDirectory (string &dir);

/**
 * @brief	obtain the Path of an URL
 *
 * @param 	url	the URL to process
 * @return	url protocol.
 */
string getURLFilePath(const string &url);

string getURLObjectName(const string &url);

bool isInteger(const char *s, int64_t &i);

int findName(const char *s, void *name_st, int size, int rec_sz);

string getDimension(const string &dim_attr);

string getAttribute(const string &dim_attr);

string nameToObjectURL(const string &protocol, const string &name,
  const string &ext);

string nameToBigTableURL(const string &name, const string &ext = s_bt());

string nameToTagBigTableURL(const string &name, const string &ext= s_bt());

string nameToEntityBigTableURL(const string &name, const string &ext = s_bt());

#if defined(COLUMN_GROUP)
string nameToColumnGroupBigTableURL(const string &name);
#endif

string genTempObjectURL(const string &src_url);

int getDataTypeSize(int type);

int getDataTypeSize(AttributeInfo &info);

int setAttributeInfo(vector<AttributeInfo> &info, int encoding = DICTIONARY,
  ErrorInfo &err_info = getDummyErrorInfo());

vector<string> AttributeInfoToString(const vector<AttributeInfo> &attr_info);

// return the length of normalized string
int normalizeString(char *s);

// WARNING:
// string s will be changed; use with care.
// compiler/STL optimization on string might result in undesirable behavior.
// e.g., string a = b;     // reference instead of copy
//       normalizeString(a); will change b as well. (a refers to b)
// use string a = b.substr(0) instead  // a is a copy of b
// string a = string(b); won't work at this moment; a still a reference of b.
// or use normalize() to replace it.
void normalizeString(string &s);

// return a normalized string of s
string normalize(const string &s);

// Returns C++ string from char * string.
// If data is NULL, it returns an empty string.
string toString(const char *data);

inline int stringToInt(const string &str)
{ return atoi(str.c_str()); }

inline int32_t stringToInt(const char *str) {
  int64_t v = strtol(str, nullptr, 10);
  if (v >= numeric_limits<int32_t>::max())
    v = numeric_limits<int32_t>::max();
  else if (v <= numeric_limits<int32_t>::min())
    v = numeric_limits<int32_t>::min();
  return (int32_t)v;
}

inline long int stringToLongInt(const string &str)
{ return strtol(str.c_str(), nullptr, 10); }

inline string intToString(int64_t i) {
#if defined(__ANDROID__)
  stringstream ss;
  ss << i;
  return ss.str();
#else
  return std::to_string(i);
#endif
}

inline string uintToString(uint64_t i) {
#if defined(__ANDROID__)
  stringstream ss;
  ss << i;
  return ss.str();
#else
  return std::to_string(i);
#endif
}

inline string doubleToString(double d) {
#if defined(__ANDROID__)
  stringstream ss;
  ss << d;
  return ss.str();
#else
  return std::to_string(d);
#endif
}

int getIndex(const vector<AttributeInfo> &schema, const string &str);

string toString(const char *str, size_t len);

string quoteString(const string &str, char quote = '\'');

string toLower(const string &str);
string toUpper(const string &str);

#if defined(__ANDROID__)
inline void toUpperText (string &str)
{ std::transform(str.begin(), str.end(), str.begin(), ::toupper); }
#endif

inline off_t getPageOffset(off_t offset, size_t ps =
  BigObjectConfig::pageSize())
{ return ((offset + ps - 1) & ~(ps - 1)); }

int getFileCC(const string &file_path);
int getURLCC(const string &url, const string &sand_box = string(""));

string normalizePath(const string &path);

string makeDirectoryPath(const string &tbl_sub_path);

int makeDirectory(const string &dir);

template <typename T>
void assign(T &right, const T &value) {
  T tmp = value;      // avoid crash in arm
  right = value;
}

inline void fletcher64(uint64_t *sum, IDTYPE id, uint32_t num_child) {
  sum[0] += id + num_child;
  sum[0] = (sum[0] & 0xffffffff) + (sum[0] >> 32);
  sum[1] += sum[0];
  sum[1] = (sum[1] & 0xffffffff) + (sum[1] >> 32);
}

// partition table into chunks for threading and circular table
#define MINIMUM_PARTITION_SIZE  20000
#define MAXIMUM_PARTITION_SIZE  200000

typedef vector<std::pair<size_t, size_t>> table_partition;

// set null bitmap
inline void set_null_bitmap(unsigned char *null_bitmap, int col) {
  // unsigned char bit_pos = (1 << (col % 8));
  // col = col / 8;
  unsigned char bit_pos = (1 << (col & 7));
  col = col >> 3;
  null_bitmap[col] |= bit_pos;
}

// unset null bitmap
inline void unset_null_bitmap(unsigned char *null_bitmap, int col) {
  // unsigned char bit_pos = (1 << (col % 8));
  // col = col / 8;
  unsigned char bit_pos = (1 << (col & 7));
  col = col >> 3;
  null_bitmap[col] &= ~bit_pos;
}

// get null bitmap status bit
inline unsigned char get_null_bitmap (unsigned char *null_bitmap, int col) {
  // unsigned char bit_pos = (1 << (col % 8));
  unsigned char bit_pos = (1 << (col & 7));
  // col = col / 8;
  col = col >> 3;
  return (null_bitmap[col] & bit_pos);
}

#endif /* BIGOBJECTUTILS_H_ */
