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


#ifndef INCLUDE_STRING_MMAPSTRING_H_
#define INCLUDE_STRING_MMAPSTRING_H_

#include <vector>
#include "DataType.h"
#include <cmath>
using namespace std;

// length of dst = siz + 1; will append NULL at end of string
size_t mmap_strlcpy(char *dst, const char *src, size_t siz);

// length of dst = siz.
size_t CHARcpy(char *dst, const char *src, size_t siz);

void strmove(uchar *d, uchar *s, int len);

// compare a NULL ended string with CHAR
// cp: L_CHAR(0): CHAR on the left
//     R_CHAR(1): CHAR on the right
//            2:  maxlen(L) = maxlen(R)
// n = min (max_len(NULL-ended string) + 1, max_len(CHAR))
int strncmpCHAR (const char *l, const char *r, size_t n, int cp);

// tbl_sub_path: path of object which is probably ended with '/', e.g., "path/"
// db: database name.
int pathcmp(const char *tbl_sub_path, const char *db);

int dataTypeMaxTextLen(const AttributeInfo &col);

// make tail of string to NULL (for CHAR)
// return string length
int  nullTail(char *s, int sz);

// null non-null string
int nullString(char *s);

// true if s is empty or s = 'NULL'
bool isNULL(char *s);

// returns 1 on success. 0 or -1 on error.
int stringToIPV4(const char *str, void *ipv4);

int stringToIPV6(const char *str, void *ipv6);

// Return length of binary string.
int unHex(const char *str, unsigned char *hex_data, int max_len);


// the hex result will be reversed. ex: 1234 will be "3930", but actually it should be "3039"
int binaryToHexString(unsigned char *hex_data, char *s, int32_t len);
// 1234 will be "3039"
int binaryToHexString2(unsigned char *hex_data, char *s, int32_t len);

inline uint32_t getOctStringLen(uint32_t rLen, int *leftbit) {
    uint32_t wLen = rLen * 8 / 3; // 3 bits will be a group, each group can be represented by a character;
    int t = rLen * 8 % 3;
    if (t > 0) { // 8, 16, 32, 64 bits are not divisible by 3, so +1 (it usually always happen)
        wLen ++; // there are bits left, so need one more character.
    }
    if (leftbit) {
        *leftbit = t;
    }
    return wLen;
};

int binaryToOctString(unsigned char *hex_data, char *s, int32_t rLen);

inline uint32_t getDecStringLen(uint32_t rLen) {
  // this algorithm is true and tested, at least when rLen is 1~8
  double dTmp = log2(rLen);
  int i = dTmp;
  if (dTmp > i || i == 0) {
    i++;
  }
  uint32_t wLen = i+(rLen*2);
  return wLen;
};


int binaryToDecString(unsigned char *hex_data, char *s, int32_t rLen);

vector<string> tokenize(const string &s, const string &delimiters);

// return a string by replacing quote 'c' within a string to '\c'
string escapeString(const string &str, char c);

string toCSVString(const char *s);

namespace bigobject {

// len: length of s.
// return the length of 3-bytes utf8-rectified s
int rectifyUtf8(const char *s, int len);

class DoubleFormatToString {
protected:
  int precision_;
  bool is_remove_trailing_zeros_;
  double max_v_;
  double min_v_;

  void setMinMax();
public:
  DoubleFormatToString(int precision, bool is_rm_tz = true);
  DoubleFormatToString(const DoubleFormatToString &rhs);
  virtual ~DoubleFormatToString();
  int toString(char *str, double v);
};

// aes ofb encrypt/decrypt string; length of in & out should be multiples of 16
void cryptString(const unsigned char *in, unsigned char*out, int len);

// Returns the first non-whitespace character in s.
const char * strNonWSpace(const char *s);

int geomToWKTMaxLen(int type);


char * toNULL(char *s);

// Convert Point to String
class PointToString {
protected:
  DoubleFormatToString dbl_to_str_;
  char sep_;
public:
  PointToString(int precision, char s = ' ');
  PointToString(const PointToString &rhs);
  virtual ~PointToString();
  // Return the pointer at the end of converted string s.
  void setSeperator(char sep);
  // x, y
  char * toString(char *s, Point *p);
};

} // namespace bigboject


#endif /* INCLUDE_STRING_MMAPSTRING_H_ */
