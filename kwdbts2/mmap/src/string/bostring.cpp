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


#include <arpa/inet.h>
#include <cctype>
#include <cfloat>
#include <cmath>
#include <cstring>
#include "string/mmapstring.h"
#include "DataOperation.h"
#include "BigObjectConfig.h"
#include "BigObjectUtils.h"

#if __cplusplus > 199711L
#define register      // Deprecated in C++11.
#endif  // #if __cplusplus > 199711L

size_t mmap_strlcpy(char *dst, const char *src, size_t siz) {
  char *d = dst;
  const char *s = src;
  size_t n = 0;

  while (n < siz) {
    if ((*d++ = *s++) == 0)
      break;
    n++;
  }
  /* Not enough room in dst, add NUL */
  if (n == siz) {
    *d = '\0'; /* NUL-terminate dst */
  }
  return n;
}

size_t CHARcpy(char *dst, const char *src, size_t siz) {
  char *d = dst;
  const char *s = src;
  size_t n = 0;

  while (n < siz) {
    if ((*d++ = *s++) == 0)
      break;
    n++;
  }
  return n;
}

void strmove(uchar *d, uchar *s, int len) {
  for (int i = 0; i < len; ++i) {
    *d++ = *s++;
  }
}

int strncmpCHAR(const char *l, const char *r, size_t n, int cp) {
  unsigned char c1 = 0;
  unsigned char c2 = 0;
  while (n > 0) {
    c1 = (unsigned char)*l++;
    c2 = (unsigned char)*r++;
    if (c1 == '\0' || c1 != c2)
      return c1 - c2;
    n--;
  }
  if (cp == 1)
    return (*l != 0) ? 1 : 0;   // CHAR on the right side
  if (cp == 0)
    return (*r != 0) ? -1 : 0;
  return 1;     // cp == 2
//  if (cp)
//    return (*l != 0) ? 1 : 0;   // CHAR on the right side
//  return (*r != 0) ? -1 : 0;
}

int pathcmp(const char *tbl_sub_path, const char *db) {
  char c1;
  char c2;
  if (strcmp(db, s_bigobject().c_str()) == 0)
    db = s_emptyString().c_str();
  do {
    c1 = *tbl_sub_path++;
    c2 = *db++;
    if (c1 == '\\' && c2 == 0)
      return 0;
    if (c1 != c2)
      return c1 - c2;
  } while (c2);
  return c1 - c2;
}

int dataTypeMaxTextLen(const AttributeInfo &col) {
  switch (col.type) {
    case STRING:
      if (col.max_len == 0)
        return STRING_MAX_LEN;
      return col.max_len;
//    case RAWSTRING:
    case STRING_CONST:
    case CHAR:
      return col.size;
    case INT32:
    case TIMESTAMP:
      return MAX_INT32_STR_LEN;
    case INT64:
    case ROWID:
    case HYPERLOGLOG:
    case TIMESTAMP64:
      return MAX_INT64_STR_LEN;
    case FLOAT:
      return MAX_DOUBLE_STR_LEN;
//        return FLT_MAX_10_EXP + BigObjectConfig::precision() + 1;
    case TDIGEST:
    case DOUBLE:
      return MAX_DOUBLE_STR_LEN;
//        return DBL_MAX_10_EXP + BigObjectConfig::precision() + 1;
    case VARSTRING:
    case LINESTRING:
    case POLYGON:
    case MULTIPOLYGON:
    case GEOMCOLLECT:
    case GEOMETRY:
    case BINARY:
    case VARBINARY:
    case BLOB:
    case WKB_LINESTRING:
    case WKB_POLYGON:
    case WKB_MULTIPOLYGON:
    case WKB_GEOMCOLLECT:
    case WKB_GEOMETRY:
      if (col.max_len == 0)
        return VARSTRING_MAX_LEN;
      return col.max_len;
//    case BINARY:
//    case VARBINARY:
//    case BLOB: {
//      int len = (col.max_len == 0) ? VARSTRING_MAX_LEN : col.max_len;
//      return len * 2 + 2;
//    }
        // TODO: based user time format
    case DATE32:            // yyyy-mm-dd
      return 10;
    case DATETIMEDOS:
    case DATETIME32:        // yyyy-mm-dd hh:mm:ss
    case DATETIME64:
      return 19;
    case INT16:
      return MAX_INT16_STR_LEN;
    case BOOL:
    case BYTE:
    case INT8:
      return MAX_INT8_STR_LEN;
    case WEEK:            // yyyyWww
      return 7;
    case IPV4:              // xxx.xxx.xxx.xxx
      return INET_ADDRSTRLEN - 1;
    case IPV6:
      return  INET6_ADDRSTRLEN - 1;
    case TIME:              // -????00:00:00 (2^31/3600 = 596523)
      return MAX_TIME_STR_LEN;
    case TIME64:
      return MAX_TIME64_STR_LEN;
    case POINT:
      return 25;
    }
    return STRING_MAX_LEN;
}

int nullTail(char *s, int sz) {
  char *t = s + (sz - 1);
  while (*t != '\0' && t > s) {
    *t-- = '\0';
  }
  return t - s;
}

int nullString(char *s) {
  while (*s != '\0') {
    *s++ = 0;
  }
  return 0;
}

bool isNULL(char *s) {
  //to-do(allison&thomas): distinguish  and ''
  if (s == NULL || s == nullptr || s == 0x0) {
    return true;
  }
  char c = *s;
  if (c == 'n' || c == 'N') {
    c = s[1];
    if ((c == 'u') || (c == 'U')) {
      c = s[2];
      if ((c == 'l') || (c == 'L')) {
        c = s[3];
        if (((c == 'l') || (c == 'L')) && s[4] == 0x0) {
          return true;
        }
      }
    }
  }
  return false;
}

int stringToIPV4(const char *str, void *ipv4) {
  int r;
  if (*str == 0) {
    r = 1;
    goto zero_ipv4;
  }
  r = inet_pton(AF_INET, str, ipv4);
  if (r != 1) {
zero_ipv4:
    *((int32_t *)ipv4) = 0;
  }
  return r;
}

int stringToIPV6(const char *str, void *ipv6) {
  int r;
  if (*str == 0) {
    r = 1;
    goto zero_ipv6;
  }
  r = inet_pton(AF_INET6, str, ipv6);
  if (r != 1) {       // failed to convert to IPV6 and fall back to ipv4
    r = inet_pton(AF_INET, str, offsetAddr(ipv6, 12));
    if (r == 1)
      memcpy(ipv6, ipv4_prefix, 12);
    else {
zero_ipv6:
      memset(ipv6, 0, sizeof(struct in6_addr));
    }
  }
  return r;
}

int hexToInt(char ch) {
  if (ch >= '0' && ch <= '9')
    return ch - '0';
  if (ch >= 'A' && ch <= 'F')
    return ch - 'A' + 10;
  if (ch >= 'a' && ch <= 'f')
    return ch - 'a' + 10;
  return -1;
}

char intToHex(uint32_t i) {
  if (i < 10)
    return (char )(i + '0');
  return (char )((i - 10) + 'A');
}

// Return length of binary string.
int unHex(const char *s, unsigned char *data, int max_len) {
  int data_len = 0;
  int b;
  int v;
  const char *os = s; // original string
  char c = *s++;
  int is_even = false;

  if (c == '0') {                        // 0x01AF | 0x01af
    if (*s == 'x') {
      s++;
    } else {
      v = 0;
      is_even = true;
    }
unhex:
    while ((c = *s++) != '\0') {
      if ((b = hexToInt(c)) < 0)
        return -1;  // hex error
      if (is_even) {
        data[data_len++] = (unsigned char)(v | b);
        if (data_len >= max_len) {
          if (*s != '\0')
            return -1;
          return data_len;
        }
        is_even = false;
      } else {
        v = b << 4;
        is_even = true;
      }
    }
  } else if (c == 'x' || c == 'X') {    // X'01AF' | x'01af'
    if (*s++ != '\'')
      return -1;  // hex error
    while ((c = *s++) != '\0' && c != '\'') {
       if ((b = hexToInt(c)) < 0)
         return -1;  // hex error
       if (is_even) {
         data[data_len++] = (unsigned char)(v | b);
         if (data_len >= max_len)
           return data_len;
         is_even = false;
       } else {
         v = b << 4;
         is_even = true;
       }
     }
  } else if (c == '\0') {
    return 0;
  } else {
    // assume null-terminated string input
    int len = strlen(os);
    if (len > max_len)
      return -1;
    memcpy((char *)data, os, len);
    return len;
//    if ((b = hexToInt(c)) < 0)
//      return -1;  // hex error
//    v = b << 4;
//    is_even = true;
//    goto unhex;
  }
  if (is_even)
    data[data_len++] = (unsigned char)v;
  return data_len;
}


// the hex result will be reversed. ex: 1234 will be "3930", but actually it should be "3039"
int binaryToHexString(unsigned char *input, char *s, int32_t len) {
  char *hs = s;
  if (len > 0) {
    for (int32_t i = 0; i < len; ++i) {
      uint32_t v = (uint32_t)*input++;
      *hs++ = intToHex((v >> 4));
      *hs++ = intToHex((v & 0x0F));
    }
  }
  return hs - s;
}



// 1234 will be "3039", different from binaryToHexString
int binaryToHexString2(unsigned char *input, char *s, int32_t rLen) {
  if (rLen <= 0) {
        return 0;
    }
    int represent = 2; // each byte can be represented in 2 characters
    int32_t wLen = rLen * represent;
    char *ws = s;
    char *tail = s + wLen;
    input += rLen-1;
    bool write = false;

    for (int32_t i = 0; i < rLen; ++i) { // ex: rLen = 8 for, int64
        uint32_t v = (uint32_t)*input; // assign a byte to v
        input--; // then move a byte length.
        char tmp1, tmp2;
        bool tmp1W = false, tmp2W = false;
        tmp1 = intToHex((v >> 4)); // 0011 0001 left shift 4 bit, so it become 0000 0011, which is 3
        tmp2 = intToHex((v & 0x0F)); // 0011 0001 & 0000 1111, it become 0000 0001, which is 1
        if (!write && (tmp1 != '0')) {
            write = true;
            tmp1W = true;
            *ws++ = tmp1;
        }
        if (!write && (tmp2 != '0')) {
          write = true;
          tmp1W = true;
          tmp2W = true;
          *ws++ = tmp2;
        }
        if (write) {
          if (!tmp1W) {
            *ws++ = tmp1;
          }
          if (!tmp2W) {
            *ws++ = tmp2;
          }

        }
    }
    if (!write) { // it means the value is 0, then write a '0';
      *ws = '0';
      ws++;
    }
    for ( ; ws != tail; ) {
        *ws++ = 0;
    }
    ws = nullptr;
    return wLen;
};


/* $$ this function align to right, with '0' padding on left
int binaryToOctString2(unsigned char *input_data, char *s, int32_t rLen) {
    int32_t wLen = getOctStringLen(rLen, nullptr);
    if (wLen <= 0) {
      return 0;
    }

    char *ws = s + wLen-1;
    char *tail = s + wLen;

    uint32_t lastData = 0; // the bits retrieved but not written yet from the last round.
    int lastBit = 0; // bit number of lastData.

    for (int i = 0; i < rLen ; ++i) { // move times, each time a byte
        uint32_t thisByte = *input_data;
        input_data++; // move a byte

        int shiftTime = (BYTE_BIT + lastBit) / OCT_BIT_GROUP;

        for (int j = 0; j < shiftTime; j++) { // shift 3 bits
            uint32_t writeData = 0;
            uint8_t mask = 7;
            if (j == 0 && lastBit > 0) { // combine bits from last round
                uint32_t remain = (thisByte & (mask >> lastBit));
                writeData = (remain << lastBit) | lastData;
            } else {
                int maskMove = 0;
                if (lastBit > 0) {
                    maskMove = OCT_BIT_GROUP * (j-1) + OCT_BIT_GROUP - lastBit;
                } else {
                   maskMove = OCT_BIT_GROUP * j;
                }

                writeData = (thisByte & (mask << maskMove) ) >> maskMove;
            }

            // write data to buffer
            *ws = char(writeData+48);
            ws--;
        }

        lastBit = (BYTE_BIT + lastBit) % OCT_BIT_GROUP;
        if (lastBit > 0) {
            // there are bits left, keep it leave it to the next round
            lastData = thisByte >> (BYTE_BIT - lastBit);
        }
    }
    if (lastBit > 0) { // deal with the last
        *ws = char(lastData+48);
        ws--;
    }

    for ( ; ws == s; ws --) {
        *ws = '0'; // padding with '0'
    }

    return tail - s;
};
*/


int binaryToOctString(unsigned char *input, char *s, int32_t rLen) {
    int danglingBitNum = 0;
    int32_t wLen = getOctStringLen(rLen, &danglingBitNum);

    if (wLen <= 0) {
      return 0;
    }
    char *ws = s;
    char *tail = s + wLen;
    bool write = false;
    const uint8_t mask = 7;
    input += rLen-1; // move pointer to highest byte

    uint32_t lastData = 0; // the bits retrieved but not written yet from the last round.
    int lastBit = 0; // bit number of lastData.


    for (int i = 0; i < rLen ; ++i) { // move times, each time a byte
        uint32_t thisByte = *input;
        input -- ; // move a byte

        if (danglingBitNum > 0) { // enter here no more than once.
            uint32_t writeData = 0;

            lastBit = 3 - danglingBitNum; // fake the lastBit, use it for later bits
            danglingBitNum = 0; // from now, the remaining bits are dividable by 8, so no more danglingBit
            lastData = 0;
        }

        int shiftTime = (8 + lastBit) / 3;
        for (int j = 0; j < shiftTime; j++) { // shift 3 bits
            uint32_t writeData = 0;
            //=========
            int thisRemainBit = 3 - lastBit;
            if (j == 0 && lastBit > 0) { // combine bits from last round
                writeData = (lastData << thisRemainBit) | (thisByte >> (8 - thisRemainBit) ); // since thisByte is unsinged, padding will always be 0 when right shifting
            } else {
                if (lastBit > 0) {
                    writeData = (thisByte >> (8-thisRemainBit-3*j)) & mask;
                } else {
                   writeData = thisByte >> (8-3*(j+1)) & mask;
                }
            }
             //=========
            if (!write && writeData != 0 ) {
                write = true;
            }
            if (write) {
                *ws = char(writeData+48);
                ws++;
            }
        }
        lastBit = (8 + lastBit) % 3;
        if (lastBit > 0) {
            lastData = thisByte & (mask >> (3-lastBit));
        } else {
            lastData = 0;
        }

    }
    if (lastBit > 0) { // it is abnormal and should never happen, because danglingBitNum is handled, the rest bits should be dividable by 8.
        cout<<"abnormal !!!!!!!!!!!!!!!!!\n";
    }
    if (!write) { // it means the value is 0, then write a '0';
      *ws = '0';
      ws++;
    }
    for ( ; ws != tail; ) {
        *ws++ = 0;
    }
    ws = nullptr;
    return wLen;
};



vector<string> tokenize(const string &s, const string &delimiters) {
    string source_str(s);
    vector<string> return_vec;
    char *saveptr;
    char * pch;
    pch = strtok_r((char*)source_str.c_str(), delimiters.c_str() , &saveptr);
    while (pch != NULL) {
        return_vec.push_back(pch);
        pch = strtok_r(NULL, delimiters.c_str() , &saveptr);
    }
    return return_vec;
}

string escapeString(const string &str, char c) {
  string e_str;
  size_t s_sz = str.size();
  e_str.resize(2 * s_sz);
  size_t e_pos = 0;
  for (size_t i = 0; i < s_sz; ++i) {
    char sc = str[i];
    if (sc == '\\' || sc == c) {
      e_str[e_pos++] = '\\';
    }
    e_str[e_pos++] = sc;
  }
  e_str.resize(e_pos);
  return e_str;
}

string toCSVString(const char *s) {
  string csv_s;
  csv_s.reserve(1000);
  csv_s.push_back('"');
  while (*s != 0) {
    if (*s == '"')
      csv_s.push_back('"');
    csv_s.push_back(*s);
    s++;
  }
  csv_s.push_back('"');
  return csv_s;
}

namespace bigobject {

int rectifyUtf8(const char *s, int len) {
  if (len > 0) {
    unsigned char c = s[len - 1] & 0xC0;
    if (c == 0xC0) { // 0x1100---- | 0x1110---- | 0x1111----
      len--;
    } else if (c == 0x80) { // ... [10xxxxxx] ...
      if (len < 2)
        len = 0;
      else if ((s[len - 2] & 0xE0) == 0xE0) {
        len -= 2;     // 1110xxxx [10xxxxxx] 10xxxxxx
      } else if ((s[len - 3] & 0xF0) == 0xF0) {
        len -= 3;     // 1111xxxx 10xxxxxx [10xxxxxx] 10xxxxxx
      }
    }
  }
  return len;
}

void DoubleFormatToString::setMinMax() {
  max_v_ = (pow(10, MAX_DOUBLE_STR_LEN - 1 - precision_));
  min_v_ = -(pow(10, MAX_DOUBLE_STR_LEN - 2 - precision_));
}

DoubleFormatToString::DoubleFormatToString(int precision, bool is_rm_tz) {
  if (precision >  MAX_PRECISION)
    precision_ = MAX_PRECISION;
  else if (precision < 0)
    precision_ = BigObjectConfig::precision();
  else
    precision_ = precision;
  setMinMax();
  is_remove_trailing_zeros_ = is_rm_tz;
}

DoubleFormatToString::DoubleFormatToString(const DoubleFormatToString &rhs):
  precision_(rhs.precision_) {
  setMinMax();
  is_remove_trailing_zeros_ = rhs.is_remove_trailing_zeros_;
}

DoubleFormatToString::~DoubleFormatToString() {}

int numDigit(double v) {
  double x = abs(v);
  int d = 0;
  while (x > 1.0) {
    x = x / 10;
    d++;
  }
  return d + (v < 0);
}

char double_fmt[] = "%.*f";

// return string length after removing trailing zeros
int removeTrailingZeros(char *s) {
  char *p = strchr (s, '.');
  if (p != NULL) {
    while (*p != '\0')    // go to string end NULL.
      p++;
    p--;
    while (*p == '0') // remove trailing zeros.
      *p-- = '\0';
    if (*p == '.') {  // if all decimals were zeros, remove ".".
      *p = '\0';
    }
    return  (*p == '\0') ? (int)(p - s) : (int)(p - s + 1);
  }
  return strlen(s); // s without '.'
}


int DoubleFormatToString::toString(char *str, double v) {
  if (v < max_v_ && v > min_v_) {
    sprintf(str, double_fmt, precision_, v);
    if (is_remove_trailing_zeros_)
      return removeTrailingZeros(str);
    return strlen(str);
  } else {
    if (isfinite(v)) {
      int d = numDigit(v);
      if (d <= MAX_DOUBLE_STR_LEN - 2) {
        sprintf(str, double_fmt, MAX_DOUBLE_STR_LEN - d - 1, v);
        return removeTrailingZeros(str);
      } else {
    	/*
    	 * if MAX_DOUBLE_STR_LEN is 22, it means it allow string like "1.824679000000000e+100", which has length 22
    	 * its d should be 101
    	 * len means the length between '.' and 'e', ex: "824679000000000" of "1.824679000000000e+100" has length 15
    	*/
        int len = 0;
        if (d > 100)
          len = MAX_DOUBLE_STR_LEN - 7; // e+100 ~ e+307
        else if (d > 10) // means (d > MAX_DOUBLE_STR_LEN - 2 && d <= 100)
          len = MAX_DOUBLE_STR_LEN - 6; // <e+99
        else // means d <= 10, will never enter here, since this case was handled by "if (d <= MAX_DOUBLE_STR_LEN - 2)"
          len = MAX_DOUBLE_STR_LEN - 5; // <- this may be meaningless

        sprintf(str, "%.*e", len, v);
        return strlen(str);
      }
    }
#if defined(IOT_MODE)
    else if (isinf(v)) {
      if (v >= 0) {
        mmap_strlcpy(str, cstr_inf, 3);
        return 3;
      } else {
        mmap_strlcpy(str, cstr_n_inf, 4);
        return 4;
      }
    }
#endif
    else {
      mmap_strlcpy(str, s_NULL().c_str(), 4);
      return 4;
    }
  }
}

const char * strNonWSpace(const char *s) {
  if (s) {
    while (isspace(*s)) {
      s++;
    }
  }
  return s;
}

int geomToWKTMaxLen(int type) {
  switch(type) {
    case POINT:
    case WKB_POINT:
      return MAX_DOUBLE_STR_LEN * 2 + 8;  // POINT( )
    case LINESTRING:
    case WKB_LINESTRING:
      return VARSTRING_MAX_LEN;
  }
  return VARSTRING_MAX_LEN;
}

char * toNULL(char *s) {
  memcpy(s, s_NULL().data(), 5);
  return s + 4;
}

PointToString::PointToString(int precision, char s): dbl_to_str_(precision),
  sep_(s) {}

PointToString::PointToString(const PointToString &rhs):
  dbl_to_str_(rhs.dbl_to_str_), sep_(rhs.sep_) {}

PointToString::~PointToString() {}

void PointToString::setSeperator(char sep) { sep_ = sep; }

char * PointToString::toString(char *s, Point *p) {
  s += dbl_to_str_.toString(s, p->x);
  *s++ = sep_;
  if (sep_ != ' ')
    *s++ = ' ';
  s += dbl_to_str_.toString(s, p->y);
  return s;
}

} // namespace bigboject
