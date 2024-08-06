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

#ifndef DATATYPE_H_
#define DATATYPE_H_


#include <inttypes.h>
#include <vector>
#include <string>
#include <sstream>

using namespace std;

/******************************************************************************
 * Possible combination of data types:
 *
 * OS           | IDTYPE    | HASH  | PTRTYPE   | NS/BT/BO limit
 * -------------+-----------+-------+-----------+---------------------
 * x64 (BO64)   | 64        | 64    | 64        | 16EB / 16EB / 16EB
 * (unused)     | 32        | 64    | 64        | 4GB / 16EB / 16EB
 * (default)    | 32        | 32    | 64        | 4GB / 4GB / 16EB
 * (BO32)       | 32        | 32    | 32        | 4GB / 4GB / 4GB
 * -------------+-----------+-------+-----------+---------------------
 * x32          | 32        | 32    | 32        | 4GB / 4GB / 4GB
 *****************************************************************************/

#ifdef BO64
#define ID64
#define PTR64
#define HASH64
#endif

#ifdef BO32
#define ID32
#define HASH32
#define PTR32
#endif

typedef uint64_t ID64TYPE;

//#define ID64

#ifndef ID64
typedef uint32_t IDTYPE;
#else
typedef uint64_t IDTYPE;
#endif

#define PTR64
#ifndef PTR64
typedef uint32_t PTRTYPE;
#else
typedef intptr_t PTRTYPE;
#endif

#ifndef HASH64
#define HASH32
typedef uint32_t HASHTYPE;
#else
#undef HASH32
typedef uint64_t HASHTYPE;
#endif

/* Test for GCC < 4.6. However, gcc 4.4 or above is required */
#if __GNUC__ < 4 || (__GNUC__ == 4 && __GNUC_MINOR__ < 6)
#define nullptr		NULL
#endif

#if defined(ANDROID)
#if !defined(__ANDROID__)
#define __ANDROID__
#endif
#endif

//              fixup   INT64   DOUBLE  FLOAT
//---------------------------------------------
// Android      X       X       X       O
// raspberry    O       O       X       X
// arm-64       ?       ?       ?       O
// <= 4 bytes unaligned integers are assumed to be supported by hardware

#if defined(__ANDROID__) || defined(__x86_64__) || defined(_M_X64)
#define __UNALIGNED_FLOAT__
#endif

#if defined(__x86_64__) || defined(_M_X64) || (__aarch64__)
#define __UNALIGNED_ACCESS__
#endif

// non-android (ubuntu) on 64-bit arm (fixup)
#if defined(__arm__)
#undef __UNALIGNED_ACCESS__
#endif

// No aligned access support(use memcpy)
//#define ALIGNED
#if defined(ALIGNED)
#undef __UNALIGNED_ACCESS__
#endif

#define DICTIONARY              0x00000000
#define DEFAULT_STRING_MAX_LEN      63
#define DEFAULT_CHAR_MAX_LEN        32
#define DEFAULT_VARSTRING_MAX_LEN   255
#define STRING_MAX_LEN              1023
#define CHAR_MAX_LEN                786432      // 768K
#define VARSTRING_MAX_LEN           786432      // 768K

#define MAX_ATTR_LEN                30
#define MAX_COLUMNATTR_LEN          63     // 30 + . + 30
#define MAX_DATABASE_NAME_LEN       63
#define MAX_USER_NAME_LEN           32
#define MAX_PASSWORD_LEN            63

#define MAX_INT8_STR_LEN        4
#define MAX_INT16_STR_LEN       6
#define MAX_INT32_STR_LEN       12
#define MAX_INT64_STR_LEN       21
#define MAX_TIME_STR_LEN        13
#define MAX_TIME64_STR_LEN      29  //time64 is time and millisecond, 2023-06-13 10:00:00.123456789

#define MAX_DOUBLE_STR_LEN      22
#define MAX_PRECISION           15

#define L_CHAR                  0
#define R_CHAR                  1

typedef unsigned char uchar;

/**
 * time type to be used in data type or time_unit.
 * @see DataType class
 */
enum TIMEUNITTYPE {
  DAILY, WEEKLY, BIWEEKLY, SEMIMONTHLY, MONTHLY, BIMONTHLY, QUARTERLY,
  SEMIANNUAL, ANNUAL
};


//////////////////////
// BigObject Data type
enum DATATYPE {
  NO_TYPE = -2,             // for create tree
  INVALID = -1,
  TIMESTAMP64 = 0,
  INT16 = 1,
  INT32 = 2,
  INT64 = 3,
  FLOAT = 4,
  DOUBLE = 5,
  BYTE = 6,
  CHAR = 7,
  BINARY = 8,

  VARSTRING = 10,
  VARBINARY = 11,

  STRING = 13,
  INT32_ARRAY = 14,
  INT64_ARRAY = 15,
  FLOAT_ARRAY = 16,
  DOUBLE_ARRAY = 17,
  RESERVED_TYPE18 = 18,     // CLOB, BLOB,
  DATE32 = 19,
  DATETIMEDOS = 20,         // DATE, TIME MS DOS format
  DATETIME64 = 21,
  WEEK = 22,
  BOOL_ARRAY = 23,
  BOOL = 24,
  FUNCVARBINARY = 25,       // function generated variable length BINARY
  TIMESTAMP = 26,           // 4 bytes time stamp up to 2038/01/19
  INT16_ARRAY = 27,
  TIME = 28,                // 4 bytes, curtime(), timediff()
  INT8 = 29,
  BYTE_ARRAY = 30,
  BLOB = 31,
  TIMESTAMP64_LSN = 32,     // 16 bytes, first 8 bytes for timestamp, next 8 bytes for lsn
  RESERVED_9 = 33,
  INT8_ARRAY = 34,
  DATETIME32 = 50,          // 32-bit date time.
  FUNCCHAR = 51,            // function generated CHAR (fixed length)
  HYPERLOGLOG = 52,         // HyperLogLog
  IPV4 = 53,
  IPV6 = 54,
  TDIGEST = 55,             // T-Digest
                            // reserved
  EXTENSION = 57,
  TIME64 = 58,              // 8 bytes integer time with microsecond
  POINT = 101,                // point(x y)
  LINESTRING = 102,           // linestring(x1 y1, x2 y2,...)
  POLYGON = 103,
  MULTIPOINT = 104,
  MULTILINESTRING = 105,
  MULTIPOLYGON = 106,
  GEOMCOLLECT = 107,          // GEOMETRYCOLLECTION
  GEOMETRY = 108,             // GEOMETRY
  WKB_POINT = 141,            // WKB pseudo type for ST_ASBINARY
  WKB_LINESTRING = 142,
  WKB_POLYGON = 143,
  WKB_MULTIPOINT = 144,
  WKB_MULTILINESTRING = 145,
  WKB_MULTIPOLYGON = 146,
  WKB_GEOMCOLLECT = 147,
  WKB_GEOMETRY = 148,
  BBOX = 199,
  // 9091 ver 3: <= 256 reserved.
  // 9091 ver 4: 200-998 reserved.
  NULL_TYPE = 999,
  ROWID = 1000,			    /// ROWID item
  STRUCT = 0x10000,         /// STRUCT (CLASS)

  // constant data type.
  INTEGER_CONST = 20000,
  REAL_CONST = 20001,
  STRING_CONST = 20002,
  EXPR_LIST = 2000000,  // expression list
};

struct ObjectNameInfo {
  std::string db;
  std::string name;
  std::string alias;

  void clear();
  std::string combinedName() const;
  bool isNameFit(const string &s) const;
};

struct ColumnNameInfo {
  string obj;
  string name;
  int col;

  void clear();
  string combinedName() const;
};

string combinedName(const string &db, const string &name);

struct ObjectColumnInfo {
  const char *db;
  const char *obj;
  const char *name;

  ObjectColumnInfo();
  string toObjectColumnName();
};

struct ForeignKeyConstraint {
  string cstr_name;
  ObjectNameInfo ref_tbl;
  vector<string> col;
  vector<string> ref_col;
};

#define NORMAL_JOIN     0x00000001
#define RANGE_JOIN      0x00000010

//struct JoinCond {
//  ColumnNameInfo l;
//  ColumnNameInfo r;     // l between r and r2
//  ColumnNameInfo r2;
//  ExprNode *expr_l;
//  ExprNode *expr_r;
//  int type;
//
//  void toLowerCase();
//};

typedef uint32_t    timestamp;
typedef int64_t    timestamp64;
typedef uint64_t    TS_LSN;

union NumericValue {
  int8_t i8;
  int16_t i16;
  int32_t i32;
  int64_t i64;
  float f;
  double d;
  timestamp64 ts64;
  size_t st;
  intptr_t ptr;     // pointer to string/variable-length data
};

bool isIntegerType(int type);       // is integer type
bool isFloatType(int type);         // is FLOAT or DOUBLE?
bool isNumericType(int type);       // integer or float

bool isDateTimeType(int type);

// check whether type is a string or not
bool isStringType(int type);

bool isStringExcludeVartringType(int type);

// return type if type == VARSTRING | STRING | STRING_CONST
bool isNullTerminatedStringType(int type);

// return true if type == CHAR or FUNCCHAR
bool isCHARStringType(int type);

// check whether type is a string (except STRING) or not
bool isCharStringType(int type);

bool isBinaryType(int type);    // BINARY or VARBINARY

bool isConstantType(int type);  // string or numeric constant

bool isGeometryType(int type);

bool isWKBType(int type);

bool isLenDataType(int type);

// Check whether the type needs a string file or not.
bool isStringFileNeeded(int type);

bool isNullType(int type);

struct DimensionInfo {
  std::string name;       ///< name of dimension.
  std::string url;        ///< URL of dimension.
};

#define AINFO_INTERNAL              0x00000001  // internal column
#define AINFO_HAS_DEFAULT           0x00000002  // has default value set
#define AINFO_NOT_NULL              0x00000004  // NOT NULL
#define AINFO_DROPPED               0x00000008  // column has been dropped
#define AINFO_EXTERNAL              0x00000010  // external VARSTRING,...,etc.
#define AINFO_GEOHASH               0x00000100
#define AINFO_BITMAP_INDEX          0x00001000
#define AINFO_GRAPHID               0x00010000  // Node id in graph.
#define AINFO_OOO_VARSTRING         0x00100000  // out of order VARSTRING
#define AINFO_KEY                   0x10000000
#define AINFO_INDEX_ORDER           0x01000000
#if defined(COLUMN_GROUP)
#define AINFO_COLGROUP              0x00000010
#endif

#define ATTRINFO_VALUES_SIZE        sizeof(int32_t) * 8
#define ATTRINFO_VALUES_FIXED_SIZE  sizeof(int32_t) * 5   // before flag.

enum LASTAGGFLAG{
  OTHERS,
  FIRST,
  LAST,
  LASTROW,
  LAST_TS,
  LAST_ROW_TS
};

enum AttrType {
    ATTR_INVALID=-1,
    ATTR_TS_DATA,
    ATTR_GENERAL_TAG,
    ATTR_PRIMARY_TAG,
};

enum class AlterType {
  ADD_COLUMN,
  DROP_COLUMN,
  MODIFY_COLUMN
};

#define AttrInfoTailSize      40 // (sizeof(AttributeInfo) - 2 * sizeof(string) - sizeof(uint32_t))
struct AttributeInfo {
  uint32_t id;           /// < column id.
  std::string name;       ///< Attribute name.
  int32_t type;           ///< Attribute type.
  int32_t offset;         ///< Offset.
  int32_t size;           ///< Size.
  int32_t length;         ///< Length.
  int32_t encoding;
  int32_t flag;           ///< internal use.
  int32_t max_len;        ///< max length for string; Geohash precision;
                          // timestamp precision, time precision
  uint32_t version;       /// table column version
  AttrType attr_type;
  LASTAGGFLAG lastFlag; // 1 = first 2= last 3= lastrow
  std::string default_str;  // default value

  AttributeInfo();

  bool isValid() { return !(type == INVALID); }

  bool isEqual(const AttributeInfo& other) const { return (id == other.id) && (flag == other.flag); }

  inline void setFlag(int32_t f) { flag |= f; }
  inline void unsetFlag(int32_t f) { flag &= ~f; }
  inline int isFlag(int32_t f) const { return (flag & f); }

  inline void setAttrType(int attrType) { attr_type = (AttrType)attrType; }
  inline bool isAttrType(int expAttrType) const
	{ return (attr_type == (AttrType)expAttrType); }

  void setBitMapIndex() { flag |= AINFO_BITMAP_INDEX; }

  bool hasBigMapInex() { return (flag & AINFO_BITMAP_INDEX); }

#if defined(COLUMN_GROUP)
  void setColumnGroup() { flag |= AINFO_COLGROUP; }

  bool isColumnGroup() const { return (flag & AINFO_COLGROUP); }
#endif

  bool operator==(AttributeInfo& rhs) const;
};

enum DurationType {
  DUR_LAST_LIMIT, DUR_LAST_TIME, DUR_SINCE, DUR_BETWEEN,
};

// Duration flags
#define DUR_START_DATETIME              0x00000001      // start is a datetime
#define DUR_END_DATETIME                0x00000010      // end is a datetime
#define DUR_FIXED                       0x10000000      // start/end are fixed

struct Duration {
  std::string name;
  string start_str;
  string end_str;
  size_t start;
  size_t end;
  int type;
  int flag;
  int start_prec;                           // timestamp precision
  int end_prec;

  Duration() {
    type = INVALID;
    start = (size_t) -1;
    end = (size_t) -1;
    flag = 0;
    start_prec = end_prec = 0;
  }

  bool isValid() const { return !(type == INVALID); }

  void clear() { type = INVALID; }
};

#if defined(COLUMN_GROUP)
struct ColumnGroup
{
    std::string name;
    std::vector<AttributeInfo> schema;
    std::vector<std::string> key;
};
#endif


inline void * offsetAddr(const void *addr, size_t offset)
{ return (void *)((unsigned char *)addr + offset); }

// macro for LD(length data): VarBinary | Geometry
// LD = [len][data]
// data length = sizeof(int32_t) + length of data
#define LD_LEN(x) (*(int32_t *)x)
#define LD_DATALEN(x) ((*(int32_t *)x) - sizeof(int32_t))
#define LD_GETDATA(x) ((unsigned char *)x + sizeof(int32_t))

// calculate the total length of LD
#define CALC_LD_LEN(n) (n + sizeof(int32_t))

// assign total length to LD
inline void __attribute__ ((always_inline)) LD_setLen(void *ld, int n)
{ *((int32_t *)ld) = n; }

// assign data length to LD
inline void __attribute__ ((always_inline)) LD_setDataLen(void *ld, int n)
{ *((int32_t *)ld) = n + sizeof(int32_t); }


struct SamplingFilter {
  int type;                 // INT64 | DOUBLE
  size_t size;
  double ratio;

  SamplingFilter() { type = 0; }
};

struct TreeFilter {
  std::string xpath;
  std::string name;

  TreeFilter() { name = ""; }
};

enum wkbGeometryType {
  wkbPoint = 1,
  wkbLineString = 2,
  wkbPolygon = 3,
  wkbMultiPoint = 4,
  wkbMultiLineString = 5,
  wkbMultiPolygon = 6,
  wkbGeomCollect = 7,
};

struct Point {
  double x, y;

  Point() {}
  Point(double x, double y) { this->x = x; this->y = y; }
  Point(const Point &b) { x = b.x; y = b.y; }
  Point(Point &&b) { x = b.x; y = b.y; }
  // interpolated point = (1 - t) * p1 + t * p2;.
  Point(const Point *p1, const Point *p2, double t) {
    double one_minus_t = 1 - t;
    x = one_minus_t * p1->x + t * p2->x;
    y = one_minus_t * p1->y + t * p2->y;
  }
  void toNull();
  bool isNull() const;

  inline bool operator==(const Point &b) { return ((x == b.x) && (y == b.y)); }
  inline bool operator!=(const Point &b) { return ((x != b.x) || (y != b.y)); }
  Point & operator=(const Point &b) { x = b.x; y = b.y; return *this; }
  Point & operator=(Point &&b) { x = b.x; y = b.y; return *this; }
  Point operator-(const Point &b) const { return Point(x - b.x, y - b.y); }
  inline double operator*(const Point &b) const { return (x * b.x + y * b.y); }
//  void interpolate(Point *p1, Point *p2, double t);
};

void initData();

// MultiPoint
struct LineString {
  int32_t len;
  int32_t num_points;
  Point points;
};

// MultiLineString
struct Polygon {
  int32_t len;
  int32_t num_lines;
  LineString lines;

  inline bool isEmpty() const
  { return (num_lines == 0 || lines.num_points == 0); }

  const Point * startPoint() const;
};

struct MultiPolygon {
  int32_t len;
  int32_t num_polygons;
  Polygon polygons;

  bool isEmpty() const;
  const Point * startPoint() const;
};

struct Geometry {
  int32_t len;
  int32_t type;
  union  {
    Point point;
    LineString linestr;
    Polygon polygon;
    MultiPolygon multipoly;
    // size of GeomCollect = other non-point geometry types.
  } data;
  bool isEmpty() const { return (type == 0); }
};

struct GeomCollect {
  int32_t len;
  int32_t num_geometrys;
  Geometry geometry;

  bool isEmpty() const;
};


// return the next lengthy object.
#define nextObject(o) offsetAddr(o,o->len)

namespace bigobject {
class Action;
};

// set Common Type.
struct CommonType {
  int type;
  int size;
  int max_len;

  void setType(bigobject::Action *act);
  int toCommonType(bigobject::Action *act);
};

// return the common type of t1 and t2; -1 if there is no common type
// e.g. (INT32, INT64) => INT64
int toCommonType (int t1, int t2);

// return the data size of common type for sz1 and sz2 with max size max_sz
int toCommonSize(int common_type, int max_sz, int sz1, int sz2);

struct InputString {
  bool is_ok;           // the string is set by user or not.
  string str;
};

struct TimeStamp64LSN {
  timestamp64 ts64;
  TS_LSN lsn;

  TimeStamp64LSN(timestamp64 ts, TS_LSN lsn) : ts64(ts), lsn(lsn){}

  TimeStamp64LSN(void * buf) {
    ts64 = (*(static_cast<timestamp64 *>(buf)));
    lsn = (*(static_cast<TS_LSN *>((void *)((intptr_t)buf + 8))));
  }

  static timestamp64 * getTimestampAddr(void * buf) {
    return static_cast<timestamp64*>(buf);
  }

  static TS_LSN * getLsnAddr(void * buf) {
    return static_cast<TS_LSN*>((void *)((intptr_t)buf+8));
  }

  std::string to_string() {
    std::stringstream ss;
    ss << ts64 << "," << lsn;
    return ss.str();
  }
};

std::string getDataTypeName(int32_t data_type);

#endif /* DATATYPE_H_ */
