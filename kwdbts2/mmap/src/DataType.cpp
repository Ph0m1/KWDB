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

#include <cmath>
#include "DataType.h"
//#include "action/Action.h"
#include "BigObjectUtils.h"


void ObjectNameInfo::clear() {
  db.clear();
  name.clear();
  alias.clear();
}

std::string ObjectNameInfo::combinedName() const {
    return (db.empty()) ? name : (db + '.' + name);
}

bool ObjectNameInfo::isNameFit(const string &s) const
{ return ((s == alias || s == name)); }

void ColumnNameInfo::clear() {
  obj.clear();
  name.clear();
}

ObjectColumnInfo::ObjectColumnInfo()
{ db = obj = name = s_emptyString().c_str(); }

string ObjectColumnInfo::toObjectColumnName() {
  if (obj == nullptr)
    return string(name);
  return string(obj) + '.' + string(name);
}

std::string ColumnNameInfo::combinedName() const
{ return (obj.empty()) ? name : (obj + '.' + name); }

//void JoinCond::toLowerCase() {
//  toLowerText(l.obj);
//  toLowerText(l.name);
//  toLowerText(r.obj);
//  toLowerText(r.name);
//  toLowerText(r2.obj);
//  toLowerText(r2.name);
//}

string combinedName(const string &db, const string &name) {
  return (db.empty()) ? name : (db + '.' + name);
}

bool isIntegerType(int type)
{ return ((BOOL <= type && type <= INT64)); }

bool isFloatType(int type)
{ return ((type == FLOAT) || (type == DOUBLE)); }

bool isNumericType(int type)
{ return ((BOOL <= type && type <= DOUBLE)); }

bool isDateTimeType(int type) {
  return (type == DATE32 || type == DATETIMEDOS || type == DATETIME32
    || type == DATETIME64 || type == TIMESTAMP64 || type == TIMESTAMP64_LSN || type == TIMESTAMP
    || type == TIME || type == TIME64);
}

//bool isRealType(int type) { return (type == FLOAT || type == DOUBLE); }

bool isStringType(int type) {
  return (type == STRING || type == CHAR || type == VARSTRING ||
    type == STRING_CONST || type == FUNCCHAR);
}

bool isStringExcludeVartringType(int type) {
  return (type == STRING || type == CHAR ||
    type == STRING_CONST || type == FUNCCHAR);
}

bool isNullTerminatedStringType(int type)
{ return (type == STRING || type == VARSTRING || type == STRING_CONST); }

bool isCHARStringType(int type) { return (type == CHAR || type == FUNCCHAR); }

bool isCharStringType(int type) {
  return (type == CHAR || type == VARSTRING || type == STRING_CONST ||
    type == FUNCCHAR);
}

bool isBinaryType(int type) { return (type == BINARY || type == VARBINARY); }

bool isConstantType(int type) {
  return (type == STRING_CONST || type == INTEGER_CONST ||
    type == REAL_CONST);
}

bool isGeometryType(int type)
{ return (type >= POINT && type <= GEOMETRY); }

bool isWKBType(int type)
{ return (type >= WKB_POINT && type <= WKB_GEOMETRY); }

bool isLenDataType(int type) {
  return ((type == VARBINARY) || (type == FUNCVARBINARY) ||
    ((type >= LINESTRING) && (type <= GEOMETRY)) ||
    ((type >= WKB_LINESTRING) && (type <= WKB_GEOMETRY)));
}

bool isStringFileNeeded(int type) {
  return ((type == VARSTRING) || (type == BLOB) || (type == VARBINARY) ||
    (type == FUNCVARBINARY) || ((type >= LINESTRING) && (type <= GEOMETRY)) ||
    ((type >= WKB_LINESTRING) && (type <= WKB_GEOMETRY)));
}

bool isNullType(int type){
  return type==NULL_TYPE;
}

AttributeInfo::AttributeInfo() {
  id = 0;
  type = INVALID;
  offset = size = 0;
  length = 1;
  encoding = 0;
  flag = 0;
  max_len = 0;
  version = 0;
  lastFlag = OTHERS;
  attr_type = ATTR_INVALID;
}

bool AttributeInfo::operator==(AttributeInfo& rhs) const {
  if (name != rhs.name)
    return false;
  return true;
}

Point null_point;

void initData() {
  null_point.toNull();
}

const Point * Polygon::startPoint() const {
  if (isEmpty())
    return &null_point;
  return &(lines.points);
}

bool MultiPolygon::isEmpty() const {
  if (num_polygons == 0)
    return true;
  const Polygon *py = &polygons;
  for (int i = 0; i < num_polygons; ++i) {
    if (!py->isEmpty())
      return false;
  }
  return true;
}

bool GeomCollect::isEmpty() const {
  if (num_geometrys == 0)
    return true;
  const Geometry *g = &geometry;
  for (int i = 0; i < num_geometrys; ++i) {
    switch(g->type) {
    case POINT:
      if (!(g->data.point.isNull()))
        return false;
      break;
    case LINESTRING:
      if (g->data.linestr.num_points)
        return false;
      break;
    case POLYGON:
      if (!(g->data.polygon.isEmpty()))
        return false;
      break;
    case MULTIPOLYGON:
      if (!(g->data.multipoly.isEmpty()))
        return false;
      break;
    case GEOMCOLLECT: {
      GeomCollect *gc = (GeomCollect *)&(g->data);
      if (!gc->isEmpty())
        return false;
    }
    }
  }
  return true;
}

const Point * MultiPolygon::startPoint() const {
  if (num_polygons == 0)
    return &null_point;
  const Polygon *py = &polygons;
  for (int i = 0; i < num_polygons; ++i) {
    if (!py->isEmpty())
      return py->startPoint();
  }
  return &null_point;
}


void Point::toNull()
{ x = y = std::numeric_limits<double>::quiet_NaN(); }

bool Point::isNull() const {
  return (!isfinite(x) || !isfinite(y));
}

//void Point::interpolate(Point *p1, Point *p2, double t) {
//  double one_minus_t = 1 - t;
//  x = one_minus_t * p1->x + t * p2->x;
//  y = one_minus_t * p1->y + t * p2->y;
//}


int toStringType(int type, int max_len1, int max_len2) {
  return (abs(max_len1 - max_len2) > 12) ? VARSTRING : type;
}


int toCommonType (int t1, int t2) {
  if (t1 > t2) {           // make sure t1 <= t2
    std::swap(t1, t2);
  }
  if (t1 == t2) {
    return (t1 == STRING_CONST) ? FUNCCHAR : t1;
  }

  if (isIntegerType(t1)) {
    if (isIntegerType(t2))
      return t2;
    if (t2 == NULL_TYPE)
      return t1;
  }
  switch(t1) {
    case STRING:
      switch(t2) {
        case STRING:    return STRING;
      }
      break;
    case CHAR:
      switch(t2) {
        case STRING_CONST:  return CHAR;
        case FUNCCHAR:     return FUNCCHAR;
      }
      break;
    case FUNCCHAR:
      switch(t2) {
        case STRING_CONST:  return FUNCCHAR;
      }
    case VARSTRING: return VARSTRING;
  }
  return INVALID;
}

int toCommonSize(int common_type,  int max_sz, int sz1, int sz2) {
  switch (common_type) {
    case VARSTRING: return sizeof(size_t);
    case STRING: return sizeof(IDTYPE);
    case CHAR: return max_sz;
  }
  return std::max(sz1, sz2);
}

std::string getDataTypeName(int32_t data_type) {
  switch (data_type)
  {
    case DATATYPE::INT16:   return "INT16";
    case DATATYPE::INT32:   return "INT32";
    case DATATYPE::INT64:   return "INT64";
    case DATATYPE::FLOAT:   return "FLOAT";
    case DATATYPE::DOUBLE:  return "DOUBLE";
    case DATATYPE::CHAR:    return "CHAR";
    case DATATYPE::BINARY:  return "BINARY";
    case DATATYPE::VARSTRING: return "VARSTRING";
    case DATATYPE::VARBINARY: return "VARBINARY";
    default:
      break;
  }
  return "UNKNOW_TYPE";
}
