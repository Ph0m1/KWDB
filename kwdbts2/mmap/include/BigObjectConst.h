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

#ifndef INCLUDE_BIGOBJECTCONST_H_
#define INCLUDE_BIGOBJECTCONST_H_

#include <string>
#include "default.h"

using namespace std;

namespace bigobject {

inline const string& s_emptyString() { static string s = ""; return s; }
inline const string& s_NULL() { static string s = "NULL"; return s; }
inline const string& s_true() { static string s = "true"; return s; }
inline const string& s_false() { static string s = "false"; return s; }
inline const string& s_hierarchical_part() { static string s = ":///"; return s; }
inline const string& s_deletable() { static string s = "_d"; return s; }
inline const string& s_cts() { static string s = "_cts"; return s; }
inline const string& s_bt() { static string s = ".bt"; return s; }
inline const string& s_bg() { static string s = ".bg"; return s; }
inline const string& s_bo() { static string s = ".bo"; return s; }
inline const string& s_ns() { static string s = ".ns"; return s; }
inline const string& s_ht() { static string s = ".ht"; return s; }
inline const string& s_qbo() { static string s = ".qbo"; return s; }
inline const string& s_bi() { static string s = ".bi"; return s; }
inline const string& s_bit() { static string s = ".bit"; return s; }
inline const string& s_s() { static string s = ".s"; return s; }
inline const string& s_new() { static string s = "-new"; return s; }
inline const string& s_old() { static string s = "-old"; return s; }
#if defined(KAIWU)
inline const string& s_bigobject() { static string s = "kaiwudb"; return s; }
inline const string& s_BigObject() { static string s = "KaiwuDB"; return s; }
extern const char *cstr_kaiwudb;
extern const char *cstr_Kaiwu;
#else
inline const string& s_bigobject() { static string s = "bigobject"; return s; }
inline const string& s_BigObject() { static string s = "BigObject"; return s; }
#endif
inline const string& s_STRING() { static string s = "STRING"; return s; }
inline const string& s_BOOL() { static string s = "BOOL"; return s; }
inline const string& s_BYTE() { static string s = "BYTE"; return s; }
inline const string& s_INT8() { static string s = "INT8"; return s; }
inline const string& s_INT16() { static string s = "INT16"; return s; }
inline const string& s_INT32() { static string s = "INT32"; return s; }
inline const string& s_INT64() { static string s = "INT64"; return s; }
inline const string& s_FLOAT() { static string s = "FLOAT"; return s; }
inline const string& s_DOUBLE() { static string s = "DOUBLE"; return s; }
inline const string& s_DATE32() { static string s = "DATE32"; return s; }
inline const string& s_DATETIMEDOS() { static string s = "DATETIMEDOS"; return s; }
inline const string& s_DATETIME32() { static string s = "DATETIME32"; return s; }
inline const string& s_DATETIME64() { static string s = "DATETIME64"; return s; }
inline const string& s_WEEK() { static string s = "WEEK"; return s; }
inline const string& s_VARSTRING() { static string s = "VARSTRING"; return s; }
inline const string& s_CHAR() { static string s = "CHAR"; return s; }
inline const string& s_ROWID() { static string s = "ROWID"; return s; }
inline const string& s_TIMESTAMP() { static string s = "TIMESTAMP"; return s; }
inline const string& s_Unknown() { static string s = "Unknown"; return s; }
inline const string& s_PRI() { static string s = "PRI"; return s; }
inline const string& s_NO() { static string s = "NO"; return s; }
inline const string& s_YES() { static string s = "YES"; return s; }

inline const string& s_fulltext() { static string s = "fulltext"; return s; }
//inline const string& s_bitmap() { static string s = "bitmap"; return s; }
inline const string& s_circular() { static string s = "circular"; return s; }
inline const string& s_window() { static string s = "window "; return s; }
inline const string& s_column() { static string s = "column"; return s; }
inline const string& s_row() { static string s = "row"; return s; }
inline const string& s_asterisk() { static string s = "*"; return s; }

inline const string& s_cast() { static string s = "cast"; return s; }

inline const string& s_SUM() { static string s = "SUM"; return s; }
inline const string& s_corr() { static string s = "corr"; return s; }

// environment variable
#if defined(KAIWU)
extern const char *cstr_KW_ALIAS;
extern const char *cstr_metadata;
#else
extern const char *cstr_BO_ALIAS;
#endif



extern const char *cstr_BLOB;
extern const char *cstr_LINESTRING;
extern const char *cstr_LineString;
extern const char *cstr_POINT;
extern const char *cstr_Point;
extern const char *cstr_POLYGON;
extern const char *cstr_Polygon;
extern const char *cstr_MULTIPOLYGON;
extern const char *cstr_MultiPolygon;
extern const char *cstr_GEOMETRYCOLLECTION;
extern const char *cstr_GeometryCollection;
extern const char *cstr_GEOMETRY;
extern const char *cstr_Geometry;
extern const char *cstr_TIMESTAMP64;
extern const char *cstr_VARBINARY;
#define cstr_BINARY  &(cstr_VARBINARY[3])

extern const char *cstr_Create_Table;
#define cstr_Table  &(cstr_Create_Table[7])

extern const char *cstr_associations;
extern const char *cstr_charset;
extern const char *cstr_columns;
extern const char *cstr_comment;
extern const char *cstr_config;
extern const char *cstr_databases;
extern const char *cstr_engines;
extern const char *cstr_graph;
extern const char *cstr_graphs;
extern const char *cstr_grants;
extern const char *cstr_keys;
extern const char *cstr_procedure;
extern const char *cstr_processlist;
extern const char *cstr_table;
extern const char *cstr_tables;
extern const char *cstr_trees;
extern const char *cstr_triggers;
extern const char *cstr__variables;
#define cstr_variables  &(cstr__variables[1])

extern const char *cstr_Charset;
extern const char *cstr_Collation;
extern const char *cstr_Engine;

extern const char *cstr_null;
extern const char *cstr_inf;
extern const char *cstr_n_inf;

extern const char *cstr_database;
extern const char *cstr_references;
extern const char *cstr_status;
extern const char *cstr_Error;

extern const char *cstr_LEFT_;
extern const char *cstr_INNER_;
extern const char *cstr_JOIN;
extern const char *cstr_FULL;
extern const char *cstr_OUTER_;
extern const char *cstr__ON_;
extern const char *cstr__BETWEEN_;

extern const char *cstr_COUNT;
extern const char *cstr_GEOHASH;
extern const char *cstr_HYPERLOGLOG;
extern const char *cstr_VARIANCE;
extern const char *cstr_MEDIAN;
extern const char *cstr_QUANTILE;
extern const char *cstr_TDIGEST;

extern const char *cstr_aes_decrypt;
extern const char *cstr_aes_encrypt;
extern const char *cstr_year;
extern const char *cstr_quarter;
extern const char *cstr_month;
extern const char *cstr_day;
extern const char *cstr_hour;
extern const char *cstr_minute;
extern const char *cstr_second;
extern const char *cstr_millisecond;
extern const char *cstr_week;
extern const char *cstr_isoweek;
extern const char *cstr_yearweek;
extern const char *cstr_yearday;
extern const char *cstr_weekday;

extern const char *cstr_default;
extern const char *cstr_sliding;
extern const char *cstr_datediff;

#define cstr_diff   (&(cstr_datediff[4]))

extern const char *cstr_timestamp;
extern const char *cstr_time64;
extern const char *cstr_cumsum;
extern const char *cstr_cummax;
extern const char *cstr_group_rank;
extern const char *cstr_group_row_number;
extern const char *cstr_zero_byte;

extern const char *cstr_CREATE;
extern const char *cstr_UPDATE;

extern const char *cstr_INDEX;
extern const char *cstr_FOREIGN_KEY;
#define cstr_KEY  &(cstr_FOREIGN_KEY[8])

extern const char *cstr_LAST;
extern const char *cstr_WHERE;
extern const char *cstr_SELECT;
extern const char *cstr_AS;
extern const char *cstr_ORDER;
extern const char *cstr__DESC;
extern const char *cstr_FROM;

extern const char *cstr_ALTER;
extern const char *cstr_CLUSTER;
extern const char *cstr_DELETE;
extern const char *cstr_DROP;
extern const char *cstr_FILE;
extern const char *cstr_INSERT;
extern const char *cstr_BY;

extern const char *cstr_SUM;

extern const char *cstr_lbracket;
extern const char *cstr_rbracket;

extern const char *cstr_AND;
extern const char *cstr__AND_;
extern const char *cstr_OR;
extern const char *cstr_NOT;

extern const char *cstr_APPROXIMATE;
extern const char *cstr_filter;
extern const char *cstr_IPv4;
extern const char *cstr_IPv6;
extern const char ipv4_prefix[];
extern const char *cstr_gh_adjacent;
extern const char *cstr_gh_expand;

extern const char *cstr_gh_neighbors;

extern const char cstr_DEFAULT[];
extern const char cstr_TABLE[];

extern const char *cstr_alias;
extern const char *cstr_rank;
extern const char *cstr_at;
extern const char *cstr_bottom;
extern const char *cstr_def;
extern const char *cstr_fact;
extern const char *cstr_freq;
extern const char *cstr_prob;

extern const char *cstr_information_schema;
extern const char *cstr_remotehosts;

#define cstr_schema   (&(cstr_information_schema[12]))

extern const char *cstr__lbracket;
extern const char *cstr__WHERE_;        // " WHERE "
extern const char *csym__bq;            // " `"
extern const char *csym_bq_;            // "` "
extern const char *csym_bq_comma_bq;    // "`,`"
extern const char *csym_bq_comma;       // "`,"
extern const char *csym__eq_;           // " = "

#define csym_comma_bq &(csym_bq_comma_bq[1])   // ",`"

extern const char *cstr_access_denied;

extern const char *cstr___;             // dummy table "___"

extern const char *cstr_root;
extern const char *cstr_super;

const char *SQLTokenToChar(int token_value);

string booleanToString(bool v);
string tableTypeToString(int type);

#if defined (KAIWU)
extern const char *cstr_INT4;
extern const char *cstr_BIGINTEGER;
extern const char *cstr_FLOAT4;
#endif

};

#endif /* INCLUDE_BIGOBJECTCONST_H_ */
