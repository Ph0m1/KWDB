// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//

#include "BigObjectConst.h"
#include "BigTable.h"

namespace bigobject {

#if defined(KAIWU)
const char *cstr_kaiwudb = "kaiwudb";
const char *cstr_KaiwuDB = "KaiwuDB";
#endif

// environment variable
#if defined(KAIWU)
const char *cstr_KW_ALIAS = "KW_ALIAS";
const char *cstr_metadata = "metadata";
#else
const char *cstr_BO_ALIAS = "BO_ALIAS";
#endif

const char *cstr_BLOB = "BLOB";
const char *cstr_LINESTRING = "LINESTRING";
const char *cstr_LineString = "LineString";
const char *cstr_POINT = "POINT";
const char *cstr_Point = "Point";
const char *cstr_POLYGON = "POLYGON";
const char *cstr_Polygon = "Polygon";
const char *cstr_MULTIPOLYGON = "MULTIPOLYGON";
const char *cstr_MultiPolygon = "MultiPolygon";
const char *cstr_GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
const char *cstr_GeometryCollection = "GeometryCollection";
const char *cstr_GEOMETRY = "GEOMETRY";
const char *cstr_Geometry = "Geometry";

const char *cstr_TIMESTAMP64 = "TIMESTAMP64";
const char *cstr_VARBINARY = "VARBINARY";

const char *cstr_Create_Table = "Create Table";

const char *cstr_associations = "associations";
const char *cstr_charset = "charset";
const char *cstr_columns = "columns";
const char *cstr_comment = "comment";
const char *cstr_config = "config";
const char *cstr_databases = "databases";
const char *cstr_engines = "engines";
const char *cstr_graph = "graph";
const char *cstr_graphs = "graphs";
const char *cstr_grants = "grants";
const char *cstr_keys = "keys";
const char *cstr_procedure = "procedure";
const char *cstr_processlist = "processlist";
const char *cstr_table = "table";
const char *cstr_tables = "tables";
const char *cstr_trees = "trees";
const char *cstr_triggers = "triggers";
const char *cstr__variables = "_variables";

const char *cstr_Charset = "Charset";
const char *cstr_Collation = "Collation";
const char *cstr_Engine = "Engine";

const char *cstr_null = "null";
const char *cstr_inf = "inf";
const char *cstr_n_inf = "-inf";
//const char *cstr_from_id = "from_id";

const char *cstr_database = "database";
const char *cstr_references = "references";
const char *cstr_status = "status";
const char *cstr_Error = "Error";

const char *cstr_LEFT_ = "LEFT ";
const char *cstr_INNER_ = "INNER ";
const char *cstr_JOIN = "JOIN";
const char *cstr_FULL = "FULL";
const char *cstr_OUTER_ = "OUTER ";
const char *cstr__ON_ = " ON ";
const char *cstr__BETWEEN_ = " BETWEEN ";

const char *cstr_LAST = "LAST";
const char *cstr_LASTROW = "LAST_ROW";

const char *cstr_COUNT = "COUNT";
const char *cstr_GEOHASH = "GEOHASH";
const char *cstr_HYPERLOGLOG = "HYPERLOGLOG";
const char *cstr_VARIANCE = "VARIANCE";
const char *cstr_MEDIAN = "MEDIAN";
const char *cstr_QUANTILE = "QUANTILE";
const char *cstr_TDIGEST = "TDIGEST";

const char *cstr_aes_decrypt = "aes_decrypt";
const char *cstr_aes_encrypt = "aes_encrypt";
const char *cstr_year = "year";
const char *cstr_quarter = "quarter";
const char *cstr_month = "month";
const char *cstr_day = "day";
const char *cstr_hour = "hour";
const char *cstr_minute = "minute";
const char *cstr_second = "second";
const char *cstr_millisecond = "millisecond";
const char *cstr_week = "week";
const char *cstr_isoweek = "isoweek";
const char *cstr_yearweek = "yearweek";
const char *cstr_yearday = "yearday";
const char *cstr_weekday = "weekday";

const char *cstr_default = "default";
const char *cstr_sliding = "sliding";
const char *cstr_fulltext = "fulltext";

const char *cstr_datediff = "datediff";
const char *cstr_timestamp = "timestamp";
const char *cstr_time64 = "time64";

const char *cstr_cumsum = "cumsum";
const char *cstr_cummax = "cummax";
const char *cstr_group_rank = "group_rank";
const char *cstr_group_row_number = "group_row_number";
const char *cstr_zero_byte = "zero_byte";

const char *cstr_CREATE = "CREATE";
const char *cstr_UPDATE = "UPDATE";

const char cstr_DEFAULT[] = "DEFAULT";
const char cstr_TABLE[] = "TABLE";
const char *cstr_FROM = "FROM";

const char *cstr_ALTER = "ALTER";
const char *cstr_CLUSTER = "CLUSTER";
const char *cstr_DELETE = "DELETE";
const char *cstr_DROP = "DROP";
const char *cstr_FILE = "FILE";
const char *cstr_INSERT = "INSERT";
const char *cstr_INTO = "INTO";
const char *cstr_BY = "BY";
const char *cstr_SUM = "SUM";
//const char *cstr_DSUM = "DSUM";
const char *cstr_WHERE = "WHERE";
const char *cstr_lbracket = "(";
const char *cstr_rbracket = ")";
const char *cstr_SELECT = "SELECT";
const char *cstr_ORDER = "ORDER";
const char *cstr__DESC = " DESC";
const char *cstr_AS = "AS";
const char *cstr_INDEX = "INDEX";
const char *cstr_FOREIGN_KEY = "FOREIGN KEY";
//const char *cstr_KEY = "KEY";
const char *cstr_SQL_VARIABLE = "@@";
const char *cstr_TREE = "TREE";
const char *cstr_ALL = "ALL";
const char *cstr_AND = "AND";
const char *cstr__AND_ = " AND ";
const char *cstr_OR = "OR";
const char *cstr_NOT = "NOT";
const char *cstr_APPROXIMATE = "APPROXIMATE";
const char *cstr_filter = "filter";
const char *cstr_IPv4 = "IPv4";
const char *cstr_IPv6 = "IPv6";
const char ipv4_prefix[13] = "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\xff\xff";
const char *cstr_gh_adjacent = "gh_adjacent";
const char *cstr_gh_expand = "gh_expand";
const char *cstr_gh_neighbors = "gh_neighbors";

const char *cstr_SET = "SET";

const char *cstr_alias = "alias";
const char *cstr_rank = "rank";
const char *cstr_at = "at";
const char *cstr_bottom = "bottom";
const char *cstr_def = "def";
const char *cstr_fact = "fact";
const char *cstr_freq = "freq";
const char *cstr_prob = "prob";

const char *cstr_information_schema = "information_schema";
const char *cstr_remotehosts = "remotehosts";

const char *cstr__lbracket = " (";
const char *cstr__WHERE_ = " WHERE ";
const char *csym__bq = " `";
const char *csym_bq_ = "` ";
const char *csym_bq_comma_bq = "`,`";
const char *csym_bq_comma = "`,";
const char *csym__eq_ = " = ";

const char *cstr_access_denied = "access denied";

const char *cstr___ = "___";    // dummy table "___"

const char *cstr_root = "root";
const char *cstr_super = "super";

string booleanToString(bool v) {
  return (v) ? s_true() : s_false();
}

string tableTypeToString(int type)
{ return (type & COL_TABLE) ? s_column() : s_row(); }


#if defined (KAIWU)
const char *cstr_INT4 = "INT4";
const char *cstr_BIGINTEGER = "BIGINTEGER";
const char *cstr_FLOAT4 = "FLOAT4";
#endif


};
