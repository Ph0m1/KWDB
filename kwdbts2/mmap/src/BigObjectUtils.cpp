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

#include <sys/stat.h>
#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <netinet/in.h>
#include <parallel/algorithm>
#include "BigObjectUtils.h"
#include "BigObjectApplication.h"
#include "BigObjectConfig.h"
#include "mmap/MMapFile.h"
#include "mmap/MMapBigTable.h"
#include "VarString.h"
#include "lib/HyperLogLog.h"

vector<AttributeInfo> dummy_schema;
string empty_hierarchical_part = ":///";
vector<string> dummy_string_vector;

string nameToURL(const string &name, const string &ext) {
  size_t pos = name.rfind(ext);
  if (pos != string::npos)
    return name;
  return name + ext;
}

const string & getActualWorkspace(const string &db) {
#if defined(KAIWU)
  if (strncmp(db.c_str(), s_bigobject().c_str(), 7) == 0) {
    char c = db[7];
#else
  if (strncmp(db.c_str(), s_bigobject().c_str(), 9) == 0) {
    char c = db[9];
#endif
    if (c == 0 || c == BigObjectConfig::directorySeperator())
      return s_emptyString();
  }
  return db;
}

const string & worksapceToDatabase(string &ws) {
  if (!ws.empty() && ws.back() == '/') {
    ws.resize(ws.size() - 1);
  }
  return ws;
}

int getDeletableColumn(BigTable *bt, DataHelper *helper, void *rec_data) {
  int deleted_col = -1;
  int8_t v = 1;
  if (bt->isDeletable() && bt->structVersion() < BT_STRUCT_FORMAT_V8) {
    deleted_col = bt->getDeletableColumn();
    helper->toColumn(deleted_col, &v, rec_data);
  }

  return deleted_col;
}

vector<AttributeInfo> & getDummySchema() { return dummy_schema; }

string & toParentDirectory(string &dir)
{
    size_t found;
    int tmp_end = dir.size() - 2;
    size_t end = tmp_end < 0 ? 0 : tmp_end;

    found = dir.find_last_of(BigObjectConfig::directorySeperator(), end);
    if (found == string::npos) {
	    found = 0;
    }

    dir = makeDirectoryPath(const_cast<const string &>(dir.erase(found)));
    return dir;
}

bool isURL(const string &url)
{
    string file_path;

    if (url.size() >= 8 && url.compare(4, 4, s_hierarchical_part()) == 0) {
        return true;
    }
    return false;
}

string getURLScheme(const string &url) {
  if (url.size() >= 8 && url.compare(4, 4, s_hierarchical_part()) == 0) {
    return url.substr(0,4);
  }
  return bigobject::s_emptyString();
}

// URL has the form: XXXX:///
string getURLFilePath(const string &url) {
  string file_path = url;

  size_t pos = file_path.find_first_not_of('/');
  if (pos != 0 && pos != string::npos)
    file_path = file_path.substr(pos);
  return file_path;
}

string getURLObjectName(const string &url) {
  string fpath = getURLFilePath(url);
  size_t found;
  found = fpath.find_last_of('.');
  return (found == std::string::npos) ? fpath : fpath.substr(0, found);
}

string nameToObjectURL(const string &name, const string &ext) {
    return nameToURL(name, ext);
}

string nameToTagBigTableURL(const string &name, const string &ext)
{
  return nameToObjectURL(name, ".pt");
}

string nameToEntityBigTableURL(const string &name, const string &ext)
{ return nameToObjectURL(name, ext); }


string genTempObjectURL(const string &src_url) {
  static std::atomic<size_t> ts_inc = 0;
  ostringstream oss;
  int pos = src_url.find('.');
  string src_head = src_url.substr(0, pos);
  int64_t t = ts_inc.fetch_add(1);
  oss << "_t_" << hex << t;

  return oss.str();
}

bool isInteger(const char *s, int64_t &i) {
  if(s == nullptr || ((!isdigit(s[0])) && (s[0] != '-') && (s[0] != '+')))
    return false;

  char *p ;
  i = (int64_t)strtol(s, &p, 10) ;
  return (*p == 0) ;
}

// binary search an array of named structure
int findName(const char *s, void *name_st, int size, int rec_sz) {
    int first = 0;
    int last = size - 1;

    while (first <= last) {
        int mid = (first + last) / 2;
        int v= strcmp(*((const char **)offsetAddr(name_st, rec_sz * mid)), s);
        if (v < 0)
            first = mid + 1;
        else if (v > 0)
            last = mid - 1;
        else {  // v = 0
            return mid;
        }
    }
    return -1;
}

string getPrefix(const string &str, char seperator)
{
    size_t d_pos = str.find(seperator);
    return (d_pos != string::npos) ? str.substr(0, d_pos) :
        bigobject::s_emptyString();
}

string getDimension(const string &dim_attr)
{
    size_t g_pos = dim_attr.find(GROUP_SEPARATOR);
    if (g_pos != string::npos) {
        size_t d_pos = dim_attr.find(DIMENSION_SEPARATOR);
        return (d_pos != string::npos) ?
            dim_attr.substr(g_pos + 1, d_pos - g_pos - 1) :
            dim_attr.substr(g_pos+1);
    }
    else
        return getPrefix(dim_attr, DIMENSION_SEPARATOR);
}

string getAttribute(const string &dim_attr) {
  size_t d_pos = dim_attr.find('.');
  return dim_attr.substr(d_pos + 1);
}

int getDataTypeSize(int type) {
  switch (type) {
    case STRING:
    case ROWID:
      return sizeof(IDTYPE);
    case DATETIME64:
      return sizeof(int64_t);
    case BOOL:
      return sizeof(bool);
    case BYTE:
    case INT8:
      return sizeof(int8_t);
    case INT16:
      return sizeof(int16_t);
    case INT32:
    case DATE32:
    case DATETIME32:
    case DATETIMEDOS:
    case TIMESTAMP:
    case TIME:
    case IPV4:
      return sizeof(int32_t);
    case INT64:
    case TIME64:
    case TIMESTAMP64:
      return sizeof(int64_t);
    case TIMESTAMP64_LSN:
      return sizeof(TimeStamp64LSN);
    case FLOAT:
      return sizeof(float);
    case DOUBLE:
      return sizeof(double);
    case WEEK:
      return sizeof(int);
    case VARSTRING:
    case VARBINARY:
    case FUNCVARBINARY:
    case LINESTRING:
    case POLYGON:
    case MULTIPOLYGON:
    case GEOMCOLLECT:
    case GEOMETRY:
      return sizeof(intptr_t);
    case IPV6:
      return sizeof(struct in6_addr);
    case POINT:
    case WKB_POINT: return sizeof(double) * 2;
  }
  return 0;
}


int getDataTypeSize(AttributeInfo &info) {
  switch (info.type) {
    case STRING:
      if (info.max_len == 0)
        info.max_len = DEFAULT_STRING_MAX_LEN;
    case ROWID:
      return sizeof(IDTYPE);
    case DATE32:
    case DATETIMEDOS:
    case DATETIME32:
      return sizeof(int32_t);
    case DATETIME64:
      return sizeof(int64_t);
    case BOOL:
      return sizeof(bool);
    case BYTE:
    case INT8:
      return sizeof(int8_t);
    case INT16:
      return sizeof(int16_t);
    case INT32:
    case IPV4:
      return sizeof(int32_t);
    case INT64:
    case TIME64:
    case TIMESTAMP64:
      return sizeof(int64_t);
    case FLOAT:
      return sizeof(float);
    case DOUBLE:
      return sizeof(double);
    case WEEK:
      return sizeof(int);
    case CHAR:
//    case RAWSTRING:
    case STRING_CONST:
    case FUNCCHAR:
    case BINARY:
      if (info.max_len == 0)
        info.max_len = DEFAULT_CHAR_MAX_LEN;
      return info.max_len;
//    case VARBINARY:
//      if (info.max_len == 0)
//        info.max_len = DEFAULT_VARSTRING_MAX_LEN;
//      return info.max_len + 4;
    case VARSTRING:
    case VARBINARY:
    case FUNCVARBINARY:
    case BLOB:
    case LINESTRING:
    case POLYGON:
    case MULTIPOLYGON:
    case GEOMCOLLECT:
    case GEOMETRY:
    case WKB_LINESTRING:
    case WKB_POLYGON:
    case WKB_MULTIPOLYGON:
    case WKB_GEOMCOLLECT:
    case WKB_GEOMETRY:
      if (info.max_len == 0)
        info.max_len = DEFAULT_VARSTRING_MAX_LEN;
      return sizeof(intptr_t);
    case TIMESTAMP:
      return sizeof(int32_t);
    case TIME:          return sizeof(int32_t);
    case HYPERLOGLOG:   return hllSize(info.max_len);
    case IPV6:          return sizeof(struct in6_addr);
    case POINT:
    case WKB_POINT:     return sizeof(double) * 2;
    case NULL_TYPE:     return 0;
    case TIMESTAMP64_LSN: return sizeof(TimeStamp64LSN);
  }
  return 0;
}

int setAttributeInfo(vector<AttributeInfo> &info, int encoding,
  ErrorInfo &err_info) {
  int offset = 0;

  for (vector<AttributeInfo>::iterator it = info.begin(); it != info.end();
    ++it) {
    if (it->length <= 0)
      it->length = 1;
#if defined(COLUMN_GROUP)
    if (encoding & DELETABLE_TABLE && it->isColumnGroup())
      continue;
#endif
    if ((it->size = getDataTypeSize(*it)) == -1)
      return -1;
    if (it->max_len == 0)
      it->max_len = it->size;
    it->offset = offset;
    offset += it->size;
    if (it->type == STRING) {
#if defined(USE_SMART_INDEX)
      if ((encoding & SMART_INDEX) && !actual_dim.empty()) {
        it->encoding = SMART_INDEX;
      } else
#endif
      it->encoding = DICTIONARY;
    }
  }
  return offset;
}

vector<string> AttributeInfoToString(const vector<AttributeInfo> &attr_info) {
    std::vector<std::string> rank;
    for (size_t i = 0; i < attr_info.size(); ++i) {
        if (!(attr_info[i].isFlag(AINFO_INTERNAL)) && !(attr_info[i].isFlag(AINFO_DROPPED)))
            rank.push_back(attr_info[i].name);
    }
    return rank;
}

int normalizeString(char *s) {
  if (*s == 0)
    return 0;
  char quote = 0;
  size_t i = 0;
  size_t end = strlen(s);
  size_t back = strlen(s) - 1;

  if (end > 1) {      // at least 2 characters in the string
    if (s[0] == '\'' && s[back] == '\'') {
      quote = '\'';
      i++;
    }
    if (s[0] == '"' && s[back] == '"') {
      quote = '"';
      i++;
    }
    if (s[0] == '`' && s[back] == '`') {
      quote = '`';
      i++;
    }
    if (s[0] == '[' && s[back] == ']') {
      quote = '\\';
      i++;
    }
  }

  size_t j = 0;
  while (i <= back) {
    if (i == back) {
      if (!quote) {
        s[j] = s[i];
        j++;
      }
    } else {
      char si = s[i];
      if (si == '\\' && (i + 1 != back)) {
        char sn = s[i + 1];
        switch (sn) {
        case '0':
          s[j] = '\0';
          break;
        case 'b':
          s[j] = '\b';
          break;
        case 'n':
          s[j] = '\n';
          break;
        case 'r':
          s[j] = '\r';
          break;
        case 't':
          s[j] = '\t';
          break;
        case 'Z':
          s[j] = 0x26;
          break;
        case '\\':
        case '\'':
        case '"':
        case '`':
          s[j] = sn;
          break;
        default:
          s[j++] = '\\';
          s[j] = sn;
        }
        i++;
      } else {
        if (si == quote && (i + 1 != back) && (s[i + 1] == quote)) {
          i++;
        }
        s[j] = si;
      }
      j++;
    }
    i++;
  }
  s[j] = 0;
  return j;
}

void normalizeString(string &s) {
  int len = normalizeString((char *)s.c_str());
  s.resize(len);
}

string normalize(const string &s) {
  string ns = s.substr(0);
  normalizeString(ns);
  return ns;
}

string toString(const char *data)
{ return (data == nullptr) ? bigobject::s_emptyString() : string(data); }

int getIndex(const vector<AttributeInfo> &schema, const string &str) {
  for (size_t i = 0; i < schema.size(); ++i)
    if (schema[i].name == str)
      return (int)i;
  return -1;
}

string toString(const char *str, size_t len) {
  string s;
  s.resize(len);
  size_t i = 0;
  for (; i < len && *str != 0; ++i) {
    s[i] = *str++;
  }
  s[i] = 0;
  s.resize(i);
  return s;
}

string quoteString(const string &str, char quote) {
  return quote + str + quote;
}

string toLower(const string &str) {
  string l_str = str.substr(0);
  std::transform(l_str.begin(), l_str.end(), l_str.begin(), ::tolower);
  return l_str;
}

string toUpper(const string &str) {
  string l_str = str.substr(0);
  std::transform(l_str.begin(), l_str.end(), l_str.begin(), ::toupper);
  return l_str;
}

string normalizePath(const string &path) {
  string rpath = path;
  size_t pos = 0;

  ///TODO: optimization
  while (true) {
    /* Locate the substring to replace. */
    size_t pos1 = rpath.find("./", pos);
    if (pos1 != string::npos) {
      rpath.replace(pos1, 2, "/");
    }
    size_t pos2 = rpath.find("//", pos);
    if (pos2 != string::npos) {
      rpath.replace(pos2, 2, "/");
    }
    pos = std::min(pos1, pos2);
    if (pos == string::npos)
      break;
  }
  const string &db = getActualWorkspace(rpath);

  return makeDirectoryPath(db);
}

string makeDirectoryPath(const string &tbl_sub_path) {
  string dir_path = tbl_sub_path;
  int size = dir_path.size();
  if (size > 0
    && (dir_path.at(size - 1) != BigObjectConfig::directorySeperator()))
    dir_path.push_back(BigObjectConfig::directorySeperator());
  return dir_path;
}

int makeDirectory(const string &dir) {
  int ret_code = 0;
  if (!dir.empty()) {
    struct stat st;
    size_t e_pos = 1;
    char *path = (char *)dir.data();
    while(e_pos < dir.size()) {
      e_pos = dir.find_first_of('/', e_pos);
      if (e_pos != string::npos)
        path[e_pos] = 0;
      if (stat(path, &st) != 0) {
        if (mkdir(path,
          S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) < 0)
          return -1;
        ret_code = 0;
      } else {
        if (!S_ISDIR(st.st_mode)) {
          return -1;
        } else
          ret_code = 1;
      }
      if (e_pos != string::npos)
        path[e_pos] = '/';
      else
        break;      // reach end of directory path
      e_pos++;
    }
  }
  return ret_code;
}

int getFileCC(const string &file_path) {
  int fd;
  int cc = 0;
  if ((fd = open(file_path.c_str(), O_RDONLY)) > 0) {
    ssize_t cc_size = read(fd, &cc, sizeof(cc));
    if (cc_size < (ssize_t)sizeof(cc))
      cc = 0;
    close(fd);
  }
  return cc;
}

int getURLCC(const string &url, const string &sand_box) {
  string file_path = BigObjectConfig::home() + sand_box + getURLFilePath(url);
  return getFileCC(file_path);;
}

#undef NEW_COUNT

