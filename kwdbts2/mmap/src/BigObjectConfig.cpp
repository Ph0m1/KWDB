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
#include <iostream>
#include <thread>
#include "DateTime.h"
#include "BigObjectConfig.h"
#include "BigObjectUtils.h"
#include "BigTable.h"
#include "sys_utils.h"
#include "objcntl.h"

#if defined(KAIWU)
#define ENV_BO_CONFIG               "KW_CONFIG"
#define ENV_BO_CLUSTER_LIMIT        "KW_CLUSTER_LIMIT"
#define ENV_BO_CLUSTER_QUERY_CHECK  "KW_CLUSTER_QUERY_CHECK"
#define ENV_BO_HOME                 "KW_HOME"
#define ENV_CLUSTER_CONFIG_HOME     "KW_CLUSTER_HOME"
#define ENV_BO_MAX_LOG_FILES        "KW_MAX_LOG_FILES"
#define ENV_BO_AUTO_CLUSTER_QUERY   "KW_AUTO_CLUSTER_QUERY"
#define ENV_BO_ZERO_IF_NULL         "KW_ZERO_IF_NULL"
#define ENV_BO_ENCRYPT_KEY          "KW_ENCRYPT_KEY"
#define ENV_BO_SSL                  "KW_SSL"
#define ENV_BO_STREAM_CONN_LIMIT    "KW_STREAM_CONN_LIMIT"
#define ENV_BO_STREAM_CACHE_SIZE    "KW_STREAM_CACHE_SIZE"
#define ENV_BO_IOT_AUTH             "KW_IOT_AUTH"
#define ENV_BO_IOT_MODE             "KW_IOT_MODE"
#define ENV_BO_IOT_INTERVAL         "KW_IOT_INTERVAL"
#define ENV_BO_UNION_PARALLEL_NUM   "KW_UNION_PARALLEL_NUM"
#define ENV_BO_LOGGING              "KW_LOGGING"
#define ENV_BO_SPP                  "KW_SPP"
#define ENV_BO_TS_TABLE             "KW_TS_TABLE"
#define ENV_BO_TS_PRECALCULATION    "KW_TS_PRECALCULATION"
#define ENV_BO_TS_PARTITION         "KW_TS_PARTITION"
#define ENV_BO_SPP_LOGGING          "KW_SPP_LOGGING"
#define ENV_BO_MAX_CONNECTIONS      "KW_MAX_CONNECTIONS"
#define ENV_BO_DOUBLE_PRECISION     "KW_DOUBLE_PRECISION"
#define ENV_BO_FLOAT_PRECISION      "KW_FLOAT_PRECISION"
#define ENV_BO_MAX_MAP_CACHE_SIZE   "KW_MAX_MAP_CACHE_SIZE"
#define ENV_BO_MAX_ANNON_MEM_SIZE   "KW_MAX_ANON_MEM_SIZE"
#define ENV_BO_MINIMUM_PARTITION_SIZE   "KW_MINIMUM_PARTITION_SIZE"
#define ENV_BO_BEHAVIOR_NUM_THREADS   "KW_BEHAVIOR_NUM_THREADS"
#define ENV_BO_MIN_ORDER_BY_PARALLEL_SIZE   "KW_MIN_ORDER_BY_PARALLEL_SIZE"
#else
#define ENV_BO_CONFIG               "BO_CONFIG"
#define ENV_BO_CLUSTER_LIMIT        "BO_CLUSTER_LIMIT"
#define ENV_BO_CLUSTER_QUERY_CHECK  "BO_CLUSTER_QUERY_CHECK"
#define ENV_BO_HOME                 "BO_HOME"
#define ENV_BO_MAX_LOG_FILES        "BO_MAX_LOG_FILES"
#define ENV_BO_AUTO_CLUSTER_QUERY   "BO_AUTO_CLUSTER_QUERY"
#define ENV_BO_ZERO_IF_NULL         "BO_ZERO_IF_NULL"
#define ENV_BO_ENCRYPT_KEY          "BO_ENCRYPT_KEY"
#define ENV_BO_SSL                  "BO_SSL"
#define ENV_BO_STREAM_CONN_LIMIT    "BO_STREAM_CONN_LIMIT"
#define ENV_BO_STREAM_CACHE_SIZE    "BO_STREAM_CACHE_SIZE"
#define ENV_BO_IOT_AUTH             "BO_IOT_AUTH"
#define ENV_BO_IOT_MODE             "BO_IOT_MODE"
#define ENV_BO_IOT_INTERVAL         "BO_IOT_INTERVAL"
#define ENV_BO_LOGGING              "BO_LOGGING"
#define ENV_BO_MAX_MAP_CACHE_SIZE   "BO_MAX_MAP_CACHE_SIZE"
#define ENV_BO_MAX_ANNON_MEM_SIZE   "BO_MAX_ANON_MEM_SIZE"
#endif

#define DEFAULT_NAMESERVICE	        "default"
#define TMP_DIRECTORY               "_tmp"
#define DEFAULT_TMP_DIRECTORY       "/tmp/"
#if defined(KAIWU)
#define DEFAULT_LOG_DIRECTORY       "/srv/kaiwudb/log"
#define DEFAULT_SCRIPT_DIRECTORY    "/kwscript/"
#else
#define DEFAULT_LOG_DIRECTORY   	  "/srv/bo/log"
#define DEFAULT_SCRIPT_DIRECTORY    "/boscript/"
#endif

#define DEFAULT_PAGE_SIZE_OFFSET    8

#if defined(__x86_64__) || defined(_M_X64)
#define DEFAULT_CLUSTER_THREADS     16
#define MAX_CLUSTER_THREADS         512
#else
#define DEFAULT_CLUSTER_THREADS     4
#define MAX_CLUSTER_THREADS         128
#endif

#define DEFAULT_STREAM_CACHE_SIZE   1024*1024
#define MIN_STREAM_CACHE_SIZE       512
#define MAX_STREAM_CACHE_SIZE       16*1024*1024
#define DEFAULT_STREAM_CONN_LIMIT   500
#define MIN_STREAM_CONN_LIMIT       10
#define MAX_STREAM_CONN_LIMIT       700
#define DEFAULT_CLUSTER_LIMIT       100000
#define DEFAULT_MAX_CONNECTIONS     200
#define DEFAULT_MAX_LOG_FILES       30
#define MAX_MAX_CONNECTIONS         1000
#define MIN_MAX_CONNECTIONS         10
#define MAX_CONN_TIMEOUT            300
#define DEFAULT_CONN_TIMEOUT        60
#define MAX_FLUSH_TIMEOUT           3000000          // 3000000
#define MIN_FLUSH_TIMEOUT           -1
#define DEFAULT_FLUSH_TIMEOUT       3000          // 3000
#define MAX_QUERY_TIMEOUT           86400
#define MIN_QUERY_TIMEOUT           30
#define DEFAULT_MAX_MAP_COUNT       65530
#define DEFAULT_UNION_PARALLEL_NUM  8

#if !defined(IOT_MODE)
#define DEFAULT_QUERY_TIMEOUT       300
#else
#define DEFAULT_QUERY_TIMEOUT       600
#endif

#if defined(__ANDROID__)
#define DEFAULT_NS_ALIGN_SIZE       0  // 4GB
#else
#define DEFAULT_NS_ALIGN_SIZE       2  // 16GB name service
#endif

std::atomic<long> bo_used_anon_memory_size;
BigObjectConfig * BigObjectConfig::config_ = nullptr;
string BigObjectConfig::config_url_;
string BigObjectConfig::home_;
//string BigObjectConfig::name_service_;
#if !defined(IOT_MODE) || defined(__x86_64__) || defined(_M_X64)
string BigObjectConfig::tmp_dir_;
#endif
#if !defined(IOT_MODE) || defined(__x86_64__) || defined(_M_X64)
string BigObjectConfig::log_dir_;
#endif
size_t BigObjectConfig::ps_ = sysconf(_SC_PAGESIZE);
string BigObjectConfig::script_dir_;
int BigObjectConfig::max_connections_ = DEFAULT_MAX_CONNECTIONS;
int BigObjectConfig::max_log_files_ = DEFAULT_MAX_LOG_FILES;
int BigObjectConfig::conn_timeout_ = DEFAULT_CONN_TIMEOUT;
int BigObjectConfig::query_timeout_ = DEFAULT_QUERY_TIMEOUT;
int BigObjectConfig::flush_timeout_ = DEFAULT_FLUSH_TIMEOUT;
int BigObjectConfig::num_threads_;
int BigObjectConfig::num_cluster_threads_ = DEFAULT_CLUSTER_THREADS;
int BigObjectConfig::ns_align_size_ = DEFAULT_NS_ALIGN_SIZE;
int BigObjectConfig::boost_level_= 1;

int BigObjectConfig::stream_cache_size_ = DEFAULT_STREAM_CACHE_SIZE;
int BigObjectConfig::stream_conn_limit_ = DEFAULT_STREAM_CONN_LIMIT;
int BigObjectConfig::table_type_ = COL_TABLE;
#if defined(KAIWU)
int BigObjectConfig::double_precision_ = 12;
int BigObjectConfig::float_precision_ = 6;
int BigObjectConfig::max_map_cache_size_ = DEFAULT_MAX_MAP_COUNT;
int BigObjectConfig::union_parallel_num_ = 0;
int BigObjectConfig::minimum_partition_size_ = MINIMUM_PARTITION_SIZE;
int BigObjectConfig::min_order_by_parallel_size_ = 0;
long BigObjectConfig::max_anon_memory_size_ = 1*1024*1024*1024; // 1G
#else
int BigObjectConfig::precision_ = 6;
#endif
int BigObjectConfig::cluster_limit_ = DEFAULT_CLUSTER_LIMIT;
int BigObjectConfig::logging_ = BO_LOG_FATAL;
int BigObjectConfig::dt32_base_year_ = 2000;
int BigObjectConfig::timestamp_prec_ = 3;
int BigObjectConfig::cluster_query_retry_ = 2;
bool BigObjectConfig::cluster_query_check_ = false;
bool BigObjectConfig::auto_cluster_query_ = false;
#if defined(KAIWU)
bool BigObjectConfig::zero_if_null_ = false;
#else
bool BigObjectConfig::zero_if_null_ = true;
#endif
bool BigObjectConfig::orig_order_sampling_ = false;
#if defined(IOT_MODE)
bool BigObjectConfig::ssl_ = true;
#else
bool BigObjectConfig::ssl_ = false;
#endif

#if defined(KAIWU) 
#if defined(IOT_MODE)
bool BigObjectConfig::spp_ = false;
#else
bool BigObjectConfig::spp_ = true;
#endif

bool BigObjectConfig::ts_table_ = true;
bool BigObjectConfig::ts_precalculation_ = true;
bool BigObjectConfig::ts_partition_ = false;
int BigObjectConfig::spp_logging_ = 1;
#endif


unsigned char BigObjectConfig::iv_[16];

#if defined(KAIWU)
const char *etc_kw_dir = "/etc/kw/";
const char *def_config_dir = "/srv/kaiwudb/config/";
#else
const char *etc_bo_dir = "/etc/bo/";
const char *def_config_dir = "/srv/bo/config/";
#endif


char cfg_config_str[] = "config";
char cfg_home_str[] = "home";
const string BigObjectConfig::cfg_date_format_str = "date_format";
const string BigObjectConfig::cfg_datetime_format_str = "datetime_format";
const string BigObjectConfig::cfg_cluster_limit_str = "cluster_limit";
const string BigObjectConfig::cfg_cluster_query_retry_str = "cluster_query_retry";
const string BigObjectConfig::cfg_cluster_query_check_str = "cluster_query_check";
const string BigObjectConfig::cfg_max_connections_str = "max_connections";
const string BigObjectConfig::cfg_max_log_files_str = "max_log_files";
const string BigObjectConfig::cfg_conn_timeout_str = "conn_timeout";
const char *BigObjectConfig::cfg_query_timeout_str = "query_timeout";
const char *BigObjectConfig::cfg_flush_timeout_str = "flush_timeout";
const string BigObjectConfig::cfg_num_threads_str = "num_threads";
const string BigObjectConfig::cfg_num_cluster_threads_str = "num_cluster_threads";
const string BigObjectConfig::default_cfg_url = "config.xml";
const string BigObjectConfig::cfg_nameservice_str = "nameservice";
const string BigObjectConfig::cfg_ns_align_str = "ns_align";
const string BigObjectConfig::cfg_boost_level_str = "boost_level";
//const string BigObjectConfig::cfg_api_ver_str = "api_ver";
#if !defined(COMMUNITY_VERSION)
const string BigObjectConfig::cfg_encrypt_str = "encrypt";
const string BigObjectConfig::cfg_encrypt_key_str = "encrypt_key";
#endif
const string BigObjectConfig::cfg_logging_str = "logging";
const string BigObjectConfig::cfg_dt32_base_year_str = "dt32_base_year";

#if defined(DEV)
size_t BigObjectConfig::table_size_ = 128;
const string BigObjectConfig::cfg_table_size_str = "table_size";
#endif
const string BigObjectConfig::cfg_stream_cache_size_str = "stream_cache_size";
const string BigObjectConfig::cfg_stream_conn_limit_str = "stream_conn_limit";
const string BigObjectConfig::cfg_table_type_str = "table_type";
#if defined(KAIWU)
const string BigObjectConfig::cfg_double_precision_str = "double_precision";
const string BigObjectConfig::cfg_float_precision_str = "float_precision";
const char   *BigObjectConfig::cfg_max_map_cache_size_str = "max_map_cache_size";
const char *BigObjectConfig::cfg_union_parallel_num_str = "union_parallel_num";
const char *BigObjectConfig::cfg_minimum_partition_size_str = "minimum_partition_size";
const char *BigObjectConfig::cfg_min_order_by_parallel_size_str = "min_order_by_parallel_size";
#else
const string BigObjectConfig::cfg_precision_str = "precision";
#endif
const string BigObjectConfig::cfg_auto_cluster_query_str = "auto_cluster_query";
const string BigObjectConfig::cfg_zero_if_null_str = "zero_if_null";
const string BigObjectConfig::cfg_orig_order_sampling_str = "orig_order_sampling";
const string BigObjectConfig::cfg_ssl_str = "ssl";
#if defined(KAIWU)
const char *BigObjectConfig::cfg_spp_str = "spp";
const char *BigObjectConfig::cfg_ts_table_str = "ts_table";
const char *BigObjectConfig::cfg_ts_precalculation_str = "ts_precalculation";
const char *BigObjectConfig::cfg_ts_partition_str = "ts_partition";
const char *BigObjectConfig::cfg_spp_logging_str = "spp_logging";

#endif

#if defined(IOT_MODE)
#if defined(DEV)
bool BigObjectConfig::iot_mode = false;
#else
bool BigObjectConfig::iot_mode = true;
#endif
int32_t BigObjectConfig::iot_auth_ = 0;
int32_t BigObjectConfig::iot_interval  = 864000;
const char *BigObjectConfig::cfg_iot_auth_str = "iot_auth";
const char * BigObjectConfig::cfg_iot_mode_str = "iot_mode";
const char *BigObjectConfig::cfg_iot_interval_str = "iot_interval";
#endif



map<string, string> BigObjectConfig::options_;
KLatch BigObjectConfig::latch_(LATCH_ID_BIGOBJECT_CONFIG_MUTEX);

bool BigObjectConfig::already_read = false;

unsigned char BigObjectConfig::server_salt[20];

#if defined(_WINDOWS_)
const char BigObjectConfig::slash_ = '\\';
#else
const char BigObjectConfig::slash_ = '/';
#endif

BigObjectConfig::BigObjectConfig() {
#if defined(_WINDOWS_)

#else
#endif
  ps_ = sysconf(_SC_PAGESIZE);

  options_[cfg_date_format_str] = bigobject::s_defaultDateFormat();
  options_[cfg_datetime_format_str] = bigobject::s_defaultDateTimeFormat();
  num_threads_ = std::thread::hardware_concurrency();
  options_[cfg_cluster_limit_str] = intToString(cluster_limit_);
  options_[cfg_cluster_query_retry_str] = intToString(cluster_query_retry_);
  options_[cfg_cluster_query_check_str] = booleanToString(cluster_query_check_);
  options_[cfg_max_connections_str] = intToString(max_connections_);
  options_[cfg_max_log_files_str] = intToString(max_log_files_);
  options_[cfg_conn_timeout_str] = intToString(conn_timeout_);
  options_[string(cfg_flush_timeout_str)] = intToString(flush_timeout_);
  options_[string(cfg_query_timeout_str)] = intToString(query_timeout_);
  options_[cfg_num_threads_str] = intToString(num_threads_);
  options_[cfg_num_cluster_threads_str] = intToString(num_cluster_threads_);
  options_[cfg_nameservice_str] = string(cstr_default);
  options_[cfg_ns_align_str] = intToString(ns_align_size_);
  options_[cfg_boost_level_str] = intToString(boost_level_);
#if defined(IOT_MODE)
  options_[cfg_iot_auth_str] = booleanToString(iot_auth_);
  options_[cfg_iot_mode_str] = booleanToString(iot_mode);
  options_[cfg_iot_interval_str] = intToString(iot_interval);
#endif

#if defined(DEV)
  options_[cfg_table_size_str] = intToString(table_size_);
#endif
  options_[cfg_stream_cache_size_str] = intToString(stream_cache_size_);
  options_[cfg_stream_conn_limit_str] = intToString(stream_conn_limit_);
  options_[cfg_table_type_str] = tableTypeToString(table_type_);
#if defined(KAIWU)
  options_[cfg_double_precision_str] = intToString(double_precision_);
  options_[cfg_float_precision_str] = intToString(float_precision_);
#else
  options_[cfg_precision_str] = intToString(precision_);
#endif
  options_[cfg_logging_str] = intToString(logging_);
  options_[cfg_dt32_base_year_str] = intToString(dt32_base_year_);
  options_[cfg_auto_cluster_query_str] = booleanToString(auto_cluster_query_);
  options_[cfg_zero_if_null_str] = booleanToString(zero_if_null_);
  options_[cfg_orig_order_sampling_str] = booleanToString(orig_order_sampling_);
  options_[cfg_ssl_str] = booleanToString(ssl_);
#if defined(KAIWU)
  options_[cfg_spp_str] = booleanToString(spp_);
  options_[cfg_ts_table_str] = booleanToString(ts_table_);
  options_[cfg_ts_precalculation_str] = booleanToString(ts_precalculation_);
  options_[cfg_ts_partition_str] = booleanToString(ts_partition_);
  options_[cfg_spp_logging_str] = intToString(spp_logging_);
  options_[cfg_max_map_cache_size_str] = intToString(max_map_cache_size_);
  options_[cfg_union_parallel_num_str] = intToString(union_parallel_num_);
  options_[cfg_minimum_partition_size_str] = intToString(minimum_partition_size_);
  options_[cfg_min_order_by_parallel_size_str] = intToString(min_order_by_parallel_size_);
#endif
  script_dir_ = DEFAULT_SCRIPT_DIRECTORY;
  for(size_t i = 0; i < 20; ++i) {
    server_salt[i] = rand() % 16;
  }
  memcpy(server_salt, "BigObjDBbigobjectinc", 20);
}

BigObjectConfig::~BigObjectConfig() {}

BigObjectConfig * BigObjectConfig::getBigObjectConfig() {
  if (config_ == nullptr) {
    config_ = new BigObjectConfig();
  }
  return config_;
}

int setTrueFalse(bool &value, const string &str) {
  if (str == bigobject::s_true()) {
    value = true;
    return 1;
  }
  if (str == bigobject::s_false()) {
    value = false;
    return 0;
  }
  return -1;
}

struct LoggingLevel {
    const char *log_level_str;
    int  level;
};

struct LoggingLevel logging_level[] = {
  "detail",                   5,  // LOG_DETAIL
  "fatal",                    1,  // LOG_FATAL
  "finer",                    6,  // LOG_FINER
  "finest",                   7,  // LOG_FINEST
  "info",                     4,  // LOG_INFO
  "off",                      0,  // LOG_OFF
  "severe",                   2,  // LOG_SEVERE
  "warning",                  3,  // LOG_WARNING
};

int getLogging(const string &value) {
  int64_t log_value;
  int logging = -1;
  if (isInteger(value.c_str(), log_value)) {
    logging = (int)log_value;
  } else {
    string str = toLower(value);
    int log_lvl = findName(str.c_str(), logging_level,
      (sizeof(logging_level) / sizeof(LoggingLevel)),
      sizeof(LoggingLevel));
    if (log_lvl >= 0)
      logging = logging_level[log_lvl].level;
  }
  if (logging < 0 || logging > BO_LOG_FINEST + 1)
    return -1;
  return logging;
}

int setInteger(int &n, const string &val_str, int min, int max =
  std::numeric_limits<int>::max()) {
  int64_t lv;
  if (!isInteger(val_str.c_str(), lv))
    return -1;
  int v = stringToInt(val_str);
  if (v < min || v > max)
    return -1;
  n = v;
  return 0;
}
int setLong(long &n, const string &val_str, long min, long max =
  std::numeric_limits<long>::max()) {
  int64_t lv;
  if (!isInteger(val_str.c_str(), lv))
    return -1;
  long v = stringToLongInt(val_str);
  if (v < min || v > max)
    return -1;
  n = v;
  return 0;
}
/**
 * Check whether a date_format is valid.
 * A valid date_format config must contain %Y, %m, and %d.
 */
bool isValidDateFormat(const string &format) {
  if (format.empty() || format == "0") {
	return false;
  }
  bool hasYear = false;
  bool hasMonth = false;
  bool hasDay = false;

  size_t len = format.length();

  for (size_t pos = 0; pos != string::npos; pos += 2) {
    pos = format.find("%", pos);

    if (pos == string::npos || pos + 1 >= len) {
    	break;
    }

    char next = format[pos + 1];
    hasYear = hasYear || next == 'Y';
    hasMonth = hasMonth || next == 'm';
    hasDay = hasDay || next == 'd';
  }

  return hasYear && hasMonth && hasDay;
}

/**
 * Check whether a datetime_format is valid.
 * A valid datetime_format must to be "%Y-%m-%d %H:%M:%S" or "%Y/%m/%d %H:%M:%S".
 */
bool isValidDateTimeFormat(const string &format) {
  return format == "%Y-%m-%d %H:%M:%S" || format == "%Y/%m/%d %H:%M:%S";
}

int BigObjectConfig::setConfigValue(const string &cfg, const string &value,
  bool update_config){
  MUTEX_LOCK(&latch_);
  string real_value = value;
  normalizeString(real_value);
  bool is_updated = true;
  int err_code = 0;

  if (cfg == cfg_max_connections_str) {
    if (setInteger(max_connections_, real_value, MIN_MAX_CONNECTIONS,
    MAX_MAX_CONNECTIONS) < 0)
      err_code = -1;
  } else if (cfg == cfg_conn_timeout_str) {
    if (setInteger(conn_timeout_, real_value, 1, MAX_CONN_TIMEOUT) < 0)
      err_code = -1;
  } else if (cfg == cfg_flush_timeout_str) {
    if (setInteger(flush_timeout_, real_value, MIN_FLUSH_TIMEOUT,
      MAX_FLUSH_TIMEOUT) < 0)
      err_code = -1;
  } else if (cfg == cfg_query_timeout_str) {
    if (setInteger(query_timeout_, real_value, MIN_QUERY_TIMEOUT,
    MAX_QUERY_TIMEOUT) < 0)
      err_code = -1;
  } else if (cfg == cfg_ns_align_str) {
    int align = stringToInt(real_value);
    if (align != 0 && align != 2 && align != 4)
      err_code = -1;
    else
      ns_align_size_ = align;
  } else if (cfg == cfg_boost_level_str) {
    if (setInteger(boost_level_, real_value, 0, 3) < 0)
      err_code = -1;
  } else if (cfg == cfg_table_type_str) {
    if (real_value == s_column())
      table_type_ = COL_TABLE;
    else if (real_value == s_row())
      table_type_ = ROW_TABLE;
    else
      is_updated = false;
  } else if (cfg == cfg_logging_str) {
    int logging = getLogging(real_value);
    if (logging < 0)
      err_code = -1;
    else
      logging_ = logging;
  } else if (cfg == cfg_dt32_base_year_str) {
    if (setInteger(dt32_base_year_, real_value, 2000, 2200) < 0)
      err_code = -1;
  } else if (cfg == cfg_cluster_limit_str) {
    if (setInteger(cluster_limit_, real_value, 0) < 0)
      err_code = -1;
  } else if (cfg == cfg_cluster_query_retry_str) {
    if (setInteger(cluster_query_retry_, real_value, 0, 5) < 0)
      err_code = -1;
  } else if (cfg == cfg_cluster_query_check_str) {
    if (setTrueFalse(cluster_query_check_, real_value) < 0)
      err_code = -1;
  } else if (cfg == cfg_auto_cluster_query_str) {
    if (setTrueFalse(auto_cluster_query_, real_value) < 0)
      err_code = -1;
  } else if (cfg == cfg_zero_if_null_str) {
    if (setTrueFalse(zero_if_null_, real_value) < 0)
      err_code = -1;
  } else if (cfg == cfg_orig_order_sampling_str) {
    if (setTrueFalse(orig_order_sampling_, real_value) < 0)
      err_code = -1;
  } else if (cfg == cfg_ssl_str) {
    if (setTrueFalse(ssl_, real_value) < 0)
      err_code = -1;
  } else if (cfg == cfg_max_log_files_str) {
    if (setInteger(max_log_files_, real_value, 0) < 0)
      err_code = -1;
  }
#if defined(KAIWU)
  else if (cfg == cfg_datetime_format_str) {
	if (!isValidDateTimeFormat(real_value))
	  err_code = -1;
  } else if (cfg == cfg_date_format_str) {
  	if (!isValidDateFormat(real_value))
  	  err_code = -1;
  }
#endif
#if defined(KAIWU)
  else if (strcmp(cfg.c_str(), cfg_spp_str) == 0) {
    if (setTrueFalse(spp_, real_value) < 0)
      err_code = -1;
  }
  else if (strcmp(cfg.c_str(), cfg_ts_table_str) == 0) {
    if (setTrueFalse(ts_table_, real_value) < 0)
      err_code = -1;
  }
  else if (strcmp(cfg.c_str(), cfg_ts_precalculation_str) == 0) {
    if (setTrueFalse(ts_precalculation_, real_value) < 0)
      err_code = -1;
  }
  else if (strcmp(cfg.c_str(), cfg_ts_partition_str) == 0) {
    if (setTrueFalse(ts_partition_, real_value) < 0)
      err_code = -1;
  }
  else if (strcmp(cfg.c_str(), cfg_spp_logging_str) == 0) {
    if (setInteger(spp_logging_, real_value, 0) < 0)
      err_code = -1;
  } else if (cfg == cfg_double_precision_str) {
    if (setInteger(double_precision_, real_value, 0, 15) < 0)
      err_code = -1;
  } else if (cfg == cfg_float_precision_str) {
    if (setInteger(float_precision_, real_value, 0, 7) < 0)
      err_code = -1;
  } else if (strcmp(cfg.c_str(), cfg_minimum_partition_size_str) == 0) {
    if (setInteger(minimum_partition_size_, real_value, 1, MAXIMUM_PARTITION_SIZE) < 0) {
      err_code = -1;
    }
  } else if (strcmp(cfg.c_str(), cfg_min_order_by_parallel_size_str) == 0) {
    if (setInteger(union_parallel_num_, real_value, 0) < 0) {
      err_code = -1;
    }
  }
#else
  else if (cfg == cfg_precision_str) {
    if (setInteger(precision_, real_value, 0, 15) < 0)
      err_code = -1;
  }
#endif
#if defined(DEV)
  else if (cfg == cfg_table_size_str) {
    int64_t table_size = stringToLongInt(real_value);
    if (table_size < 0)
      err_code = -1;
    else
      table_size_ = (size_t)table_size;
  }
#endif
  else if (cfg == cfg_stream_cache_size_str) {
    if (setInteger(stream_cache_size_, real_value, MIN_STREAM_CACHE_SIZE,
      MAX_STREAM_CACHE_SIZE) < 0)
      err_code = -1;
  } else if (cfg == cfg_stream_conn_limit_str) {
    if (setInteger(stream_conn_limit_, real_value, MIN_STREAM_CONN_LIMIT,
      MAX_STREAM_CONN_LIMIT) < 0)
      err_code = -1;
  }
#if defined(IOT_MODE)
  else if (strcmp(cfg.c_str(), cfg_iot_mode_str) == 0) {
    if (setTrueFalse(iot_mode, real_value) < 0)
      err_code = -1;
  } else if (strcmp(cfg.c_str(), cfg_iot_interval_str) == 0) {
    if (setInteger(iot_interval, real_value, 0) < 0)
      err_code = -1;
  }
#endif
  else {
    std::map<string, string>::iterator it = options_.find(cfg);
    if (it == options_.end())
      err_code = -2;
  }

  if (err_code >= 0 && update_config && is_updated) {
    LOG_FATAL("cannot exec here.");
    exit(1);
  }
  MUTEX_UNLOCK(&latch_);
  return err_code;
}

string setPath(const char *dir, const char *def_dir) {
  struct stat stat_buffer;

  string path = makeDirectoryPath(BigObjectConfig::home() + string(dir));
  if (stat(path.c_str(), &stat_buffer) != 0) {
    if (!MakeDirectory(path)) {
      goto def_tmp;
    }
test_tmp_dir:
    int fd;
    string tmp_f = path + "bo.tmp";
    if ((fd = open(tmp_f.c_str(), MMAP_CREATTRUNC,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)) < 0) {
      goto def_tmp;
    }
    remove(tmp_f.c_str());
  } else if (!S_ISDIR(stat_buffer.st_mode)) {
def_tmp:
    path = string(def_dir);
  } else
    goto test_tmp_dir;
  return path;
}

/**
 * @brief readConfigFromFile read configure file and set BigObjectConfig
*/
void BigObjectConfig::readConfigFromFile(const string &config_url, char *env_var, struct stat stat_buffer) {

    // get configure file directory
    env_var = getenv(ENV_BO_CONFIG);
    if (env_var) {
        config_url_ = string(env_var) + slash_ + config_url;
    } else {
        config_url_ = config_url;
        if (stat(config_url_.c_str(), &stat_buffer) != 0) {
            if (stat(def_config_dir, &stat_buffer) == 0 &&
                !S_ISDIR(stat_buffer.st_mode)) {
#if defined(KAIWU)
                config_url_ = string(etc_kw_dir) + config_url;
#else
                config_url_ = string(etc_bo_dir) + config_url;
#endif
            } else {
                string def_config_dir_str = string(def_config_dir);
                MakeDirectory(def_config_dir_str);
                config_url_ = def_config_dir_str + config_url;
            }
        }
    }
    env_var = getenv(ENV_BO_HOME);
    if (env_var) {
        home_ = string(env_var);
    } else {
#if defined(KAIWU)
        home_ =  getenv(ENV_CLUSTER_CONFIG_HOME);;
#else
        home_ = "/srv/bo/ds/";
#endif
    }

    if (already_read)
        return;

    already_read = true;
}

void BigObjectConfig::readConfig(const string &config_url) {
  char *env_var = nullptr;
  struct stat stat_buffer;

  BigObjectConfig::readConfigFromFile(config_url, env_var, stat_buffer);

  // check user license file first.
  home_ = makeDirectoryPath(home_);
  if (!MakeDirectory(home_))
    home_.clear();

  env_var = getenv(ENV_BO_CLUSTER_LIMIT);
  if (env_var) {
    if (setInteger(cluster_limit_, string(env_var), 0) >= 0)
      options_[cfg_cluster_limit_str] = intToString(cluster_limit_);
  }

  env_var = getenv(ENV_BO_CLUSTER_QUERY_CHECK);
  if (env_var) {
    if (setTrueFalse(cluster_query_check_, string(env_var)) >= 0)
      options_[cfg_cluster_query_check_str] = intToString(cluster_limit_);
  }

  env_var = getenv(ENV_BO_STREAM_CACHE_SIZE);
  if (env_var) {
    if (setInteger(stream_cache_size_, string(env_var),
    MIN_STREAM_CACHE_SIZE, MAX_STREAM_CACHE_SIZE) >= 0)
      options_[cfg_stream_cache_size_str] = intToString(stream_cache_size_);
  }

  env_var = getenv(ENV_BO_STREAM_CONN_LIMIT);
  if (env_var) {
    if (setInteger(stream_conn_limit_, string(env_var),
    MIN_STREAM_CONN_LIMIT, MAX_STREAM_CONN_LIMIT) >= 0)
      options_[cfg_stream_conn_limit_str] = intToString(stream_conn_limit_);
  }

  env_var = getenv(ENV_BO_AUTO_CLUSTER_QUERY);
  if (env_var) {
    if (setTrueFalse(auto_cluster_query_, string(env_var)) >= 0)
      options_[cfg_auto_cluster_query_str] = booleanToString(auto_cluster_query_);
  }
  env_var = getenv(ENV_BO_ZERO_IF_NULL);
  if (env_var) {
    if (setTrueFalse(zero_if_null_, string(env_var)) >= 0)
      options_[cfg_zero_if_null_str] = booleanToString(zero_if_null_);
  }

  env_var = getenv(ENV_BO_MAX_LOG_FILES);
  if (env_var) {
    if (setInteger(max_log_files_, string(env_var), 0) >= 0)
      options_[cfg_max_log_files_str] = intToString(max_log_files_);
  }

  env_var = getenv(ENV_BO_SSL);
  if (env_var) {
    if (setTrueFalse(ssl_, string(env_var)) >= 0)
      options_[cfg_ssl_str] = booleanToString(ssl_);
  }
#if defined(KAIWU)
  env_var = getenv(ENV_BO_SPP);
  if (env_var) {
    if (setTrueFalse(spp_, string(env_var)) >= 0)
      options_[cfg_spp_str] = booleanToString(spp_);
  }
  env_var = getenv(ENV_BO_TS_TABLE);
  if (env_var) {
    if (setTrueFalse(ts_table_, string(env_var)) >= 0)
      options_[cfg_ts_table_str] = booleanToString(ts_table_);
  }
  env_var = getenv(ENV_BO_TS_PRECALCULATION);
  if (env_var) {
    if (setTrueFalse(ts_precalculation_, string(env_var)) >= 0)
      options_[cfg_ts_precalculation_str] = booleanToString(ts_precalculation_);
  }
  env_var = getenv(ENV_BO_TS_PARTITION);
  if (env_var) {
    if (setTrueFalse(ts_partition_, string(env_var)) >= 0)
      options_[cfg_ts_partition_str] = booleanToString(ts_partition_);
  }
  env_var = getenv(ENV_BO_SPP_LOGGING);
  if (env_var) {
    if (setInteger(spp_logging_, string(env_var), 0) >= 0)
      options_[cfg_spp_logging_str] = intToString(spp_logging_);
  }
  env_var = getenv(ENV_BO_MAX_CONNECTIONS);
  if (env_var) {
    if (setInteger(max_connections_, string(env_var), 0) >= 0)
      options_[cfg_max_connections_str] = intToString(max_connections_);
  }
    env_var = getenv(ENV_BO_DOUBLE_PRECISION);
  if (env_var) {
    if (setInteger(double_precision_, string(env_var), 0) >= 0)
      options_[cfg_double_precision_str] = intToString(double_precision_);
  }
    env_var = getenv(ENV_BO_FLOAT_PRECISION);
  if (env_var) {
    if (setInteger(float_precision_, string(env_var), 0) >= 0)
      options_[cfg_float_precision_str] = intToString(float_precision_);
  }
  env_var = getenv(ENV_BO_MAX_MAP_CACHE_SIZE);
  if (env_var) {
    if (setInteger(max_map_cache_size_, string(env_var), 100) >= 0) {
      options_[cfg_max_map_cache_size_str] = intToString(max_map_cache_size_);
    }
  }
  env_var = getenv(ENV_BO_MINIMUM_PARTITION_SIZE);
  if (env_var) {
    if (setInteger(minimum_partition_size_, string(env_var), 1, MAXIMUM_PARTITION_SIZE) >= 0) {
      options_[cfg_minimum_partition_size_str] = intToString(minimum_partition_size_);
    }
  }

  env_var = getenv(ENV_BO_MIN_ORDER_BY_PARALLEL_SIZE);
  if (env_var) {
    setInteger(min_order_by_parallel_size_, string(env_var), 0);
  }
  env_var = getenv(ENV_BO_MAX_ANNON_MEM_SIZE);
  if (env_var) {
    setLong(max_anon_memory_size_, string(env_var), 16*1024*1024);
  }
#endif

#if !defined(IOT_MODE) || defined(__x86_64__) || defined(_M_X64)
  tmp_dir_ = setPath("_tmp", DEFAULT_TMP_DIRECTORY);
#endif

#if !defined(IOT_MODE) || defined(__x86_64__) || defined(_M_X64)
  log_dir_ = setPath("_log", "");
#endif

  env_var = getenv(ENV_BO_LOGGING);
  if (env_var) {
    if (setInteger(logging_, string(env_var), 0) >= 0)
       options_[cfg_logging_str] = intToString(logging_);
  }

#if defined(IOT_MODE)
  env_var = getenv(ENV_BO_IOT_AUTH);
  if (env_var) {
    if (setInteger(iot_auth_, string(env_var), 0) >= 0)
       options_[cfg_iot_auth_str] = intToString(iot_auth_);
  }
  env_var = getenv(ENV_BO_IOT_MODE);
  if (env_var) {
    if (setTrueFalse(iot_mode, string(env_var)) >= 0)
       options_[cfg_iot_mode_str] = booleanToString(iot_mode);
  }
  env_var = getenv(ENV_BO_IOT_INTERVAL);
  if (env_var) {
    if (setInteger(iot_interval, string(env_var), 30) >= 0)
       options_[cfg_iot_interval_str] = intToString(iot_interval);
  }
#endif

}



