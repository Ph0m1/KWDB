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



#ifndef BIGOBJECTCONFIG_H_
#define BIGOBJECTCONFIG_H_

#include <map>
#include <string>
#include <vector>
#include <atomic>
#include <string.h>
#include "default.h"
#include "lt_rw_latch.h"


using namespace std;

#if defined(WIN32) || defined(_WIN32) || defined(WIN64) || defined(_WIN64) || \
    defined(__WIN32) && !defined(__CYGWIN__) || \
    defined(__WIN32) && !defined(__CYGWIN__)
#define _WINDOWS_
#endif
//#define DEV

#if defined(KAIWU)
extern const char *etc_kw_dir;
#else
extern const char *etc_bo_dir;
#endif
extern const char *def_config_dir;      // /srv/bo/config/
extern std::atomic<long> bo_used_anon_memory_size;
enum LOGGING_LEVEL {
  BO_LOG_OFF,           // 0
  BO_LOG_FATAL,         // 1
  BO_LOG_SEVERE,
  BO_LOG_WARNING,
  BO_LOG_INFO,          // query statement
  BO_LOG_DETAIL,        // cluster query,
  BO_LOG_FINER,         // socket connection, get object.
  BO_LOG_FINEST,        // object level info: get/release, lock, memory, tasks.
};

class BigObjectConfig {
protected:
  static string config_url_;
  static BigObjectConfig *config_;
  static string home_;
  static const char slash_;
  static size_t ps_;
#if !defined(IOT_MODE) || defined(__x86_64__) || defined(_M_X64)
  static string tmp_dir_;
#endif
#if !defined(IOT_MODE) || defined(__x86_64__) || defined(_M_X64)
  static string log_dir_;
#endif
  static string script_dir_;
  static int max_connections_;          // mysql conection #
  static int cluster_query_retry_;
  static bool cluster_query_check_;      // check cluster query
  static int conn_timeout_;
  static int query_timeout_;
  static int flush_timeout_;
  static int max_num_threads_;
  static int max_log_files_;
  static int num_threads_;
  static int num_cluster_threads_;
  static int ns_align_size_;
  static int boost_level_;
  static int stream_cache_size_;
  static int stream_conn_limit_;
  static int table_type_;
  static int precision_;
  static int cluster_limit_;
  static int logging_;
  static int dt32_base_year_;           // DateTime32 base year.
  static int timestamp_prec_;
  static bool auto_cluster_query_;
  static bool zero_if_null_;
  static bool orig_order_sampling_;
  static bool ssl_;
#if defined(KAIWU)
  static bool spp_;
  static bool ts_table_;
  static bool ts_precalculation_;
  static bool ts_partition_;
  static int  spp_logging_;
  static int  double_precision_;
  static int  float_precision_;
#endif
  static int max_map_cache_size_;
  static int union_parallel_num_;
  static int minimum_partition_size_;
  static int min_order_by_parallel_size_;
  static long max_anon_memory_size_;

#if defined(IOT_MODE)
  static int32_t iot_auth_;
#endif

  static unsigned char iv_[16];

#if defined(DEV)
  static size_t table_size_;
#endif

  typedef map<string, string>::iterator config_iter;

  static map<string, string> options_;

  static KLatch latch_;
//    static void setEncrypt(const string &value);

//  static int parser_;   // 0: old parser;  1: new parser

 // configure file is parsed.
 static bool already_read;

public:
  BigObjectConfig();

  virtual ~BigObjectConfig();

  static BigObjectConfig * getBigObjectConfig();

  void readConfig(const string &config_url = default_cfg_url);

  static const string & home() { return home_; }

  static string scriptDir() { return script_dir_; }

  static string & dateFormat() { return options_[cfg_date_format_str]; }

  static string & dateTimeFormat() { return options_[cfg_datetime_format_str]; }

  static int clusterLimit() { return cluster_limit_; }
  static int clusterQueryRetry() { return cluster_query_retry_; }
  static bool clusterQueryCheck() { return cluster_query_check_; }
  static int maxConnections() { return max_connections_; }
  static int maxLogFiles() { return max_log_files_; }
  static int connTimeout() { return conn_timeout_; }
  static int queryTimeout() { return query_timeout_; }
  static int flushTimeout() { return flush_timeout_; }
  static int numThreads() { return num_threads_; }
  static int boostLevel() { return boost_level_; }

#if defined(DEV)
  static size_t tableSize() { return table_size_; }
#endif

  static int streamCacheSize() { return stream_cache_size_; }
  static int streamConnLimit() { return stream_conn_limit_; }

  static int tableType() { return table_type_; }
#if defined(KAIWU)
  static int precision() { return double_precision_; }
  static int float_precision() { return float_precision_; }
  static int max_map_cache_size() {return max_map_cache_size_;}
  static int union_parallel_num() {return union_parallel_num_;}
  static int minimum_partition_size() {return minimum_partition_size_;}
  static int min_order_by_parallel_size() {return min_order_by_parallel_size_;}
#else
  static int precision() { return precision_; }
#endif
  static long max_anon_memory_size() {return max_anon_memory_size_;}
  static int logging() { return logging_; }

  static int dt32BaseYear() { return dt32_base_year_; }

  static int timestampPrecision() { return timestamp_prec_; }

  static bool autoClusterQuery() { return auto_cluster_query_; }

  static bool zeroIfNull() { return zero_if_null_; }

  static bool origOrderSampling() { return orig_order_sampling_; }

  static bool ssl() { return ssl_; }
#if defined(KAIWU)
  static bool spp() { return spp_; }
  static bool ts_table() { return ts_table_; }
  static bool ts_precalculation() { return ts_precalculation_; }
  static bool ts_partition() { return ts_partition_; }
  static int spp_logging() { return spp_logging_; }
#endif

  static void setLogging(int ll) { logging_ = ll; }

//  static int parser() { return parser_; }

  static int setConfigValue(const string &cfg, const string &value,
    bool update_config);

#if !defined(IOT_MODE)
  static string tmpDirectory() { return tmp_dir_; }
#endif
#if !defined(IOT_MODE) || defined(__x86_64__) || defined(_M_X64)
  static string logDirectory() { return log_dir_; }
#endif

  static string nameService() { return options_[cfg_nameservice_str]; }

  static int nameServiceAlignSize() { return ns_align_size_; }

  static char directorySeperator() { return slash_; }

  static size_t pageSize() { return ps_; }

  static unsigned char * encryptVector() { return iv_; }
  static void getIV(unsigned char *iv) { memcpy(iv, iv_, 16); }

  static unsigned char server_salt[20]; // NULL-terminated

  static const string default_cfg_url;
  static const string cfg_date_format_str;
  static const string cfg_datetime_format_str;
  static const string cfg_cluster_limit_str;
  static const string cfg_cluster_query_retry_str;
  static const string cfg_cluster_query_check_str;
  static const string cfg_max_connections_str;
  static const string cfg_max_log_files_str;
  static const string cfg_conn_timeout_str;
  static const char *cfg_query_timeout_str;
  static const char *cfg_flush_timeout_str;
  static const string cfg_num_threads_str;
  static const string cfg_num_cluster_threads_str;
  static const string cfg_nameservice_str;
  static const string cfg_ns_align_str;
  static const string cfg_boost_level_str;
  static const string cfg_api_ver_str;
  static const string cfg_encrypt_str;
  static const string cfg_encrypt_key_str;
  static const string cfg_boscript_str;
#if defined(DEV)
  static const string cfg_table_size_str;
#endif
  static const string cfg_stream_cache_size_str;
  static const string cfg_stream_conn_limit_str;
  static const string cfg_table_type_str;
  static const string cfg_precision_str;
  static const string cfg_logging_str;
  static const string cfg_dt32_base_year_str;
  static const string cfg_auto_cluster_query_str;
  static const string cfg_zero_if_null_str;
  static const string cfg_orig_order_sampling_str;
  static const string cfg_ssl_str;
#if defined(KAIWU)
  static const char *cfg_spp_str;
  static const char *cfg_ts_table_str;
  static const char *cfg_ts_precalculation_str;
  static const char *cfg_ts_partition_str;
  static const char *cfg_spp_logging_str;
  static const string cfg_double_precision_str;
  static const string cfg_float_precision_str;
  static const char  *cfg_max_map_cache_size_str;
  static const char *cfg_union_parallel_num_str;
  static const char *cfg_minimum_partition_size_str;
  static const char *cfg_min_order_by_parallel_size_str;
#endif
#if defined(IOT_MODE)
  static bool iot_mode;
  static int32_t iot_interval;
  static const char *cfg_iot_mode_str;
  static const char *cfg_iot_interval_str;
  static const char *cfg_iot_auth_str;
#endif

  static  void readConfigFromFile(const string &config_url, char *env_var, struct stat stat_buffer);
};

#endif /* BIGOBJECTCONFIG_H_ */
