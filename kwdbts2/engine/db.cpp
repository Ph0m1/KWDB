// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <libkwdbts2.h>
#include <regex>
#include <thread>
#include "include/engine.h"
#include "cm_exception.h"
#include "cm_backtrace.h"
#include "cm_fault_injection.h"
#include "cm_task.h"
#include "perf_stat.h"
#include "lru_cache_manager.h"
#include "utils/compress_utils.h"

std::map<std::string, std::string> g_cluster_settings;
DedupRule g_dedup_rule = kwdbts::DedupRule::OVERRIDE;
std::shared_mutex g_settings_mutex;
bool g_engine_initialized = false;

int64_t g_input_autovacuum_interval = 0;  // interval of compaction, 0: stop.
std::thread g_db_threads;
// inform SettingChangedSensor from TriggerSettingCallback that the setting is changed
std::condition_variable g_setting_changed_cv;
std::atomic<bool> g_setting_changed(false);

TSStatus TSOpen(TSEngine** engine, TSSlice dir, TSOptions options,
                AppliedRangeIndex* applied_indexes, size_t range_num) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  KStatus s = InitServerKWDBContext(ctx);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  EngineOptions opts;
  std::string db_path(dir.data, dir.len);
  opts.db_path = db_path;
  // TODO(rongtianyang): set wal level by cluster setting rather than env val.
  // If cluster setting support Dynamic-Update, cancel this env val.
  char* wal_env = getenv("KW_WAL_LEVEL");
  if (wal_env != nullptr) {
    opts.wal_level = *wal_env - '0';
  } else {
    opts.wal_level = options.wal_level;
  }
  opts.wal_buffer_size = options.wal_buffer_size;
  opts.wal_file_size = options.wal_file_size;
  opts.wal_file_in_group = options.wal_file_in_group;

  // TODO(LSY): log settings from kwbase start params
  string lg_path = db_path;
  try {
    opts.lg_opts.path = string(options.lg_opts.Dir.data, options.lg_opts.Dir.len);
  } catch (...) {
    cerr << "InitTsServerLog Error! log path is nullptr. using current dir to log\n";
    opts.lg_opts.path = db_path;
  }
  opts.lg_opts.file_max_size = options.lg_opts.LogFileMaxSize;
  opts.lg_opts.level = kLgSeverityMap.find(options.lg_opts.LogFileVerbosityThreshold)->second;
  opts.lg_opts.dir_max_size = options.lg_opts.LogFilesCombinedMaxSize;
  try {
    opts.lg_opts.trace_on_off = string(options.lg_opts.Trace_on_off_list.data, options.lg_opts.Trace_on_off_list.len);
  } catch (...) {
    opts.lg_opts.trace_on_off = "";
  }

  opts.thread_pool_size = options.thread_pool_size;
  opts.task_queue_size = options.task_queue_size;
  opts.buffer_pool_size = options.buffer_pool_size;

  setenv("KW_HOME", db_path.c_str(), 1);

#ifndef K_DO_NOT_SHIP
  char* port_str;
  if (port_str = getenv("KW_ERR_INJECT_PORT")) {
    int port = atoi(port_str);
    if (port > 0) {
      k_int64 server_args[1] = {port};
      s = CreateTask(ctx, &server_args, "InjectFaultServer", "TSOpen", InjectFaultServer);
      if (s == KStatus::FAIL) {
        return ToTsStatus("CreateTask[InjectFaultServer] Internal Error!");
      }
    }
  }
#endif

  // check mksquashfs & unsquashfs
  std::string cmd = "which mksquashfs > /dev/null 2>&1";
  if (system(cmd.c_str()) != 0) {
    cerr << "mksquashfs is not installed, please install squashfs-tools\n";
    return ToTsStatus("mksquashfs is not installed, please install squashfs-tools");
  }
  cmd = "which unsquashfs > /dev/null 2>&1";
  if (system(cmd.c_str()) != 0) {
    cerr << "unsquashfs is not installed, please install squashfs-tools\n";
    return ToTsStatus("unsquashfs is not installed, please install squashfs-tools");
  }

  InitCompressInfo(opts.db_path);

  TSEngine* ts_engine;
  s = TSEngineImpl::OpenTSEngine(ctx, opts.db_path, opts, &ts_engine, applied_indexes, range_num);
  if (s == KStatus::FAIL) {
    return ToTsStatus("OpenTSEngine Internal Error!");
  }
  *engine = ts_engine;
  g_db_threads = std::thread([ts_engine](){
    ts_engine->SettingChangedSensor();
  });
  g_engine_initialized = true;
  return kTsSuccess;
}

TSStatus TSCreateTsTable(TSEngine* engine, TSTableID table_id, TSSlice schema, RangeGroups range_groups) {
  KWDB_DURATION(StStatistics::Get().create_table);
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  INJECT_DATA_FAULT(FAULT_CONTEXT_INIT_FAIL, s, KStatus::FAIL, nullptr);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  roachpb::CreateTsTable meta;  // Convert according to schema protobuf
  if (!meta.ParseFromArray(schema.data, schema.len)) {
    return ToTsStatus("ParseFromArray Internal Error!");
  }

  std::vector<RangeGroup> ranges(range_groups.ranges, range_groups.ranges + range_groups.len);
  s = engine->CreateTsTable(ctx_p, table_id, &meta, ranges);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("CreateTsTable Error!");
  }
  return kTsSuccess;
}

TSStatus TSGetMetaData(TSEngine* engine, TSTableID table_id, RangeGroup range, TSSlice* schema) {
  KWDB_DURATION(StStatistics::Get().create_table);
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  INJECT_DATA_FAULT(FAULT_CONTEXT_INIT_FAIL, s, KStatus::FAIL, nullptr);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  roachpb::CreateTsTable meta;  // Convert according to schema protobuf
  s = engine->GetMetaData(ctx_p, table_id, range, &meta);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }
  string meta_str;
  if (!meta.SerializeToString(&meta_str)) {
    return ToTsStatus("SerializeToArray Internal Error!");
  }
  schema->len = meta_str.size();
  schema->data = static_cast<char*>(malloc(schema->len));
  memcpy(schema->data, meta_str.data(), meta_str.size());
  return kTsSuccess;
}

TSStatus TSGetRangeGroups(TSEngine* engine, TSTableID table_id, RangeGroups *range_groups) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  if (range_groups == nullptr) {
    return ToTsStatus("RangeGroups is nullptr!");
  }
  s = engine->GetRangeGroups(ctx_p, table_id, range_groups);
  return kTsSuccess;
}

TSStatus TSUpdateRangeGroup(TSEngine* engine, TSTableID table_id, RangeGroups range_groups) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  for (int i = 0; i < range_groups.len; ++i) {
    LOG_INFO("table %lu, update rangeGroup %lu to %s", table_id, range_groups.ranges[i].range_group_id,
      range_groups.ranges[i].typ == 0 ? "leader" : "follower");
  }
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  std::vector<RangeGroup> ranges(range_groups.ranges, range_groups.ranges + range_groups.len);
  for (int i = 0; i < ranges.size(); i++) {
    s = engine->UpdateRangeGroup(ctx_p, table_id, ranges[i]);
    if (s != KStatus::SUCCESS) {
        LOG_INFO("table %lu, update rangeGroup %lu to %s,error!!!!!!!!!!!!!!!!",
                  table_id, range_groups.ranges[i].range_group_id,
                  range_groups.ranges[i].typ == 0 ? "leader" : "follower");
        // TODO(fyx): tmp ignore no exist RangeGroup Update Error
      return ToTsStatus("UpdateRangeGroup Error!");
    }
  }
  return kTsSuccess;
}

TSStatus TSCreateRangeGroup(TSEngine* engine, TSTableID table_id, TSSlice schema, RangeGroups range_groups) {
  KWDB_DURATION(StStatistics::Get().create_group);
  kwdbContext_t context;
  LOG_INFO("table %lu, create rangeGroup %lu to %s", table_id, range_groups.ranges[0].range_group_id,
            range_groups.ranges[0].typ == 0 ? "leader" : "follower")
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  roachpb::CreateTsTable meta;
  // parse from schema protobuf
  if (!meta.ParseFromArray(schema.data, schema.len)) {
    return ToTsStatus("ParseFromArray Internal Error!");
  }
  std::vector<RangeGroup> ranges(range_groups.ranges, range_groups.ranges + range_groups.len);
  for (int i = 0; i < ranges.size(); i++) {
    s = engine->CreateRangeGroup(ctx_p, table_id, &meta, ranges[i]);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("table %lu, create rangeGroupFailedAAAAAAAAA %lu to %s", table_id, range_groups.ranges[0].range_group_id,
                range_groups.ranges[0].typ == 0 ? "leader" : "follower")
      return ToTsStatus("CreateRangeGroup Error!");
    }
  }
  return kTsSuccess;
}

TSStatus TSIsTsTableExist(TSEngine* engine, TSTableID table_id, bool* find) {
  *find = KFALSE;
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::shared_ptr<TsTable> tags_table;
  s = engine->GetTsTable(ctx_p, table_id, tags_table);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }
  if (tags_table != nullptr) {
    *find = tags_table->IsExist();
  }
  return kTsSuccess;
}

TSStatus TSDropTsTable(TSEngine* engine, TSTableID table_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->DropTsTable(ctx_p, table_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DropTsTable Error!");
  }
  return kTsSuccess;
}

TSStatus TSCompressTsTable(TSEngine* engine, TSTableID table_id, timestamp64 ts) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  LOG_INFO("compress table[%lu] start, end_ts: %lu", table_id, ts);
  s = engine->CompressTsTable(ctx_p, table_id, ts);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("compress table[%lu] failed", table_id);
    return ToTsStatus("CompressTsTable Error!");
  }
  LOG_INFO("compress table[%lu] succeeded", table_id);
  return kTsSuccess;
}

TSStatus TSPutEntity(TSEngine* engine, TSTableID table_id, TSSlice* payload, size_t payload_num, RangeGroup range_group,
                     uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->PutEntity(ctx_p, table_id, range_group.range_group_id, payload, payload_num, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("PutEntity Error!");
  }
  return kTsSuccess;
}

TSStatus TSPutData(TSEngine* engine, TSTableID table_id, TSSlice* payload, size_t payload_num, RangeGroup range_group,
                   uint64_t mtr_id, DedupResult* dedup_result) {
  KWDB_DURATION(StStatistics::Get().ts_put);
  // The CGO calls the interface, and the GO layer code will call this interface to write data
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  // Parsing table_id from payload
  TSTableID tmp_table_id = *reinterpret_cast<uint64_t*>(payload[0].data + Payload::table_id_offset_);
  // Parse range_group_id from payload
  uint64_t tmp_range_group_id = *reinterpret_cast<uint16_t*>(payload[0].data + Payload::range_group_id_offset_);
  s = engine->PutData(ctx_p, tmp_table_id, tmp_range_group_id, payload, payload_num, mtr_id, dedup_result);
  if (s != KStatus::SUCCESS) {
    std::ostringstream ss;
    ss << tmp_range_group_id;
    return ToTsStatus("PutData Error!,RangeGroup:" + ss.str());
  }
  return TSStatus{nullptr, 0};
}

TSStatus TSExecQuery(TSEngine* engine, QueryInfo* req, RespInfo* resp, TsFetcher* fetchers, void* fetcher) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  auto *fet = static_cast<VecTsFetcher *>(fetcher);
  if (fet != nullptr && fet->collected) {
    fet->TsFetchers = fetchers;
    ctx_p->fetcher = fetcher;
  }
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->Execute(ctx_p, req, resp);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Execute Error!");
  }
  return kTsSuccess;
}

TSStatus TSGetWaitThreadNum(TSEngine* engine, void* resp) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->GetTsWaitThreadNum(ctx_p, resp);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Get ts wait threads num Error!");
  }
  return kTsSuccess;
}

TSStatus TsDeleteEntities(TSEngine* engine, TSTableID table_id, TSSlice* primary_tags, size_t primary_tags_num,
                          uint64_t range_group_id, uint64_t* count, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::vector<string> p_tags;
  for (size_t i = 0; i < primary_tags_num; ++i) {
    p_tags.emplace_back(primary_tags[i].data, primary_tags[i].len);
  }
  s = engine->DeleteEntities(ctx_p, table_id, range_group_id, p_tags, count, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DeleteEntities Error!");
  }
  return kTsSuccess;
}

TSStatus TsDeleteRangeData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                      HashIdSpan hash_span, KwTsSpans ts_spans, uint64_t* count, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::vector<KwTsSpan> spans(ts_spans.spans, ts_spans.spans + ts_spans.len);
  s = engine->DeleteRangeData(ctx_p, table_id, range_group_id, hash_span, spans, count, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DeleteRangeData Error!");
  }
  return kTsSuccess;
}

TSStatus TsDeleteData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                      TSSlice primary_tag, KwTsSpans ts_spans, uint64_t* count, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::string p_tag(primary_tag.data, primary_tag.len);
  std::vector<KwTsSpan> spans(ts_spans.spans, ts_spans.spans + ts_spans.len);
  s = engine->DeleteData(ctx_p, table_id, range_group_id, p_tag, spans, count, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DeleteData Error!");
  }
  return kTsSuccess;
}

TSStatus TSFlushBuffer(TSEngine* engine) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->FlushBuffer(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("FlushBuffer Error!");
  }
  return kTsSuccess;
}

TSStatus TSCreateCheckpoint(TSEngine* engine) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->CreateCheckpoint(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Checkpoint Error!");
  }
  return kTsSuccess;
}

TSStatus TSMtrBegin(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                    uint64_t range_id, uint64_t index, uint64_t* mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSMtrBegin(ctx_p, table_id, range_group_id, range_id, index, *mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to begin the TS mini-transaction!");
  }
  return kTsSuccess;
}

TSStatus TSMtrCommit(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSMtrCommit(ctx_p, table_id, range_group_id, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to commit the TS mini-transaction!");
  }
  return kTsSuccess;
}

TSStatus TSMtrRollback(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t mtr_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSMtrRollback(ctx_p, table_id, range_group_id, mtr_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to rollback the TS mini-transaction!");
  }
  return kTsSuccess;
}

TSStatus TSxBegin(TSEngine* engine, TSTableID table_id, char* transaction_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSxBegin(ctx_p, table_id, transaction_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to begin the TS transaction!");
  }
  return kTsSuccess;
}

TSStatus TSxCommit(TSEngine* engine, TSTableID table_id, char* transaction_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSxCommit(ctx_p, table_id, transaction_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to commit the TS transaction!");
  }
  return kTsSuccess;
}

TSStatus TSxRollback(TSEngine* engine, TSTableID table_id, char* transaction_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->TSxRollback(ctx_p, table_id, transaction_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Failed to rollback the TS transaction!");
  }
  return kTsSuccess;
}

int64_t ParseInterval(const std::string& value) {
  // ts.autovaccum.interval should be format like '1Y2M3W4D5h6m7s' or numeric like '36'
  int64_t current_value = 0;
  int64_t rtn = 0;
  char t_unit[] = {'Y', 'M', 'W', 'D', 'h', 'm', 's'};  // 1Y2M3W4D5h6m7s
  uint64_t to_sec[] = {31556926, 2592000, 604800, 86400, 3600, 60, 1};
  int i = 0;
  bool has_num = false;
  for (char c : value) {
    if (i == sizeof(t_unit)) {  // if there are other numbers after 's'
      return -1;
    }
    if (std::isdigit(c)) {
      has_num = true;
      current_value = current_value * 10 + (c - '0');
    } else {  // find non-numeric
      bool valid = false;
      for ( ; has_num && i < sizeof(t_unit); i++) {  // check if it is valid
        if (c == t_unit[i]) {
          rtn += current_value * to_sec[i];
          current_value = 0;
          i++;
          valid = true;
          has_num = false;
          break;
        }
      }
      if (!valid) {  // invalid
        return -1;
      }
    }
  }
  if (i == 0) {  // numeric
    rtn = current_value;
  }
  return rtn;
}

void TriggerSettingCallback(const std::string& key, const std::string& value) {
  if (TRACE_CONFIG_NAME == key) {
    TRACER.SetTraceConfigStr(value);
  } else if ("ts.dedup.rule" == key) {
    if ("override" == value) {
      g_dedup_rule = kwdbts::DedupRule::OVERRIDE;
    } else if ("merge" == value) {
      g_dedup_rule = kwdbts::DedupRule::MERGE;
    } else if ("keep" == value) {
      g_dedup_rule = kwdbts::DedupRule::KEEP;
    } else if ("reject" == value) {
      g_dedup_rule = kwdbts::DedupRule::REJECT;
    } else if ("discard" == value) {
      g_dedup_rule = kwdbts::DedupRule::DISCARD;
    }
  } else if ("ts.mount.max_limit" == key) {
    g_max_mount_cnt_ = atoi(value.c_str());
  } else if ("ts.cached_partitions_per_subgroup.max_limit" == key) {
    g_partition_caches_mgr.SetCapacity(atoi(value.c_str()));
  } else if ("ts.entities_per_subgroup.max_limit" == key) {
    CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP = atoi(value.c_str());
  } else if ("ts.rows_per_block.max_limit" == key) {
    CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = atoi(value.c_str());
  } else if ("ts.blocks_per_segment.max_limit" == key) {
    CLUSTER_SETTING_MAX_BLOCK_PER_SEGMENT = atoi(value.c_str());
  } else if ("ts.compress_interval" == key) {
    kwdbts::g_compress_interval = atoi(value.c_str());
  } else if ("ts.autovacuum.interval" == key) {
    // If ts.autovacuum.interval was set before shutting down,
    // a set cluster command will be received immediately after rebooting to restore the setting.
    int64_t new_interval = ParseInterval(value);
    if (new_interval < 0) {
      LOG_ERROR("Invalid time format");
    } else {
      g_input_autovacuum_interval = new_interval;
      g_setting_changed.store(true);
      g_setting_changed_cv.notify_one();  // inform SettingChangedSensor that the setting is changed.
    }
  } else if ("ts.compression.type" == key) {
    CompressionType type = kwdbts::CompressionType::GZIP;
    if ("gzip" == value) {
      type = kwdbts::CompressionType::GZIP;
    } else if ("lz4" == value) {
      type = kwdbts::CompressionType::LZ4;
    } else if ("lzma" == value) {
      type = kwdbts::CompressionType::LZMA;
    } else if ("lzo" == value) {
      type = kwdbts::CompressionType::LZO;
    } else if ("xz" == value) {
      type = kwdbts::CompressionType::XZ;
    } else if ("zstd" == value) {
      type = kwdbts::CompressionType::ZSTD;
    }
    if (g_mk_squashfs_option.compressions.find(type) ==
        g_mk_squashfs_option.compressions.end()) {
      LOG_WARN("mksquashfs does not support the %s algorithm and uses gzip by default. "
                "Please upgrade the mksquashfs version.", value.c_str())
      type = kwdbts::CompressionType::GZIP;
    } else if (g_mount_option.mount_compression_types.find(type) ==
               g_mount_option.mount_compression_types.end()) {
      LOG_WARN("mount does not support the %s algorithm and uses gzip by default. "
                "Upgrade to a linux kernel version that supports this algorithm", value.c_str())
      type = kwdbts::CompressionType::GZIP;
    }
    g_compression = g_mk_squashfs_option.compressions.find(type)->second;
  } else if ("ts.compression.level" == key) {
    kwdbts::CompressionLevel level = kwdbts::CompressionLevel::MIDDLE;
    if ("low" == value) {
      level = kwdbts::CompressionLevel::LOW;
    } else if ("middle" == value) {
      level = kwdbts::CompressionLevel::MIDDLE;
    } else if ("high" == value) {
      level = kwdbts::CompressionLevel::HIGH;
    }
    for (auto& compression : g_mk_squashfs_option.compressions) {
      compression.second.compression_level = level;
    }
    g_compression.compression_level = level;
  } else {
    LOG_INFO("Cluster setting %s has no callback function.", key.c_str());
  }
}

void TSSetClusterSetting(TSSlice key, TSSlice value) {
  std::string key_set;
  std::string value_set;

  try {
    key_set = string(key.data, key.len);
  } catch (...) {
    LOG_ERROR("cluster setting get key %s failed!", key.data);
    return;
  }

  try {
    value_set = string(value.data, value.len);
  } catch (...) {
    LOG_ERROR("cluster setting %s get value %s failed!", key.data, value.data);
    return;
  }

  // callback
  TriggerSettingCallback(key_set, value_set);

  // save cluster setting to map
  std::shared_lock<std::shared_mutex> rlock(g_settings_mutex);
  std::map<std::string, std::string>::iterator iter = g_cluster_settings.find(key_set);
  if (iter == g_cluster_settings.end()) {
      rlock.unlock();
      std::map<std::string, std::string>::value_type value(key_set, value_set);
      std::unique_lock<std::shared_mutex> wlock(g_settings_mutex);
      g_cluster_settings.insert(value);
      wlock.unlock();
  } else {
    iter->second = value_set;
  }
  return;
}

TSStatus TSDeleteExpiredData(TSEngine* engine, TSTableID table_id, KTimestamp end_ts) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::shared_ptr<TsTable> ts_tb;
  s = engine->GetTsTable(ctx_p, table_id, ts_tb);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetTsTable Error!");
  }
  LOG_INFO("table[%lu] delete expired data start, expired data end time[%ld]", table_id, end_ts);
  // May be data that has expired but not deleted, so we don't care about start,
  // just delete all data older than the end timestamp.
  s = ts_tb->DeleteExpiredData(ctx_p, end_ts);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("table[%lu] delete expired data failed", table_id);
    return ToTsStatus("TsTable delete expired data Error!");
  }
  LOG_INFO("table[%lu] delete expired data succeeded", table_id);
  return kTsSuccess;
}

TSStatus TSCreateSnapshot(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                          uint64_t begin_hash, uint64_t end_hash, uint64_t* snapshot_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->CreateSnapshot(ctx_p, table_id, range_group_id, begin_hash, end_hash, snapshot_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("CreateSnapshot Error!");
  }
  return kTsSuccess;
}

TSStatus TSDropSnapshot(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->DropSnapshot(ctx_p, table_id, range_group_id, snapshot_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("DropSnapshot Error!");
  }
  return kTsSuccess;
}

TSStatus TSGetSnapshotData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id,
                           size_t offset, size_t limit, TSSlice* data, size_t* total) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->GetSnapshotData(ctx_p, table_id, range_group_id, snapshot_id, offset, limit, data, total);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("GetSnapshotData Error!");
  }
  return kTsSuccess;
}

TSStatus TSInitSnapshotForWrite(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id,
                              size_t snapshot_size) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->InitSnapshotForWrite(ctx_p, table_id, range_group_id, snapshot_id, snapshot_size);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitSnapshot Error!");
  }
  return kTsSuccess;
}

TSStatus TSWriteSnapshotData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id,
                             size_t offset, TSSlice data, bool finished) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->WriteSnapshotData(ctx_p, table_id, range_group_id, snapshot_id, offset, data, finished);
  if (s != KStatus::SUCCESS) {
      return ToTsStatus("WriteSnapshotData Error!");
  }
  return kTsSuccess;
}

TSStatus TSEnableSnapshot(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  s = engine->EnableSnapshot(ctx_p, table_id, range_group_id, snapshot_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("ApplySnapShot Error!");
  }
  return kTsSuccess;
}

TSStatus TSClose(TSEngine* engine) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  KStatus s = InitServerKWDBContext(ctx);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  LOG_INFO("TSClose")
  engine->CloseSettingChangedSensor();
  if (g_db_threads.joinable()) {
    g_db_threads.join();
  }
  engine->CreateCheckpoint(ctx);
  delete engine;
  return TSStatus{nullptr, 0};
}

void TSFree(void* ptr) {
  free(ptr);
}

void TSRegisterExceptionHandler(char *dir) {
  kwdbts::RegisterExceptionHandler(dir);
  kwdbts::RegisterBacktraceSignalHandler();
}

TSStatus TSAddColumn(TSEngine* engine, TSTableID table_id, char* transaction_id, TSSlice column,
                     uint32_t cur_version, uint32_t new_version) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error");
  }

  string err_msg;
  s = engine->AddColumn(ctx_p, table_id, transaction_id, column, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    if (err_msg.empty()) {
      err_msg = "unknown error";
    }
    return ToTsStatus(err_msg);
  }

  return kTsSuccess;
}

TSStatus TSDropColumn(TSEngine* engine, TSTableID table_id, char* transaction_id, TSSlice column,
                      uint32_t cur_version, uint32_t new_version) {
    kwdbContext_t context;
    kwdbContext_p ctx_p = &context;
    KStatus s = InitServerKWDBContext(ctx_p);
    if (s != KStatus::SUCCESS) {
        return ToTsStatus("InitServerKWDBContext Error");
    }
    string err_msg;
    s = engine->DropColumn(ctx_p, table_id, transaction_id, column, cur_version, new_version, err_msg);
    if (s != KStatus::SUCCESS) {
      if (err_msg.empty()) {
        err_msg = "unknown error";
      }
      return ToTsStatus(err_msg);
    }

    return kTsSuccess;
}

TSStatus TSAlterColumnType(TSEngine* engine, TSTableID table_id, char* transaction_id,
                           TSSlice new_column, TSSlice origin_column,
                           uint32_t cur_version, uint32_t new_version) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error");
  }

  string err_msg;
  s = engine->AlterColumnType(ctx_p, table_id, transaction_id, new_column, origin_column,
                              cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    if (err_msg.empty()) {
      err_msg = "unknown error";
    }
    return ToTsStatus(err_msg);
  }

    return kTsSuccess;
}

TSStatus TSAlterPartitionInterval(TSEngine* engine, TSTableID table_id, uint64_t partition_interval) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->AlterPartitionInterval(ctx_p, table_id, partition_interval);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("AlterPartitionInterval Error!");
  }
  return kTsSuccess;
}

TSStatus TSDeleteRangeGroup(TSEngine* engine, TSTableID table_id, RangeGroup range) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
      return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->DeleteRangeGroup(ctx_p, table_id, range);
  if (s != KStatus::SUCCESS) {
      return ToTsStatus("DeleteRangeGroup Error!");
  }
  return kTsSuccess;
}

bool TSDumpAllThreadBacktrace(char* folder, char* now_time_stamp) {
  return kwdbts::DumpAllThreadBacktrace(folder, now_time_stamp);
}
