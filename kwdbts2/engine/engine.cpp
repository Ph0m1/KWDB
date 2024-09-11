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

#include "engine.h"

#include <dirent.h>
#include <iostream>
#include <utility>
#include <shared_mutex>

#include "ee_dml_exec.h"
#include "ee_executor.h"
#include "st_wal_table.h"
#include "sys_utils.h"
#include "ts_table.h"
#include "th_kwdb_dynamic_thread_pool.h"
#include "ee_exec_pool.h"

extern std::map<std::string, std::string> g_cluster_settings;
extern DedupRule g_dedup_rule;
extern std::shared_mutex g_settings_mutex;

std::thread compact_timer_thread;  // thread for timer of compaction
std::thread compact_thread;  // thread for running compaction
std::atomic<bool> compact_timer_running(false);  // compaction timer is running
std::atomic<bool> compact_running(false);  // compaction is running
std::condition_variable compact_timer_cv;  // to stop compactTimer
std::mutex setting_changed_lock;
std::mutex compact_mtx;

void runCompact(kwdbts::kwdbContext_p ctx, TSEngineImpl *engine) {
  compact_running.store(true);
  auto now = std::chrono::system_clock::now();
  auto t_c = std::chrono::system_clock::to_time_t(now);
  auto ts1 = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  LOG_INFO("Reorganization start at: %s, timestamp: %ld", std::ctime(&t_c), ts1);
  engine->CompactData(ctx);
  now = std::chrono::system_clock::now();
  t_c = std::chrono::system_clock::to_time_t(now);
  auto ts2 = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  auto ts = ts2 - ts1;
  LOG_INFO("Reorganization finish at: %s, timestamp: %ld", std::ctime(&t_c), ts2);
  LOG_INFO("Time: %ld ms", ts);
  compact_running.store(false);
}

void compactTimer(kwdbts::kwdbContext_p ctx, TSEngineImpl *engine, int64_t sec) {
  while (true) {
    if (compact_running.load()) {  // compaction is running, wait for the next time up
      LOG_INFO("compaction is running, wait for the next time up");
    } else {  // compaction is not running, then join the last run and start the next run
      if (compact_thread.joinable()) {
        compact_thread.join();
      }
      compact_thread = std::thread(runCompact, ctx, engine);
    }

    std::unique_lock<std::mutex> lock(compact_mtx);
    // escape wait_for only when time up or compact_timer_running
    compact_timer_cv.wait_for(lock, std::chrono::seconds(sec), [] { return !compact_timer_running.load(); });
    if (!compact_timer_running.load()) {
      LOG_INFO("%ld sec Timer stop", sec);
      if (compact_thread.joinable()) {  // join thread and stop
        compact_thread.join();
      }
      return;
    }
    LOG_INFO("%ld Timer is up!", sec);
  }
}

namespace kwdbts {
int32_t EngineOptions::iot_interval  = 864000;
string EngineOptions::home_;  // NOLINT
size_t EngineOptions::ps_ = sysconf(_SC_PAGESIZE);

#define DEFAULT_NS_ALIGN_SIZE       2  // 16GB name service

int EngineOptions::ns_align_size_ = DEFAULT_NS_ALIGN_SIZE;
int EngineOptions::table_type_ = ROW_TABLE;
int EngineOptions::double_precision_ = 12;
int EngineOptions::float_precision_ = 6;
int64_t EngineOptions::max_anon_memory_size_ = 1*1024*1024*1024;  // 1G
int EngineOptions::dt32_base_year_ = 2000;
bool EngineOptions::zero_if_null_ = false;
#if defined(_WINDOWS_)
const char BigObjectConfig::slash_ = '\\';
#else
const char EngineOptions::slash_ = '/';
#endif

std::atomic<int64_t> kw_used_anon_memory_size;

void EngineOptions::init() {
  char * env_var = getenv(ENV_KW_HOME);
  if (env_var) {
    home_ = string(env_var);
  } else {
    home_ =  getenv(ENV_CLUSTER_CONFIG_HOME);;
  }

  env_var = getenv(ENV_KW_IOT_INTERVAL);
  if (env_var) {
    setInteger(iot_interval, string(env_var), 30);
  }
}

TSEngineImpl::TSEngineImpl(kwdbContext_p ctx, std::string dir_path,
                           const EngineOptions& engine_options) : options_(engine_options) {
  LogInit();
  tables_lock_ = new KLatch(LATCH_ID_TSTABLE_CACHE_LOCK);
  tables_cache_ = new SharedLruUnorderedMap<KTableKey, TsTable>(1000, true);
}

TSEngineImpl::~TSEngineImpl() {
  delete tsx_manager_sys_;
  tsx_manager_sys_ = nullptr;

  delete wal_sys_;
  wal_sys_ = nullptr;

  DestoryExecutor();
  delete tables_cache_;
  delete tables_lock_;
  KWDBDynamicThreadPool::GetThreadPool().Stop();
  LOG_DESTORY();
}

KStatus TSEngineImpl::CreateTsTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta,
                                    std::vector<RangeGroup> ranges) {
  EnterFunc()
  LOG_INFO("Create TsTable %lu begin.", table_id);
  KStatus s;

  MUTEX_LOCK(tables_lock_);
  Defer defer([&]() {
    MUTEX_UNLOCK(tables_lock_);
  });
  if (IsExists(options_.db_path + '/' + std::to_string(table_id))) {
    LOG_WARN("CreateTsTable failed, TsTable[%lu] is exists", table_id);
    Return(FAIL);
  }

  std::shared_ptr<TsTable> table = nullptr;
  UpdateSetting(ctx);
  if (options_.wal_level > 0) {
    s = wal_sys_->WriteDDLCreateWAL(ctx, 0, table_id, meta, &ranges);
    if (s == FAIL) {
      Return(s);
    }
    table = std::make_shared<LoggedTsTable>(ctx, options_.db_path, table_id, &options_);
  } else {
    table = std::make_shared<TsTable>(ctx, options_.db_path, table_id);
  }

  std::vector<TagInfo> tag_schema;
  std::vector<AttributeInfo> metric_schema;
  s = parseMetaSchema(ctx, meta, metric_schema, tag_schema);
  if (s != KStatus::SUCCESS) {
    Return(s);
  }
  uint32_t ts_version = 1;
  if (meta->ts_table().has_ts_version()) {
    ts_version = meta->ts_table().ts_version();
  }
  uint64_t partition_interval = EngineOptions::iot_interval;
  if (meta->ts_table().has_partition_interval()) {
    partition_interval = meta->ts_table().partition_interval();
  }
  s = table->Create(ctx, metric_schema, ts_version, partition_interval);
  if (s != KStatus::SUCCESS) {
    Return(s);
  }
  for (int i = 0; i < ranges.size(); i++) {
    std::shared_ptr<TsEntityGroup> table_range;
    s = table->CreateEntityGroup(ctx, ranges[i], tag_schema, &table_range);
    if (s != KStatus::SUCCESS) {
      Return(s);
    }
  }
  tables_cache_->Put(table_id, table);
  auto it = tables_range_groups_.find(table_id);
  if (it != tables_range_groups_.end()) {
    tables_range_groups_.erase(it);
  }
  std::unordered_map<uint64_t, int8_t> range_groups;
  for (auto & range : ranges) {
    range_groups.insert({range.range_group_id, range.typ});
  }
  tables_range_groups_.insert({table_id, range_groups});
  LOG_INFO("Create TsTable %lu success.", table_id);
  Return(s);
}

KStatus TSEngineImpl::DropTsTable(kwdbContext_p ctx, const KTableKey& table_id) {
  EnterFunc()
  LOG_INFO("start drop table %ld", table_id);
  // Create TsTable in the database root directory
  {
    std::shared_ptr<TsTable> table;
    ErrorInfo err_info;
    KStatus s = GetTsTable(ctx, table_id, table, err_info);
    if (s == FAIL) {
      s = err_info.errcode == KWENOOBJ ? SUCCESS : FAIL;
      if (s == FAIL) {
        LOG_INFO("drop table %ld failed", table_id);
      } else {
        LOG_INFO("drop table %ld succeeded", table_id);
      }
      Return(s);
    }

    if (options_.wal_level > 0) {
      s = wal_sys_->WriteDDLDropWAL(ctx, 0, table_id);
      if (s == FAIL) {
        LOG_INFO("drop table %ld failed", table_id);
        Return(s);
      }
    }

    table->SetDropped();
  }
  tables_cache_->EraseAndCheckRef(table_id);

  MUTEX_LOCK(tables_lock_);
  Defer defer([&]() {
    MUTEX_UNLOCK(tables_lock_);
  });
  auto it = tables_range_groups_.find(table_id);
  if (it != tables_range_groups_.end()) {
    tables_range_groups_.erase(it);
  }
  LOG_INFO("drop table %ld succeeded", table_id);
  Return(SUCCESS);
}

KStatus TSEngineImpl::CompressTsTable(kwdbContext_p ctx, const KTableKey& table_id, KTimestamp ts) {
  EnterFunc()
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) {
    LOG_ERROR("GetTsTable failed")
    Return(s);
  }

  s = table->Compress(ctx, ts);
  if (s == KStatus::FAIL) {
    LOG_ERROR("table[%lu] compress failed", table_id);
  }
  Return(s);
}

KStatus TSEngineImpl::GetTsTable(kwdbContext_p ctx, const KTableKey& table_id, std::shared_ptr<TsTable>& tags_table,
                                 ErrorInfo& err_info) {
  EnterFunc()
  tags_table = nullptr;
  // 0. First, query the cache based on table_id and pt_time
  std::shared_ptr<TsTable> table = tables_cache_->Get(table_id);
  if (table && !table->IsDropped()) {
    tags_table = table;
    Return(SUCCESS);
  } else if (table && table->IsDropped()) {
    LOG_ERROR("GetTsTable failed: table [%lu] is dropped", table_id)
    Return(FAIL);
  }

  MUTEX_LOCK(tables_lock_);
  Defer defer([&]() {
    MUTEX_UNLOCK(tables_lock_);
  });
  table = tables_cache_->Get(table_id);
  if (table && !table->IsDropped()) {
    tags_table = table;
    Return(SUCCESS);
  } else if (table && table->IsDropped()) {
    LOG_ERROR("GetTsTable failed: table [%lu] is dropped", table_id)
    Return(FAIL);
  }

  UpdateSetting(ctx);
  if (options_.wal_level > 0) {
    table = std::make_shared<LoggedTsTable>(ctx, options_.db_path, table_id, &options_);
  } else {
    table = std::make_shared<TsTable>(ctx, options_.db_path, table_id);
  }

  std::unordered_map<k_uint64, int8_t> range_groups;
  auto it = tables_range_groups_.find(table_id);
  if (it != tables_range_groups_.end()) {
    range_groups = it->second;
  } else {
    LOG_INFO("GetTsTable failed: RangeGroups is not exists in table [%lu], need initial.", table_id);
  }
  KStatus s = table->Init(ctx, range_groups, err_info);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetTsTable failed: table Init failed, table id [%lu]", table_id)
    Return(FAIL);
  }

  if (!table->IsDropped()) {
    tables_cache_->Put(table_id, table);
    // Load into cache
    tags_table = table;
    Return(SUCCESS);
  }

  LOG_ERROR("GetTsTable failed: table [%lu] is dropped", table_id)
  Return(FAIL);
}

KStatus TSEngineImpl::GetMetaData(kwdbContext_p ctx, const KTableKey& table_id,  RangeGroup range,
                                  roachpb::CreateTsTable* meta) {
  EnterFunc();
  LOG_INFO("TSEngineImpl::GetMetaData Begin!");
  std::shared_ptr<TsTable> table;
  ErrorInfo err_info;
  KStatus s = GetTsTable(ctx, table_id, table, err_info);
  if (s == FAIL) {
    s = err_info.errcode == KWENOOBJ ? SUCCESS : FAIL;
    Return(s);
  }

  // Construct roachpb::CreateTsTable.
  // Set table configures.
  auto ts_table = meta->mutable_ts_table();
  ts_table->set_ts_table_id(table_id);
  ts_table->set_ts_version(table->GetCurrentTableVersion());
  ts_table->set_partition_interval(table->GetPartitionInterval());

  // Get table data schema.
  std::vector<AttributeInfo> data_schema;
  s = table->GetDataSchema(ctx, &data_schema);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetDataSchema failed during GetMetaData, table id is %ld.", table_id)
    Return(s);
  }
  // Get table tag schema.
  std::vector<TagColumn*> tag_schema;
  s = table->GetTagSchema(ctx, range, &tag_schema);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetTagSchema failed during GetMetaData, table id is %ld.", table_id)
    Return(s);
  }
  // Convert TagColumn to TagInfo.
  std::vector<TagInfo> tag_schema_info;
  for (int i = 0; i < tag_schema.size(); i++) {
    tag_schema_info.push_back(tag_schema[i]->attributeInfo());
  }
  // Use data schema and tag schema to construct meta.
  s = generateMetaSchema(ctx, meta, data_schema, tag_schema_info);
  if (s == KStatus::FAIL) {
    LOG_ERROR("generateMetaSchema failed during GetMetaData, table id is %ld.", table_id)
    Return(s);
  }
  Return(s);
}

KStatus TSEngineImpl::PutEntity(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                TSSlice* payload, int payload_num, uint64_t mtr_id) {
  std::shared_ptr<TsTable> table;
  KStatus s;
  EnterFunc()
  s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) {
    LOG_ERROR("GetTsTable failed, table id: %lu", table_id);
    Return(s);
  }

  std::shared_ptr<TsEntityGroup> table_range;
  s = table->GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == FAIL) {
    LOG_ERROR("GetEntityGroup failed table_id: %lu entity_group_id: %lu", table_id, range_group_id);
    Return(s);
  }
  if (table_range) {
    for (int idx = 0; idx < payload_num; ++idx) {
      s = table_range->PutEntity(ctx, payload[idx], mtr_id);
      if (s == KStatus::FAIL) {
        LOG_ERROR("PutEntity failed, table id: %lu, range group id: %lu", table->GetTableId(), range_group_id)
        Return(s)
      }
    }
  }
  LOG_INFO("PutEntity succeed, table id: %lu, range group id: %lu", table->GetTableId(), range_group_id);
  Return(KStatus::SUCCESS)
}

KStatus TSEngineImpl::PutData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                              TSSlice* payload, int payload_num, uint64_t mtr_id, DedupResult* dedup_result) {
  EnterFunc()
  std::shared_ptr<TsTable> table;
  KStatus s;
  s = GetTsTable(ctx, table_id, table);
  if (s == FAIL) {
    LOG_ERROR("PutData failed, GetTsTable failed, table id: %lu", table_id)
    Return(s)
  }

  std::shared_ptr<TsEntityGroup> table_range;
  // Get EntityGroup and call PutData to write data
  s = table->GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == FAIL) {
    LOG_ERROR("PutData failed, GetEntityGroup failed %lu", range_group_id)
    Return(s)
  }
  dedup_result->payload_num = payload_num;
  dedup_result->dedup_rule = static_cast<int>(g_dedup_rule);
  s = table_range->PutData(ctx, payload, payload_num, mtr_id, dedup_result, g_dedup_rule);
  if (s == FAIL) {
    LOG_ERROR("PutData failed, table id: %lu, range group id: %lu", table->GetTableId(), range_group_id)
    Return(s)
  }
  Return(s)
}

KStatus TSEngineImpl::DeleteRangeData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                      HashIdSpan& hash_span, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                                      uint64_t mtr_id) {
  EnterFunc()
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("DeleteRangeData failed: GetTsTable failed, table id [%lu]", table_id)
    Return(s)
  }
  s = table->DeleteRangeData(ctx, range_group_id, hash_span, ts_spans, count, mtr_id);
  Return(s)
}

KStatus TSEngineImpl::DeleteData(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                 std::string& primary_tag, const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                                 uint64_t mtr_id) {
  EnterFunc()
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("DeleteData failed: GetTsTable failed, table id [%lu]", table_id)
    Return(s)
  }
  s = table->DeleteData(ctx, range_group_id, primary_tag, ts_spans, count, mtr_id);
  Return(s)
}

KStatus TSEngineImpl::DeleteEntities(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                     std::vector<std::string> primary_tags, uint64_t* count, uint64_t mtr_id) {
  EnterFunc()
  std::shared_ptr<TsTable> table;
  KStatus s;

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("DeleteEntities failed: GetTsTable failed, table id [%lu]", table_id)
    Return(s)
  }

  std::shared_ptr<TsEntityGroup> table_range;
  s = table->GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("DeleteEntities failed: GetEntityGroup failed, range group id [%lu]", range_group_id)
    Return(s)
  }

  if (table_range) {
    s = table_range->DeleteEntities(ctx, primary_tags, count, mtr_id);
    if (s == KStatus::FAIL) {
      Return(s)
    } else {
      Return(KStatus::SUCCESS)
    }
  }
  Return(KStatus::FAIL)
}

KStatus TSEngineImpl::GetBatchRepr(kwdbContext_p ctx, TSSlice* batch) {
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::ApplyBatchRepr(kwdbContext_p ctx, TSSlice* batch) {
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::Execute(kwdbContext_p ctx, QueryInfo* req, RespInfo* resp) {
  ctx->ts_engine = this;
  KStatus ret = DmlExec::ExecQuery(ctx, req, resp);
  return ret;
}

KStatus TSEngineImpl::LogInit() {
  LogConf cfg = {
    options_.lg_opts.path.c_str(),
    options_.lg_opts.file_max_size,
    options_.lg_opts.dir_max_size,
    options_.lg_opts.level
  };
  LOG_INIT(cfg);
  if (options_.lg_opts.trace_on_off != "") {
    TRACER.SetTraceConfigStr(options_.lg_opts.trace_on_off);
  }
  // comment's breif: if you want to check LOG/TRACER 's function can call next lines
  // LOG_ERROR("TEST FOR log");
  // TRACE_MM_LEVEL1("TEST FOR TRACE aaaaa\n");
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::Init(kwdbContext_p ctx) {
  // TODO(jiadx): Create the root directory and test the initialization of GO layer nodes
  char* env_home = getenv("KW_HOME");
  if (env_home == nullptr) {
    LOG_ERROR("no env : KW_HOME ")
    return KStatus::FAIL;
  }
  InitExecutor(ctx, options_);
  LOG_INFO("wal setting: %s.", getWalModeString((WALMode)options_.wal_level).c_str());
  wal_sys_ = KNEW WALMgr(options_.db_path, 0, 0, &options_);
  tsx_manager_sys_ = KNEW TSxMgr(wal_sys_);
  KStatus s = wal_sys_->Init(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("wal_sys_::Init fail.")
    return s;
  }
  tables_cache_->Init();

  // loop and recover all the TS tables, and then close them.
  // all incoming request should be blocked before the recover jobs are done.
  s = Recover(ctx);

  // the range applied indexes are only used by recovery process,
  // empty it when the job is done.
  range_indexes_map_.clear();

  if (s == KStatus::FAIL) {
    LOG_ERROR("wal_sys_::Recovery fail.")
    return s;
  }

  EngineOptions::init();
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::OpenTSEngine(kwdbContext_p ctx, const std::string& dir_path, const EngineOptions& engine_config,
                                   TSEngine** engine) {
  return OpenTSEngine(ctx, dir_path, engine_config, engine, nullptr, 0);
}

KStatus TSEngineImpl::OpenTSEngine(kwdbContext_p ctx, const std::string& dir_path, const EngineOptions& engine_config,
                                   TSEngine** engine, AppliedRangeIndex* applied_indexes, size_t range_num) {
  char* env_home = getenv("KW_HOME");
  if (env_home == nullptr) {
    setenv("KW_HOME", dir_path.c_str(), 1);
  }
  ErrorInfo err_info;
  // initBigObjectApplication(err_info);

  auto* t_engine = new TSEngineImpl(ctx, dir_path, engine_config);

  // Initialize the map for Applied Range Indexes.
  t_engine->initRangeIndexMap(applied_indexes, range_num);
  // Initialize: Check if the engine has been opened and if the file system is valid
  // Check WAL and REDO/UNDO data
  // Check range
  // Start various backend threads, such as compression
  KStatus s = t_engine->Init(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSEngineImpl::Init fail.")
    delete t_engine;
    return s;
  }
  *engine = t_engine;
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::checkpoint(kwdbts::kwdbContext_p ctx) {
  wal_sys_->Lock();
  KStatus s = wal_sys_->CreateCheckpoint(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to create checkpoint for TS Engine.")
    return s;
  }
  wal_sys_->Unlock();

  return SUCCESS;
}

KStatus TSEngineImpl::CreateCheckpoint(kwdbts::kwdbContext_p ctx) {
  EnterFunc()

  if (options_.wal_level == 0) {
    Return(KStatus::SUCCESS)
  }
  LOG_DEBUG("creating checkpoint ...");

  // Traverse all EntityGroups in each timeline of the current node
  // For each EntityGroup, call the interface of WALMgr to start the timing library checkpoint operation
  tables_cache_->Traversal([&](KTableKey table_id, std::shared_ptr<TsTable> table) -> bool {
    if (!table || table->IsDropped()) {
      return true;
    }
    // ignore checkpoint error here, will continue to do the checkpoint for next TS table.
    // for the failed one, will do checkpoint again in next interval.
    table->CreateCheckpoint(ctx);
    return true;
  });

  Return(KStatus::SUCCESS)
}

KStatus TSEngineImpl::recover(kwdbts::kwdbContext_p ctx) {
  /*
  *   DDL alter table crash recovery logic, always enabled
  * 1. The log only contains the beginning, indicating that the storage alter has not been performed and the copy has not received a successful message.
  *    The log is discarded. The restored schema is old.
  * 2 logs include the begin alter commit, which indicates that the storage alter has been completed.
  *    If the replica crashes after receiving the commit successfully or before receiving the commit,
  *    there is no need to redo, discard the logs, and restore the schema to a new one.
  * 3 logs include a start alter rollback, which indicates that the storage alter has been completed and it is uncertain whether the rollback has been completed.
  *    The replica crashes after receiving a successful rollback or before receiving a rollback,
  *    Call undo alter to roll back. If the storage determines that the rollback has already occurred, it will be directly reversed. After recovery, the schema will be old.
  * 4 logs include begin alter. It is uncertain whether the storage alter has been completed. If the copy fails to receive a commit, it crashes and calls undo alter,
  *    If an alter has already been executed, it is necessary to clean up the new ones and keep the old ones. The restored schema is old.
   */
  EnterFunc()
  KStatus s;

  TS_LSN checkpoint_lsn = wal_sys_->FetchCheckpointLSN();
  TS_LSN current_lsn = wal_sys_->FetchCurrentLSN();

  std::vector<LogEntry*> redo_logs;
  Defer defer{[&]() {
    for (auto& log : redo_logs) {
      delete log;
    }
  }};

  s = wal_sys_->ReadWALLog(redo_logs, checkpoint_lsn, current_lsn);
  if (s == KStatus::FAIL && !redo_logs.empty()) {
    LOG_ERROR("Failed to read the TS Engine WAL logs.")
#ifdef WITH_TESTS
    Return(s)
#endif
  }

  std::unordered_map<TS_LSN, LogEntry*> incomplete;
  for (auto wal_log : redo_logs) {
    // From checkpoint loop to the latest commit, including only ddl
    auto mtr_id = wal_log->getXID();

    switch (wal_log->getType()) {
      case WALLogType::TS_BEGIN: {
        incomplete.insert(std::pair<TS_LSN, LogEntry*>(mtr_id, wal_log));
        tsx_manager_sys_->insertMtrID(wal_log->getTsxID().c_str(), mtr_id);
        break;
      }
      case WALLogType::TS_COMMIT: {
        incomplete.erase(mtr_id);
        tsx_manager_sys_->eraseMtrID(mtr_id);
        break;
      }
      case WALLogType::TS_ROLLBACK: {
        if (!incomplete[mtr_id]) {
          break;
        }
        auto* ddl_log = reinterpret_cast<DDLEntry*>(incomplete[mtr_id]);
        uint64_t table_id = ddl_log->getObjectID();
        TsTable table = TsTable(ctx, options_.db_path, table_id);
        std::unordered_map<uint64_t, int8_t> range_groups;
        s = table.Init(ctx, range_groups);
        if (s == KStatus::FAIL) {
          LOG_ERROR("Failed to init table %ld.", table_id)
#ifdef WITH_TESTS
          Return(s)
#endif
        }

        s = table.UndoAlterTable(ctx, incomplete[mtr_id]);
        if (s == KStatus::FAIL) {
          LOG_ERROR("Failed to recover alter table %ld.", table_id)
#ifdef WITH_TESTS
          Return(s)
#endif
        } else {
          table.TSxClean(ctx);
        }
        incomplete.erase(mtr_id);
        tsx_manager_sys_->eraseMtrID(mtr_id);
        break;
      }
      case WALLogType::DDL_ALTER_COLUMN: {
        auto* ddl_log = reinterpret_cast<DDLEntry*>(wal_log);
        incomplete[mtr_id] = ddl_log;
        break;
      }
      default:
        break;
    }
  }
  incomplete.clear();

  Return(SUCCESS)
}

KStatus TSEngineImpl::Recover(kwdbts::kwdbContext_p ctx) {
  EnterFunc()
  LOG_INFO("recover Start.");
  KStatus s = recover(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to recover DDL")
#ifdef WITH_TESTS
    Return(s)
#endif
  }

  if (options_.wal_level == 0) {
    Return(KStatus::SUCCESS);
  }

  // Traverse all EntityGroups in each timeline of the current node
  // For each EntityGroup, call the interface of WALMgr to obtain checkpoint information and start log recovery operations
  std::vector<KTableKey> table_id_list;
  GetTableIDList(ctx, table_id_list);

  UpdateSetting(ctx);
  for (KTableKey table_id : table_id_list) {
    LoggedTsTable table = LoggedTsTable(ctx, options_.db_path, table_id, &options_);
    std::unordered_map<uint64_t, int8_t> range_groups;
    s = table.Init(ctx, range_groups);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to init table %ld.", table_id)
#ifdef WITH_TESTS
        Return(s)
#else
        continue;
#endif
    }

    LOG_DEBUG("Start recover table %ld", table_id);
    s = table.Recover(ctx, range_indexes_map_);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to recover table %ld.", table_id)
#ifdef WITH_TESTS
      Return(s)
#endif
    } else {
      s = table.CreateCheckpoint(ctx);
      if (s == KStatus::FAIL) {
        LOG_ERROR("Failed to CreateCheckpoint table %ld.", table_id)
#ifdef WITH_TESTS
        Return(s)
#endif
      }
    }
  }

  LOG_INFO("Recover success.");
  range_indexes_map_.clear();
  Return(KStatus::SUCCESS)
}

KStatus TSEngineImpl::FlushBuffer(kwdbContext_p ctx) {
  if (options_.wal_level == 0) {
    return KStatus::SUCCESS;
  }
  LOG_DEBUG("Start flush WAL buffer");

  KStatus s = wal_sys_->Flush(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Flush Buffer failed.")
    return s;
  }

  tables_cache_->Traversal([&](KTableKey table_id, std::shared_ptr<TsTable> table) -> bool {
    if (!table || table->IsDropped()) {
      return true;
    }
    // ignore the recover error, record it into ts engine log.
    table->FlushBuffer(ctx);
    return true;
  });

  return SUCCESS;
}

KStatus TSEngineImpl::TSMtrBegin(kwdbContext_p ctx, const KTableKey& table_id, uint64_t range_group_id,
                                 uint64_t range_id, uint64_t index, uint64_t& mtr_id) {
  EnterFunc()
  if (options_.wal_level == 0) {
    mtr_id = 0;
    Return(KStatus::SUCCESS)
  }

  std::shared_ptr<TsTable> table;
  KStatus s;

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrBegin failed, GetTsTable failed, table id: %lu", table_id)
    Return(s)
  }

  std::shared_ptr<TsEntityGroup> table_range;
  s = table->GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrBegin failed, GetEntityGroup failed, range group id: %lu", range_group_id)
    Return(s)
  }

  if (table_range) {
    // only LoggedTsEntityGroup provides the mini-transaction support.
    auto entity_group = static_pointer_cast<LoggedTsEntityGroup>(table_range);
    if (entity_group == nullptr) {
      LOG_ERROR("The TS mini-transaction support is disabled, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      Return(FAIL)
    }

    s = entity_group->MtrBegin(ctx, range_id, index, mtr_id);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to begin the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      Return(s)
    } else {
      LOG_DEBUG("Succeed to begin the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id);
      Return(KStatus::SUCCESS)
    }
  }
  Return(KStatus::FAIL)
}

KStatus TSEngineImpl::TSMtrCommit(kwdbContext_p ctx, const KTableKey& table_id,
                                  uint64_t range_group_id, uint64_t mtr_id) {
  EnterFunc()
  if (options_.wal_level == 0) {
    Return(KStatus::SUCCESS)
  }

  std::shared_ptr<TsTable> table;
  KStatus s;

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrCommit failed, GetTsTable failed, table id: %lu", table_id)
    Return(s)
  }

  std::shared_ptr<TsEntityGroup> table_range;
  s = table->GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrCommit failed, GetEntityGroup failed, range group id: %lu", range_group_id)
    Return(s)
  }

  if (table_range) {
    // only LoggedTsEntityGroup provides the mini-transaction support.
    auto entity_group = static_pointer_cast<LoggedTsEntityGroup>(table_range);
    if (entity_group == nullptr) {
      LOG_ERROR("The TS mini-transaction support is disabled, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      Return(FAIL)
    }

    s = entity_group->MtrCommit(ctx, mtr_id);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to commit the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      Return(s)
    } else {
      LOG_DEBUG("Succeed to commit the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id);
      Return(KStatus::SUCCESS)
    }
  }
  Return(KStatus::FAIL)
}

KStatus TSEngineImpl::TSMtrRollback(kwdbContext_p ctx, const KTableKey& table_id,
                                    uint64_t range_group_id, uint64_t mtr_id) {
  EnterFunc()
  if (options_.wal_level == 0 || mtr_id == 0) {
    Return(KStatus::SUCCESS)
  }

  std::shared_ptr<TsTable> table;
  KStatus s;

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrRollback failed, GetTsTable failed, table id: %lu", table_id)
    Return(s)
  }

  std::shared_ptr<TsEntityGroup> table_range;
  s = table->GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSMtrCommit failed, GetEntityGroup failed, range group id: %lu", range_group_id)
    Return(s)
  }

  if (table_range) {
    // only LoggedTsEntityGroup provides the mini-transaction support.
    auto entity_group = static_pointer_cast<LoggedTsEntityGroup>(table_range);
    if (entity_group == nullptr) {
      LOG_ERROR("The TS mini-transaction support is disabled, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      Return(FAIL);
    }

    s = entity_group->MtrRollback(ctx, mtr_id);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to rollback the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id)
      Return(s)
    } else {
      LOG_DEBUG("Succeed to rollback the TS mini-transaction, table id: %lu, range group id: %lu",
                table->GetTableId(), range_group_id);
      Return(KStatus::SUCCESS)
    }
  }
  Return(KStatus::FAIL)
}

KStatus TSEngineImpl::TSxBegin(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) {
  EnterFunc()

  std::shared_ptr<TsTable> table;
  KStatus s;

  tsx_manager_sys_->TSxBegin(ctx, transaction_id);

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxBegin failed, The target table is not available, table id: %lu", table_id)
    Return(s)
  }

  s = table->CreateCheckpoint(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to CreateCheckpoint table %ld.", table_id)
#ifdef WITH_TESTS
    Return(s)
#endif
  }

  Return(KStatus::SUCCESS)
}

KStatus TSEngineImpl::TSxCommit(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) {
  EnterFunc()

  std::shared_ptr<TsTable> table;
  KStatus s;

  uint64_t mtr_id = tsx_manager_sys_->getMtrID(transaction_id);
  if (mtr_id != 0) {
    if (tsx_manager_sys_->TSxCommit(ctx, transaction_id) == KStatus::FAIL) {
      LOG_ERROR("TSxCommit failed, system wal failed, table id: %lu", table_id)
      Return(s)
    }
  }

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxCommit failed, The target table is not available, table id: %lu", table_id)
    Return(s)
  }

  s = table->TSxClean(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxCommit failed, Failed to clean the TS transaction, table id: %lu", table->GetTableId())
    Return(s)
  }

  if (checkpoint(ctx) == KStatus::FAIL) {
    LOG_ERROR("TSxCommit failed, system wal checkpoint failed, table id: %lu", table_id)
    Return(s)
  }

  Return(KStatus::SUCCESS)
}

KStatus TSEngineImpl::TSxRollback(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id) {
  EnterFunc()

  std::shared_ptr<TsTable> table;
  KStatus s;

  uint64_t mtr_id = tsx_manager_sys_->getMtrID(transaction_id);
  if (mtr_id == 0) {
    if (checkpoint(ctx) == KStatus::FAIL) {
      LOG_ERROR("TSxCommit failed, system wal checkpoint failed, table id: %lu", table_id)
      Return(s)
    }

    Return(KStatus::SUCCESS)
  }

  s = tsx_manager_sys_->TSxRollback(ctx, transaction_id);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, TSxRollback failed, table id: %lu", table_id)
    Return(s)
  }

  s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, The target table is not available, table id: %lu", table_id)
    Return(s)
  }

  std::vector<LogEntry*> logs;
  s = wal_sys_->ReadWALLogForMtr(mtr_id, logs);
  if (s == KStatus::FAIL && !logs.empty()) {
    for (auto log : logs) {
      delete log;
    }
    Return(s)
  }

  std::reverse(logs.begin(), logs.end());
  for (auto log : logs) {
    if (log->getXID() == mtr_id && log->getType() == WALLogType::DDL_ALTER_COLUMN && s != FAIL) {
      s = table->UndoAlterTable(ctx, log);
      if (s == KStatus::SUCCESS) {
        table->TSxClean(ctx);
      }
    }
    delete log;
  }

  if (s == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, Failed to ROLLBACK the TS transaction, table id: %lu", table_id)
    tables_cache_->EraseAndCheckRef(table_id);
    Return(s)
  }

  if (checkpoint(ctx) == KStatus::FAIL) {
    LOG_ERROR("TSxRollback failed, system wal checkpoint failed, table id: %lu", table_id)
    Return(s)
  }

  Return(KStatus::SUCCESS)
}

void TSEngineImpl::GetTableIDList(kwdbContext_p ctx, std::vector<KTableKey>& table_id_list) {
  DIR* dir_ptr = opendir((options_.db_path).c_str());
  if (dir_ptr) {
    struct dirent* entity;
    while ((entity = readdir(dir_ptr)) != nullptr) {
      if (entity->d_type == DT_DIR) {
        bool is_table = true;
        for (int i = 0; i < strlen(entity->d_name); i++) {
          if (entity->d_name[i] < '0' || entity->d_name[i] > '9') {
            is_table = false;
            break;
          }
        }
        if (!is_table) {
          continue;
        }

        KTableKey table_id = std::stoull(entity->d_name);
        table_id_list.push_back(table_id);
      }
    }
    closedir(dir_ptr);
  }
}

KStatus TSEngineImpl::parseMetaSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta,
                                      std::vector<AttributeInfo>& metric_schema,
                                      std::vector<TagInfo>& tag_schema) {
  EnterFunc()
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    KStatus s = TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
    if (s != KStatus::SUCCESS) {
      Return(s);
    }

    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_schema.push_back(std::move(TagInfo{col.column_id(), col_var.type,
                                             static_cast<uint32_t>(col_var.length), 0,
                                             static_cast<uint32_t>(col_var.size),
                                             col_var.isAttrType(COL_PRIMARY_TAG) ? PRIMARY_TAG : GENERAL_TAG}));
    } else {
      metric_schema.push_back(std::move(col_var));
    }
  }
  Return(KStatus::SUCCESS);
}

KStatus TSEngineImpl::CloseTSEngine(kwdbContext_p ctx, TSEngine* engine) {
  engine->CreateCheckpoint(ctx);
  delete engine;
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::UpdateSetting(kwdbContext_p ctx) {
  EnterFunc()
  // After changing the WAL configuration parameters, the already opened table will not change,
  // and the newly opened table will follow the new configuration.
  string value;

  if (GetClusterSetting(ctx, "ts.wal.wal_level", &value) == SUCCESS) {
    options_.wal_level = std::stoll(value);
    LOG_INFO("update wal level to %d", options_.wal_level)
  }

  if (GetClusterSetting(ctx, "ts.wal.buffer_size", &value) == SUCCESS) {
    options_.wal_buffer_size = std::stoll(value);
    LOG_INFO("update wal buffer size to %d Mib", options_.wal_buffer_size)
  }

  if (GetClusterSetting(ctx, "ts.wal.file_size", &value) == SUCCESS) {
    options_.wal_file_size = std::stoll(value);
    LOG_INFO("update wal file size to %d Mib", options_.wal_file_size)
  }

  if (GetClusterSetting(ctx, "ts.wal.files_in_group", &value) == SUCCESS) {
    options_.wal_file_in_group = std::stoll(value);
    LOG_INFO("update wal file num in group to %d", options_.wal_file_in_group)
  }

  Return(KStatus::SUCCESS)
}

KStatus TSEngineImpl::GetClusterSetting(kwdbContext_p ctx, const std::string& key, std::string* value) {
  EnterFunc()
  std::shared_lock<std::shared_mutex> lock(g_settings_mutex);
  std::map<std::string, std::string>::iterator iter = g_cluster_settings.find(key);
  if (iter != g_cluster_settings.end()) {
    *value = iter->second;
    Return(KStatus::SUCCESS)
  } else {
    Return(KStatus::FAIL)
  }
}

KStatus TSEngineImpl::AddColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                                TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    err_msg = "Table does not exist";
    return s;
  }

  // Get transaction ID.
  uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

  // Convert TSSlice to roachpb::KWDBKTSColumn.
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    err_msg = "Parse protobuf error";
    Return(KStatus::FAIL);
  }
  // Write Alter DDL into WAL, which type is ADD_COLUMN.
  s = wal_sys_->WriteDDLAlterWAL(ctx, x_id, table_id, AlterType::ADD_COLUMN, cur_version, new_version, column);
  if (s != KStatus::SUCCESS) {
    err_msg = "Write WAL error";
    return s;
  }

  s = table->AlterTable(ctx, AlterType::ADD_COLUMN, &column_meta, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::DropColumn(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                                 TSSlice column, uint32_t cur_version, uint32_t new_version, string& err_msg) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    return s;
  }

  // Get transaction id.
  uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

  // Convert TSSlice to roachpb::KWDBKTSColumn.
  roachpb::KWDBKTSColumn column_meta;
  if (!column_meta.ParseFromArray(column.data, column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    Return(KStatus::FAIL);
  }

  // Write Alter DDL into WAL, which type is DROP_COLUMN.
  s = wal_sys_->WriteDDLAlterWAL(ctx, x_id, table_id, AlterType::DROP_COLUMN, cur_version, new_version, column);
  if (s == KStatus::FAIL) {
    return s;
  }

  s = table->AlterTable(ctx, AlterType::DROP_COLUMN, &column_meta, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::AlterPartitionInterval(kwdbContext_p ctx, const KTableKey& table_id, uint64_t partition_interval) {
  EnterFunc();
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    Return(s);
  }

  // Table alters partition interval.
  s = table->AlterPartitionInterval(ctx, partition_interval);
  Return(s);
}

// Gets the number of remaining threads from the thread pool
KStatus TSEngineImpl::GetTsWaitThreadNum(kwdbContext_p ctx, void *resp) {
  EnterFunc()

  // Get wait thread num
  k_uint32 wait_threads = ExecPool::GetInstance().GetWaitThreadNum();

  // Prepare response
  auto *return_info = static_cast<ThreadInfo *>(resp);
  if (return_info == nullptr) {
    LOG_ERROR("invalid resp pointer")
    Return(KStatus::FAIL)
  }

  return_info->wait_threads = wait_threads;
  Return(KStatus::SUCCESS)
}

KStatus TSEngineImpl::AlterColumnType(kwdbContext_p ctx, const KTableKey& table_id, char* transaction_id,
                                      TSSlice new_column, TSSlice origin_column,
                                      uint32_t cur_version, uint32_t new_version, string& err_msg) {
  std::shared_ptr<TsTable> table;
  KStatus s = GetTsTable(ctx, table_id, table);
  if (s == KStatus::FAIL) {
    return s;
  }

  // Get transaction id.
  uint64_t x_id = tsx_manager_sys_->getMtrID(transaction_id);

  // Write Alter DDL into WAL, which type is ALTER_COLUMN_TYPE.
  s = wal_sys_->WriteDDLAlterWAL(ctx, x_id, table_id, AlterType::ALTER_COLUMN_TYPE, cur_version, new_version, origin_column);
  if (s == KStatus::FAIL) {
    return s;
  }

  // Convert TSSlice to roachpb::KWDBKTSColumn.
  roachpb::KWDBKTSColumn new_col_meta;
  if (!new_col_meta.ParseFromArray(new_column.data, new_column.len)) {
    LOG_ERROR("ParseFromArray Internal Error");
    Return(KStatus::FAIL);
  }
  s = table->AlterTable(ctx, AlterType::ALTER_COLUMN_TYPE, &new_col_meta, cur_version, new_version, err_msg);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::generateMetaSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta,
                                         std::vector<AttributeInfo>& metric_schema,
                                         std::vector<TagInfo>& tag_schema) {
  EnterFunc()
  // Traverse metric schema and use attribute info to construct metric column info of meta.
  for (auto col_var : metric_schema) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    KStatus s = TsEntityGroup::GetMetricColumnInfo(ctx, col_var, *col);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColTypeStr failed during generate metric Schema");
      Return(s);
    }
  }

  // Traverse tag schema and use tag info to construct metric column info of meta
  for (auto tag_info : tag_schema) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    // XXX Notice: tag_info don't has tag column name,
    KStatus s = TsEntityGroup::GetTagColumnInfo(ctx, tag_info, *col);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColTypeStr failed during generate tag Schema");
      Return(s);
    }
    // Set storage length.
    if (col->has_storage_len() && col->storage_len() == 0) {
      col->set_storage_len(tag_info.m_size);
    }
  }

  Return(KStatus::SUCCESS);
}

KStatus TSEngineImpl::SettingChangedSensor() {
  while (wait_setting_) {  // false only when CloseSettingChangedSensor
    std::unique_lock<std::mutex> lock(setting_changed_lock);
    // waiting for the setting is changed
    // wait() will release setting_changed_lock, and lock it back when waking up.
    g_setting_changed_cv.wait(lock, [this] { return g_setting_changed.load() || !wait_setting_; } );
    if (!wait_setting_) {
      break;
    }
    g_setting_changed.store(false);

    // check which setting is changed
    if (engine_autovacuum_interval_ != g_input_autovacuum_interval) {
      engine_autovacuum_interval_ = g_input_autovacuum_interval;
      kwdbContext_t context;
      kwdbContext_p ctx_p = &context;
      resetCompactTimer(ctx_p);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::CloseSettingChangedSensor() {
  {
    std::unique_lock<std::mutex> lock(setting_changed_lock);
    wait_setting_ = false;
    g_setting_changed.store(true);
  }
  g_setting_changed_cv.notify_one();  // let SettingChangedSensor escape while loop
  return KStatus::SUCCESS;
}

KStatus TSEngineImpl::resetCompactTimer(kwdbContext_p ctx) {
  EnterFunc();
  // Coming to this point indicates that engine_autovacuum_interval_ is different from the original
  if (compact_timer_running.load()) {  // stop the running timer
    compact_timer_running.store(false);
    compact_timer_cv.notify_one();  // notify compactTimer to stop.
    if (compact_timer_thread.joinable()) {
      compact_timer_thread.join();
    }
  }

  if (engine_autovacuum_interval_ > 0) {
    compact_timer_running.store(true);
    compact_timer_thread = std::thread(compactTimer, ctx, this, engine_autovacuum_interval_);
  }
  Return(KStatus::SUCCESS);
}

KStatus TSEngineImpl::CompactData(kwdbContext_p ctx) {
  EnterFunc();
  KwTsSpan ts_span = KwTsSpan{INT64_MIN, INT64_MAX};

  std::vector<TSTableID> table_id_list;
  GetTableIDList(ctx, table_id_list);
  for (TSTableID table_id : table_id_list) {
    std::shared_ptr<TsTable> table;
    ErrorInfo err_info;

    KStatus s = GetTsTable(ctx, table_id, table, err_info);
    if (s != KStatus::SUCCESS) {
      s = err_info.errcode == KWENOOBJ ? SUCCESS : FAIL;
      Return(s);
    }
    RangeGroups groups;
    Defer defer{[&]() {
      free(groups.ranges);
    }};
    s = table->GetEntityGroups(ctx, &groups);
    if (s != KStatus::SUCCESS) {
      Return(s);
    }
    for (int i = 0; i < groups.len; i++) {
      s = table->CompactData(ctx, groups.ranges[i].range_group_id, ts_span);
      if (s != KStatus::SUCCESS) {
        Return(s);
      }
    }
  }
  Return(KStatus::SUCCESS);
}

bool AggCalculator::cmp(void* l, void* r) {
  switch (type_) {
    case DATATYPE::INT8:
    case DATATYPE::BYTE:
    case DATATYPE::CHAR:
    case DATATYPE::BOOL:
    case DATATYPE::BINARY: {
      k_int32 ret = memcmp(l, r, size_);
      return ret > 0;
    }
    case DATATYPE::INT16:
      return (*(static_cast<k_int16*>(l))) > (*(static_cast<k_int16*>(r)));
    case DATATYPE::INT32:
    case DATATYPE::TIMESTAMP:
      return (*(static_cast<k_int32*>(l))) > (*(static_cast<k_int32*>(r)));
    case DATATYPE::INT64:
    case DATATYPE::TIMESTAMP64:
      return (*(static_cast<k_int64*>(l))) > (*(static_cast<k_int64*>(r)));
    case DATATYPE::TIMESTAMP64_LSN:
      return (*(static_cast<TimeStamp64LSN*>(l))).ts64 > (*(static_cast<TimeStamp64LSN*>(r))).ts64;
    case DATATYPE::FLOAT:
      return (*(static_cast<float*>(l))) > (*(static_cast<float*>(r)));
    case DATATYPE::DOUBLE:
      return (*(static_cast<double*>(l))) > (*(static_cast<double*>(r)));
    case DATATYPE::STRING: {
      k_int32 ret = strncmp(static_cast<char*>(l), static_cast<char*>(r), size_);
      return ret > 0;
    }
      break;
    default:
      break;
  }
  return false;
}

bool AggCalculator::isnull(size_t row) {
  if (!bitmap_) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(bitmap_)[byte] & bit;
}

bool AggCalculator::isDeleted(char* delete_flags, size_t row) {
  if (!delete_flags) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(delete_flags)[byte] & bit;
}

void* AggCalculator::GetMax(void* base, bool need_to_new) {
  void* max = nullptr;
  for (int i = 0; i < count_; ++i) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    if (!max || cmp(current, max)) {
      max = current;
    }
  }
  if (base && cmp(base, max)) {
    max = base;
  }
  if (need_to_new && max) {
    void* new_max = malloc(size_);
    memcpy(new_max, max, size_);
    max = new_max;
  }
  return max;
}

void* AggCalculator::GetMin(void* base, bool need_to_new) {
  void* min = nullptr;
  for (int i = 0; i < count_; i++) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    if (!min || !cmp(current, min)) {
      min = current;
    }
  }
  if (base && !cmp(base, min)) {
    min = base;
  }
  if (need_to_new && min) {
    void* new_min = malloc(size_);
    memcpy(new_min, min, size_);
    min = new_min;
  }
  return min;
}

template<class T>
bool AddAggInteger(T& a, T b) {
  T c;
  if (__builtin_add_overflow(a, b, &c)) {
    return true;
  }
  a = a + b;
  return false;
}

template<class T1, class T2>
bool AddAggInteger(T1& a, T2 b) {
  T1 c;
  if (__builtin_add_overflow(a, b, &c)) {
    return true;
  }
  a = a + b;
  return false;
}

template<class T1, class T2>
void AddAggFloat(T1& a, T2 b) {
  a = a + b;
}

template<class T>
void SubAgg(T& a, T b) {
  a = a - b;
}

void* AggCalculator::changeBaseType(void* base) {
  void* sum_base = malloc(sizeof(double));
  memset(sum_base, 0, sizeof(double));
  switch (type_) {
    case DATATYPE::INT8:
      *(static_cast<double*>(sum_base)) = *(static_cast<int8_t*>(base));
      break;
    case DATATYPE::INT16:
      *(static_cast<double*>(sum_base)) = *(static_cast<int16_t*>(base));
      break;
    case DATATYPE::INT32:
      *(static_cast<double*>(sum_base)) = *(static_cast<int32_t*>(base));
      break;
    case DATATYPE::INT64:
      *(static_cast<double*>(sum_base)) = *(static_cast<int64_t*>(base));
      break;
    case DATATYPE::FLOAT:
      *(static_cast<double*>(sum_base)) = *(static_cast<float*>(base));
    default:
      break;
  }
  free(base);
  base = nullptr;
  return sum_base;
}

bool AggCalculator::GetSum(void** sum_res, void* base, bool is_overflow) {
  if (!isSumType(type_)) {
    *sum_res = nullptr;
    return false;
  }
  void* sum_base = nullptr, *new_sum_base = nullptr;
  if (base) {
    if (!is_overflow && is_overflow_) {
      sum_base = changeBaseType(base);
    } else if (is_overflow && !is_overflow_) {
      new_sum_base = base;
    } else {
      sum_base = base;
    }
  } else {
    sum_base = malloc(sum_size_);
    memset(sum_base, 0, sum_size_);
  }

  for (int i = 0; i < count_; i++) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    switch (sum_type_) {
      case DATATYPE::INT8:
        if (sum_base) {
          if (AddAggInteger<int64_t, int8_t>((*(static_cast<int64_t*>(sum_base))),
                                             (*(static_cast<int8_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int8_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int8_t*>(current))));
        }
        break;
      case DATATYPE::INT16:
        if (sum_base) {
          if (AddAggInteger<int64_t, int16_t>((*(static_cast<int64_t*>(sum_base))),
                                              (*(static_cast<int16_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int16_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int16_t*>(current))));
        }
        break;
      case DATATYPE::INT32:
        if (sum_base) {
          if (AddAggInteger<int64_t, int32_t>((*(static_cast<int64_t*>(sum_base))),
                                              (*(static_cast<int32_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int32_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int32_t*>(current))));
        }
        break;
      case DATATYPE::INT64:
        if (sum_base) {
          if (AddAggInteger<int64_t>((*(static_cast<int64_t*>(sum_base))), (*(static_cast<int64_t*>(current))))) {
            new_sum_base = malloc(sizeof(double));
            memset(new_sum_base, 0, sizeof(double));
            *(static_cast<double*>(new_sum_base)) = *(static_cast<int64_t*>(sum_base));
            free(sum_base);
            sum_base = nullptr;
          }
        }
        if (new_sum_base) {
          AddAggFloat<double, int64_t>((*(static_cast<double*>(new_sum_base))), (*(static_cast<int64_t*>(current))));
        }
        break;
      case DATATYPE::DOUBLE:
        AddAggFloat<double>((*(static_cast<double*>(sum_base))), (*(static_cast<double*>(current))));
        break;
      case DATATYPE::FLOAT:
        AddAggFloat<double, float>((*(static_cast<double*>(sum_base))), (*(static_cast<float*>(current))));
        break;
      default:
        break;
    }
  }
  *sum_res = sum_base ? sum_base : new_sum_base;
  return (new_sum_base != nullptr) || is_overflow_;
}

bool AggCalculator::CalAllAgg(void* min_base, void* max_base, void* sum_base, void* count_base,
                              bool block_first_line, const BlockSpan& span) {
  void* min = block_first_line ? nullptr : min_base;
  void* max = block_first_line ? nullptr : max_base;

  if (block_first_line && sum_base) {
    memset(sum_base, 0, size_);
  }

  bool is_overflow = false;
  bool hasDeleted = span.block_item->getDeletedCount() > 0;
  for (int i = 0; i < count_; ++i) {
    if (hasDeleted && isDeleted(span.block_item->rows_delete_flags, first_row_ + i)) {
      continue;
    }
    if (isnull(first_row_ + i)) {
      continue;
    }
    if (count_base) {
      *reinterpret_cast<uint16_t*>(count_base) += 1;
    }

    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    if (!max || cmp(current, max)) {
      max = current;
    }
    if (!min || !cmp(current, min)) {
      min = current;
    }
    if (isSumType(type_)) {
      // When a memory overflow has occurred, there is no need to calculate the sum result again
      if (is_overflow) {
        continue;
      }
      // sum
      switch (type_) {
        case DATATYPE::INT8:
          is_overflow = AddAggInteger<int8_t>((*(static_cast<int8_t*>(sum_base))), (*(static_cast<int8_t*>(current))));
          break;
        case DATATYPE::INT16:
          is_overflow = AddAggInteger<int16_t>((*(static_cast<int16_t*>(sum_base))),
                                               (*(static_cast<int16_t*>(current))));
          break;
        case DATATYPE::INT32:
          is_overflow = AddAggInteger<int32_t>((*(static_cast<int32_t*>(sum_base))),
                                               (*(static_cast<int32_t*>(current))));
          break;
        case DATATYPE::INT64:
          is_overflow = AddAggInteger<int64_t>((*(static_cast<int64_t*>(sum_base))),
                                               (*(static_cast<int64_t*>(current))));
          break;
        case DATATYPE::DOUBLE:
          AddAggFloat<double>((*(static_cast<double*>(sum_base))), (*(static_cast<double*>(current))));
          break;
        case DATATYPE::FLOAT:
          AddAggFloat<float>((*(static_cast<float*>(sum_base))), (*(static_cast<float*>(current))));
          break;
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64:
        case DATATYPE::TIMESTAMP64_LSN:
          break;
        default:
          break;
      }
    }
  }

  if (min != nullptr && min != min_base) {
    memcpy(min_base, min, size_);
  }

  if (max != nullptr && max != max_base) {
    memcpy(max_base, max, size_);
  }
  return is_overflow;
}

bool VarColAggCalculator::cmp(void* l, void* r) {
  uint16_t l_len = *(reinterpret_cast<uint16_t*>(l));
  uint16_t r_len = *(reinterpret_cast<uint16_t*>(r));
  uint16_t len = min(l_len, r_len);
  void* l_data = reinterpret_cast<void*>((intptr_t)(l) + sizeof(uint16_t));
  void* r_data = reinterpret_cast<void*>((intptr_t)(r) + sizeof(uint16_t));
  k_int32 ret = memcmp(l_data, r_data, len);
  return (ret > 0) || (ret == 0 && l_len > r_len);
}

bool VarColAggCalculator::isnull(size_t row) {
  if (!bitmap_) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(bitmap_)[byte] & bit;
}

bool VarColAggCalculator::isDeleted(char* delete_flags, size_t row) {
  if (!delete_flags) {
    return false;
  }
  size_t byte = (row - 1) >> 3;
  size_t bit = 1 << ((row - 1) & 7);
  return static_cast<char*>(delete_flags)[byte] & bit;
}

std::shared_ptr<void> VarColAggCalculator::GetMax(std::shared_ptr<void> base) {
  void* max = nullptr;
  for (int i = 0; i < count_; ++i) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    size_t offset = start_offset_;
    if (i > 0) {
      offset = *reinterpret_cast<uint64_t*>(reinterpret_cast<void*>((intptr_t) (mem_) + i * size_));
    }
    void* var_data = reinterpret_cast<void*>((intptr_t)var_mem_.get() + (offset - start_offset_));
    if (!max || cmp(var_data, max)) {
      max = var_data;
    }
  }
  if (base && cmp(base.get(), max)) {
    max = base.get();
  }

  uint16_t len = *(reinterpret_cast<uint16_t*>(max));
  void* data = std::malloc(len + MMapStringFile::kStringLenLen);
  memcpy(data, max, len + MMapStringFile::kStringLenLen);
  std::shared_ptr<void> ptr(data, free);
  return ptr;
}

std::shared_ptr<void> VarColAggCalculator::GetMin(std::shared_ptr<void> base) {
  void* min = nullptr;
  for (int i = 0; i < count_; i++) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    size_t offset = start_offset_;
    if (i > 0) {
      offset = *reinterpret_cast<uint64_t*>(reinterpret_cast<void*>((intptr_t) (mem_) + i * size_));
    }
    void* var_data = reinterpret_cast<void*>((intptr_t)var_mem_.get() + (offset - start_offset_));
    if (!min || !cmp(var_data, min)) {
      min = var_data;
    }
  }
  if (base && !cmp(base.get(), min)) {
    min = base.get();
  }
  uint16_t len = *(reinterpret_cast<uint16_t*>(min));
  void* data = std::malloc(len + MMapStringFile::kStringLenLen);
  memcpy(data, min, len + MMapStringFile::kStringLenLen);
  std::shared_ptr<void> ptr(data, free);
  return ptr;
}

void VarColAggCalculator::CalAllAgg(void* min_base, void* max_base, std::shared_ptr<void> var_min_base,
                                    std::shared_ptr<void> var_max_base, void* count_base,
                                    bool block_first_line, const BlockSpan& span) {
  void* min = block_first_line ? nullptr : min_base;
  void* max = block_first_line ? nullptr : max_base;
  void* var_min = block_first_line ? nullptr : var_min_base.get();
  void* var_max = block_first_line ? nullptr : var_max_base.get();

  bool hasDeleted = span.block_item->getDeletedCount() > 0;
  for (int i = 0; i < count_; ++i) {
    if (hasDeleted && isDeleted(span.block_item->rows_delete_flags, first_row_ + i)) {
      continue;
    }
    if (isnull(first_row_ + i)) {
      continue;
    }
    if (count_base) {
      *reinterpret_cast<uint16_t*>(count_base) += 1;
    }
    void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
    size_t offset = *reinterpret_cast<uint64_t*>(current);
    void* var_data = reinterpret_cast<void*>((intptr_t)var_mem_.get() + (offset - start_offset_));
    if (!max || cmp(var_data, var_max)) {
      max = current;
      var_max = var_data;
    }
    if (!min || !cmp(var_data, var_min)) {
      min = current;
      var_min = var_data;
    }
  }

  if (min != nullptr && min != min_base) {
    memcpy(min_base, min, size_);
  }

  if (max != nullptr && max != max_base) {
    memcpy(max_base, max, size_);
  }
}

void AggCalculator::UndoAgg(void* min_base, void* max_base, void* sum_base, void* count_base) {
//  if (block_first_line && sum_base) {
//    memset(sum_base, 0, size_);
//  }

  for (int i = 0; i < count_; ++i) {
    if (isnull(first_row_ + i)) {
      continue;
    }
    if (count_base) {
      *reinterpret_cast<uint16_t*>(count_base) -= 1;
    }

    if (isSumType(type_)) {
      void* current = reinterpret_cast<void*>((intptr_t) (mem_) + i * size_);
//      if (!max || (cmp(current, max) > 0)) {
//        max = current;
//      }
//      if (!min_base || (cmp(current, min) < 0)) {
//        min = current;
//      }

      // sum
      switch (type_) {
        case DATATYPE::INT8:
          SubAgg<int8_t>((*(static_cast<int8_t*>(sum_base))), (*(static_cast<int8_t*>(current))));
          break;
        case DATATYPE::INT16:
          SubAgg<int16_t>((*(static_cast<int16_t*>(sum_base))), (*(static_cast<int16_t*>(current))));
          break;
        case DATATYPE::INT32:
          SubAgg<int32_t>((*(static_cast<int32_t*>(sum_base))), (*(static_cast<int32_t*>(current))));
          break;
        case DATATYPE::INT64:
          SubAgg<int64_t>((*(static_cast<int64_t*>(sum_base))), (*(static_cast<int64_t*>(current))));
          break;
        case DATATYPE::DOUBLE:
          SubAgg<double>((*(static_cast<double*>(sum_base))), (*(static_cast<double*>(current))));
          break;
        case DATATYPE::FLOAT:
          SubAgg<float>((*(static_cast<float*>(sum_base))), (*(static_cast<float*>(current))));
          break;
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64:
        case DATATYPE::TIMESTAMP64_LSN:
          break;
        default:
          break;
      }
    }
  }
}

}  //  namespace kwdbts
