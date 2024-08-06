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
/*
 * @author jiadx
 * @date 2022/8/15
 * @version 1.0
 */
#include <embed/st_worker.h>
#include "bench_tool.h"
#include "cm_exception.h"
#include "th_kwdb_dynamic_thread_pool.h"

class EmbedBenchTool : public BenchTool {
 private:
  kwdbts::kwdbContext_t g_context;
  kwdbts::kwdbContext_p g_contet_p;
 public:
  virtual void parse_params(int argc, char** argv) override {
    BenchTool::parse_params(argc, argv);

    g_contet_p = &g_context;
    if (kwdbts::InitServerKWDBContext(g_contet_p) != kwdbts::KStatus::SUCCESS) {
      log_ERROR("InitServerKWDBContext error. ");
      exit(1);
    }

    if (KWDBDynamicThreadPool::GetThreadPool(main_param.WORK_THREAD + 100)
    .Init(main_param.WORK_THREAD, g_contet_p) != KStatus::SUCCESS) {
      log_ERROR("InitThreadPool error. ");
      exit(1);
    }

    RegisterEmbedWorker(params);
  }

  int start_threads(std::vector<std::shared_ptr<WorkerRoutine>>& all_routines) override {
    // Start the write thread and get the latest data thread
    KWDBOperatorInfo kwdb_operator_info;
    kwdb_operator_info.SetOperatorName("kwdb_bench");
    kwdb_operator_info.SetOperatorOwner("kwdb_bench");
    time_t now;
    uint32_t thread_num = all_routines.size();
    kwdb_operator_info.SetOperatorStartTime((k_uint64) time(&now));
    KWDBDynamicThreadPool& kwdb_thread_pool = KWDBDynamicThreadPool::GetThreadPool();
    for (uint32_t i = 0 ; i < thread_num ; i++) {
      if (main_param.WORK_THREAD > params.meta_param.TABLE_NUM) {
        std::this_thread::sleep_for(std::chrono::milliseconds(i * 100));
      }
      kwdbts::KThreadID thread_id = kwdb_thread_pool.ApplyThread(WorkerRoutine::Start, all_routines[i].get(),
                                                                 &kwdb_operator_info);
      if (thread_id == 0) {
        fprintf(stdout, "kwdb_bench apply thread failed\n");
        return -1;
      }
      threads_id.emplace_back(thread_id);
    }

    for (k_uint32 i = 0 ; i < thread_num ; i++) {
      kwdb_thread_pool.JoinThread(threads_id[i], 3000);
    }
    return 0;
  }
  std::vector<kwdbts::KThreadID> threads_id;
};

int main(int argc, char** argv) {
  if (argc < 2) {
    ParseUtil::PrintUsage("kdp_bench usage : \n");
    return -1;
  }
  EmbedBenchTool b_tool;
  b_tool.parse_params(argc, argv);
  int ret = b_tool.run_main();
  exit(ret);
}
