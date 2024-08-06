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

#pragma once
#include <cstdio>
#include <iostream>
#include <memory>
#include <vector>
#include <atomic>
#include <stdlib.h>
#include "worker.h"

using namespace kwdbts;

class BenchTool {
 private:
  int total_thread_num = 0;
  int read_thread_num = 0;
  uint32_t start_table_id = 78;  // the first table ID of the user table

 public:
  BenchParams params;
  MainParams main_param;

  virtual void parse_params(int argc, char** argv) {
    srand(time(0));
    setbuf(stdout, NULL);

    ParseUtil::ParseOpts(argc, argv, &params, &main_param);

    // ParseUtil::PrintMainParams(main_param);
    // ParseUtil::PrintBenchParams(params);

    if (params.read_wait == 0) {
      params.read_wait = params.wait_ms;
    }
    if (params.data_types.size() == 0) {
      std::vector<int> data_type;
      data_type.emplace_back(2);
      params.data_types = data_type;
    }

    total_thread_num = main_param.WORK_THREAD;
    read_thread_num = 0;
    if (main_param.single_read && total_thread_num > 1) {
      // if the read thread executes independently, a thread is separated from the bus program for reading
      total_thread_num -= 1;
      read_thread_num = 1;
    }
    if (params.meta_param.RETENTIONS_TIME != 0 &&
    params.meta_param.RETENTIONS_TIME < params.meta_param.PARTITION_INTERVAL) {
      params.meta_param.RETENTIONS_TIME = params.meta_param.PARTITION_INTERVAL;
    }
  }

  int run_main() {
    std::vector<uint32_t> all_table;
    int table_num = params.meta_param.TABLE_NUM;
    for (int di = 0; di < table_num; di++) {
      all_table.emplace_back(di + start_table_id);
    }

    std::shared_ptr<WorkerRoutine> r_routine = std::make_shared<WorkerRoutine>("read_thread", params);
    std::shared_ptr<WorkerRoutine> retentions_routine =
        std::make_shared<WorkerRoutine>("retentions_thread", params);
    std::shared_ptr<WorkerRoutine> compress_routine =
        std::make_shared<WorkerRoutine>("compress_thread", params);
    std::vector<std::shared_ptr<WorkerRoutine>> all_routines;
    int entity_num = params.meta_param.ENTITY_NUM / total_thread_num;
    for (int i = 0; i < total_thread_num; i++) {
      std::string thread_name = "thread-" + std::to_string(i);
      std::shared_ptr<WorkerRoutine> w_routine = std::make_shared<WorkerRoutine>(thread_name.c_str(), params);
      int entity_begin = i * entity_num + 1;
      int entity_end = (i + 1) * entity_num;
      if (i == total_thread_num - 1) {
        entity_end = params.meta_param.ENTITY_NUM;
      }
      for (std::string& name : main_param.benchmarks) {
        std::map<std::string, std::shared_ptr<WorkerFactory>>::iterator it =
            WorkerFactory::factories.find(name);
        if (it == WorkerFactory::factories.end()) {
          // invalid test task name, output warning message
          fprintf(stderr, "Invalid benchmark name : %s\n",name.c_str());
          exit(1);
        }
        std::shared_ptr<Worker> worker;
        if (read_thread_num > 0 && it->second->GetBenchType() == Worker::BenchType::READ) {
          if (i == 0) { // main_param.benchmarks will loop multiple times and only record once
            // if it is an independent read thread, it needs to handle all tables
            worker = it->second->NewWorker(name + "_" + std::to_string(i), params, all_table);
            r_routine->AddWorker(worker);
          }
        } else {
          worker = it->second->NewWorker(name + "_" + std::to_string(i), params, all_table);
          worker->SetEntity(entity_begin, entity_end);
          w_routine->AddWorker(worker);
        }
      }
      all_routines.push_back(std::move(w_routine));
    }
    if (read_thread_num > 0 ){
      all_routines.push_back(r_routine);
    }
    if (params.meta_param.RETENTIONS_TIME > 0) {
      std::map<std::string, std::shared_ptr<WorkerFactory>>::iterator it =
          WorkerFactory::factories.find("retentions");
      std::shared_ptr<Worker> worker;
      worker = it->second->NewWorker("retentions_0", params, all_table);
      retentions_routine->AddWorker(worker);
      all_routines.push_back(retentions_routine);
    }
    if (params.meta_param.COMPRESS_TIME > 0) {
      std::map<std::string, std::shared_ptr<WorkerFactory>>::iterator it =
          WorkerFactory::factories.find("compress");
      std::shared_ptr<Worker> worker;
      worker = it->second->NewWorker("compress_0", params, all_table);
      compress_routine->AddWorker(worker);
      all_routines.push_back(compress_routine);
    }

    // Waiting for initialization
    std::this_thread::sleep_for(std::chrono::seconds(1));

    StInstance::Get()->Init(params, all_table);

    fprintf(stdout, "***START KWDB bench.....\n");
    kwdbts::KTimestamp tBegin = GetTimeNow();

    int ret = start_threads(all_routines);
    if (ret != 0 ) return 0;

    // waiting for all threads to end
    uint32_t thread_num = all_routines.size();
    bool stop_cmd = false;
    while (true) {
      int run_threads = 0;
      for (int i = 0; i < thread_num; i++) {
        if (stop_cmd) {
          all_routines[i]->Stop();
        }
        if (!all_routines[i]->isRunning()) run_threads++;
        if (params.ERROR_EXIT && all_routines[i]->run_error >0) {
          // if any thread fails, stop all threads
          stop_cmd = true;
        }
      }
      if (run_threads >= thread_num)
        break;
      if (!stop_cmd) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }
    fprintf(stdout, "#####All Thread FINISHED######\n");

    long run_error = 0;
    long run_success = 0;
    for (int i = 0; i < all_routines.size(); i++) {
      run_error += all_routines[i]->run_error;
      run_success += all_routines[i]->run_success;
    }
    long total_num = run_error + run_success;

    float time = (GetTimeNow() - tBegin) / 1e6 / 1000;
    fprintf(stdout, "#####\nKWDB Bench Summary[Write %ldw rows, in %.1f second, Valid IOPS = %.2fw/s\n"
                    "#####Errors: %ld \n",
            total_num / 10000, time,
            run_success / 10000 / time, run_error );

    if(params.CLEAN_DATA) {
      // clean up metadata and data
      for (int i = 0; i < all_routines.size(); i++) {
        all_routines[i]->Destroy();
      }
    }

    for (int i = 0; i < thread_num; i++) {
      all_routines[i].reset();
    }

    // 4:close engine
    StInstance::Stop();

    fprintf(stdout, "############All run success. Stopped #######\n");

    if (!params.ERROR_EXIT) {
      return 0;
    }

    return run_error;
  }

  virtual int start_threads(std::vector<std::shared_ptr<WorkerRoutine>> &all_routines) = 0;

};
