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



#ifndef KWDBTS2_COMMON_INCLUDE_EXPLAIN_ANALYSE_H_
#define KWDBTS2_COMMON_INCLUDE_EXPLAIN_ANALYSE_H_

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cm_kwdb_context.h"
#include "libkwdbts2.h"
#include "ee_data_chunk.h"
#include "cm_func.h"

namespace kwdbts {
// collect information of analyse
inline void analyseFetcher(kwdbts::kwdbContext_p ctx, int32_t processor_id, int64_t duration, int64_t read_row_num,
                           int64_t bytes_read, int64_t max_allocated_mem,
                           int64_t max_allocated_disk, int64_t output_row_num) {
  auto *fetchers = static_cast<VecTsFetcher *>(ctx->fetcher);
  if (fetchers != nullptr && fetchers->collected) {
    goLock(fetchers->goMutux);
    for (int i = 0; i < fetchers->size; ++i) {
      TsFetcher *fetcher = &fetchers->TsFetchers[i];
      if (fetcher->processor_id == processor_id) {
        if (duration > 0) {
          fetcher->stall_time += duration;
        }
        if (read_row_num > 0) {
          fetcher->row_num += read_row_num;
        }
        if (bytes_read > 0) {
          fetcher->bytes_read += bytes_read;
        }
        if (max_allocated_mem > 0) {
          fetcher->max_allocated_mem += max_allocated_mem;
        }
        if (max_allocated_disk > 0) {
          fetcher->max_allocated_disk += max_allocated_disk;
        }
        if (output_row_num > 0) {
          fetcher->output_row_num = output_row_num;
        }
      }
    }
    goUnLock(fetchers->goMutux);
  }
}

struct VecTsFetcherVector {
    std::vector<TsFetcher> fetchers_vec_;

    // add data of analyse to chunk
    kwdbts::KStatus AddAnalyse(kwdbContext_p ctx, int32_t processor_id, int64_t duration, int64_t read_row_num,
                               int64_t bytes_read, int64_t max_allocated_mem, int64_t max_allocated_disk) {
      EnterFunc();
      auto *fetchers = static_cast<VecTsFetcher *>(ctx->fetcher);
      if (fetchers != nullptr && fetchers->collected) {
        TsFetcher fetcher;
        fetcher.processor_id = processor_id;
        if (duration > 0) {
          fetcher.stall_time = duration;
        }
        if (read_row_num > 0) {
          fetcher.row_num = read_row_num;
        }
        if (bytes_read > 0) {
          fetcher.bytes_read = bytes_read;
        }
        if (max_allocated_mem > 0) {
          fetcher.max_allocated_mem = max_allocated_mem;
        }
        if (max_allocated_disk > 0) {
          fetcher.max_allocated_disk = max_allocated_disk;
        }
        fetchers_vec_.push_back(fetcher);
      }
      Return(KStatus::SUCCESS);
    }

    // get data of analyse from chunk
    kwdbts::KStatus GetAnalyse(kwdbContext_p ctx) {
      EnterFunc();
      auto *fetchers = static_cast<VecTsFetcher *>(ctx->fetcher);
      if (fetchers != nullptr && fetchers->collected) {
        for (int i = 0; i < fetchers->size; ++i) {
          TsFetcher *fetcher = &fetchers->TsFetchers[i];
          for (int j = 0; j < fetchers_vec_.size(); ++j) {
            if (fetcher->processor_id == fetchers_vec_[j].processor_id) {
              fetcher->row_num += fetchers_vec_[j].row_num;
              fetcher->stall_time += fetchers_vec_[j].stall_time;
              fetcher->bytes_read += fetchers_vec_[j].bytes_read;
              fetcher->max_allocated_mem += fetchers_vec_[j].max_allocated_mem;
              fetcher->max_allocated_disk += fetchers_vec_[j].max_allocated_disk;
            }
          }
        }
      }
      Return(KStatus::SUCCESS);
    }
};
}  //  namespace kwdbts

#endif  // KWDBTS2_COMMON_INCLUDE_EXPLAIN_ANALYSE_H_
