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
#pragma once

#include <deque>
#include <memory>
#include <string>
#include <condition_variable>
#include <mutex>

#include "cm_assert.h"
#include "ee_global.h"
#include "ee_task.h"

#define EE_TIMER_EVENT_POOL_SIZE 10240
#define EE_ENABLE_PARALLEL 1

namespace kwdbts {
class TimerEventPool;
/**
 * @brief the scheduling class
 */
class ExecPool {
 private:
  /**
   * @brief time_event_pool_ timer
   *
   */
  TimerEventPool *timer_event_pool_{nullptr};

  typedef std::deque<ExecTaskPtr> TaskQueue;
  /**
   * @brief task_queue_ task queue
   */
  TaskQueue task_queue_;
  /**
   * @brief tq_num_ queue length limit
   */
  k_uint32 tq_num_;

  /**
   * @brief Number of threads executed
   */
  k_uint32 tp_size_;
  /**
   * @brief Concurrency lock, which controls the concurrency of thread pool
   * tasks
   */
  mutable std::mutex lock_;
  /**
   * @brief Condition variable, which needs to wait after the queue reaches the
   * upper limit
   *
   */
  std::condition_variable not_fill_cv_;
  /*
   * @brief Conditional variables, when the task queue is empty waiting, when
   * there is a task to send a signal
   */
  std::condition_variable wait_cond_;
  /*
   * @brief Stop Thread Pool
   */
  bool is_tp_stop_;

  /**
   * @brief The ID of the thread 
   * 
   */
  KThreadID *thread_ids_;

  /**
   * @brief Number of idle threads
   *
   */
  k_uint32 wait_thread_num_;

  std::atomic<k_uint32> start_tp_size_{0};

 public:
  /**
   * @brief Constructor
   * @param[in]  tq_num         the max length of the tq
   * @param[in]  tp_size        the thread num
   * @return
   */
  ExecPool(k_uint32 tq_num, k_uint32 tp_size);

  /**
   * @brief Destructors
   */
  ~ExecPool();

  ExecPool(const ExecPool &) = delete;
  ExecPool& operator=(const ExecPool &) = delete;

  /**
   * @brief add task
   *
   * @param task_ptr
   */
  KStatus PushTask(ExecTaskPtr task_ptr);

  /**
  * @brief add timed tasks
  *
  * @param event_ptr
  */
  void PushTimeEvent(TimerEventPtr event_ptr);

  /**
   * @brief Threads are scheduled to execute tasks
   */
  void Routine(void *);

  /**
   * @brief gets the number of idle threads
   */
  k_uint32 GetWaitThreadNum() const;

  KStatus Init(kwdbContext_p ctx);
  void Stop();
  static ExecPool &GetInstance(k_uint32 tq_num = 1024, k_uint32 tp_size = 10) {
    static ExecPool instance(tq_num, tp_size);
    return instance;
  }
  k_bool IsFull();
  std::string db_path_;
};  // ExecPool
};  // namespace kwdbts
