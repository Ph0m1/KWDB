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
#include "ee_exec_pool.h"

#include "ee_kwthd.h"
#include "ee_task.h"
#include "ee_timer_event.h"
#include "er_api.h"
#include "lg_api.h"
#include "th_kwdb_dynamic_thread_pool.h"
#include "kwdb_type.h"
#include "cm_fault_injection.h"
#include "ee_cpuinfo.h"

namespace kwdbts {

#define EE_MIN_TOPIC_WINDOW_OFFSET 500
#define EE_START_THREAD_TIMEOUT 10000

ExecPool::ExecPool(k_uint32 tq_num, k_uint32 tp_size) {
  tq_num_ = tq_num;
  tp_size_ = tp_size;
  is_tp_stop_ = true;
  thread_ids_ = nullptr;
}

KStatus ExecPool::Init(kwdbContext_p ctx) {
  EnterFunc();
  if (EE_ENABLE_PARALLEL > 0) {
    CpuInfo::Init();
    k_int32 cpu_cores = CpuInfo::Get_Num_Cores();
    if (cpu_cores > 0) {
      //  tp_size_ = cpu_cores;
    }
  }

  thread_ids_ = static_cast<KThreadID *>(malloc(sizeof(KThreadID) * tp_size_));
  if (!thread_ids_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  memset(thread_ids_, 0, sizeof(KThreadID) * tp_size_);
  /*
  timer_event_pool_ = KNEW TimerEventPool(EE_TIMER_EVENT_POOL_SIZE);
  if (timer_event_pool_ == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  if (KStatus::SUCCESS != timer_event_pool_->Init()) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  */
  is_tp_stop_ = false;
  for (int i = 0; i < tp_size_; i++) {
    KWDBOperatorInfo kwdb_operator_info;
    // add attribute for KWDBOperatorInfo
    kwdb_operator_info.SetOperatorName("Executor");
    kwdb_operator_info.SetOperatorOwner("Executor");
    time_t now;
    kwdb_operator_info.SetOperatorStartTime((k_uint64)time(&now));
    // succeed :thread ID, failed:0
    KThreadID tid = KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
        std::bind(&ExecPool::Routine, this, std::placeholders::_1), this,
        &kwdb_operator_info);
    INJECT_DATA_FAULT(FAULT_EE_EXECUTOR_APPLY_THREAD_MSG_FAIL, tid, 0, nullptr);
    if (tid < 1) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Init execution thread pool");
      Return(KStatus::FAIL);
    }
    thread_ids_[i] = tid;
  }
  wait_thread_num_ = tp_size_;
  k_uint32 wait_time = 0;
  while (start_tp_size_ < tp_size_) {
    if (wait_time > EE_START_THREAD_TIMEOUT) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "execution thread start timeout");
      Return(FAIL);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    wait_time = wait_time + 10;
  }
  // timer_event_pool_->Start();
  Return(KStatus::SUCCESS);
}

void ExecPool::Stop() {
  is_tp_stop_ = true;
  wait_cond_.notify_all();
  not_fill_cv_.notify_all();
  // if (timer_event_pool_) {
  //   timer_event_pool_->Stop();
  //   delete timer_event_pool_;
  //   timer_event_pool_ = nullptr;
  // }
  if (thread_ids_) {
    for (k_uint32 i = 0; i < tp_size_; ++i) {
      if (thread_ids_[i] > 0) {
        KWDBDynamicThreadPool::GetThreadPool().JoinThread(thread_ids_[i], 0);
      }
    }
    free(thread_ids_);
    thread_ids_ = nullptr;
  }

  task_queue_.clear();
}

ExecPool::~ExecPool() {
  if (!is_tp_stop_) {
    Stop();
  }
}
/*
 * execute tasks in the task queue
 */
void ExecPool::Routine(void *arg) {
  auto ctx = ContextManager::GetThreadContext();
  start_tp_size_.fetch_add(1);
  std::unique_lock l(lock_);
  while (true) {
    if (KWDBDynamicThreadPool::GetThreadPool().IsCancel() || is_tp_stop_) {
      break;
    }
    // task_queue_ null
    if (task_queue_.empty()) {
      wait_cond_.wait_for(l, std::chrono::seconds(2));
      continue;
    }

    // get task
    ExecTaskPtr task_ptr = task_queue_.front();
    task_queue_.pop_front();
    not_fill_cv_.notify_one();
    if (task_ptr) {
      wait_thread_num_--;
      l.unlock();
      // try {
        task_ptr->SetState(ExecTaskState::EXEC_TASK_STATE_RUNNING);
        // execute task
        task_ptr->Run(ctx);
        task_ptr->SetState(ExecTaskState::EXEC_TASK_STATE_IDLE);
      // } catch (...) {
      //   constexpr char ERROR_MESSAGE[128] =
      //       "some unknown exception was thrown in execution of the thread pool!";
      //   PUSH_ERR_1(IN_EXEC_ERR, ERROR_MESSAGE);
      //   LOG_ERROR(ERROR_MESSAGE);
      // }
      l.lock();
      wait_thread_num_++;
    }
  }
}

KStatus ExecPool::PushTask(ExecTaskPtr task_ptr) {
  if (ExecTaskState::EXEC_TASK_STATE_IDLE != task_ptr->GetState()) {
    return KStatus::FAIL;
  }
  std::unique_lock unique_lock(lock_);
  not_fill_cv_.wait(unique_lock, [this]() -> bool {
    return ((task_queue_.size() < tq_num_) || is_tp_stop_);
  });
  if (is_tp_stop_) {
    return KStatus::FAIL;
  }
  // insert task queue
  task_queue_.push_back(task_ptr);
  task_ptr->SetState(ExecTaskState::EXEC_TASK_STATE_WAITING);
  // notify
  wait_cond_.notify_one();
  return KStatus::SUCCESS;
}

k_bool ExecPool::IsFull() { return task_queue_.size() >= tq_num_; }

void ExecPool::PushTimeEvent(TimerEventPtr event_ptr) {
  return timer_event_pool_->PushTimeEvent(event_ptr);
}
k_uint32 ExecPool::GetWaitThreadNum() const {
  return wait_thread_num_;
}

}  // namespace kwdbts
