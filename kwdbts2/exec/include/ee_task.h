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

#include <memory>
#include <string>

#include "cm_assert.h"
#include "ee_timer_event.h"

namespace kwdbts {

/**
 * @brief Task Type
 *
 */
enum ExecTaskType {
  EXEC_TASK_TYPE_NORMAL = 0,  // general query
  EXEC_TASK_TYPE_TOPIC,       // Stream computing
  EXEC_TASK_TYPE_PRE          // precomputing
};

enum ExecTaskState {
  EXEC_TASK_STATE_IDLE,
  EXEC_TASK_STATE_WAITING,
  EXEC_TASK_STATE_RUNNING
};

/**
 * @brief ExecTask task
 */
class ExecTask {
 public:
  ~ExecTask() {}

  ExecTask(const ExecTask &) = delete;
  ExecTask& operator=(const ExecTask &) = delete;
  ExecTask(ExecTask &&) = delete;
  ExecTask& operator=(ExecTask &&) = delete;

  /**
   * @brief op interface
   */
  virtual void Run(kwdbContext_p ctx) {}

  /**
   * @brief Construct a new ExecTask object
   *
   */
  ExecTask() : task_type_(EXEC_TASK_TYPE_NORMAL), state_(EXEC_TASK_STATE_IDLE) {}
  /**
   * @brief Set the TaskType
   *
   * @param task_type
   */
  void SetTaskType(ExecTaskType task_type) { task_type_ = task_type; }

  /**
   * @brief Get the TaskType
   *
   * @return ExecTaskType
   */
  ExecTaskType GetTaskType() { return task_type_; }

  void SetState(ExecTaskState state) { state_ = state; }
  ExecTaskState GetState() { return state_; }

 private:
  ExecTaskType task_type_;
  ExecTaskState state_;
};

typedef std::shared_ptr<ExecTask> ExecTaskPtr;
};  // namespace kwdbts
