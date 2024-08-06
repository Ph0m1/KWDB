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

#include <mutex>
#include <condition_variable>

#include "ee_completion_list.h"

namespace kwdbts {

template<typename T> struct ee_wqueue_t;

/**
 * @brief 
 *                Create a new work queue.
 * @return ib_wqueue_t* 
 */
template<typename T>
ee_wqueue_t<T> *ee_wqueue_create() {
  ee_wqueue_t<T> *wq = new ee_wqueue_t<T>();
  if (nullptr == wq) {
    return nullptr;
  }

  wq->items_ = ee_list_create<T>();
  wq->count_ = 0;

  return (wq);
}

/**
 * @brief 
 *              Free a work queue.
 * @param wq 
 */
template<typename T>
void ee_wqueue_free(ee_wqueue_t<T> *wq) {
  ee_list_free(wq->items_);
  delete (wq);
}

/**
 * @brief 
 *                  Add a work item to the queue.
 * @param wq 
 * @param item 
 * @param heap 
 */
template<typename T>
void ee_wqueue_add(ee_wqueue_t<T> *wq, T item) {
  std::unique_lock<std::mutex> lcm(wq->mutex_);
  ee_list_add_last(wq->items_, item);
  wq->event_.notify_one();
}

/**
 * @brief 
 * 
 * @param wq 
 * @return k_uint32 
 */
template<typename T>
k_uint32 ee_wqueue_get_count(ee_wqueue_t<T> *wq) {
  std::unique_lock<std::mutex> lcm(wq->mutex_);
  return wq->items_->total_;
}

/**
 * @brief 
 *                Check if queue is empty.
 * @param wq 
 * 
 * @return true 
 * @return false 
 */
template<typename T>
bool ee_wqueue_is_empty(const ee_wqueue_t<T> *wq) {
  return (ee_list_is_empty(wq->items_));
}

/**
 * @brief 
 *                Wait for a work item to appear in the queue for specified time.
 * @param wq 
 * @param wait 
 * 
 * @return T 
 */
template<typename T>
T ee_wqueue_pop_front(ee_wqueue_t<T> *wq) {
  ee_list_node_t<T> *node = nullptr;

  for (;;) {
    std::unique_lock<std::mutex> lcm(wq->mutex_);

    node = ee_list_get_first(wq->items_);
    if (nullptr != node) {
      ee_list_remove(wq->items_, node);

      break;
    }

    wq->event_.wait(lcm);
  }

  T data = node->data_;
  free(node);
  return data;
}

template<typename T>
struct ee_wqueue_t {
  ee_list_t<T> *items_{nullptr};              /*!< work item list */
  k_uint32 count_{0};                         /*!< total number of work items */
  std::mutex mutex_;                          /*!< mutex protecting everything */
  std::condition_variable event_;             /*!< event we use to signal additions to list */
};

}  // namespace kwdbts
