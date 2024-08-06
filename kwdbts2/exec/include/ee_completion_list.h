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

#include <assert.h>
#include "kwdb_type.h"
#include "ee_global.h"

namespace kwdbts {

template <typename T>
struct ee_list_t;
template <typename T>
struct ee_list_node_t;
/**
 * @brief
 *              Create a new list using mem_alloc
 *
 * @return ee_list_t*
 */
template <typename T>
extern ee_list_t<T> *ee_list_create() {
  void *list = malloc(sizeof(ee_list_t<T>));
  memset(list, 0, sizeof(ee_list_t<T>));

  return static_cast<ee_list_t<T> *>(list);
}

/**
 * @brief
 *              Free ee_list_t
 * @param list
 */
template <typename T>
extern void ee_list_free(ee_list_t<T> *list) {
  ee_list_node_t<T> *node = list->first_;
  while (nullptr != node) {
    ee_list_node_t<T> *temp = node;
    node = node->next_;
    SafeFreePointer(temp);
  }

  free(list);
}

/**
 * @brief
 *                      Add the data to the end of the list.
 * @param list
 * @param data
 *
 * @return ee_list_node_t*  new list node
 */
template <typename T>
extern ee_list_node_t<T> *ee_list_add_last(ee_list_t<T> *list, T data) {
  return (ee_list_add_after(list, data, ee_list_get_last(list)));
}

/**
 * @brief
 *                            Add the data after the indicated prev_node.
 * @param list
 * @param data
 * @param prev_node
 *
 * @return ee_list_node_t*    new list node
 */
template <typename T>
extern ee_list_node_t<T> *ee_list_add_after(ee_list_t<T> *list, T data, ee_list_node_t<T> *prev_node) {
  ee_list_node_t<T> *node = static_cast<ee_list_node_t<T> *>(malloc(sizeof(ee_list_node_t<T>)));
  if (nullptr == node) {
    return node;
  }

  node->data_ = data;

  if (nullptr == list->first_) {
    /* Empty list. */
    assert(nullptr == prev_node);

    node->prev_ = nullptr;
    node->next_ = nullptr;

    list->first_ = node;
    list->last_ = node;
  } else if (nullptr == prev_node) {
    /* Start of list. */
    node->prev_ = nullptr;
    node->next_ = list->first_;

    list->first_->prev_ = node;
    list->first_ = node;
  } else {
    /* Middle or end of list. */
    node->prev_ = prev_node;
    node->next_ = prev_node->next_;

    prev_node->next_ = node;

    if (node->next_) {
      node->next_->prev_ = node;
    } else {
      list->last_ = node;
    }
  }

  ++list->total_;

  return (node);
}

/**
 * @brief
 *                            Add the data before the indicated next_node.
 * @param list
 * @param data
 * @param node
 *
 * @return ee_list_node_t*    new list node
 */
template <typename T>
extern ee_list_node_t<T> *ee_list_add_before(ee_list_t<T> *list, T data, ee_list_node_t<T> *next_node) {
  ee_list_node_t<T> *node = static_cast<ee_list_node_t<T> *>(malloc(sizeof(ee_list_node_t<T>)));
  if (nullptr == node) {
    return node;
  }

  node->data_ = data;

  if (nullptr == list->first_) {
    /* Empty list. */
    assert(nullptr == next_node);

    node->prev_ = nullptr;
    node->next_ = nullptr;

    list->first_ = node;
    list->last_ = node;
  } else if (nullptr == next_node || list->first_ == next_node) {
    /* Start of list. */
    node->prev_ = nullptr;
    node->next_ = list->first_;

    list->first_->prev_ = node;
    list->first_ = node;
  } else {
    /* Middle */
    node->next_ = next_node;
    node->prev_ = next_node->prev_;
    next_node->prev_->next_ = node;
    next_node->prev_ = node;
  }

  ++list->total_;

  return (node);
}

/**
 * @brief
 *                  Remove the node from the list.
 * @param list
 * @param node
 */
template <typename T>
extern void ee_list_remove(ee_list_t<T> *list, ee_list_node_t<T> *node) {
  if (node->prev_) {
    node->prev_->next_ = node->next_;
  } else {
    /* First item in list. */
    assert(list->first_ == node);
    list->first_ = node->next_;
  }

  if (node->next_) {
    node->next_->prev_ = node->prev_;
  } else {
    /* Last item in list. */
    assert(list->last_ == node);

    list->last_ = node->prev_;
  }

  node->prev_ = node->next_ = nullptr;

  --list->total_;
}

/**
 * @brief
 *                  Get the first node in the list.
 * @param list
 *
 * @return ee_list_node_t*
 */
template <typename T>
extern ee_list_node_t<T> *ee_list_get_first(ee_list_t<T> *list) {
  return (list->first_);
}

/**
 * @brief
 *                          Get the last node in the list.
 * @param list
 *
 * @return ib_list_node_t*
 */
template <typename T>
extern ee_list_node_t<T> *ee_list_get_last(ee_list_t<T> *list) {
  return (list->last_);
}

/**
 * @brief
 *                    Get the number of elements in the list
 * @param list
 *
 * @return k_uint32
 */
template <typename T>
extern k_uint32 ee_list_get_size(ee_list_t<T> *list) {
  return (list->total_);
}

/**
 * @brief 
 * 
 * @param list 
 * 
 * @return true 
 * @return false 
 */
template <typename T>
extern bool ee_list_is_empty(const ee_list_t<T> *list) {
  return list->total_ == 0;
}

template <typename T>
struct ee_list_node_t {
  T data_;
  ee_list_node_t *prev_{nullptr};
  ee_list_node_t *next_{nullptr};
};

template <typename T>
struct ee_list_t {
  ee_list_node_t<T> *first_{nullptr};
  ee_list_node_t<T> *last_{nullptr};
  k_uint32 total_{0};
};

}  // namespace kwdbts
