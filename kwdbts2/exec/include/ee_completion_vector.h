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

#include "ee_global.h"
#include "kwdb_type.h"

namespace kwdbts {

template <typename T>
struct ee_vector_t;

#define IB_VEC_OFFSET(v, i) (vec->sizeof_value_ * i)

typedef int (*ee_compare_t)(const void *, const void *);

#define MY_FIND_ERROR ((k_uint32)-1)

template <typename T>
static k_uint32 ee_unguarded_partition(ee_vector_t<T> *arr, k_uint32 first,
                                       k_uint32 last, k_uint32 pivot,
                                       ee_compare_t comp) {
  while (true) {
    while (comp(&((*arr)[first]), &((*arr)[pivot])) < 0) {
      ++first;
    }

    while (comp(&((*arr)[pivot]), &((*arr)[last])) < 0) {
      --last;
    }
    if (!(first < last)) {
      return first;
    }

    ee_vector_swap(arr, first, last);
    ++first;
  }
}

template <typename T>
static void ee_move_median_to_first(ee_vector_t<T> *arr, k_uint32 first,
                                    k_uint32 first_next, k_uint32 mid,
                                    k_uint32 end, ee_compare_t comp) {
  if (comp(&((*arr)[first_next]), &((*arr)[mid])) < 0) {
    if (comp(&((*arr)[mid]), &((*arr)[end])) < 0) {
      ee_vector_swap(arr, first, mid);
    } else if (comp(&((*arr)[first_next]), &((*arr)[end])) < 0) {
      ee_vector_swap(arr, first, end);
    } else {
      ee_vector_swap(arr, first, first_next);
    }
  } else if (comp(&((*arr)[first_next]), &((*arr)[end])) < 0) {
    ee_vector_swap(arr, first, first_next);
  } else if (comp(&((*arr)[mid]), &((*arr)[end])) < 0) {
    ee_vector_swap(arr, first, end);
  } else {
    ee_vector_swap(arr, first, mid);
  }
}

template <typename T>
void ee_select(ee_vector_t<T> *arr, k_uint32 left, k_uint32 right, k_uint32 k,
               ee_compare_t comp) {
  if (left >= right) {
    return;
  }
  k_uint32 i = left;
  k_uint32 j = right;
  T pivot = (*arr)[left];

  while (true) {
    do {
      i = i + 1;
      if (i > right) {
        break;
      }
    } while (comp(&((*arr)[i]), &pivot) < 0);
    // find items that less or equal to pivot on the right side
    do {
      j = j - 1;
      if (j < left) {
        break;
      }
    } while (comp(&((*arr)[j]), &pivot) > 0);
    if (i >= j) {
      break;  // jump out of the loop when no item to swap
    }
    ee_vector_swap(arr, i, j);
  }
  // nleft = j-left
  if (j - left + 1 == k) {
    return;
  }

  (*arr)[left] = (*arr)[j];  // swap pivot and a[j]
  (*arr)[j] = pivot;        // left & right subset based on j
  if (j - left + 1 < k)
    return ee_select(arr, j + 1, right, k - j + left - 1, comp);  // find in the right subset
  else
    return ee_select(arr, left, j - 1, k, comp);  // find in the left subset
}

/**
 * @brief
 *                            create ee_vector_t
 * @param vec                 ee_vector_t
 * @param sizeof_value        value length
 * @param size                initial num_
 *
 * @return true               Error
 * @return false              Success
 */
template <typename T>
extern bool ee_vector_create(ee_vector_t<T> *vec, k_uint32 size) {
  vec->used_ = 0;
  vec->total_ = size;

  vec->data_ = static_cast<T *>(malloc(sizeof(T) * size));
  if (nullptr == vec->data_) {
    return true;
  }

  return false;
}

/**
 * @brief
 *                free ee_vector_t
 * @param vec     ee_vector_t
 */
template <typename T>
extern void ee_vector_free(ee_vector_t<T> *vec) {
  if (nullptr != vec->data_) {
    SafeFreePointer(vec->data_);
    vec->used_ = 0;
    vec->total_ = 0;
  }
}

void *ee_my_malloc(k_uint32 size) {
  return malloc(size);
}

void ee_my_free(void *ptr) {
  free(ptr);
}

/**
 * @brief
 *                resize the vector
 * @param vec     ee_vector_t
 *
 * @return true   Error
 * @return false  Success
 */
template <typename T>
extern bool ee_vector_resize(ee_vector_t<T> *vec) {
  k_uint32 new_total = vec->total_ * 2;
  k_uint32 old_size = vec->used_ * sizeof(T);
  k_uint32 new_size = new_total * sizeof(T);

  if (0 == new_size) {
    return false;
  }

  T *new_data = static_cast<T *>(ee_my_malloc(new_size));
  if (nullptr == new_data) {
    return true;
  }
  memset(new_data, 0, new_size);
  memcpy(new_data, vec->data_, old_size);
  ee_my_free(vec->data_);
  vec->data_ = new_data;
  vec->total_ = new_total;

  return false;
}

/**
 * @brief
 *              Get the number of elements in the vector.
 * @param vec   ee_vector_t
 *
 * @return k_uint32   number of elements in vector
 */
template <typename T>
extern k_uint32 ee_vector_size(const ee_vector_t<T> *vec) {
  return (vec->used_);
}

/**
 * @brief
 *                  Test whether a vector is empty or not.
 * @param vec       ee_vector_t
 *
 * @return true     if empty
 * @return false    not empty
 */
template <typename T>
extern bool ee_vector_is_empty(const ee_vector_t<T> *vec) {
  return (0 == vec->used_);
}

/**
 * @brief
 *                copy data into vector back
 * @param vec     ee_vector_t
 * @param elem    elem
 *
 * @return true     Error
 * @return false    Success
 */
template <typename T>
extern bool ee_vector_push(ee_vector_t<T> *vec, T elem) {
  if (vec->used_ >= vec->total_) {
    if (true == ee_vector_resize(vec)) {
      return true;
    }
  }

  vec->data_[vec->used_] = elem;

  ++vec->used_;

  return false;
}

/**
 * @brief
 *                  Set the n'th element.
 * @param vec       ee_vector_t
 * @param n         element index to set
 * @param elem      elem
 *
 * @return true     Error
 * @return false    Success
 */
template <typename T>
extern bool ee_vector_set(ee_vector_t<T> *vec, k_uint32 n, T elem) {
  if (n >= vec->used_) {
    return true;
  }

  vec->data_[n] = elem;

  return false;
}

/**
 * @brief
 *                Pop the last element from the vector
 * @param vec     ee_vector_t
 * @return void*  The last value
 */
template <typename T>
extern T ee_vector_pop(ee_vector_t<T> *vec) {
  T elem = ee_vector_last(vec);
  --vec->used_;

  return elem;
}

/**
 * @brief
 *                Get the last element of the vector.
 * @param vec     ee_vector_t
 *
 * @return void*  pointer to last element
 */
template <typename T>
extern T ee_vector_last(ee_vector_t<T> *vec) {
  return (ee_vector_get(vec, vec->used_ - 1));
}

/**
 * @brief
 *                  Get the n'th element.
 * @param vec       ee_vector_t
 * @param n         element index to get
 *
 * @return T        n'th element
 */
template <typename T>
extern T ee_vector_get(ee_vector_t<T> *vec, k_uint32 n) {
  return vec->data_[n];
}

/**
 * @brief
 *                  Remove an element to the vector
 * @param vec       ee_vector_t
 * @param elem      elem to remove
 *
 * @return true     Error
 * @return false    Success
 */
template <typename T>
extern bool ee_vector_remove(ee_vector_t<T> *vec, T elem) {
  for (k_uint32 i = 0; i < vec->used_; ++i) {
    T current = ee_vector_get(vec, i);

    if (current == elem) {
      if (i == vec->used_ - 1) {  // at the end
        return (ee_vector_pop(vec));
      }

      memmove(vec->data_ + i * sizeof(T), vec->data_ + (i + 1) * sizeof(T),
              sizeof(T) * (vec->used_ - i - 1));
      --vec->used_;
      return false;
    }
  }

  return true;
}

/**
 * @brief
 *                Swap vector data
 * @param vec1
 * @param vec2
 */
template <typename T>
extern void ee_vector_swap(ee_vector_t<T> *vec1, ee_vector_t<T> *vec2) {
  T *data2 = vec2->data_;
  k_uint32 used = vec2->used_;
  k_uint32 total = vec2->total_;

  vec2->data_ = vec1->data_;
  vec2->used_ = vec1->used_;
  vec2->total_ = vec1->total_;

  vec1->data_ = data2;
  vec1->used_ = used;
  vec1->total_ = total;
}

template <typename T>
extern void ee_vector_swap(ee_vector_t<T> *vec1, k_uint32 l, k_uint32 r) {
  T tmp = ee_vector_get(vec1, l);
  (*vec1)[l] = (*vec1)[r];
  (*vec1)[r] = tmp;
}

/**
 * @brief
 *                  Sort a sequence just enough to find a particular position
 * using a predicate for comparison.
 * @param vec1      ee_vector_t
 * @param begin     ee_vector_t begin position
 * @param nth       position
 * @param end       ee_vector_t end position
 * @param compare   comp func
 *
 */
template <typename T>
void ee_vector_nth_element(ee_vector_t<T> *vec1, k_uint32 begin, k_uint32 nth,
                           k_uint32 end, ee_compare_t compare) {
  while (end + 1 - begin > 3) {
    k_uint32 mid = begin + (end + 1 - begin) / 2;
    ee_move_median_to_first(vec1, begin, begin + 1, mid, end, compare);
    k_uint32 cut = ee_unguarded_partition(vec1, begin + 1, end, begin, compare);
    if (cut <= nth) {
      begin = cut;
    } else {
      end = cut;
    }
  }

  for (k_uint32 i = begin + 1; i <= end; ++i) {
    T key = (*vec1)[i];
    k_uint32 j = i - 1;
    while (j >= begin && compare(&key, &(*vec1)[j]) < 0) {
        (*vec1)[j + 1] = (*vec1)[j];
        --j;
    }

    (*vec1)[j + 1] = key;
  }
}

/**
 * @brief
 *                        Sort the vector elements.
 * @param vec
 * @param num
 * @param compare
 */
template <typename T>
void ee_vector_sort(ee_vector_t<T> *vec, k_uint32 num, ee_compare_t compare) {
  qsort(vec->data_, num, sizeof(T), compare);
}

template <typename T>
struct ee_vector_t {
  T *data_{nullptr};  /* data elements */
  k_uint32 used_{0};  /* number of elements currently used */
  k_uint32 total_{0}; /* number of elements allocated */

  T &operator[](k_uint32 i) { return data_[i]; }
};

}  // namespace kwdbts
