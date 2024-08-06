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

#include <chrono>
#include <unordered_map>
#include "ee_completion_hashtable.h"
#include "gtest/gtest.h"

namespace kwdbts {

#define SIZE 200000
#define HASH_BUCKET_SIZE 200000

class TestHashTable : public ::testing::Test {
 protected:
  virtual void SetUp() {}

  virtual void TearDown() {}

  HashTable<char *> hashtable_;
  std::unordered_map<k_uint64, char *> map_slot_;
};

TEST_F(TestHashTable, hashtable) {
  char key[10] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
  char value[10][10] = { {"hello1"}, {"hello2"}, {"hello3"}, {"hello4"}, {"hello5"},
                         {"hello6"}, {"hello7"}, {"hello8"}, {"hello9"}, {"hello10"} };
  // init hash table
  EXPECT_EQ(completion_hash_init(&hashtable_, 4), KStatus::SUCCESS);
  // add to hash table
  for (k_uint32 i = 0; i < 10; ++i) {
    EXPECT_EQ(completion_hash_add(&hashtable_, key[i], value[i]), KStatus::SUCCESS);
  }
  // determine if it exists
  for (k_uint32 i = 0; i < 10; ++i) {
    EXPECT_TRUE(completion_hash_exists(&hashtable_, key[i]));
  }

  // read value
  for (k_uint32 i = 0; i < 10; ++i) {
    Bucket<char *> *bucket = completion_hash_find_bucket(&hashtable_, key[i]);
    EXPECT_TRUE(bucket != nullptr);
    EXPECT_STREQ(bucket->enrty_->node_, value[i]);
  }
  completion_hash_free(&hashtable_);
}

// The multiple of the HashTable that is faster than the unordered_map in the
// same data depends on the number of buchets
TEST_F(TestHashTable, compare) {
  struct Entry2 {
    ~Entry2() {
      SafeDeletePointer(next_);
    }
    char *data_{nullptr};
    struct Entry2 *next_{nullptr};
  };
  KStatus ret = KStatus::FAIL;
  k_int32 key[SIZE];
  memset(key, 0, SIZE * 4);
  srand((unsigned)time(NULL));
  for (k_uint32 i = 0; i < SIZE; ++i) {
    key[i] = i;
  }

  Entry2 **entrys = static_cast<Entry2 **>(malloc(SIZE *sizeof(Entry2 *)));

  char value[SIZE][20];
  memset(value, 0, SIZE * 20);
  for (k_uint32 i = 0; i < SIZE; ++i) {
    char buf[20];
    memset(key, 0, 20);
    memcpy(buf, "hello", strlen("hello"));
    memcpy(buf + strlen("hello"), &key[i], sizeof(k_int32));
    memcpy(value[i], buf, strlen(buf));

    Entry2 *entry = new Entry2;
    entry->data_ = value[i];
    entrys[i] = entry;
  }

  HashTable<Entry2 *> hashtable2_;
  // init hash table
  EXPECT_EQ(completion_hash_init(&hashtable_, HASH_BUCKET_SIZE), KStatus::SUCCESS);
  EXPECT_EQ(completion_hash_init(&hashtable2_, HASH_BUCKET_SIZE), KStatus::SUCCESS);
  std::cout << "hash table bucket count " << HASH_BUCKET_SIZE << std::endl;
  // add to hash table
  auto start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < SIZE; ++i) {
    completion_hash_add(&hashtable_, key[i], value[i]);
  }
  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double> diff1 = end - start;

  start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < SIZE; ++i) {
    map_slot_.insert(std::pair(key[i], value[i]));
  }
  end = std::chrono::steady_clock::now();
  std::chrono::duration<double> diff2 = end - start;

  start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < SIZE; ++i) {
    HASH_INSERT(Entry2*, (&hashtable2_), key[i], entrys[i], 1, ret);
  }
  end = std::chrono::steady_clock::now();
  std::chrono::duration<double> diff3 = end - start;

  std::cout << "unordered_map bucket count " << map_slot_.bucket_count() <<
              " max buchet count " << map_slot_.max_bucket_count() << std::endl;
  std::cout << "hashtable add " << SIZE << " data take: " << diff1.count() << "s" << std::endl;
  std::cout << "unordered_map add  " << SIZE << "  data take: " << diff2.count() << "s" << std::endl;
  std::cout << "hashtable2 add " << SIZE << " data take: " << diff3.count() << "s" << std::endl;



  // determine if it exists
  start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < SIZE; ++i) {
    completion_hash_exists(&hashtable_, key[i]);
  }
  end = std::chrono::steady_clock::now();
  diff1 = end - start;

  start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < SIZE; ++i) {
    map_slot_.find(key[i]);
  }
  end = std::chrono::steady_clock::now();
  diff2 = end - start;

  bool exist = 0;
  start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < SIZE; ++i) {
    HASH_FIND_KEY_EXISTS(Entry2*, (&hashtable2_), key[i], exist);
  }
  end = std::chrono::steady_clock::now();
  diff3 = end - start;

  std::cout << "hashtable find " << SIZE << " data take: " << diff1.count() << "s" << std::endl;
  std::cout << "unordered_map find " << SIZE << " data take: " << diff2.count() << "s" << std::endl;
  std::cout << "hashtable2 find " << SIZE << " data take: " << diff3.count() << "s" << std::endl;

  completion_hash_free(&hashtable_);
  HASH_TABLE_FREE((&hashtable2_), Entry2*, true);
  SafeFreePointer(entrys);
}

}  // namespace kwdbts
