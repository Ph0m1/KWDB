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

/*
#include <cstdio>
#include <fstream>
#include <sys/mman.h>
#include "test_util.h"
#include "mmap/MMapHashIndex.h"
#include "mmap/MMapBigHash.h"
#include <chrono>
#include <gtest/gtest.h>

#include "BigObjectApplication.h"
#include "BigObjectUtils.h"


const string TestBigTableInstance::kw_home = "./data_kw/";
const string TestBigTableInstance::db_name = "testdb"; // database name
const uint64_t TestBigTableInstance::iot_interval = 3600;

class MMapHashIndexTest : public TestBigTableInstance {
 public:
  MMapHashIndexTest() {
    db_name_ = TestBigTableInstance::db_name;
    db_path_ = TestBigTableInstance::kw_home;
  }

  std::string db_name_;
  std::string db_path_;

 protected:
  static void setUpTestCase() {}

  static void tearDownTestCase() {}

};
long cnt = 100;

TEST(MMapHashIndexTest, TestMMapNewHashOpen) {
  ErrorInfo err_info;
  // init
  std::string db_home = "./db_home_1/";
  setenv("KW_HOME", db_home.c_str(), 1);
  int ret = initBigObjectApplication(err_info);
  EXPECT_GE(ret, 0);
  EXPECT_EQ(err_info.errcode, 0);
  // create database dir
  std::string db_name = "test_db1";
  string db_path = normalizePath(db_name);
  string ws = worksapceToDatabase(db_path);
  if (ws.empty())
    err_info.setError(BOEINVALIDNAME, db_name);
  int err_code = IsDbNameValid(ws);
  if (err_code != 0)
    err_info.setError(err_code, db_name);
  string dir_path = makeDirectoryPath(BigObjectConfig::home() + ws);
  ret = MakeDirectory(dir_path);
  if (ret < 0) {
    err_info.setError(BOENOFUNC, "Cannot create workspace.: " + db_name);
  } else if (ret > 0) {  // directory exists
    err_info.setError(BOEEXIST, "directory: " + db_name);
  }
  EXPECT_EQ(err_info.errcode, 0);
  // create table
  Def_Column(col_1, "k_timestamp", DATATYPE::TIMESTAMP64, 0, 0, 1, 0, AINFO_NOT_NULL, 3, -1,1);
  Def_Column(col_2, "c1", DATATYPE::INT64, 0, 0, 1, 0, 0, 0, -1,1);
  Def_Column(col_3, "c2", DATATYPE::DOUBLE, 0, 0, 1, 0, 0, 0, -1,1);
  vector<AttributeInfo> schema = {std::move(col_1), std::move(col_2), std::move(col_3)};
  vector<string> key = {"k_timestamp"};
  string key_order;
  string ns_url = "default";
  BigTable *bt = createBigTable("t1", schema, key, key_order,
                                   ns_url, "", db_name,
                                   BO_CREAT_EXCL, ROW_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);
  bt->reserve(cnt);
  std::cout << "name: " << bt->name() << std::endl;

  // MMapBigTable *mbt = dynamic_cast<MMapBigTable *>(bt);
  // std::cout << mbt->hrchy_index_ << std::endl;

  int rec_size = bt->recordSize();
  std::cout << "rec_size: " << rec_size << std::endl;

  // record helper
  DataHelper *rec_helper = bt->getRecordHelper();

  ///////////////////// Create Index ///////////////////
  // MMapBucketHash *hash = new MMapBucketHash();
  //MMapBigHash *hash = new MMapBigHash();
  MMapHashIndex *hash = new MMapHashIndex(1,16);

  string hash_url = "test.ht";
  std::cout << "hash_url: " << hash_url << std::endl;

  if (hash->open(hash_url, bt->tbl_sub_path(), BO_CREATTRUNC | BO_CREATOPEN,
                 err_info) < 0) {
    std::cout << "open error" << std::endl;
  }
  hash->reserve(cnt*2);
  hash->init(8, bt);
  vector<int> key_idx = {1};
  vector<AttributeInfo> key_info = {schema[1]};
  auto key_len = setAttributeInfo(key_info, bt->encoding());
  std::cout << "key_len: " << key_len << std::endl;

  // hash->setKey(bt, key_len, key_idx, key_info, bt->getSchemaInfo());

  // char *rec = new char[rec_size];

  auto start = std::chrono::high_resolution_clock::now();
  ///////////////////// Insert ///////////////////
  MMapHashIndex::HASHTYPE h_val;
  for (long i = 1; i <= cnt; i++) {
    // insert table
    bt->setColumnValue(i, 0, "1687522675");
    bt->setColumnValue(i, 1, std::to_string(i));
    bt->setColumnValue(i, 2, std::to_string(i));
    // insert index
    // hash->put((const char *)(&i), key_len, i);
    h_val.bt_row_ = i;
    hash->put((const char *)(&i), key_len, h_val);
//    size_t hash_loc;
//    hash->getNextEmptySlot((const char *)(&i), key_len, hash_loc);
//    hash->set(hash_loc, (HASHTYPE) i);
//    //printf("hash_loc: %lu val: %d\n", hash_loc, i);
//
//    if ((double)hash->metaData().load / (double)hash->metaData().size > 0.7) {
//      hash->reserve(hash->metaData().size * 2);
//    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto ins_dur =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  std::cout << "insert duration=" << ins_dur << std::endl;

  // delete[] rec;

  start = std::chrono::high_resolution_clock::now();
  ///////////////////// Query ///////////////////
  for (long idx = 1; idx <= cnt; idx++) {
    // snprintf(rec, 10, "%0*ld", 8, idx);
    // HASHTYPE h_val = hash->get((const char *)(&idx), key_len);
    auto ret = hash->get((const char *)(&idx), key_len, h_val);
    if(!ret) {
      printf("=======Error: Not Found======= %ld \n", idx);
    }
//    size_t hash_loc;
//    HASHTYPE h_val = hash->get((const char *)(&idx), key_len, hash_loc);
//    // printf("h_val: %llu, hash_loc: %llu\n", h_val, hash_loc);
//    if (h_val == 0) {
//      printf("=======Error: Not Found======= \n");
//    }
  }

  end = std::chrono::high_resolution_clock::now();
  auto get_dur =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  std::cout << "get duration=" << get_dur << std::endl;
  
  // drop table
  releaseObject(bt);
  // remove db home dir
  ret = RemoveDirectory(db_home.c_str());
  EXPECT_NE(ret, -1);
}

//int main() {
//  // testing::InitGoogleTest();
//  return RUN_ALL_TESTS();
//}
*/
