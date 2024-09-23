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
#if 0
#include <gtest/gtest.h>
#include <random>
#include "test_util.h"
#include "mmap/mmap_tag_column_table.h"
#include "utils/big_table_utils.h"
#include "test_tag_util.h"
#include "payload_generator.h"

const string TestBigTableInstance::kw_home_ = "./data_kw/";  // NOLINT
const string TestBigTableInstance::db_name_ = "testdb/";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestTagBigTable : public TestBigTableInstance {
 public:
  TestTagBigTable() : db_name_(TestBigTableInstance::db_name_), db_path_(TestBigTableInstance::kw_home_) { }

  std::string db_name_;
  std::string db_path_;

 protected:
  static void setUpTestCase() {}

  static void tearDownTestCase() {}
};

TEST_F(TestTagBigTable, TEST_CREATE_DROP) {
  vector<TagInfo> schema = {
      {0, DATATYPE::TIMESTAMP64, 8, 0, 8, PRIMARY_TAG},
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::DOUBLE, 8, 0, 8, GENERAL_TAG}
  };
  ErrorInfo err_info;
  MMapTagColumnTable *bt = CreateTagTable(schema, db_path_, db_name_, 5, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);
//  bt = (MMapTagColumnTable*)getTagBigTable("t1",db_name_, BO_OPEN_NORECURSIVE, err_info);
  EXPECT_EQ(bt->remove(), 0);
//  releaseObject(bt);
  delete bt;
  bt = nullptr;
}

TEST_F(TestTagBigTable, insert) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {0, DATATYPE::TIMESTAMP64, 8, 0, 8, PRIMARY_TAG},
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::DOUBLE, 8, 0, 8, GENERAL_TAG},
      {3, DATATYPE::CHAR, 10, 0, 10, GENERAL_TAG}
  };
  MMapTagColumnTable *bt = CreateTagTable(schema, db_path_, db_name_, 2, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  // Write 10 pieces of data
  int cnt = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  TagBtUtil::InitTagData2(reinterpret_cast<MMapTagColumnTable*>(bt), ts_now, cnt, 100);
  EXPECT_EQ(bt->size(), cnt);
  uint32_t  entity_id, group_id;
  int64_t c1_val;

  bt->getColumnValue(1, 1, reinterpret_cast<void*>(&c1_val));
  EXPECT_EQ(c1_val, 100);

  bt->getColumnValue(cnt, 1, reinterpret_cast<void*>(&c1_val));
  EXPECT_EQ(c1_val, 100 + cnt - 1);

  char c4_val[11] = {0};
  bt->getColumnValue(1, 3, reinterpret_cast<void*>(&c4_val));
  EXPECT_EQ(memcmp(c4_val, "1111111111", 10), 0);

  EXPECT_EQ(bt->getEntityIdGroupId(reinterpret_cast<char*>(bt->record(1)), bt->primaryTagSize(),
                                   entity_id, group_id), 0);
  EXPECT_EQ(entity_id, 0);
  EXPECT_EQ(group_id, 0);

  EXPECT_EQ(bt->getEntityIdGroupId(reinterpret_cast<char*>(bt->record(cnt)), bt->primaryTagSize(),
                                   entity_id, group_id), 0);
  EXPECT_EQ(entity_id, cnt - 1);
  EXPECT_EQ(group_id, cnt - 1);

  EXPECT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;
}


TEST_F(TestTagBigTable, insert_varstring) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {0, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {1, DATATYPE::VARSTRING, 32, 0, 32, PRIMARY_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 32, GENERAL_TAG}
  };
  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 3, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  // Write 10 pieces of data
  int cnt = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  TagBtUtil::InitTagData2(reinterpret_cast<MMapTagColumnTable*>(bt), ts_now, cnt, 100);
  ASSERT_EQ(bt->size(), cnt);
  uint32_t entity_id, group_id;
  char col1_val[32] = {0};

  bt->getColumnValue(1, 1, reinterpret_cast<void*>(col1_val));
  ASSERT_EQ(strcmp(col1_val, std::to_string(ts_now).c_str()), 0);

  memset(col1_val, 0x00, 32);
  bt->getColumnValue(cnt, 1, reinterpret_cast<void*>(col1_val));
  ASSERT_EQ(strcmp(col1_val, std::to_string(ts_now + cnt - 1).c_str()), 0);

  char col4_val[32] = {0};
  bt->getColumnValue(1, 3, reinterpret_cast<void*>(col4_val));
  ASSERT_EQ(strcmp(col4_val, std::to_string(ts_now).c_str()), 0);
  memset(col4_val, 0x00, 32);
  bt->getColumnValue(cnt, 3, reinterpret_cast<void*>(col4_val));
  ASSERT_EQ(strcmp(col4_val, std::to_string(ts_now + cnt - 1).c_str()), 0);

  ASSERT_EQ(bt->getEntityIdGroupId(reinterpret_cast<char*>(bt->record(1)),
                                   bt->primaryTagSize(), entity_id, group_id), 0);
  ASSERT_EQ(entity_id, 0);
  ASSERT_EQ(group_id, 0);

  ASSERT_EQ(bt->getEntityIdGroupId(reinterpret_cast<char*>(bt->record(cnt)),
                                   bt->primaryTagSize(), entity_id, group_id), 0);
  ASSERT_EQ(entity_id, cnt - 1);
  ASSERT_EQ(group_id, cnt - 1);

  ASSERT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;
//  releaseObject(bt);
}


TEST_F(TestTagBigTable, insert_null) {
  /**
   * @brief payload of tag
   * | null bitmap | tag1 | tag2 | tag3(fixed offset) | tag4 | tag3(extend data) |
   */
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {0, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {1, DATATYPE::VARSTRING, 32, 0, 32, PRIMARY_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 32, GENERAL_TAG}
  };
  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 5, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  // time format lambda
  auto clock_fmt = [](uint64_t timestamp_us) -> string {
    std::chrono::system_clock::time_point tp{std::chrono::microseconds(timestamp_us)};
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    struct std::tm* ptm = std::localtime(&tt);
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", ptm);
    return buffer;
  };
  int cnt = 10, pseudo_off = 1000000, pseudo_off_bak = 1000000;
  auto ts_now = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  auto ts_bak = ts_now;
  vector<string> not_null_col4;
  PayloadGenerator pg(schema);
  static const char* loc[] = {"beijing", "shanghai", "guangzhou", "shenzhen", "nanjing", "jinan"};
  static const size_t loc_cnt = sizeof(loc) / sizeof(loc[0]);
  for (int i = 0; i < cnt; ++i) {
    // payload
    uint8_t null_bitmap = 0;
    uint8_t * pl = nullptr;
    if (i % 2) {
      set_null_bitmap(&null_bitmap, 2);
      set_null_bitmap(&null_bitmap, 3);
      pl = pg.Construct(&null_bitmap,
                        ts_now,
                        clock_fmt(ts_now),
                        nullptr,
                        nullptr);
    } else {
      const char * pos = loc[random_device()() % loc_cnt];
      pl = pg.Construct(nullptr,
                        ts_now,
                        clock_fmt(ts_now),
                        i,
                        pos);
      not_null_col4.emplace_back(pos);
    }
    // insert
    bt->insert(i, i % 500, pg.GetHashPoint(), reinterpret_cast<char*>(pl));
    ts_now += pseudo_off;
    pseudo_off *= 2;
    // destroy
    PayloadGenerator::Destroy(pl);
  }
  EXPECT_EQ(bt->size(), cnt);
  // check column value
  uint64_t col1 = 0;
  char col2[32] = {0};
  int64_t col3 = 0;
  char col4[32] = {0};
  for (int i = 1; i <= cnt; ++i) {
    // column value
    bt->getColumnValue(i, 0, &col1);
    bt->getColumnValue(i, 1, col2);
    EXPECT_EQ(col1, ts_bak);
    EXPECT_EQ(string(col2), clock_fmt(ts_bak));

    if (i % 2 == 0) {
      // it should be null
      ASSERT_EQ(bt->isNull(i, 2), true);
      ASSERT_EQ(bt->isNull(i, 3), true);
    } else {
      bt->getColumnValue(i, 2, &col3);
      bt->getColumnValue(i, 3, col4);
      ASSERT_EQ(col3, i - 1);
      ASSERT_EQ(string(col4), not_null_col4[i / 2]);
    }
    ts_bak += pseudo_off_bak;
    pseudo_off_bak *= 2;
  }

  // print
  cout << "Last 10 after delete: "
       << bt->printRecord(bt->size() - 9, bt->size()) << endl;

  // remove
  EXPECT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;
//  releaseObject(bt);
}

TEST_F(TestTagBigTable, insert_non_continuous_primary) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::VARSTRING, 32, 0, 32, PRIMARY_TAG},
      {5, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG}
  };
  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 12, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  size_t cnt = 1000;
  PayloadGenerator pg(schema);
  for (size_t i = 0; i < cnt; ++i) {
    auto payload = pg.Construct(nullptr,
                                i,
                                i * 10,
                                std::to_string(i * 10),
                                std::to_string(i),
                                "Hello_" + std::to_string(i));
    // insert
    bt->insert(i, i % 500, pg.GetHashPoint(), reinterpret_cast<const char *>(payload));
    PayloadGenerator::Destroy(payload);
  }

  // print
  cout << "Last 10 after delete: "
       << bt->printRecord(bt->size() - 9, bt->size()) << endl;

  // check
  ASSERT_EQ(bt->size(), cnt);
  int64_t c1 = 0, c2 = 0;
  char c3[32] = {}, c4[32] = {}, c5[32] = {};
  for (size_t i = 1; i <= cnt; ++i) {
    bt->getColumnValue(i, 0, &c1);
    bt->getColumnValue(i, 1, &c2);
    bt->getColumnValue(i, 2, c3);
    bt->getColumnValue(i, 3, c4);
    bt->getColumnValue(i, 4, c5);
    ASSERT_EQ(c1, i - 1);
    ASSERT_EQ(c2, (i - 1) * 10);
    ASSERT_EQ(c3, std::to_string((i - 1) * 10));
    ASSERT_EQ(c4, std::to_string(i - 1));
    ASSERT_EQ(c5, "Hello_" + std::to_string(i - 1));
  }
  EXPECT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;
}

TEST_F(TestTagBigTable, insert_concurrency) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {0, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {1, DATATYPE::VARSTRING, 32, 0, 32, PRIMARY_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 32, GENERAL_TAG},
      {4, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG},
      {5, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG},
      {6, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {7, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG},
      {8, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG},
      {9, DATATYPE::VARSTRING, 32, 0, 32, GENERAL_TAG}
  };
  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 6, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  // time format lambda
  auto clock_now = []() -> string {
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::chrono::system_clock::time_point tp{std::chrono::microseconds(now)};
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    struct std::tm* ptm = std::localtime(&tt);
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", ptm);
    return buffer;
  };

  PayloadGenerator pg(schema);
  uint8_t null_bitmap[2] = {0};
  set_null_bitmap(null_bitmap, 2);
  set_null_bitmap(null_bitmap, 3);
  set_null_bitmap(null_bitmap, 7);
  set_null_bitmap(null_bitmap, 8);

  // insert func
  auto insert_fn = [&](size_t id_start, size_t row) {
    for (size_t i = id_start; i < id_start + row; ++i) {
      auto now = clock_now();
      auto payload = pg.Construct(null_bitmap,
                                  i,
                                  now,
                                  nullptr,
                                  nullptr,
                                  i * 4,
                                  i * 5,
                                  i * 6,
                                  nullptr,
                                  nullptr,
                                  now);
      // insert
      bt->insert(i, i % 500, pg.GetHashPoint(), reinterpret_cast<char*>(payload));
      // destroy
      PayloadGenerator::Destroy(payload);
    }
  };

  const int row_count_per_thread = 100;
  const int thread_count = 10;
  vector<thread> wl;  // wait list
  for (int i = 0; i < thread_count; ++i) {
    wl.emplace_back(insert_fn, i * row_count_per_thread, row_count_per_thread);
  }

  for (auto& t : wl) { t.join(); }

  // check
  EXPECT_EQ(bt->size(), row_count_per_thread * thread_count);

  unordered_set<uint64_t> checked;
  uint64_t col1 = 0;
  char col2[32] = {0};
  uint32_t col5 = 0, col6 = 0;
  uint64_t col7 = 0;
  char col10[32] = {0};
  for (size_t i = 1; i <= bt->size(); ++i) {
    // get primary
    bt->getColumnValue(i, 0, &col1);
    bt->getColumnValue(i, 1, col2);
    ASSERT_TRUE(col1 >= 0 && col1 < row_count_per_thread * thread_count);
    checked.insert(col1);
    // check other column
    bt->getColumnValue(i, 4, &col5);
    bt->getColumnValue(i, 5, &col6);
    bt->getColumnValue(i, 6, &col7);
    bt->getColumnValue(i, 9, col10);
    ASSERT_EQ(col5, col1 * 4);
    ASSERT_EQ(col6, col1 * 5);
    ASSERT_EQ(col7, col1 * 6);
    ASSERT_EQ(string(col10), string(col2));
  }
  ASSERT_EQ(checked.size(), row_count_per_thread * thread_count);

  // print
  cout << "Last 10 after insert: "
       << bt->printRecord(bt->size() - 9, bt->size()) << endl;

  // remove
  EXPECT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;
//  releaseObject(bt);
}


TEST_F(TestTagBigTable, delete_row) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {0,  DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {1,  DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {2,  DATATYPE::VARSTRING, 32, 0, 32, GENERAL_TAG},
      {3,  DATATYPE::INT64, 8, 0, 8, GENERAL_TAG}
  };
  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 7, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  int cnt = 4000;
  vector<std::pair<uint8_t*, size_t>> all_primary_tags;
  vector<std::pair<uint8_t*, size_t>> del_primary_tags;
  PayloadGenerator pg(schema);
  static const char* loc[] = {"beijing", "shanghai", "guangzhou", "shenzhen", "nanjing", "jinan"};
  static const size_t loc_cnt = sizeof(loc) / sizeof(loc[0]);
  for (size_t i = 0; i < cnt; ++i) {
    auto ts_now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    auto payload = pg.Construct(nullptr,
                                i,
                                ts_now,
                                loc[random_device()() % loc_cnt],
                                i);
    // deleted pk
    auto mem = new uint8_t[16];
    memcpy(mem, payload + ((schema.size() + 7) >> 3), 16);
    if (i % 3 == 1) {
      del_primary_tags.emplace_back(mem, 16);
    }
    all_primary_tags.emplace_back(mem, 16);

    // insert
    bt->insert(i, i % 500, pg.GetHashPoint(), reinterpret_cast<char*>(payload));
    // destroy
    PayloadGenerator::Destroy(payload);
  }
  EXPECT_EQ(bt->size(), cnt);
  // delete row
  for (auto& p : del_primary_tags) {
    bt->DeleteTagRecord(reinterpret_cast<char*>(p.first), static_cast<int>(p.second), err_info);
  }

  // print
  cout << "Last 10 after delete: "
       << bt->printRecord(bt->size() - 9, bt->size()) << endl;

  // check
  uint32_t entity_id = 0;
  uint32_t sub_entity_group_id = 0;
  for (size_t i = 0; i < cnt; ++i) {
    auto valid = bt->isValidRow(i + 1);
    if (i % 3 == 1) {
      ASSERT_EQ(valid, false);
    } else {
      ASSERT_EQ(valid, true);
      bt->getEntityIdGroupId(reinterpret_cast<char *>(all_primary_tags[i].first),
                             static_cast<int>(all_primary_tags[i].second), entity_id, sub_entity_group_id);
      ASSERT_EQ(entity_id, i);
    }
  }

  for (auto p : all_primary_tags) {
    delete[] p.first;
  }

  // remove
  EXPECT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;
//  releaseObject(bt);
}

TEST_F(TestTagBigTable, delete_concurrency) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {0, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {1, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {2, DATATYPE::VARSTRING, 32, 0, 32, GENERAL_TAG},
      {3, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG}
  };
  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 7, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  // insert
  PayloadGenerator pg(schema);
  int cnt = 4000;
  vector<std::pair<uint8_t*, size_t>> primary_tags;
  for (size_t i = 0; i < cnt; ++i) {
    auto ts_now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    string location;
    {
      static const char* loc[] = {"beijing", "shanghai", "guangzhou", "shenzhen", "nanjing", "jinan"};
      static const size_t loc_cnt = sizeof(loc) / sizeof(loc[0]);
      location = loc[random_device()() % loc_cnt];
    }

    // payload
    auto payload = pg.Construct(nullptr,
                                i,
                                ts_now,
                                location,
                                location.length());

    if (i % 7 == 2 || i % 7 == 5) {
      auto mem = new uint8_t[16];
      memcpy(mem, payload + ((schema.size() + 7) >> 3), 16);
      primary_tags.emplace_back(mem, 16);
    }

    // insert
    bt->insert(i, i % 500, pg.GetHashPoint(), reinterpret_cast<const char *>(payload));

    // destroy payload
    PayloadGenerator::Destroy(payload);
  }
  EXPECT_EQ(bt->size(), cnt);

  // delete row
  const size_t thr_cnt = 4;
  const size_t avg = primary_tags.size() / thr_cnt;
  vector<thread> workers;
  for (size_t i = 0; i < thr_cnt; ++i) {
    workers.emplace_back([&] (size_t thr_idx) {
      ErrorInfo err;
      for (size_t j = thr_idx * avg; j < thr_idx * avg + avg; ++j) {
        bt->DeleteTagRecord(reinterpret_cast<char *>(primary_tags[j].first),
                            static_cast<int>(primary_tags[j].second), err);
        delete[] primary_tags[j].first;
      }
    }, i);
  }
  for (size_t k = 0; k < primary_tags.size() % thr_cnt; ++k) {
    auto idx = avg * thr_cnt + k;
    bt->DeleteTagRecord(reinterpret_cast<char *>(primary_tags[idx].first),
                        static_cast<int>(primary_tags[idx].second), err_info);
    delete[] primary_tags[idx].first;
  }
  for (auto & w : workers) { w.join(); }

  // print
  cout << "Last 10 after delete: "
       << bt->printRecord(bt->size() - 9, bt->size()) << endl;

  // check
  for (size_t i = 0; i < cnt; ++i) {
    auto valid = bt->isValidRow(i + 1);
    if (i % 7 == 2 || i % 7 == 5) {
      ASSERT_EQ(valid, false);
    } else {
      ASSERT_EQ(valid, true);
    }
  }
  // remove
  EXPECT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;

}

#if 0
TEST_F(TestTagBigTable, insert_delete) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {0, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {1, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {2, DATATYPE::VARSTRING, 32, 0, 32, GENERAL_TAG},
      {3, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG}
  };
  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 8, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  const static size_t primary_len = 16;

  // payload
  static const char* loc[] = {"beijing", "shanghai", "guangzhou", "shenzhen", "nanjing", "jinan"};
  static const size_t loc_cnt = sizeof(loc) / sizeof(loc[0]);
  PayloadGenerator pg(schema);
  vector<uint8_t *> payloads;
  size_t cnt = 4000;
  for (size_t i = 0; i < cnt; ++i) {
    auto pos = loc[random_device()() % loc_cnt];
    payloads.emplace_back(
        pg.construct(nullptr,
                     i + 1,
                     std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count(),
                     pos,
                     strlen(pos)));
  }

  atomic<size_t> row_idx = 0;
  // insert
  thread ins([&row_idx, cnt, &payloads, &bt] {
    for (size_t i = 0; i < cnt; ++i) {
      bt->insert(i, i % 500, reinterpret_cast<const char *>(payloads[i]));
      row_idx.store(i + 1, memory_order_release);
    }
    row_idx.fetch_add(1, memory_order_acq_rel);
  });

  // delete
  thread del([&row_idx, cnt, &payloads, &bt] {
    ErrorInfo err;
    for (size_t i = 1; i <= cnt; i = std::min(i + 1, row_idx.load(memory_order_consume))) {
      if (i > 0 && i % 5 == 0) {
        bt->DeleteTagRecord(reinterpret_cast<const char *>(payloads[i - 1] + 1), primary_len, err);
      }
    }
  });

  ins.join();
  del.join();

  ASSERT_EQ(bt->size(), cnt);

  // print
  cout << "Last 10 after delete: "
       << bt->printRecord(bt->size() - 9, bt->size()) << endl;

  // check
  uint64_t id = 0, ts = 0, sz = 0;
  char location[32] = {0};
  for (size_t i = 1; i <= cnt; ++i) {
    if (i % 5 == 0) {
      ASSERT_EQ(bt->isValidRow(i), false);
    } else {
      ASSERT_EQ(bt->isValidRow(i), true);
      bt->getColumnValue(i, 0, &id);
      bt->getColumnValue(i, 1, &ts);
      bt->getColumnValue(i, 2, location);
      bt->getColumnValue(i, 3, &sz);
      ASSERT_EQ(id, i);
      ASSERT_EQ(strlen(location), sz);
    }
  }

  // destroy
  for (auto p : payloads) { PayloadGenerator::destroy(p); }
  // remove
  EXPECT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;
}
#endif
#endif