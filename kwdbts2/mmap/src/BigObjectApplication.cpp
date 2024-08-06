// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.


#include <sys/stat.h>
#include <cstdlib>
#include <time.h>
#include <string>
#include <fstream>
#include "mmap/MMapBigTable.h"
#include "BigObjectApplication.h"
#include "BigObjectConfig.h"
#include "BigObjectUtils.h"
#include "BigObjectConst.h"
// undef macro in syslog.h
#undef LOG_INFO
#undef LOG_DEBUG
#include "lg_api.h"
#include "lib/HyperLogLog.h"
#include "lt_rw_latch.h"


using namespace bigobject;

KLatch * create_table_latch = nullptr;

void createTableLock() { MUTEX_LOCK(create_table_latch); }
void createTableUnlock() { MUTEX_UNLOCK(create_table_latch); }

void releaseObject(TSObject *obj, bool is_force) {
  if (obj) {
    if (obj->isValid() && (obj->type() & (ORDERED_TABLE | EXTENDED_TABLE))) {
      if (obj->decRefCount() == 0)
        delete obj;
      return;
    }
    if (obj->isTemporary()) {
      if (obj->decRefCount() == 0)
        delete obj;
      return;
    }

    delete obj;
  }
}

BigTable *CreateTempTable(const vector<AttributeInfo> &schema, const std::string &db_path,
    int encoding,
    ErrorInfo &err_info) {
  std::vector<std::string> key;
  int flags = MMAP_CREAT_EXCL;
  std::string bt_actual_url = genTempObjectURL("");
  BigTable *bt = reinterpret_cast<BigTable*>(new MMapBigTable());
  if (unlikely(bt == nullptr)) {
    LOG_ERROR("new MMapBigTable failed, db_path: %s name: %s ", db_path.c_str(), bt_actual_url.c_str());
    return nullptr;
  }
  if (bt->open(bt_actual_url, db_path, "", flags, err_info) < 0) {
    LOG_ERROR("Open temporary table failed, db_path: %s name: %s ", db_path.c_str(), bt_actual_url.c_str());
    delete bt;
    return nullptr;
  }
  if (bt->create(schema, key, "", "", "", "", "", encoding, err_info) < 0 ) {
    LOG_ERROR("Create temporary table failed, db_path: %s name: %s ", db_path.c_str(), bt_actual_url.c_str());
    delete bt;
    return nullptr;
  }
  return bt;
}

void DropTempTable(BigTable* bt, ErrorInfo &err_info) {
  if (bt) {
    bt->remove();
    delete bt;
  }
  return;
}




