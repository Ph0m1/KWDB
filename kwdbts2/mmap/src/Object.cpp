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


#include <algorithm>
#include <iostream>
#include "TSObject.h"
#include "BigObjectUtils.h"
#include "BigObjectConst.h"


#define CC_MMNS     0x534E4D4D              // SNMM

const char *cstr_GET = "GET";
const char *cstr_RLS = "RLS";
const char *cstr_DEL = "DEL";

void LogObject(int type, TSObject *obj) {
  if ((BigObjectConfig::logging() >= BO_LOG_FINER)) {
#if defined(NDEBUG)
    if (BigObjectConfig::logging() >= BO_LOG_FINEST || obj->refCount() == 1) {
#endif
      const char *head;
      switch (type) {
        case OBJ_GET:   head = cstr_GET; break;
        case OBJ_RLS:   head = cstr_RLS; break;
        default:        head = cstr_DEL;
      }
      printf("%lx [KWINF] %s (%d): \"%s\", db: \"%s\" \n", pthread_self(),
        head, obj->refCount(), obj->URL().c_str(), obj->tbl_sub_path().c_str());
#if defined(NDEBUG)
    }
#endif
  }
}

enum {
  OBJ_RD_LOCK,
  OBJ_WR_LOCK,
  OBJ_RD_UNLOCK,
  OBJ_WR_UNLOCK,
  OBJ_RD_LOCK_ERROR,
  OBJ_WR_LOCK_ERROR,
};


void LogObjectLock(int type, TSObject *obj) {
  if (BigObjectConfig::logging() > BO_LOG_FINEST) {
#if !defined(NDEBUG)
    const char *head;
    int cnt;
    obj->rw_mtx_.lock();
    switch(type) {
      case OBJ_RD_LOCK:   head = "LOCK_R"; cnt = obj->rd_cnt_; break;
      case OBJ_WR_LOCK:   head = "LOCK_W"; cnt = obj->wr_cnt_; break;
      case OBJ_RD_UNLOCK: head = "UNLK_R"; cnt = obj->rd_cnt_; break;
    case OBJ_WR_UNLOCK: head = "UNLK_W"; cnt = obj->wr_cnt_; break;
    }
    obj->rw_mtx_.unlock();
    if (obj->objCC() != CC_MMNS) {
      printf("%lx [KWINF] %s(%d): \"%s\", db: \"%s\"\n", pthread_self(), head,
        cnt, obj->URL().c_str(), obj->tbl_sub_path().c_str());
    }
#else
    const char *head;
    switch(type) {
      case OBJ_RD_LOCK:   head = "LOCK_R";  break;
      case OBJ_WR_LOCK:   head = "LOCK_W";  break;
      case OBJ_RD_UNLOCK: head = "UNLK_R";  break;
      case OBJ_WR_UNLOCK: head = "UNLK_W";  break;
    }
    if (obj->objCC() != CC_MMNS) {
      printf("%lx [KWINF] %s: \"%s\", db: \"%s\"\n", pthread_self(), head,
        obj->URL().c_str(), obj->tbl_sub_path().c_str());
    }
#endif
  }
}

void LogObjectLockError(int type, TSObject *obj) {
  if (BigObjectConfig::logging() >= BO_LOG_WARNING) {
    const char *head;
    switch(type) {
      case OBJ_RD_LOCK_ERROR: head = "<Cannot lock(r)>";  break;
      case OBJ_WR_LOCK_ERROR: head = "<Cannot lock(w)>";  break;
    }

    if (obj->objCC() != CC_MMNS) {
      printf("%lx [KWINF] %s: \"%s\", db: \"%s\"\n", pthread_self(), head,
        obj->URL().c_str(), obj->tbl_sub_path().c_str());
    }
  }
}


TSObject::TSObject() {
  pthread_mutex_init(&obj_mutex_, NULL);
  pthread_cond_init(&obj_mtx_cv_, NULL);
  pthread_mutex_init(&ref_cnt_mtx_, NULL);
  pthread_cond_init(&ref_cnt_cv_, NULL);
  status_ = OBJ_NOT_READY;
  ref_count_ = 0;
  ins_ref_cnt_ = 0;
#if !defined(NDEBUG)
  rd_cnt_ = wr_cnt_ = 0;
#endif
}

TSObject::~TSObject() {
  pthread_cond_destroy(&ref_cnt_cv_);
  pthread_mutex_destroy(&ref_cnt_mtx_);
  pthread_cond_destroy(&obj_mtx_cv_);
  pthread_mutex_destroy(&obj_mutex_);
}

int TSObject::type() const {
    return 0;
}

void TSObject::incRefCount() {
  // pthread_mutex_lock(&ref_cnt_mtx_);
  refMutexLock();
  ++(ref_count_);
  LogObject(OBJ_GET, this);
  refMutexUnlock();
  // pthread_mutex_unlock(&ref_cnt_mtx_);
}

int TSObject::decRefCount() {
  int ref_count;
  // pthread_mutex_lock(&ref_cnt_mtx_);
  refMutexLock();
  LogObject(OBJ_RLS, this);
  ref_count = --(ref_count_);
  refMutexUnlock();
  // pthread_mutex_unlock(&ref_cnt_mtx_);
  return ref_count;
}

int TSObject::objCC() const { return 0; }

string TSObject::URL() const
{ return bigobject::s_emptyString(); }

string TSObject::name() const
{ return getURLObjectName(URL()); }

void TSObject::swapObject(TSObject *other) {
  std::swap(ref_count_, other->ref_count_);
  std::swap(status_, other->status_);
}

int TSObject::swap(TSObject *other, ErrorInfo &err_info) {
  pthread_mutex_lock(&obj_mutex_);
  std::swap(ref_count_, other->ref_count_);
  std::swap(status_, other->status_);
  swap_nolock(other, err_info);
  pthread_mutex_unlock(&obj_mutex_);
  return err_info.errcode;
}

int TSObject::swap_nolock(TSObject *other, ErrorInfo &err_info)
{ return -1; }

const string & TSObject::tbl_sub_path() const
{ return bigobject::s_emptyString(); }

string TSObject::directory() const {
  const string &db = tbl_sub_path();
  if (db.empty())
    return s_bigobject();
  if (db.back() == '\\')
    return db.substr(0, db.size() - 1);
  return db;
}

string TSObject::link() const { return s_emptyString(); }

int TSObject::open(const string &url, const string& db_path, const string &tbl_sub_path, int flags,
  ErrorInfo &err_info) { return 0; }

int TSObject::openInit(ErrorInfo &err_info) { return 0; }

int TSObject::version() const { return 0; }

void TSObject::incVersion () {}

void TSObject::clear() {}

void TSObject::sync(int flags) {}

/*
int64_t TSObject::getTimeStamp() const
{
    return time_stamp_;
}

void TSObject::setTimeStamp(int64_t ts)
{
    time_stamp_ = ts;
}
*/

int TSObject::remove() { return -1; }

int TSObject::repair() { return 0; }

int TSObject::rename(const string &new_name, const string &new_db,
  ErrorInfo &err_info) {
  pthread_mutex_lock(&obj_mutex_);
  rename_nolock(new_name, new_db, err_info);
  pthread_mutex_unlock(&obj_mutex_);
  return err_info.errcode;
}

int TSObject::rename_nolock(const string &new_name, const string &new_db,
  ErrorInfo &err_info) { return -1; }


#if defined(LOGISTIC_PLAN)
void TSObject::dependentFile(set<string> &objs) const
{
}
#endif


int TSObject::startRead() {
  rdLock();
  if (status_ != OBJ_READY) {
    unLock();
    LogObjectLockError(OBJ_RD_LOCK_ERROR, this);
    return BOERLOCK;
  }
#if !defined(NDEBUG)
  rw_mtx_.lock();
  rd_cnt_++;
  rw_mtx_.unlock();
#endif
  LogObjectLock(OBJ_RD_LOCK, this);
  return 0;
}

int TSObject::startRead(ErrorInfo &err_info) {
  rdLock();
  if (status_ > OBJ_READY_TO_COMPACT) {
    unLock();
    LogObjectLockError(OBJ_RD_LOCK_ERROR, this);
    return err_info.setError(BOERLOCK, getURLObjectName(URL()));
  }
#if !defined(NDEBUG)
  rw_mtx_.lock();
  rd_cnt_++;
  rw_mtx_.unlock();
#endif
  LogObjectLock(OBJ_RD_LOCK, this);
  return 0;
}

int TSObject::startWrite() {
  wrLock();
  if (status_ != OBJ_READY) {
    unLock();
    LogObjectLockError(OBJ_WR_LOCK_ERROR, this);
    return BOEWLOCK;
  }
#if !defined(NDEBUG)
  rw_mtx_.lock();
  wr_cnt_++;
  rw_mtx_.unlock();
#endif
  LogObjectLock(OBJ_WR_LOCK, this);
  return 0;
}

int TSObject::startWrite(ErrorInfo &err_info) {
  wrLock();
  if (status_ != OBJ_READY) {
    unLock();
    LogObjectLockError(OBJ_WR_LOCK_ERROR, this);
    return err_info.setError(BOEWLOCK, getURLObjectName(URL()));
  }
#if !defined(NDEBUG)
  rw_mtx_.lock();
  wr_cnt_++;
  rw_mtx_.unlock();
#endif
  LogObjectLock(OBJ_WR_LOCK, this);
  return 0;
}

//int TSObject::tryWrite()
//{
//    if (pthread_rwlock_trywrlock(&(this->rwlock)) != 0)
//	return -1;
//    if (status_ != OBJ_READY) {
//	pthread_rwlock_unlock(&(this->rwlock));
//	return -1;
//    }
//    return 0;
//}

int TSObject::stopRead() {
  int ret_val = unLock();
#if !defined(NDEBUG)
  rw_mtx_.lock();
  rd_cnt_--;
  rw_mtx_.unlock();
#endif
  LogObjectLock(OBJ_RD_UNLOCK, this);
  return ret_val;
}

int TSObject::stopWrite() {
  int ret_val = unLock();
#if !defined(NDEBUG)
  rw_mtx_.lock();
  wr_cnt_--;
  rw_mtx_.unlock();
#endif
  LogObjectLock(OBJ_WR_UNLOCK, this);
  return ret_val;
}

bool TSObject::isTemporary() const { return false; }

int TSObject::permission() const { return 0; }
void TSObject::setPermission(int perms) {};

string TSObject::toString() {
  return intToString(ref_count_);
}

