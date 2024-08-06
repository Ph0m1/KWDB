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

#ifndef OBJECT_H_
#define OBJECT_H_

#if !defined(NDEBUG)
#include <mutex>
#endif
#include <set>
#include <string>
#include "RWLock.h"
#include "BigObjectError.h"
#include "lt_rw_latch.h"

//using namespace std;


#define OBJ_PERM_NO_SELECT          0x00000001
#define OBJ_PERM_NO_INSERT          0x00000002
#define OBJ_PERM_NO_DROP            0x00000004
#define OBJ_PERM_NO_UPDATE          0x00000008

#define is_not_droppable(x)         ((x & OBJ_PERM_NO_DROP) != 0)
#define is_not_updatable(x)         ((x & OBJ_PERM_NO_UPDATE) != 0)


// only use the highest byte; the left are used in sub-class
#define READONLY    0x10000000

#define is_readonly(x)      ((x & READONLY) != 0)


enum OBJECT_STATUS {
  OBJ_READY_TO_CHANGE,
  OBJ_TO_SET_DEFUALT,       // Table to set default value.
  OBJ_READY,                // R/W lock OK on READY and previous statuses
  OBJ_READY_TO_COMPACT,     // will compact object soon
  OBJ_NOT_READY,
  OBJ_INVALID,
};

enum OBJ_MANIP_TYPE {
  OBJ_GET,
  OBJ_RLS,
  OBJ_DEL,
};

#define impl_latch_virtual_func(CLASS, latch_ptr) \
int CLASS::rdLock() { return RW_LATCH_S_LOCK(latch_ptr); }  \
int CLASS::wrLock() { return RW_LATCH_X_LOCK(latch_ptr); }  \
int CLASS::unLock() { return RW_LATCH_UNLOCK(latch_ptr); }  \

class ObjectManager;

class TSObject {
protected:
  int status_;
  int ref_count_;
  pthread_mutex_t obj_mutex_;
  pthread_cond_t obj_mtx_cv_;

  void swapObject(TSObject *obj);

  int rename(const string &new_name, const string &new_db, ErrorInfo &err_info);

  virtual int rename_nolock(const string &new_name, const string &new_db,
                            ErrorInfo &err_info);

public:
  TSObject();

  virtual ~TSObject();

  // derived class need to override
  virtual int rdLock() = 0;
  virtual int wrLock() = 0;
  virtual int unLock() = 0;

  virtual int refMutexLock() {
    return 0;
  }
  virtual int refMutexUnlock() {
    return 0;
  }

  virtual int type() const;

  int& refCount() { return ref_count_; }

  void incRefCount();

  int decRefCount();

  virtual int objCC() const;

  /**
   * @brief	Get object URL.
   *
   * @return	TSObject URL string.
   */
  virtual string URL() const;

  virtual string name() const;

  /**
   * @brief   swap table content
   */
  virtual int swap(TSObject *other, ErrorInfo &err_info);
  virtual int swap_nolock(TSObject *other, ErrorInfo &err_info);

  virtual const string & tbl_sub_path() const;

  string directory() const;

  virtual string link() const;

  ///
  /// @brief	Open an object via URL.
  ///
  /// @param	url         TSObject URL.
  /// @param	sandobx		Name of tbl_sub_path(directory).
  /// @param	flags		Access modes of the object.
  /// @return	0 succeed, otherwise -1.
  ///
  virtual int open(const string &url, const string& db_path, const string &tbl_sub_path, int flags,
    ErrorInfo &err_info);

  virtual int openInit(ErrorInfo &err_info);

  virtual int version() const;

  // increment object version if update/delete occurs.
  virtual void incVersion();

  virtual void clear();

  // used for to avoid batch insert (for insert select) and
  // push_back occur at the same time.
  // === deadlock avoidance  protocol ===
  // (1) mutex lock first
  // (2) read/write lock
  virtual void mutexLock() { pthread_mutex_lock(&obj_mutex_); }

  virtual void mutexUnlock() { pthread_mutex_unlock(&obj_mutex_); }

  void mutexWait() { pthread_cond_wait(&obj_mtx_cv_, &obj_mutex_); }

  void mutexTimeWait(timespec& timeout) { pthread_cond_timedwait(&obj_mtx_cv_, &obj_mutex_, &timeout);}

  void mutexSignal() { pthread_cond_signal(&obj_mtx_cv_); }

  // sync data to disk. MS_ASYNC or MS_SYNC
  virtual void sync(int flags);

  /**
   * @brief	Remove an object from system.
   * @return  0 if succeeds. otherwise -1.
   */
  virtual int remove();

  virtual int repair();

#if defined(LOGISTIC_PLAN)
  /**
   * @brief	obtain file URLs on which a big object depends.
   * @param	files		a set of object URLs that will be updated.
   */
  virtual void dependentFile(set<string> &files) const;
#endif


  // simple read lock
  int startRead();

  virtual int startRead(ErrorInfo &err_info);

  int startWrite();

  int startWrite(ErrorInfo &err_info);

  virtual int stopRead();

  int stopWrite();

  bool isValid() const { return (status_ == OBJ_READY); }

  virtual bool isTemporary() const;

  // return true is object is used by anyone.
  inline bool isUsed() const
  { return (isTemporary()) ? ref_count_ > 2 : ref_count_ > 1; }

  virtual int permission() const;
  virtual void setPermission(int perms);

  void setObjectReady() { status_ = OBJ_READY; }

  int getObjectStatus() const { return status_; }

  void setObjectStatus(int status) { status_ = status;}

  virtual string toString();

  // used for DROP command
  // wait when reference count > 1
  // notify when reference count = 1 in object manager
  // remove in the future
  pthread_mutex_t ref_cnt_mtx_;
  pthread_cond_t ref_cnt_cv_;

  // reference count for insertion
  int32_t ins_ref_cnt_;

#if !defined(NDEBUG)
  int rd_cnt_;
  int wr_cnt_;
  mutex rw_mtx_;
#endif

  friend class ObjectManager;
};

enum LockType {
    NO_LOCK, READ_LOCK, WRITE_LOCK, LIGHT_READ_LOCK, LIGHT_WRITE_LOCK
};

void LogObject(int type, TSObject *obj);

#if defined (LOGISTIC_PLAN)
struct ObjectLockType
{
    TSObject *object;
    int type;
    bool is_releasable;

    ObjectLockType()
    { is_releasable = false; }

    void setReleasable()
    { is_releasable = true; }

    bool isReleasable() const
    { return is_releasable; }
};
#endif

#endif /* OBJECT_H_ */
