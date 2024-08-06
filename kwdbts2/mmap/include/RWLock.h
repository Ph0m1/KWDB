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


#ifndef INCLUDE_RWLOCK_H_
#define INCLUDE_RWLOCK_H_

#include <pthread.h>

//namespace bigobject {

class RWLock {
protected:
    pthread_rwlock_t  rwlock_;
public:
    RWLock();
    virtual ~RWLock();

    inline int rdLock() { return pthread_rwlock_rdlock(&rwlock_); }
    inline int wrLock() { return pthread_rwlock_wrlock(&rwlock_); }
    inline int unLock() { return pthread_rwlock_unlock(&rwlock_); }
};

//} /* namespace bigobject */

#endif /* INCLUDE_RWLOCK_H_ */
