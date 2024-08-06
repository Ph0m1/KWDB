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

#ifndef SRC_ITERATORTHREAD_H_
#define SRC_ITERATORTHREAD_H_

#include <iterator>
#include <mutex>
#include <thread>
#include <vector>
#include <functional>
#include <iostream>


using namespace std;

//#define NO_THREAD

namespace bigobject {

template<typename Iterator, typename CallObject>
class IteratorThread
{
protected:
    vector<thread> threads_;
    mutex it_mutex_;
    Iterator begin_, end_;

    void exec(CallObject *o)
    {
        it_mutex_.lock();
        while (begin_ != end_){
            Iterator it = begin_;
            ++begin_;
            it_mutex_.unlock();
             (*o)(it);
            it_mutex_.lock();
        }
        it_mutex_.unlock();
    }

public:
    IteratorThread() {};

    virtual ~IteratorThread() {};

    void run(int n, Iterator begin, Iterator end, CallObject &call_obj)
    {
        begin_ = begin;
        end_ = end;
        if (n == 1) {
            exec(&call_obj);
        }
        else {
            vector<CallObject *> call_objs;
            threads_.resize(n);
            call_objs.resize(n);
            for (size_t i = 0; i < n; ++i) {
                CallObject *t;
                if (i == 0) {
                    t = &call_obj;
                }
                else {
                    t = call_obj.clone();
                }
                call_objs[i] = t;
            }
#if !defined(NO_THREAD)
            for(size_t i = 0; i < n; ++i) {
                threads_[i] = thread(&IteratorThread::exec, this, call_objs[i]);
            }
#endif
            for (size_t i = 0; i < n; ++i) {
#if !defined(NO_THREAD)
                threads_[i].join();
#else
                exec(call_objs[i]);
#endif
            }
            for (size_t i = 1; i < n; ++i)
                delete call_objs[i];
        }
    }
};

} /* namespace bigobject */

#endif /* SRC_ITERATORTHREAD_H_ */
