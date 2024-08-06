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


#include <sstream>
#include "BigObjectError.h"
#include "BigObjectUtils.h"

ErrorInfo dummy_error_info;

const char * boErrmsg[] = {
  "",                                   //                   0
  "Operation not permitted: ",          // BOEPERM	        -1
  "TSObject corrupted: ",                 // BOECORR	        -2
  "MMAP failed: ",                      // BOEMMAP	        -3
  "No memory is available: ",           // BOENOMEM	        -4
  "Unknown function: ",                 // BOENOFUNC        -5
  "TSObject already exists: ",    	      // BOEEXIST	        -6
  "No space left on device.",           // BOENOSPC         -7
  "Cannot open more files.",		        // BOENFILE	        -8
  "Invalid path: ",                     // BOEINVALPATH     -9
  // BOEROFS  -10    // Operation not permitted on read only FS.
  "Operation not permitted on read only file system for ",
  "TSObject doesn't exist: ",             // BOENOOBJ         -11
  "Unknown data type: ",                // BOEDATETYPE      -12
  "Unknown attribute: ",                // BOENOATTR        -13
  "Unknown column: ",                   // BOEBTNOCOL       -14
  "Unknown attribute: ",                // BOEDIMNOATTR	    -15
  "Invalid name: ",                     // BOEINVALIDNAME   -16
  "NameService full",                   // BOENAMESERVICE   -17
  "Column name exists: ",               // BOEBTCOLEXIST    -18
  "Duplicate object: ",                 // BOEDUPOBJECT     -19
  "Cannot set key on ",                 // BOEKEY           -20
  "Lua error: ",                        // BOELUA           -21
  "Cannot join: ", 		                  // BOEJOIN          -22
  "Cannot GROUP BY: ",                  // BOEGROUPBY       -23
  "Not a valid date time: ",            // BOEDATETIME      -24
  "Cannot lock(r) object: ",            // BOERLOCK         -25
  "Cannot lock(w) object: ",            // BOEWLOCK         -26
  "Not a TIMESTAMP column: ",           // BOETIMESTAMPCOL  -27
  "Table without TIMESTAMP: ",          // BOETIMETABLE	    -28
  "Other error: ",                      // BOEOTHER	        -29
  "License expired",                    // BOELICENSE       -30
  "VARSTRING is not allowed ",          // BOEVARSTRING     -31
  "Invalid argument for function: ",    // BOEARG           -32
  "Not a numeric: ",                    // BOENUMERIC       -33
  "Cluster is not set",                 // BOENOCLUSTER     -34
  "Invalid value: ",                    // BOEVALUE         -35
  "Not a default(deletable) table: ",   // BOENONDEFTABLE   -36
  "Time out: ",                         // BOETIMEOUT       -37
  "Connect failed: ",                   // BOECONNECT       -38
  "Query is interrupted:",              // BOEINTERRUPTED   -39
  "Streaming protocol:",                // BOESTRPROTO      -40
  "Connection reset.",                  // BOECONNRESET     -41
  "Unknown host.",                      // BOEHOST          -42
  // BOEHASHSIZE32   -43 // 32-bit hash size too small. 64-bit required.
  "32-bit hash size too small. 64-bit required.",
  "Alias needed: ",                     // BOENEEDALIAS     -44
  "Server is busy. Try again later.",   // BOESRVBUSY       -45
  "Out of range data: ",                // BOERANGE         -46
  "Incorrect integer value: ",          // BOEINTVALUE      -47
  "Incorrect date value: ",             // BOEINVDATE       -48
  "Incorrect data value: ",             // BOEDATA          -49
  "Invalid default value: ",            // BOEDEFAULT       -50
  "Cannot load extension: ",            // BOEEXTENSION     -51
  "Invalid input value: ",              // BOEMISSMATCHCOLNUM   -52
  "Out of length limit: ",                 // BOELENLIMIT   -53
  "Syntax error: ",                     // BOESYNTAXERROR   -54
  "Datatype mismatch: " ,               // BOEDATATYPEMISMATCH       -55
  "Create task failed: ",               // BOETHREAD         -56
};

#define BOError(x)  (-x)


ErrorInfo::ErrorInfo(bool is_mtx_needed): is_mutex_needed_(is_mtx_needed) {
  if (is_mutex_needed_)
    pthread_mutex_init(&mutex_, NULL);
  clear();
}

ErrorInfo::~ErrorInfo() {
  if (is_mutex_needed_)
    pthread_mutex_destroy(&mutex_);
};

void ErrorInfo::clear() {
  lock();
  errcode = 0;
  errmsg.clear();
  unlock();
}

void ErrorInfo::lock() {
  if (is_mutex_needed_)
    pthread_mutex_lock(&mutex_);
}

void ErrorInfo::unlock() {
  if (is_mutex_needed_)
    pthread_mutex_unlock(&mutex_);
}

const char * ErrorInfo::errorCodeString(int err_code) {
  err_code = BOError(err_code);
  if (err_code >= (int) (sizeof(boErrmsg) / sizeof(boErrmsg[0])))
    err_code = 0;
  return boErrmsg[err_code];
}

int ErrorInfo::setError(const ErrorInfo &rhs) {
  lock();
  errcode = rhs.errcode;
  errmsg = rhs.errmsg;
  unlock();
  return errcode;
}

int ErrorInfo::setError(int err_code, const string &str) {
  lock();
  errmsg = errorCodeString(err_code) + str;
  unlock();
  return (errcode = err_code);
}

int ErrorInfo::errnoToError(const string &str) {
  errcode = errnoToErrorCode();
  lock();
  errmsg = errorCodeString(errcode) + str;
  unlock();
  return errcode;
}

int ErrorInfo::unKnownColumn(const string &col_name, int type) {
  stringstream ss;
  ss << boErrmsg[BOError(BOEBTNOCOL)];
  if (type == 1)
    ss << cstr_KEY << ' ';
  ss << quoteString(col_name);
  lock();
  errmsg = ss.str();
  unlock();
  return (errcode = BOEBTNOCOL);
}

int ErrorInfo::unJoinableColumn(const string &col_name) {
  lock();
  errmsg = string(boErrmsg[BOError(BOEJOIN)]) +
    "please check your data schema for " + quoteString(col_name);
  unlock();
  return (errcode = BOEBTNOCOL);
}

/*string ErrorInfo::toString() {
  stringstream ss;
  lock();
  ss << *this;
  unlock();
  return ss.str();
}*/

ErrorInfo & getDummyErrorInfo() { return dummy_error_info; }

string toErrorString(const string &str) {
    string err_str = (str.size() < ERROR_LEN_LIMIT) ? str :
        str.substr(0, ERROR_LEN_LIMIT) + "...";
    return err_str;
}

string BOErrorToString(int errcode, const string &str) {
  if (errcode >= (int) (sizeof(boErrmsg) / sizeof(boErrmsg[0])))
    errcode = 0;
  return boErrmsg[errcode] + toErrorString(str);
}

int errnumToErrorCode(int errnum) {
  switch (errnum) {
    case EEXIST: return BOEEXIST;
    case ENOENT: return BOENOOBJ;
    case ENFILE: return BOENFILE;
    case ENOMEM: return BOENOMEM;
    case ENOSPC: {
      return BOENOSPC;
    }
    case EROFS:  return BOEROFS;
    case EACCES: return BOEPERM;
  }
  return (errno > 0) ? BOEOTHER : 0;
}

int errnoToErrorCode() {
  int errnum = errno;
  return errnumToErrorCode(errnum);
}

string nameTooLong(const string &name)
{ return name + " too long."; }
