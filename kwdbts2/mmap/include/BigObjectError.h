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


#ifndef INCLUDE_BIGOBJECTERROR_H_
#define INCLUDE_BIGOBJECTERROR_H_

#include <pthread.h>
#include <iostream>
#include "BigObjectConst.h"


using namespace std;
using namespace bigobject;

#define BOEPERM         -1  // Operation not permitted
#define BOECORR         -2	// TSObject corrupted.
#define	BOEMMAP         -3	// MMap failed.
#define BOENOMEM        -4  // Out of memory
#define BOENOFUNC       -5  // Unknown function.
#define BOEEXIST        -6	// TSObject exists.
#define BOENOSPC        -7  // No space left on device.
#define BOENFILE	      -8  // Cannot open more files.
#define BOEINVALPATH    -9  // Invalid path.
#define BOEROFS         -10 // Operation not permitted on read only FS.
#define BOENOOBJ        -11	// No such object.
#define BOEDATETYPE	    -12 // Unknown data type.
#define BOENOATTR       -13	// Unknown attribute.
#define BOEBTNOCOL	    -14	// Unknown column name.
#define BOEDIMNOATTR	  -15	// Unknown dimension attribute.
#define BOEINVALIDNAME	-16	// Invalid name
#define BOENAMESERVICE  -17	// Name service Full
#define BOEBTCOLEXIST   -18 // Column name exists
#define BOEDUPOBJECT    -19 // Duplicate object.
#define BOEKEY          -20 // Key is not allowed.
#define BOELUA          -21 // Lua error.
#define BOEJOIN		      -22	// Cannot join.
#define BOEGROUPBY      -23 // Cannot group by
#define BOEDATETIME     -24 // Not a valid date time.
#define BOERLOCK        -25 // Cannot lock(r) object.
#define BOEWLOCK        -26 // Cannot lock(w) object.
#define BOETIMESTAMPCOL -27 // Not a TimeStamp column.
#define BOETIMETABLE    -28 // Table without a timestamp.
#define BOEOTHER        -29 // Other errors.
#define BOELICENSE      -30 // License expired.
#define BOEVARSTRING    -31 // Varstring is not allowed.
#define BOEARG          -32 // Invalid argument.
#define BOENUMERIC      -33 // Not a numeric.
#define BOENOCLUSTER    -34 // Cluster is not set.
#define BOEVALUE        -35 // Invalid value
#define BOENONDEFTABLE  -36 // Not a default table.
#define BOETIMEOUT      -37 // Time out.
#define BOECONNECT      -38 // Connect failed:
#define BOEINTERRUPTED  -39 // Query is interrupted.
#define BOESTRPROTO     -40 // BigObject Streaming Protocol.
#define BOECONNRESET    -41 // Connection reset.
#define BOEHOST         -42 // Unknown Host.
#define BOEHASHSIZE32   -43 // Column name exists.
#define BOENEEDALIAS    -44 // Alias is needed
#define BOESRVBUSY      -45 // Server is busy
#define BOERANGE        -46 // Out of range data
#define BOEINTVALUE     -47 // Incorrect integer value
#define BOEDATEVALUE    -48 // Incorrect date value
#define BOEDATA         -49 // Incorrect data value
#define BOEDEFAULT      -50 // Invalid default value
#define BOEEXTENSION    -51 // Cannot load extension
#define BOEMISSMATCHCOLNUM  -52 // Invalid input value
#define BOELENLIMIT -53 // Out of len limit
#define BOESYNTAXERROR  -54 // Syntax error
#define BOEDATATYPEMISMATCH  -55 //datatype mismatch
#define BOETHREAD         -56  // create thread for task failed
#define BOENOEGMENT      -57  // need create new segment directory
#define DEDUPREJECT     -58  // reject duplicate data
#define KWERSRCBUSY   -59  // resource is busy

#define ERROR_LEN_LIMIT 128

#define ERRINF_MUTEX_NEEDED     true
#define ERRINF_NO_MUTEX         false

class ErrorInfo {
protected:
  pthread_mutex_t mutex_;
  bool is_mutex_needed_;

  void lock();
  void unlock();

public:
  int errcode;
  string errmsg;

  ErrorInfo(bool is_mtx_needed = true);

  virtual ~ErrorInfo();

  void clear();

  static const char * errorCodeString(int cerr_ode);

  int setError(const ErrorInfo &rhs);

  int setError(int err_code, const string &str = bigobject::s_emptyString());

  void appendErrMsg(const string &msg){errmsg += msg;}

  int errnoToError(const string &str = bigobject::s_emptyString());

  int unKnownColumn(const string &col_name, int type = 0);

  int unJoinableColumn(const string &col_name);

  string toString() {return "";};

  inline bool isOK() { return errcode >= 0;};
};

class TSObject;

ErrorInfo & getDummyErrorInfo();

string toErrorString(const string &attr);

int errnumToErrorCode(int errnum);
int errnoToErrorCode();

string nameTooLong(const string &name);

#endif /* INCLUDE_BIGOBJECTERROR_H_ */
