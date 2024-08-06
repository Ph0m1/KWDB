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
/*
 * @author jiadx
 * @date 2022/3/25
 * @version 1.0
 */

#pragma once

#include <string>
#include <cstring>
#include <stdarg.h>

namespace kwdbts {
#define NULLPTR nullptr

#define ERROR_STATUS(code, ...) KBStatus::ERROR(code, __VA_ARGS__)

enum class StatusCode : char {
  OK = 0,
  OutOfMemory = 1,
  KeyError = 2,
  TypeError = 3,
  Invalid = 4,
  IOError = 5,
  CapacityError = 6,
  IndexError = 7,
  Cancelled = 8,
  NotImplemented = 10,
  SerializationError = 11,
  RError = 13,
  NotFound = 14,
  // Gandiva range of errors
  CodeGenError = 40,
  ExpressionValidationError = 41,
  ExecutionError = 42,
  // Continue generic codes.
  NotExists = 44,
  AlreadyExists = 45,
  // Stale data
  ExpiredError = 46,
  // An internal or system error occurs, for example, an object creation failure
  InternalError = 47,
  SocketClose = 48,
  TimeoutError = 49,
  // Stale data
  UnknownError = 100
};

class KBStatus {
 public:
  KBStatus() noexcept {
    state_ = NULL;
  }
  explicit KBStatus(const char* msg) noexcept {
    state_ = new State;
    if (strlen(msg) == 0) {
      state_->code = StatusCode::OK;
    } else {
      state_->code = StatusCode::UnknownError;
    }
    state_->msg = std::string(msg);
  }

  KBStatus(StatusCode code, const std::string& msg) {
    state_ = new State;
    state_->code = code;
    state_->msg = msg;
  }

  KBStatus(const KBStatus& other) {
    if (other.state_ != NULL) {
      state_ = new State;
      state_->code = other.state_->code;
      state_->msg = other.state_->msg;
    } else {
      DeleteState();
    }
  }

  KBStatus& operator=(const KBStatus& other) {
    if (this == &other){
      return *this;
    }
    if (other.state_ != NULL) {
      state_ = new State;
      state_->code = other.state_->code;
      state_->msg = other.state_->msg;
    } else {
      DeleteState();
    }
    return *this;
  }
//  bool operator==(const char* msg) {
//    if ((*this).state_ != nullptr)
//      return (*this).state_->msg.compare(msg) == 0;
//    return false;
//  }
//  bool operator!=(const char* msg) {
//    if ((*this).state_ != nullptr)
//      return (*this).state_->msg.compare(msg) != 0;
//    return true;
//  }

  ~KBStatus() noexcept {
    // ARROW-2400: On certain compilers, splitting off the slow path improves
    // performance significantly.
    if ((state_ != NULL)) {
      DeleteState();
    }
  }

  void DeleteState() {
    if (state_ != NULL) {
      delete state_;
      state_ = NULLPTR;
    }
  }

  bool isOK() { return state_ == NULLPTR || state_->code == StatusCode::OK; };

  bool isNotOK() { return state_ != NULLPTR && state_->code != StatusCode::OK; };

  bool isError(StatusCode code) { return state_ != NULLPTR && state_->code == code; };

  bool isNotFound() { return isError(StatusCode::NotFound); };

  bool isExpired() { return isError(StatusCode::ExpiredError); };


  StatusCode code() {
    if (state_ == NULL)
      return StatusCode::UnknownError;
    else
      return state_->code;
  }
  std::string message() {
    if (state_ == NULL)
      return "";
    else
      return state_->msg;
  }

 public:
  /// Return a success status
  static KBStatus OK() { return KBStatus(); }

  static KBStatus ERROR(StatusCode code, const char* fmt, ...) {
    char buf[512];
    {
      va_list args;
      va_start(args, fmt);
      vsnprintf(buf, 512, fmt, args);
      va_end(args);
    }
    return KBStatus(code, buf);
  }

  static KBStatus NOT_FOUND(const std::string& msg) { return KBStatus(StatusCode::NotFound, msg); }

  static KBStatus ALREADY_EXISTS(const std::string& msg) { return KBStatus(StatusCode::AlreadyExists, msg); }

  static KBStatus NOT_IMPLEMENTED(const std::string& msg) { return KBStatus(StatusCode::NotImplemented, msg); }

  static KBStatus CapacityError(const std::string& msg) { return KBStatus(StatusCode::CapacityError, msg); }

  static KBStatus ExpiredError(const std::string& msg) { return KBStatus(StatusCode::ExpiredError, msg); }

  static KBStatus NotFound(const std::string& msg) { return KBStatus(StatusCode::NotFound, msg); }

  static KBStatus IOError(const std::string& msg) { return KBStatus(StatusCode::IOError, msg); }

  static KBStatus Invalid(const std::string& msg) { return KBStatus(StatusCode::Invalid, msg); }

  static KBStatus InternalError(const std::string& msg) { return KBStatus(StatusCode::InternalError, msg); }

 private:
  struct State {
    StatusCode code;
    std::string msg;
    //std::shared_ptr<StatusDetail> detail;
  };
  // OK status has a `NULL` state_.  Otherwise, `state_` points to
  // a `State` structure containing the error code and message(s)
  State* state_ = NULL;
};

static std::string IOErrorMsg(const std::string& context,
                              const std::string& file_name) {
  if (file_name.empty()) {
    return context;
  }
  return context + ": " + file_name;
}

static KBStatus IOError(const std::string& context, const std::string& file_name,
                        int err_number) {
  switch (err_number) {
    case ENOSPC:return KBStatus::IOError("NoSpace :" + file_name);
    case ESTALE:return KBStatus::IOError("kStaleFile");
    case ENOENT:return KBStatus::IOError("PathNotFound :" + file_name);
    default:return KBStatus::IOError("IOERROR : " + file_name);
  }
}
}