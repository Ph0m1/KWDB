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

#include <unistd.h>
#include <sys/mman.h>
#include <iostream>
#include "objcntl.h"
#include "mmap/MMapFile.h"
#include "BigObjectConfig.h"
#include "BigObjectError.h"
#include "BigObjectUtils.h"
#include "lg_api.h"

MMapFile::MMapFile() {
  mem_ = 0;
  file_length_ = 0;
  new_length_ = 1;
  flags_ = 0;
}

MMapFile::~MMapFile() { munmap(); }

int MMapFile::reportError() {
  int err_code = errnoToErrorCode();
  if (err_code == BOEOTHER)
    return BOEMMAP;
  return err_code;
}

std::string MMapFile::fileFolder(const std::string &db)
{ return BigObjectConfig::home() + db; }

void MMapFile::swap(MMapFile &rhs) {
    std::swap(real_file_path_, rhs.real_file_path_);
    std::swap(mem_, rhs.mem_);
    std::swap(file_length_, rhs.file_length_);
    std::swap(new_length_, rhs.new_length_);
    std::swap(file_path_, rhs.file_path_);
    std::swap(tbl_sub_path_, rhs.tbl_sub_path_);
    std::swap(flags_, rhs.flags_);
}

void MMapFile::LogMMapFileError(const std::string &op) {
  if ((BigObjectConfig::logging() >= BO_LOG_FATAL)) {
    LOG_ERROR("%lx [KWFTL] %s failed: os system err_no %d, \"%s\", len: %lu\n", pthread_self(),
              op.c_str(), errno, real_file_path_.c_str(), file_length_);
  }
}

int MMapFile::open() {
  int fd;
  if (flags_ & (O_ANONYMOUS|O_MATERIALIZATION))
    return 0;

  if ((fd = ::open(real_file_path_.c_str(), flags_,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)) < 0) {
    if (errno == EROFS) {
      flags_ = (flags_ & ~O_RDWR);
      if ((fd = ::open(real_file_path_.c_str(), flags_,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)) < 0) {
open_error:
        mem_ = 0;
        return reportError();
      }
    } else {
      goto open_error;
    }
  }
  file_length_ = lseek(fd, 0, SEEK_END);

  if (file_length_ != 0 && file_length_ != (off_t)-1) {
    int mmap_flags = (readOnly()) ?
      PROT_READ : PROT_READ | PROT_WRITE;
//    mmap_mtx.lock();
    mem_ = mmap(0, file_length_, mmap_flags, MAP_SHARED, fd, 0);
//    mmap_mtx.unlock();
  }
  close(fd);
  if (mem_ == MAP_FAILED) {
    mem_ = nullptr;
    LogMMapFileError("mmap");
    return reportError();
  }
#if defined(DEBUG_MMAP)
  if ((BigObjectConfig::logging() > BO_LOG_FINEST)) {
    LOG_ERROR("%lx [KWMEM] %p (%ld) mmap : \"%s\", db:\"%s\"\n", pthread_self(),
      mem_, file_length_, file_path_.c_str(), tbl_sub_path_.c_str());
  }
#endif
  new_length_ = file_length_;
  return 0;
}

int MMapFile::open(const std::string &file_path, int flags) {
  file_path_ = file_path;
  real_file_path_ = file_path_;
  flags_ = flags;
  return open();
}

int MMapFile::open(const std::string &file_path, const std::string &db_path, const std::string &tbl_sub_path,
  int flags) {
  file_path_ = file_path;
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  flags_ = flags;
  real_file_path_ = db_path_ + tbl_sub_path_ + file_path_;

  return open();
}

int MMapFile::open(const string &url, const std::string &db_path, const string &tbl_sub_path, int flags,
  size_t init_sz, ErrorInfo &err_info) {
  err_info.errcode = open(url, db_path, tbl_sub_path, flags);
  if (err_info.errcode < 0)
    return err_info.errcode;
  if (file_length_ < (off_t)init_sz) {
    err_info.errcode = mremap(getPageOffset(init_sz));
  }
  return err_info.errcode;
}

int MMapFile::munmap() {
  int err_code = 0;
  if (mem_) {
//    mmap_mtx.lock();
    err_code = ::munmap(mem_, file_length_);
//    mmap_mtx.unlock();
     if (flags_ & O_ANONYMOUS) {
      bo_used_anon_memory_size.fetch_sub(file_length_);
     }
#if defined(DEBUG_MMAP)
    if ((BigObjectConfig::logging() > BO_LOG_FINEST)) {
      LOG_ERROR("%lx [KWMEM] %p (%ld) munmap : \"%s\", db:\"%s\"\n", pthread_self(),
                mem_, file_length_, file_path_.c_str(), tbl_sub_path_.c_str());
    }
#endif
    mem_ = 0;
    file_length_ = 0;
    new_length_ = 1;
  }
  return err_code;
}

int MMapFile::mremap(size_t length) {
    if (length && (length < static_cast<size_t>(file_length_)))
        return 0;
    int err_code = resize(length);
    if (err_code == BOENOMEM) {
        ErrorInfo err_info;
        err_info.setError(BOENOMEM);
        LOG_FATAL("remmap file faild. %s", this->file_path_.c_str());
    }
    return err_code;
}

int MMapFile::incLength(size_t cur_len, size_t data_len, ErrorInfo &err_info) {
  if (data_len > static_cast<size_t>(fileLen()) - cur_len) {
    size_t new_len = getPageOffset(cur_len + data_len);
    if ((err_info.errcode = mremap(new_len)) < 0)
      err_info.setError(err_info.errcode);
  }
  return err_info.errcode;
}

int MMapFile::sync(int flags) {
    if (mem_ && !(flags_ & O_ANONYMOUS)) {
        return msync(mem_, file_length_, flags);
    }
    return 0;
}


int MMapFile::resize(size_t length) {
  // TODO(zhuderun): remove in the future
  if ((flags_ & O_ANONYMOUS) && (bo_used_anon_memory_size.load() + length)> BigObjectConfig::max_anon_memory_size()) {
    // check if ANON memory reach limit.
    LogMMapFileError("resize new "+ std::to_string(length) + " bytes");
    return BOENOMEM;
  }
  new_length_ = length;
  void *mem_tmp = nullptr;
  if (!(flags_ & O_ANONYMOUS)) {
    int fd;
    if ((fd = ::open(real_file_path_.c_str(), O_RDWR | O_CREAT,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)) < 0) {
      // mem_ = 0;  failed, reuse original address, make sure no core.
      LogMMapFileError("open file");
      return reportError();
    }
    // posix_fallocate is able to detect disk full.
    int err_code;
#if defined (__ANDROID__)
    if (ftruncate(fd, length) < 0) {
#else
    if (length >= file_length_) {
      if ((err_code = posix_fallocate(fd, 0, length)) != 0) {
        close(fd);
        LogMMapFileError("resize file");
        return errnumToErrorCode(err_code);
      }
    } else {
      if (ftruncate(fd, length) < 0) {
        close(fd);
        LogMMapFileError("resize file");
        return errnumToErrorCode(err_code);
      }
    }
#endif
    if (length) {
#if !defined(__x86_64__) && !defined(_M_X64)
      munmap();
#endif
//  mmap_mtx.lock();
      mem_tmp = (!mem_) ?
          mmap(0, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) :
          ::mremap(mem_, file_length_, length, MREMAP_MAYMOVE);
//  mmap_mtx.unlock();
    }
    close(fd);
  } else {  // anonymous memory
//    mmap_mtx.lock();
    mem_tmp = (!mem_) ? mmap(0, length, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0) :
      ::mremap(mem_, file_length_, length, MREMAP_MAYMOVE);
//    mmap_mtx.unlock();
  }
  if (mem_tmp == MAP_FAILED) {
    // mem_ = 0;
    LogMMapFileError("remap");
    return reportError();
  }
#if defined(DEBUG_MMAP)
  if ((BigObjectConfig::logging() > BO_LOG_FINEST)) {
    LOG_ERROR("%lx [KWMEM] %p (%ld:%ld) remap: \"%s\", db:\"%s\"\n", pthread_self(),
              mem_, file_length_, length, file_path_.c_str(), tbl_sub_path_.c_str());
  }
#endif
  if (flags_ & O_ANONYMOUS) {
    // Only incremental can be recorded
    bo_used_anon_memory_size.fetch_add(length - file_length_);
  }
  mem_ = mem_tmp;
  file_length_ = length;
  return 0;
}

std::string MMapFile::realFilePath() const {
    return real_file_path_;
}

int MMapFile::remove() {
  int err_code = 0;
  munmap();
  if (!real_file_path_.empty()) {
    err_code = ::remove(real_file_path_.c_str());
    real_file_path_.clear();
  }
  return err_code;
}

int MMapFile::rename(const string &new_fp) {
  int err_code = 0;
  if (flags_ & O_ANONYMOUS) {
    real_file_path_ = new_fp;
  } else {
    munmap();
    if (!real_file_path_.empty()) {
      err_code = ::rename(real_file_path_.c_str(), new_fp.c_str());
      real_file_path_ = new_fp;
//      real_file_path_.clear();
      if (err_code != 0)
        err_code = errnoToErrorCode();
    }
  }
  return err_code;
}


int MMapFile::madviseWillNeed(size_t len) {
    return madvise(mem_, len, MADV_WILLNEED);
}

