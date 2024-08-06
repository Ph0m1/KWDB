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


#pragma once

#include <fcntl.h>
#include <cstdlib>
#include <string>
#include <utility>
#include "BigObjectError.h"


void MMapLock();
void MMapUnLock();

/**
 * A memory mapped object class.
 * a file is mapped into memory via memory mapped function.
 */
class MMapFile {

 protected:
  void *mem_;  // memory mapped address.
  off_t file_length_;
  off_t new_length_;
  std::string file_path_;  // file path
  std::string db_path_;
  std::string tbl_sub_path_;
  int flags_;

  void LogMMapFileError(const string &op);

  int open();

  int reportError();

  string fileFolder(const string &db);

  void swap(MMapFile &rhs);

  inline bool isMapFailed() {  return file_length_ != new_length_;}

  std::string real_file_path_;
 public:
  /**
   * @brief	a default constructor.
   */
  MMapFile();

  /**
   * @brief	a default constructor.
   *
   * @param	url		file name URL
   */
//    MMapFile(const std::string &url, const std::string &tbl_sub_path);
  /**
   * @brief	a default dstructor.
   */
  virtual ~MMapFile();

  int open(const std::string &file_path, int flags);

  // open file in the home directory
  /**
   * @brief	open and memory map a file.
   * @param	flag		O_CREAT to create file or 0.
   * @return	>= 0 if succeed, otherwise -1.
   */
  int open(const std::string &url, const std::string &db_path, const std::string &sand_box, int flag);

  int open(const string &url, const std::string &db_path, const std::string &sand_box, int flags, size_t init_sz,
    ErrorInfo &err_info);

  /**
   * @brief	open and memory map a file.
   *
   * @param	fd		file descriptor
   * @return	returns zero on success.  On error, -1 is returned.
   */
  int munmap();

  /**
   * @brief	memory remap a file.
   * @param	length	the requested file/mmap length.
   * @return	mapped memory address if succeeds.
   * 			-1 otherwise.
   */
  int mremap(size_t length);

  int incLength(size_t cur_len, size_t data_len, ErrorInfo &err_info);

  int sync(int flags);

  int resize(size_t length);

  inline void* memAddr() const { return mem_; }

  off_t fileLen() const { return file_length_; }

  std::string filePath() const { return file_path_; }

  std::string& filePath() { return file_path_; }

  std::string realFilePath() const;

  int remove();

  int rename(const string &new_fp);

  int madviseWillNeed(size_t len);

  int flags() const { return flags_; }

  void setFlags(int flags) { flags_ = flags; }

  bool readOnly() const { return !(flags_ & O_RDWR); }
};
