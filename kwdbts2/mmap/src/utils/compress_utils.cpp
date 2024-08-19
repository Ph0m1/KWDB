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

#include <shared_mutex>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include "utils/compress_utils.h"
#include "utils/big_table_utils.h"
#include "sys_utils.h"
#include "mmap/mmap_big_table.h"

namespace kwdbts {

std::string sudo_cmd = " ";

int g_max_mount_cnt_ = 1000;

atomic<int> g_cur_mount_cnt_ = 0;

bool compress(const string& db_path, const string& tbl_sub_path, const string& dir_name, ErrorInfo& err_info) {
  string dir_path = db_path + tbl_sub_path + dir_name;
  struct stat st;
  if ((stat(dir_path.c_str(), &st) == 0) && (S_ISDIR(st.st_mode))) {
    // if current directory is mounted. return success
    struct statfs sfs;
    int ret = statfs(dir_path.c_str(), &sfs);
    if (!ret && sfs.f_type == SQUASHFS_MAGIC) {
      return true;
    }
    string file_path = dir_path + ".sqfs";
    // new create sqfs postfix with tmp, not in effect.
    string file_path_tmp = file_path + "_tmp";
    // delete sqfs if exists. sqfs may not right.
    if (IsExists(file_path_tmp)) {
      Remove(file_path_tmp);
    }
    string cmd = "nice -n 5 mksquashfs " + dir_path + ' ' + file_path_tmp
          + " -mem 16M -Xcompression-level 6 -processors 1 > /dev/null 2>&1";
    if (System(cmd)) {
      cmd = "mv " + file_path_tmp + " " + file_path;
      if (!System(cmd)) {
        err_info.errcode = KWEOTHER;
        err_info.errmsg = "mv .sqfs_tmp .sqfs failed";
        Remove(file_path_tmp);
        return false;
      }
    } else {
      err_info.errcode = KWEOTHER;
      err_info.errmsg = "mksquashfs failed";
      return false;
    }
  } else {
    err_info.errcode = KWENOOBJ;
    err_info.errmsg = db_path + " is not directory";
    return false;
  }
  return true;
}

void initSudo() {
  if (sudo_cmd == " ") {
    if (getuid() == 0) {
      sudo_cmd = "";
    } else {
      sudo_cmd = "sudo ";
    }
  }
}

void getErrorInfo(string cmd, string dir_path, string log_file_name) {
  if (!cmd.empty()) {
    cmd += " >> ./" + log_file_name + " 2>&1;";
  }
  cmd += " echo $? >> ./" + log_file_name + " 2>&1;"
      + " df -h >> ./" + log_file_name + " 2>&1;"
      + " ls -l " + dir_path + " >> ./" + log_file_name + " 2>&1";
  System(cmd);
}

bool mount(const string& sqfs_file_path, const string& dir_path, ErrorInfo& err_info) {
  initSudo();
  struct stat buffer;
  if (stat(sqfs_file_path.c_str(), &buffer) != 0) {
    err_info.errcode = KWENOOBJ;
    err_info.errmsg = sqfs_file_path + " is not exist";
    return false;
  }
  // check mount directory exists.
  struct stat st;
  if ((stat(dir_path.c_str(), &st) == 0) && (S_ISDIR(st.st_mode))) {
    struct statfs sfs;
    int ret = statfs(dir_path.c_str(), &sfs);
    if (!ret && sfs.f_type == SQUASHFS_MAGIC) {
      return true;
    } else {
      if (!Remove(dir_path, err_info)) {
        return false;
      }
    }
  }
  // create mount directory
  if (!MakeDirectory(dir_path, err_info)) {
    return false;
  }
  // mount sqfs file
  int retry = 5;
  std::string cmd  = sudo_cmd + "mount -o noatime,nodiratime -t squashfs " + sqfs_file_path + " " + dir_path;
  while(retry > 0 && !System(cmd)) {
    sleep(1);
    --retry;
  }
  if (!isMounted(dir_path)) {
    getErrorInfo(cmd, dir_path, "_log_mount_err");
    err_info.errcode = KWEOTHER;
    err_info.errmsg = sqfs_file_path + " mount failed";
    if (!Remove(dir_path)) {
      err_info.errcode = KWEEXIST;
      err_info.errmsg = sqfs_file_path + " mount failed, " + dir_path + " rm failed";
    }
    LOG_ERROR("mount failed. If it's a docker environment, check if /dev:/dev is mapped")
    abort();
  }

  g_cur_mount_cnt_++;
  return true;
}

bool umount(const string& sqfs_file_path, const string& dir_name, ErrorInfo& err_info) {
  initSudo();
  string dir_path = sqfs_file_path + dir_name;
  struct stat st;
  if ((stat(dir_path.c_str(), &st) == 0) && (S_ISDIR(st.st_mode))) {
    struct statfs sfs;
    int ret = statfs(dir_path.c_str(), &sfs);
    if (!ret && sfs.f_type == SQUASHFS_MAGIC) {
      int retry = 5;
      string cmd  = sudo_cmd + "umount " + dir_path;
      while(retry > 0 && !System(cmd)) {
        sleep(1);
        --retry;
      }
      if (isMounted(dir_path)) {
        getErrorInfo(cmd, dir_path, "_log_umount_err");
        err_info.errcode = KWEOTHER;
        err_info.errmsg = dir_path + " umount failed";
        return false;
      } else {
        g_cur_mount_cnt_--;
        if (!Remove(dir_path)) {
          err_info.errcode = KWEOTHER;
          err_info.errmsg = dir_path + " umount succeed, rm failed";
          return false;
        }
      }
    }
  }
  return true;
}

bool isMounted(const string& dir_path) {
  struct stat st;
  if ((stat(dir_path.c_str(), &st) == 0) && (S_ISDIR(st.st_mode))) {
    struct statfs sfs;
    int ret = statfs(dir_path.c_str(), &sfs);
    if (!ret && sfs.f_type == SQUASHFS_MAGIC) {
      return true;
    }
  }
  return false;
}

int executeShell(const std::string& cmd, std::string &result) {
  result = {};
  FILE *ptr = nullptr;
  int ret = -1;
  if ((ptr = popen(cmd.c_str(), "r")) != nullptr) {
    char buf_temp[1024] = {0};
    while(fgets(buf_temp, sizeof(buf_temp), ptr) != nullptr) {
      result += buf_temp;
    }
    pclose(ptr);
    ptr = nullptr;
    ret = 0;
  } else {
    ret = -1;
  }
  return ret;
}

} // namespace kwdbts
