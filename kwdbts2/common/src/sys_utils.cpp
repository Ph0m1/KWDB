// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "sys_utils.h"

#include <unistd.h>
#include <sys/stat.h>
#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif

bool IsExists(const string& path) {
  if (access(path.c_str(), F_OK) != 0) {
    return false;
  } else {
    return true;
  }
}

bool Remove(const string& path, ErrorInfo& error_info) {
  try {
    if (!fs::remove_all(path.c_str()) && errno != 0) {
      error_info.errcode = errnumToErrorCode(errno);
      error_info.errmsg = strerror(errno);
      LOG_ERROR("%s remove failed: errno[%d], strerror[%s]", path.c_str(), errno, error_info.errmsg.c_str());
      return false;
    }
  } catch (const std::exception& e) {
    LOG_ERROR("%s remove failed: errno message[%s]", path.c_str(), e.what());
    return false;
  }
  return true;
}

bool MakeDirectory(const string& dir_path, ErrorInfo& error_info) {
  struct stat st;
  size_t e_pos = 1;
  char *path = const_cast<char *>(dir_path.data());
  while (e_pos < dir_path.size()) {
    e_pos = dir_path.find_first_of('/', e_pos);
    if (e_pos != string::npos)
      path[e_pos] = 0;
    if (stat(path, &st) != 0) {
      if (mkdir(path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) < 0) {
        error_info.errcode = errnumToErrorCode(errno);
        error_info.errmsg = strerror(errno);
        LOG_ERROR("mkdir [%s] failed: errno[%d], strerror[%s]", path, errno, error_info.errmsg.c_str());
        return false;
      }
    } else {
      if (!S_ISDIR(st.st_mode)) {
        error_info.errcode = KWEOTHER;
        error_info.errmsg = std::string(path) + " is not directory";
        LOG_ERROR("mkdir [%s] failed: %s", path, error_info.errmsg.c_str());
        return false;
      }
    }
    if (e_pos != string::npos)
      path[e_pos] = '/';
    else
      break;
    e_pos++;
  }
  return true;
}

std::time_t ModifyTime(const std::string& filePath) {
  struct stat fileInfo;
  if (stat(filePath.c_str(), &fileInfo) != 0) {
    return 0;
  }
  return fileInfo.st_mtime;
}

bool System(const string& cmd, ErrorInfo& error_info) {
  int status = system(cmd.c_str());
  if (WIFEXITED(status)) {
    auto exit_code = WEXITSTATUS(status);
    if (exit_code == 0) {
      return true;
    }
    if (exit_code == 1) {
      LOG_WARN("system(%s) exit code is not 0: status[%d], exit_code[%d], errno[%d], strerror[%s]",
               cmd.c_str(), status, exit_code, errno, strerror(errno));
      return true;
    }
    LOG_ERROR("system(%s) failed: status[%d], exit_code[%d], errno[%d], strerror[%s]",
              cmd.c_str(), status, exit_code, errno, strerror(errno));
    return false;
  }

  if (status == -1) {
    cerr << "OS system fork error." << std::endl;
    return false;
  }
  char msg[1024];
  snprintf(msg, sizeof(msg), "exec-shell [%s] faild. errno[%d], shell exit code[%d,%d(%d),%d], cmd exit code[%d].",
                cmd.c_str(), errno, WIFEXITED(status), WIFSIGNALED(status), WTERMSIG(status),
                WIFSTOPPED(status), WEXITSTATUS(status));
  cerr << msg << std::endl;
  error_info.errcode = errnumToErrorCode(errno);
  error_info.errmsg = strerror(errno);
  LOG_ERROR("system(%s) failed: errno[%d], strerror[%s]", cmd.c_str(), errno, error_info.errmsg.c_str());
  return false;
}
