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


#pragma once

#include <string>
#include "lg_api.h"
#include "ts_object_error.h"

extern int64_t g_free_space_alert_threshold;

/*
 * @Description: Check whether the file or directory exists
 * @IN: path: the path of file or directory
 * @Return: false/true
 */
bool IsExists(const string& path);

/*
 * @Description: remove file or directory
 * @IN: path: the path of file or directory
 * @Return: false/true
 */
bool Remove(const string& path, ErrorInfo& error_info = getDummyErrorInfo());

/*
 * @Description: recursively create a directory (mkdir -p xxx)
 * @IN: path: the path of directory
 * @Return: true/false
 */
bool MakeDirectory(const string& dir_path, ErrorInfo& error_info = getDummyErrorInfo());

/*
 * @Description: get the modify time of file
 * @IN: filePath: the path of file
 * @Return: modify time
 */
std::time_t ModifyTime(const std::string& filePath);

/*
 * @Description: call system()
 * @IN: cmd: shell command
 * @Return: true/false
 */
bool System(const string& cmd, ErrorInfo& error_info = getDummyErrorInfo());

bool DirExists(const std::string& path);

int64_t GetDiskFreeSpace(const std::string& path);

bool IsDiskSpaceEnough(const std::string& path);
