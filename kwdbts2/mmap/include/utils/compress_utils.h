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

#include <string>
#include <atomic>
#include <vector>
#include "ts_object_error.h"
#include "data_type.h"
#include "kwdb_consts.h"

using namespace std;

namespace kwdbts {

extern std::string sudo_cmd;

extern int g_max_mount_cnt_;

extern atomic<int> g_cur_mount_cnt_;

bool compress(const string& db_path, const string& tbl_sub_path, const string& dir_name, ErrorInfo& err_info);

void initSudo();

bool mount(const string& sqfs_file_path, const string& dir_name, ErrorInfo& err_info);

bool umount(const string& sqfs_file_path, const string& dir_name, ErrorInfo& err_info);

bool isMounted(const string& dir_path);

int executeShell(const std::string& cmd, std::string &result);

} // namespace kwdbts
