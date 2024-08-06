#!/bin/bash
# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#
# This software (KWDB) is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

node_count=${1:-5}
node_name_prefix=${2:-cluster}

source $QA_DIR/util/utils.sh
clear_errlog

echo_info "start ${node_name_prefix}[1..${node_count}]"
start_cluster_single_replica ${node_count} ${node_name_prefix}

if [ $? -ne 0 ];then
  echo_err "${node_name} start failed!"
  exit 1
fi
exit 0
