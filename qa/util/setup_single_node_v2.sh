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

node_name=${1:-node1}

source $QA_DIR/util/utils.sh
clear_errlog

echo_info "start ${node_name}"
start_single_node ${node_name}

started=false
for i in {0..30};do
  if [ "1" = "$(check_kwbase_alive ${node_name})" ];then
    started=true; break
  fi
  sleep 2
done

if [ "false" = "${started}" ];then
  echo_err "${node_name} start failed!"
  exit 1
else
  exit 0
fi
