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

source $QA_DIR/util/utils.sh

topology=${1:-5c}

if [[ ${topology} =~ ([0-9]+)([a-zA-Z]+.*) ]];then
  for idx in $(seq 1 ${BASH_REMATCH[1]});do
    echo_info "stop ${BASH_REMATCH[2]}${idx}"
    if [ "1" != "$(check_kwbase_alive ${BASH_REMATCH[2]}${idx})" ];then
      echo_err "Failed shutdown: ${BASH_REMATCH[2]}${idx} - node unavailable)"
      force_clean_up
      exit 1
    fi
    stop_cluster_node ${BASH_REMATCH[2]}${idx}
    if [ "1" = "$(check_kwbase_alive ${BASH_REMATCH[2]}${idx})" ];then
      force_clean_up
      echo_err "Failed shutdown: ${BASH_REMATCH[2]}${idx} - node unavailable)"
      exit 1
    fi
  done
else
  echo_err "shutdown_cluster_v2: unrecognizable topology"
  force_clean_up
  exit 1
fi

echo_info "shutdown ${topology} succeed!"
exit 0
