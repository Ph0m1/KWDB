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

topology=${1}

if [[ ${topology} =~ ([0-9]+)([a-zA-Z]+.*) ]];then
  case ${BASH_REMATCH[2]} in
    n)
      stop_node_v2 ${BASH_REMATCH[2]}${BASH_REMATCH[1]}
    ;;
    c|cr)
      for idx in $(seq ${BASH_REMATCH[1]} -1 1);do
        stop_node_v2 ${BASH_REMATCH[2]}${idx}
      done
    ;;
    *)
      echo_err "stop_basic_v2: unrecognizable mode: ${BASH_REMATCH[2]}"
      exit 1
    ;;
  esac
  $QA_DIR/util/check_all_alived_basic_v2.sh ${topology}
  if [ 0 -eq $? ];then
    echo_err "topology ${topology} also alived after stop."
    exit 1
  fi
else
  echo_err "stop_basic_v2: unrecognizable topology: ${topology}"
  exit 1
fi
echo_succ "stop ${topology} all nodes success."