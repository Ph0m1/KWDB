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
  echo_info "clean ${topology} before start."
  $QA_DIR/util/clean_basic_v2.sh ${topology}
  if [ 0 -ne $? ];then
    echo_err "clean ${topology} failed before setup."
    exit 1
  fi
  case ${BASH_REMATCH[2]} in
    n)
      start_single_node ${BASH_REMATCH[2]}${BASH_REMATCH[1]}
    ;;
    c)
      start_cluster ${BASH_REMATCH[1]} ${BASH_REMATCH[2]}
    ;;
    cr)
      start_cluster_replica ${BASH_REMATCH[1]} ${BASH_REMATCH[2]}
    ;;
    *)
      echo_err "setup_basic_v2: unrecognizable mode: ${BASH_REMATCH[2]}"
      exit 1
    ;;
  esac
  $QA_DIR/util/check_all_alived_basic_v2.sh ${topology}
  if [ 0 -ne $? ];then
    echo_err "Checking start failed ${topology} after setup."
    exit 1
  fi
else
  echo_err "setup_basic_v2: unrecognizable topology: ${topology}"
  exit 1
fi

