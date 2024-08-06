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

all_alived=0
if [[ ${topology} =~ ([0-9]+)([a-zA-Z]+.*) ]];then
  case ${BASH_REMATCH[2]} in
    n)
      all_alived=$(check_kwbase_alive ${BASH_REMATCH[2]}${BASH_REMATCH[1]})
    ;;
    c|cr)
      nodes_status=""
      expect_nodes_status=""
      for idx in $(seq 1 ${BASH_REMATCH[1]});do
        nodes_status+="$(check_kwbase_alive ${BASH_REMATCH[2]}${idx}),"
        expect_nodes_status+="1,"
      done
      if [ "${expect_nodes_status}" = "${nodes_status}" ];then
        all_alived=1
      else
        all_alived=0
      fi
    ;;
    *)
      echo_err "check_all_alived_basic_v2: unrecognizable mode: ${BASH_REMATCH[2]}"
      exit 1
    ;;
  esac
else
  echo_err "check_all_alived_basic_v2: unrecognizable topology: ${topology}"
  exit 1
fi

if [ "1" = "${all_alived}" ];then
  exit 0
else
  exit 1
fi


