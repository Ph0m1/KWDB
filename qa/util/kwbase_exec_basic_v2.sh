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
test_file=${2}
output_file=${3}

if [[ ${topology} =~ ([0-9]+)([a-zA-Z]+.*) ]];then
  exec_node=""
  case ${BASH_REMATCH[2]} in
    n)
      exec_node="${BASH_REMATCH[2]}${BASH_REMATCH[1]}"
    ;;
    c|cr)
      exec_node="${BASH_REMATCH[2]}1"
    ;;
    *)
      echo_err "kwbase_exec_basic_v2: unrecognizable mode: ${BASH_REMATCH[2]}"
      exit 1
    ;;
  esac
  kwbase_exec_on ${exec_node} ${test_file} ${output_file}
else
  echo_err "kwbase_exec_basic_v2: unrecognizable topology: ${topology}"
  exit 1
fi