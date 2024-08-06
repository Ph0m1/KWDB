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

topology=${1:-1e}

if [ "1" != "$(check_kwbase_alive ${topology})" ] ; then
  echo_err "shutdown_single_node_v2: Failed shutdown: ${topology} - \
    node unavailable)"
  force_clean_up
  exit 1
fi

#shutdown
echo_info "stop ${topology}"
stop_single_node ${topology}

if [ "1" = "$(check_kwbase_alive ${topology})" ]; then
  echo_err "shutdown_single_node_v2: Failed shutdown: ${topology}"
  force_clean_up
  exit 1
fi

echo_info "shutdown ${topology} succeed!"
exit 0
