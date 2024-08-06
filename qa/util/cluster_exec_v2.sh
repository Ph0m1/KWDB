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

sql_file=${2}
output_file=${3}

node_annotation_regex='^\s*--\s*node:\s*(.+)'
exec_node_sed_regex='^\s*--[-[[:space:]]]*node:\s*(.+)'

declare -a stmts


declare -a topology_nodes
if [[ "${topology}" =~ ([0-9]+)([a-zA-Z]+) ]];then
  case ${BASH_REMATCH[2]} in
    c|cr)
      topology_nodes=($(seq -f "${BASH_REMATCH[2]}%g" 1 ${BASH_REMATCH[1]}))
    ;;
    *)
      echo_err "cluster_exec_v2: unknown cluster type: ${BASH_REMATCH[2]}"
    ;;
  esac
else
  echo_err "cluster_exec_v2: unrecognizable topology - ${topology}"
  exit 1
fi

declare -a target_nodes
last_section=([0]="" [1]="")

while read -r line || [ -n "${line}" ];do
  if [[ "${line}" =~ ${node_annotation_regex} ]];then
    if [ -n "${last_section[0]}" ] && [ -n "${last_section[1]}" ];then
      stmts+=("--node:${last_section[0]}\n${last_section[1]}")
      last_section=([0]="" [1]="")
    fi
    for node in ${BASH_REMATCH[1]};do
      if [[ " ${topology_nodes[*]} " =~ [[:space:]]${node}[[:space:]] ]];then
        last_section[0]="${last_section[0]} ${node}"
      fi
    done
  fi
  if [[ ! "${line}" =~ ^\s*--.* ]];then
    last_section[1]+="${line%;*};\n"
  fi
done < ${sql_file}

[ -n "${last_section[0]}" -a -n "${last_section[1]}" ] && \
  stmts+=("--node:${last_section[0]}\n${last_section[1]}")

for idx in ${!stmts[@]};do
  stmt=${stmts[${idx}]}
  if [ -n "${stmt}" ];then
    nodes=$(echo -e "${stmt}" | sed -n -re "s/${node_annotation_regex}/\1/p")
    clean_stmt="$(echo -e "${stmt}" | grep -v -P '^\s*--.*')"
    for node in ${nodes};do
      ${KWBIN} sql --host=$(get_listen_addr ${node}) --insecure \
        --format=table --set "errexit=false" --echo-sql -e "${clean_stmt}" \
        2>&1 | tee -a ${output_file} > /dev/null
    done
  fi
done