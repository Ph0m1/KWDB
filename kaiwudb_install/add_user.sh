#!/bin/bash

g_deploy_path=$(cd $(dirname $0);pwd)
g_cur_usr=$(whoami)
source $g_deploy_path/utils/kaiwudb_common.sh
source $g_deploy_path/utils/kaiwudb_log.sh

log_init $g_deploy_path $g_cur_usr

local_privileged

if ! $(install_check  >/dev/null 2>&1);then
  log_err "KaiwuDB does not exist. Please install KaiwuDB first."
  exit 1
fi

if ! $(kw_status >/dev/null 2>&1);then
  # must be running
  log_err "KaiwuDB is not runnning."
  exit 1
fi

function add_user() {
  local cmd=""
  local ret=""
  read -t60 -p "Please enter the username:" -e user_name
  if [ "$(secure_opt)" != "--insecure" ];then
    read -s -t60 -p "Please enter the password: " -n20 user_password
    echo -e "\\n"
  fi
  if [ "$(install_type)" = "bare" ];then
    cd /usr/local/kaiwudb/bin
    if [ "$(secure_opt)" != "--insecure" ];then
      cmd="$local_cmd_prefix -u $(user_name) bash -c \"./kwbase sql --host=127.0.0.1:$(local_port) $(secure_opt) -e \\\"create user $user_name with password \\\\\\\"$user_password\\\\\\\";grant admin to $user_name with admin option;\\\"\""
    else
      cmd="$local_cmd_prefix -u $(user_name) bash -c \"./kwbase sql --host=127.0.0.1:$(local_port) $(secure_opt) -e \\\"create user $user_name;grant admin to $user_name with admin option;\\\"\""
    fi
    ret=$(eval $cmd 2>&1)
    if [ $? -ne 0 ];then
      log_err "User create failed. $ret"
      exit 1
    fi
  elif [ "$(install_type)" = "container" ];then
    if [ "$(secure_opt)" != "--insecure" ];then
      ret=$(docker exec kaiwudb-container bash -c "./kwbase sql $(secure_opt) -e \"create user $user_name with password \\\"$user_password\\\";grant admin to $user_name with admin option;\"" 2>&1)
    else
      ret=$(docker exec kaiwudb-container bash -c "./kwbase sql $(secure_opt) -e \"create user $user_name;grant admin to $user_name with admin option;\"" 2>&1)
    fi
    if [ $? -ne 0 ];then
      log_err "User create failed. $ret"
      exit 1
    fi
  fi
}

add_user
echo -e "\e[1;32m[ADD USER COMPLETED]:\e[0mUser creation completed."
exit 0
