#!/bin/bash

g_deploy_path=$(cd $(dirname $0);pwd)
g_cur_usr=`whoami`
source $g_deploy_path/utils/kaiwudb_common.sh
source $g_deploy_path/utils/kaiwudb_log.sh

# Whether the current user is root, if not, input passwd
if [ "$g_cur_usr" != "root" ];then
  # passwd-free check
  timeout --foreground -k 3 3s sudo -s -p "" exit > /dev/null 2>&1
  if [ $? -ne 0 ];then
    read -e -s -t60 -p "${g_cur_usr}'s password: " g_passwd
    echo -e "\\n"
    kw_cmd_prefix="echo $g_passwd | sudo -S -p \"\""
    echo "$g_passwd" | sudo -S -p "" -k -s >/dev/null 2>&1
    if [ $? -ne 0 ];then
      echo -e "\033[31m[ERROR]\033[0m Incorrect password."
      exit 1
    fi
  else
    kw_cmd_prefix="sudo"
  fi
else
  kw_cmd_prefix="sudo"
fi

log_init $g_deploy_path $g_cur_usr

if ! $(install_check);then
  log_err "KaiwuDB does not exist. Please install KaiwuDB first."
  exit 1
fi

ins_type=$(install_type) 
kw_status $ins_type
if [ $? -ne 0 ];then
  log_err "KaiwuDB is not running, please start KaiwuDB first."
  exit 1
fi

if [ "`ls -A /etc/kaiwudb/certs`" = "" ];then
  kw_secure="--insecure"
else
  if [ "$ins_type" == "bare" ];then
    kw_secure="--certs-dir=/etc/kaiwudb/certs"
  else
    kw_secure="--certs-dir=/kaiwudb/certs"
  fi
fi

function add_user() {
  local kw_user=$(user_name)
  local kw_port=$(sed -n "5p" /etc/kaiwudb/info/MODE)
  read -t60 -p "Please enter the username:" -e user_name

  if [ "$kw_secure" != "--insecure" ];then
    read -s -t60 -p "Please enter the password: " -n20 user_password
    echo -e "\\n"
  fi
  if [ "$ins_type" == "bare" ] && [ "$kw_secure" != "--insecure" ];then
    cd /usr/local/kaiwudb/bin
    info=$($kw_cmd_prefix -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase sql --host=127.0.0.1:$kw_port $kw_secure -e \"create user $user_name with password \\\"$user_password\\\";grant admin to $user_name with admin option;\" 2>&1")
    if [ $? -ne 0 ];then
      log_err "User create failed. $info"
      exit 1
    fi
  elif [ "$ins_type" == "bare" ] && [ "$kw_secure" == "--insecure" ];then
    cd /usr/local/kaiwudb/bin
    info=$($kw_cmd_prefix -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase sql --host=127.0.0.1:$kw_port $kw_secure -e \"create user $user_name;grant admin to $user_name with admin option;\" 2>&1")
    if [ $? -ne 0 ];then
      log_err "User create failed. $info"
      exit 1
    fi
  elif [ "$ins_type" == "container" ] && [ "$kw_secure" != "--insecure" ];then
    info=$(docker exec kaiwudb-container bash -c "./kwbase sql $kw_secure -e \"create user $user_name with password \\\"$user_password\\\";grant admin to $user_name with admin option;\"" 2>&1)
    if [ $? -ne 0 ];then
      log_err "User create failed. $info"
      exit 1
    fi
  elif [ "$ins_type" == "container" ] && [ "$kw_secure" == "--insecure" ];then
    info=$(docker exec kaiwudb-container bash -c "./kwbase sql $kw_secure -e \"create user $user_name;grant admin to $user_name with admin option;\"" 2>&1)
    if [ $? -ne 0 ];then
      log_err "User create failed. $info"
      exit 1
    fi
  fi
}

add_user
echo -e "\e[1;32m[ADD USER COMPLETED]:\e[0mUser creation completed."
exit 0