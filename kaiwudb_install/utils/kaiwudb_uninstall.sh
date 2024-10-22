#! /bin/bash

# uninstall pkg
function uninstall() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  if [ "$(install_type)" = "bare" ];then
    dpkg --help >/dev/null 2>&1
    if [ $? -ne 0 ];then
      local manager="rpm"
    else
      local manager="dpkg"
    fi
    if [ "$manager" = "dpkg" ];then
      eval $prefix dpkg -r kaiwudb-server >/dev/null 2>&1
      eval $prefix dpkg -r kaiwudb-libcommon >/dev/null 2>&1
      eval $prefix dpkg -r kwdb-server >/dev/null 2>&1
      eval $prefix dpkg -r kwdb-libcommon >/dev/null 2>&1
    elif [ "$manager" = "rpm" ];then
      eval $prefix rpm -e kaiwudb-server >/dev/null 2>&1
      eval $prefix rpm -e kaiwudb-libcommon >/dev/null 2>&1
      eval $prefix rpm -e kwdb-server >/dev/null 2>&1
      eval $prefix rpm -e kwdb-libcommon >/dev/null 2>&1
    fi
    sudo rm -rf /usr/local/kaiwudb >/dev/null 2>&1
  else
    local image=$(docker ps -a --filter name=kaiwudb-container --format {{.Image}})
    if [ -n "$image" ];then
      docker rm kaiwudb-container > /dev/null 2>&1
    else
      local image=$(container_image)
    fi
    docker rmi $image > /dev/null 2>&1
  fi
}

function uninstall_dir() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  local ret=""
  local data_root=$(kw_data_dir)
  eval $prefix rm -rf /etc/kaiwudb /etc/systemd/system/kaiwudb.service
  if [ -n "$data_root" ];then
    if [ "$clear_opt" = "yes" ] || [ "$clear_opt" = "y" ]; then
      eval $prefix rm -rf $data_root
    fi
  fi
}

function delete_user() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  if [ "$(install_type)" = "bare" ];then
    # if bare mode， delete user and group
    local kw_name=$(user_name)
    eval $prefix userdel -r $kw_name >/dev/null 2>&1
    sudo sed -i "/^$kw_name ALL=(ALL)  NOPASSWD: ALL$/d" /etc/sudoers
  fi
}