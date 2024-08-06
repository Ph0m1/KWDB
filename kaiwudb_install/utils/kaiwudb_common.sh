#! /bin/bash

# whether installing KaiwuDB
function install_check() {
  if [ ! -e $config_dir/info/MODE ];then
    return 1
  fi
  return 0
}

# IP verification
function addr_check() {
  local addr=(${1//:/ })
  local valid=$(echo ${addr[0]}| awk -F. '$1<=255&&$2<=255&&$3<=255&&$4<=255{print "yes"}')
  if echo ${addr[0]}| grep -E "^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$" > /dev/null; then
    if [ "${valid:-no}" == "yes" ]; then
      echo "yes"
    else
      echo "no"
    fi
  else
    echo "no"
  fi
}

# ssh passwd-free check
function ssh_passwd_free() {
  args=${1//:/ }
  eval set -- "${args}"
  timeout 5 ssh -p $2 $3@$1 exit 2>/dev/null
  if [ $? -ne 0 ]; then
    echo $1
    return 1
  fi
  return 0
}

# whether ntpq or chronyc(client)
function clock_sync_check() {
  echo "clock sync check"
}

# bare or container
function install_type() {
	if [ -f $config_dir/info/MODE ];then
    local type=$(sed -n "1p" $config_dir/info/MODE)
    if [ "$type" != "bare" -a "$type" != "container" ];then
      return 1
    fi
    echo "$type"
    return 0
  else
    return 1
  fi
}

function install_dir() {
	if [ -f $config_dir/info/MODE ];then
    local dir=$(sed -n "3p" $config_dir/info/MODE)
    echo "$dir"
    return 0
  else
    return 1
  fi
}

# single or single-replication or multi-replication
function running_type() {
	if [ -f $config_dir/info/MODE ];then
    local type=$(sed -n "2p" $config_dir/info/MODE)
    if [ "$type" != "single" -a "$type" != "single-replication" -a "$type" != "multi-replication" ];then
      return 1
    fi
    echo "$type"
    return 0
  else
    return 1
  fi
}

function kw_data_dir() {
	if [ -f $config_dir/info/MODE ];then
    local dir=$(sed -n "4p" $config_dir/info/MODE)
    echo "$dir"
    return 0
  else
    return 1
  fi
}

function container_image() {
	if [ -f $config_dir/info/MODE ];then
    local image_name=$(sed -n "3p" $config_dir/info/MODE)
    echo "$image_name"
    return 0
  else
    return 1
  fi
}

function user_name() {
	if [ -f $config_dir/info/MODE ];then
    local user_name=$(sed -n "7p" $config_dir/info/MODE)
    echo "$user_name"
    return 0
  else
    return 1
  fi
}

function distribute_files() {
  local addr=(${1//:/ })
  local ip=${addr[0]}
  local port=${addr[1]}
  local user=${addr[2]}
  local files=${addr[3]//,/ }
  # create temporary folder
  ssh -p $port $user@$ip "
  cd ~
  if [ ! -d "~/kaiwudb_files" ];then
    mkdir kaiwudb_files
  else
    rm -rf ~/kaiwudb_files/*
  fi" >/dev/null 2>&1
  if [ $? -ne 0 ];then
    echo $ip
    return 1
  fi
  scp -r -P $port ${files} $user@$ip:~/kaiwudb_files/ >/dev/null 2>&1
  if [ $? -ne 0 ];then
    echo $ip
    return 1
  fi
  return 0
}

function ssh_passwd_free_check() {
  declare -a parallel_func
  local node_array=($1)
  local ssh_port=$2
  local ssh_user=$3
  # parallel ssh passwd-free check
  if [ ${#node_array[@]} -ne 0 ];then
    for ((i=0; i<${#node_array[@]}; i++))
    do
      tmp=(${node_array[$i]//:/ })
      parallel_func[$i]="${tmp[0]}:$ssh_port:$ssh_user"
    done
    export -f ssh_passwd_free
    local arr=($(echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "ssh_passwd_free args" 2>/dev/null))
    if [ ${#arr[@]} -ne 0 ];then
      log_err "Node ${arr[@]} Connection refused."
      exit 1
    fi
  fi
}

# parallel copy files to remote node
function parallel_copy_files() {
  declare -a parallel_func
  local node_array=($1)
  local ssh_port=$2
  local ssh_user=$3
  local tmp=$(echo $4)
  local files=${tmp// /,}
  if [ ${#node_array[@]} -ne 0 ];then
    log_info_without_console "start distribute files($files) to ${node_array[*]}"
    for ((i=0; i<${#node_array[@]}; i++))
    do
      tmp=(${node_array[$i]//:/ })
      parallel_func[$i]="${tmp[0]}:$ssh_port:$ssh_user:$files"
    done
    export -f distribute_files
    local arr=($(echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "distribute_files args" 2>/dev/null))
    # echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "distribute_files args"
    if [ ${#arr[@]} -ne 0 ];then
      log_err "Distribute files to [${arr[@]}] failed."
      return 1
    fi
    log_info_without_console "distribute files to ${node_array[*]} success"
  fi
  return 0
}

# Whether Kaiwudb is running
function kw_status() {
  if [ "$1" == "bare" ];then
    eval $kw_cmd_prefix systemctl status kaiwudb > /dev/null 2>&1
    if [ $? -eq 0 ];then
      return 0
    fi
  else
    local stat=`docker ps -a --filter name=kaiwudb-container --format {{.Status}} | awk '{if($0~/^(Up).*/){print "yes";}else{print "no";}}'`
    if [ "$stat" == "yes" ];then
      return 0
    fi
  fi
  return 1
}

function remote_node_passwd() {
  local node_array=($1)
  local ssh_port=$2
  local ssh_user=$3
  if [ ${#node_array[@]} -eq 0 ];then
    return
  fi
  if [ "$3" != "root" ];then
    ssh -t -p $ssh_port $ssh_user@${node_array[0]%:*} >/dev/null 2>&1 << end
timeout 3 sudo -s -p ""
if [ \$? -ne 0 ];then
  exit 1
fi
exit 0
end
    if [ $? -ne 0 ];then
      if [ "$g_kw_cmd" == "install" ] || [ "$g_kw_cmd" == "upgrade" ];then
        echo "pause" >&10
      fi
      read -e -s -t60 -p "remote node $ssh_user's password: " remote_passwd
      echo
      g_node_prefix="echo $remote_passwd | sudo -S -p \"\""
      ssh -t -p $ssh_port $ssh_user@${node_array[0]%:*} >/dev/null 2>&1  << end
timeout 3s echo $remote_passwd | sudo -s -S -p ""
if [ \$? -ne 0 ];then
  exit 1
fi
exit 0
end
      if [ $? -ne 0 ];then
        log_err "Incorrect password."
        exit 1
      fi
    else
      g_node_prefix="sudo"
      return
    fi
  else
    g_node_prefix="sudo"
    return
  fi
}