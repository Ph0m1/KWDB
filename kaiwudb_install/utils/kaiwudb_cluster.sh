#! /bin/bash

function check_start_type() {
	if [ -f $config_dir/info/MODE ];then
    local type=$(sed -n "2p" $config_dir/info/MODE)
    if [ "$type" != "single-replication" -a "$type" != "multi-replication" ];then
      return 1
    fi
    return 0
  else
    return 1
  fi
}

function modify_start_cmd() {
  local advertise_ip=$(sed -n "8p" $config_dir/info/MODE)
  local kw_port=$(sed -n "5p" $config_dir/info/MODE)
  if [ "$ins_type" == "bare" ];then
    if [ -f /etc/systemd/system/kaiwudb.service ];then
      sudo sed -iq "s/--join $advertise_ip:$kw_port//" /etc/systemd/system/kaiwudb.service
      sudo sed -iq "s/^ExecStart=.*/& --join $g_join_addr/" /etc/systemd/system/kaiwudb.service
      if [ $? -ne 0 ];then
        return 1
      fi
    else
      return 1
    fi
  else
    if [ -f $config_dir/script/docker-compose.yml ];then
      sudo sed -iq "s/--join $advertise_ip:$kw_port//" $config_dir/script/docker-compose.yml
      sudo sed -iq "s/.*\/kaiwudb\/bin\/kwbase.*/& --join $g_join_addr/" $config_dir/script/docker-compose.yml
      if [ $? -ne 0 ];then
        return 1
      fi
    else
      return 1
    fi
  fi

  return 0
}

function local_join() {
  local count=0
  eval $kw_cmd_prefix systemctl daemon-reload
  eval $kw_cmd_prefix systemctl start kaiwudb >/dev/null 2>&1
  if [ $? -ne 0 ];then
    return 1
  fi
  sleep 2
  if [ "$1" == "bare" ];then
    systemctl status kaiwudb >/dev/null 2>&1
    if [ $? -ne 0 ];then
      return 1
    fi
  else
    until [ $count -gt 60 ]
    do
      sleep 2
      local stat=`docker ps -a --filter name=kaiwudb-container --format {{.Status}} | awk '{if($0~/^(Up).*/){print "yes";}else if($0~/^(Exited).*/){print "failed";}else{print "no";}}'`
      if [ "$stat" == "yes" ];then
        break
      elif [ "$stat" == "failed" ];then
        return 1
      fi
      ((count++));
    done
    if [ $count -gt 60 ];then
      return 1
    fi
  fi
  return 0
}

function join_check() {
  $kw_cmd_prefix systemctl status kaiwudb >/dev/null 2>&1
  local kw_user=$(user_name)
  local kw_port=$(sed -n "5p" $config_dir/info/MODE)
  local advertise_ip=$(sed -n "8p" $config_dir/info/MODE)
  local node_array=($2)
  if [ "`ls -A $config_dir/certs`" = "" ];then
    local kw_secure="--insecure"
  else
    if [ "$ins_type" == "bare" ];then
      local kw_secure="--certs-dir=$config_dir/certs"
    else
      local kw_secure="--certs-dir=/kaiwudb/certs"
    fi
  fi
  if [ "$1" == "bare" ];then
    cd /usr/local/kaiwudb/bin
    sudo -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node status --host=127.0.0.1:$kw_port $kw_secure" >/dev/null 2>&1
    if [ $? -ne 0 ];then
      log_err "Join to cluster failed."
      exit 1
    fi
  else
    docker exec kaiwudb-container bash -c "./kwbase node status $kw_secure" >/dev/null 2>&1
    if [ $? -ne 0 ];then
      log_err "Join to cluster failed."
      exit 1
    fi
  fi
}

function local_init() {
  $kw_cmd_prefix systemctl status kaiwudb >/dev/null 2>&1
  local kw_user=$(user_name)
  local kw_port=$(sed -n "5p" $config_dir/info/MODE)
  local advertise_ip=$(sed -n "8p" $config_dir/info/MODE)
  local node_array=($2)
  if [ "`ls -A $config_dir/certs`" = "" ];then
    local kw_secure="--insecure"
  else
    if [ "$ins_type" == "bare" ];then
      local kw_secure="--certs-dir=$config_dir/certs"
    else
      local kw_secure="--certs-dir=/kaiwudb/certs"
    fi
  fi
  if [ "$1" == "bare" ];then
    cd /usr/local/kaiwudb/bin
    sudo -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase init $kw_secure --host=127.0.0.1:$kw_port"
    if [ $? -ne 0 ];then
      log_err "Cluster init failed."
      exit 1
    fi
    nodes=($(sudo -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node status --host=127.0.0.1:$kw_port $kw_secure | awk -F\" \" 'NR!=1{split(\$3,arr,\":\");print arr[1]}'"))
    if [ $? -ne 0 ];then
      if [[ ${#nodes[@]} -ne ${#node_array[@]}+1 ]];then
        for ((i=0; i<${#node_array[@]}; i++))
        do
          for ((j=0; j<${#nodes[@]}; j++))
          do
            if [ "${nodes[$j]}" == "${node_array[$i]}" ] || [ "${node_array[$i]}" == "$advertise_ip" ];then
              break
            fi
          done
          if [ $j -eq ${#nodes[@]} ];then
            log_warn "Node[${node_array[$i]}] join to cluster failed."
          fi
        done
      fi
      log_err "Cluster init failed."
      exit 1
    fi
  else
    docker exec -it kaiwudb-container bash -c "./kwbase init $kw_secure --host=127.0.0.1:26257"
    if [ $? -ne 0 ];then
      log_err "Cluster init failed."
      exit 1
    fi
    nodes=($(docker exec kaiwudb-container bash -c "./kwbase node status $kw_secure" | awk -F" " 'NR!=1{split($3,arr,":");print arr[1]}'))
    if [ $? -ne 0 ];then
      if [[ ${#nodes[@]} -ne ${#node_array[@]}+1 ]];then
        for ((i=0; i<${#node_array[@]}; i++))
        do
          for ((j=0; j<${#nodes[@]}; j++))
          do
            if [ "${nodes[$j]}" == "${node_array[$i]}" ] || [ "${node_array[$i]}" == "$advertise_ip" ];then
              break
            fi
          done
          if [ $j -eq ${#nodes[@]} ];then
            log_warn "Node[${node_array[$i]}] join to cluster failed."
          fi
        done
      fi
      log_err "Cluster init failed."
      exit 1
    fi
  fi
}

function parse_addr() {
  if [ ! -e $config_dir/info/NODE ];then
    return 1
  fi
  ip_arr=$(sed -n "1p" $config_dir/info/NODE)
  ip_arr=(${ip_arr//,/ })
  if [ ${#ip_arr[@]} -eq 0 ];then
    log_warn "No remote node join to cluster."
  fi
  ssh_port=$(sed -n "2p" $config_dir/info/NODE)
  ssh_user=$(sed -n "3p" $config_dir/info/NODE)
  return 0
}

function remote_install_check() {
  args=${1//:/ }
  eval set -- "${args}"
  ssh -t -p $2 $3@$1 >/dev/null 2>&1 "
if [ ! -f $config_dir/info/MODE ];then
  exit 1
fi
exit 0
"
  if [ $? -ne 0 ]; then
    echo $1
  fi
}

function parallel_install_check() {
  declare -a parallel_func
  local node_array=($1)
  local ssh_port=$2
  local ssh_user=$3
  if [ ${#node_array[@]} -ne 0 ];then
    for ((i=0; i<${#node_array[@]}; i++))
    do
      tmp=(${node_array[$i]//:/ })
      parallel_func[$i]="${tmp[0]}:$ssh_port:$ssh_user"
    done
    export config_dir=$config_dir
    export -f remote_install_check
    local arr=($(echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "remote_install_check args" 2>/dev/null))
    # echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "remote_install_check args"
    if [ ${#arr[@]} -ne 0 ];then
      log_err "KaiwuDB does not exist in Node[${arr[@]}]. Please install KaiwuDB first."
      return 1
    fi
  fi
  return 0
}

function remote_status() {
  args=${1//:/ }
  eval set -- "${args}"
  ssh -p $2 $3@$1 >/dev/null 2>&1 "
if [ \"$ins_type\" == \"bare\" ];then
  $g_node_prefix systemctl status kaiwudb > /dev/null 2>&1
  if [ \$? -eq 0 ];then
    exit 1
  fi
else
  local stat=\$(docker ps -a --filter name=kaiwudb-container --format {{.Status}} | awk '{if(\$0~/^(Up).*/){print \"yes\";}else{print \"no\";}}')
  if [ \"\$stat\" == \"yes\" ];then
    exit 1
  fi
fi
exit 0
"
  if [ $? -eq 1 ]; then
    echo $1
  fi
}

# parallel copy files to remote node
function parallel_status() {
  declare -a parallel_func

  if [ ${#ip_arr[@]} -ne 0 ];then
    for ((i=0; i<${#ip_arr[@]}; i++))
    do
      tmp=(${ip_arr[$i]//:/ })
      parallel_func[$i]="${tmp[0]}:$ssh_port:$ssh_user"
    done
    export -f remote_status
    export ins_type=$ins_type
    export g_node_prefix=$g_node_prefix
    local arr=($(echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "remote_status args" 2>/dev/null))
    # echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "distribute_files args"
    if [ ${#arr[@]} -ne 0 ];then
      log_warn "KaiwuDB is running in Node[${arr[@]}]."
      for ((i=0;i<${#arr[@]};i++))
      do
        for ((j=0;j<${#ip_arr[@]};j++))
        do
          if [ "${ip_arr[$j]}" == "${arr[$i]}" ];then
            unset ${ip_arr[$j]}
          fi
        done
      done
      return 1
    fi
  fi
  return 0
}

function remote_init() {
  args=${1//:/ }
  eval set -- "${args}"
  ssh -p $2 $3@$1 >/dev/null 2>&1 "
  count=0
  $g_node_prefix systemctl daemon-reload
  $g_node_prefix systemctl start kaiwudb
  if [ \$? -ne 0 ];then
    exit 1
  fi
  sleep 3
  if [ \"$ins_type\" == \"bare\" ];then
    sudo systemctl status kaiwudb >/dev/null 2>&1
    if [ \$? -eq 0 ];then
      exit 0
    else
      exit 1
    fi
  fi
  until [ \$count -gt 60 ]
  do
    sleep 2
    stat=\`docker ps -a --filter name=kaiwudb-container --format {{.Status}} | awk '{if(\$0~/^(Up).*/){print \"yes\";}else if(\$0~/^(Exited).*/){print \"failed\";}else{print \"no\"}}'\`
    if [ \"\$stat\" == \"yes\" ];then
      break
    elif [ \"\$stat\" == \"failed\" ];then
      exit 1
    fi
    ((count++));
  done
  if [ \$count -gt 60 ];then
    exit 1
  fi
  exit 0
"
  if [ $? -ne 0 ]; then
    echo $1
  fi
}

function rebuild_certs() {
  local kw_user=$(user_name)
  local info=""
  if [ "`ls -A $config_dir/certs`" != "" ];then
    read -t60 -p "Please input directory where the CA is stored :" -e ca_dir
    if [ ! -e $ca_dir/kaiwudb_certs.tar.gz ];then
      log_err "kaiwudb_certs.tar.gz is not exist."
      exit 1
    fi
    eval $kw_cmd_prefix rm -rf $config_dir/certs/* >/dev/null 2>&1
    eval $kw_cmd_prefix tar -xzf $ca_dir/kaiwudb_certs.tar.gz -C$config_dir/certs ca.key ca.crt >/dev/null 2>&1
    if [ $? -ne 0 ];then
      log_err "kaiwudb_certs.tar.gz decompress failed."
      exit 1
    fi
    local local_addr=$(sed -n "8p" $config_dir/info/MODE)
    local cluster_addr=${g_join_addr%:*}
    local ins_type=$(install_type)
    local ins_dir=$(install_dir)
    if [ "$ins_type" == "bare" ];then
      cd $ins_dir/bin
      local cmd="$kw_cmd_prefix -u $kw_user bash -c \"export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase cert create-node $local_addr $cluster_addr 127.0.0.1 0.0.0.0 localhost --certs-dir=$config_dir/certs --ca-key=$config_dir/certs/ca.key 2>&1 >/dev/null\""
      info=$(eval $cmd)
      if [ $? -ne 0 ]; then
        log_err_without_console "$info"
        log_err "node certificate creation failed."
        exit 1
      fi
      cmd="$kw_cmd_prefix -u $kw_user bash -c \"export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase cert create-client root --certs-dir=$config_dir/certs --ca-key=$config_dir/certs/ca.key 2>&1 >/dev/null\""
      info=$(eval $cmd)
      if [ $? -ne 0 ]; then
        log_err_without_console "$info"
        log_err "client certificate creation failed."
        exit 1
      fi
    else
      local certs_ret=$(docker run --rm -it --privileged --name init -w /kaiwudb/bin -v$config_dir/certs:/kaiwudb/certs -e LD_LIBRARY_PATH=/kaiwudb/lib ${ins_dir} bash -c "./kwbase cert create-node $local_addr $cluster_addr 127.0.0.1 0.0.0.0 localhost --certs-dir=/kaiwudb/certs --ca-key=/kaiwudb/certs/ca.key
      ./kwbase cert create-client root --certs-dir=/kaiwudb/certs --ca-key=/kaiwudb/certs/ca.key" 2>&1)
      if [ $? -ne 0 ]; then
        log_err_without_console "$certs_ret"
        log_err_only_console "Certificate creation failed. Please check the log to see more information."
        exit 1
      fi
    fi
  fi
}

function parallel_init() {
  declare -a parallel_func
  local node_array=($1)
  local ssh_port=$2
  local ssh_user=$3
  if [ ${#node_array[@]} -ne 0 ];then
    for ((i=0; i<${#node_array[@]}; i++))
    do
      tmp=(${node_array[$i]//:/ })
      parallel_func[$i]="${tmp[0]}:$ssh_port:$ssh_user"
    done
    export -f remote_init
    export ins_type=$ins_type
    export g_node_prefix=$g_node_prefix
    local arr=($(echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "remote_init args" 2>/dev/null))
    # echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "distribute_files args"
    if [ ${#arr[@]} -ne 0 ];then
      log_warn "KaiwuDB init failed in Node[${arr[@]}]."
      return 1
    fi
  fi
  return 0
}

# get all status in cluster
function cluster_status() {
  local kw_port=$(sed -n "5p" $config_dir/info/MODE)
  if [ "`ls -A $config_dir/certs`" = "" ];then
    local kw_secure="--insecure"
  else
    if [ "$ins_type" == "bare" ];then
      local kw_secure="--certs-dir=$config_dir/certs"
    else
      local kw_secure="--certs-dir=/kaiwudb/certs"
    fi
  fi
  if [ "$ins_type" == "bare" ];then
    local kw_user=$(user_name)
    local kw_dir=$(install_dir)
    cd $kw_dir/bin
    local cmd="$kw_cmd_prefix -u $kw_user bash -c \"export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node status --host=127.0.0.1:$kw_port $kw_secure\""
    eval $cmd
  else
    docker exec -it kaiwudb-container bash -c "./kwbase node status $kw_secure"
  fi
}

function cluster_nodeid() {
  kw_node_id=""
  local kw_port=$(sed -n "5p" $config_dir/info/MODE)
  if [ "`ls -A $config_dir/certs`" = "" ];then
    local kw_secure="--insecure"
  else
    if [ "$ins_type" == "bare" ];then
      local kw_secure="--certs-dir=$config_dir/certs"
    else
      local kw_secure="--certs-dir=/kaiwudb/certs"
    fi
  fi
  local advertise_ip=$(sed -n "8p" $config_dir/info/MODE)
  if [ "$ins_type" == "bare" ];then
    local kw_user=$(user_name)
    local kw_dir=$(install_dir)
    
    cd $kw_dir/bin
    kw_node_id=`sudo -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node status --host=127.0.0.1:$kw_port $kw_secure | awk -F\" \" -v ip=$advertise_ip '{split(\\\$3,arr,\":\");if(arr[1]==ip){print \\\$1}}'"`
    if [ $? -ne 0 ];then
      log_err "Failed to get node_id"
      exit 1
    fi
  else
    kw_node_id=`docker exec kaiwudb-container bash -c "./kwbase node status $kw_secure" | awk -F" " -v ip=$advertise_ip '{split($3,arr,":");if(arr[1]==ip)print $1}'`
    if [ $? -ne 0 ];then
      log_err "Failed to get node_id"
      exit 1
    fi
  fi
  # echo $kw_node_id
  if [ "$kw_node_id" != "" ];then
    if [[ "$kw_node_id" =~ ^[0-9]+$ ]];then
      return
    else
      log_err "Failed to get node_id"
      exit 1
    fi
  fi
}

function decommission_from_cluster() {
  local kw_port=$(sed -n "5p" $config_dir/info/MODE)
  if [ "`ls -A $config_dir/certs`" = "" ];then
    local kw_secure="--insecure"
  else
    if [ "$ins_type" == "bare" ];then
      local kw_secure="--certs-dir=$config_dir/certs"
    else
      local kw_secure="--certs-dir=/kaiwudb/certs"
    fi
  fi

  if [ "$ins_type" == "bare" ];then
    local kw_user=$(user_name)
    local kw_dir=$(install_dir)
    cd $kw_dir/bin
    info=$(sudo -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node decommission $kw_node_id $kw_secure --wait=none --host=127.0.0.1:$kw_port" 2>&1)
    if [ $? -ne 0 ];then
      info=`echo $info | sed -i 's/\\r//g'`
      log_err "Decommission failed:echo $info | sed -i 's/\\n//g'"
      exit 1
    fi
  else
    info=`docker exec -it kaiwudb-container bash -c "./kwbase node decommission $kw_node_id $kw_secure --wait=none" 2>&1`
    if [ $? -ne 0 ];then
      info=`echo $info | sed -i 's/\\r//g'`
      log_err "Decommission failed:$info"
      exit 1
    fi
  fi
  wait_for_decommission $kw_node_id &
}

function wait_for_decommission() {
  local left=""
  local kw_port=$(sed -n "5p" $config_dir/info/MODE)
  if [ "`ls -A $config_dir/certs`" = "" ];then
    local kw_secure="--insecure"
  else
    if [ "$ins_type" == "bare" ];then
      local kw_secure="--certs-dir=$config_dir/certs"
    else
      local kw_secure="--certs-dir=/kaiwudb/certs"
    fi
  fi
  
  while true
  do
    if [ "$ins_type" == "bare" ];then
      left=$(sudo -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node status $1 --decommission $kw_secure --host=127.0.0.1:$kw_port" | awk -F" " 'NR!=1{print $13}')
    else
      left=$(docker exec kaiwudb-container bash -c "./kwbase node status $1 --decommission $kw_secure --host=127.0.0.1:26257" | awk -F" " 'NR!=1{print $13}')
    fi
    if [ $left -ne 0 ];then
      sudo bash -c "echo $left >$g_deploy_path/decommission_progress"
    else
      sudo bash -c "echo 0 >$g_deploy_path/decommission_progress"
      break
    fi
    sleep 2
  done
}