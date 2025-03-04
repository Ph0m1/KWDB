#! /bin/bash

function check_start_type() {
	if [ -f /etc/kaiwudb/info/MODE ];then
    local type=$(sed -n "2p" /etc/kaiwudb/info/MODE)
    if [ "$type" != "single-replication" -a "$type" != "multi-replication" ];then
      return 1
    fi
    return 0
  else
    return 1
  fi
}

function modify_start_cmd() {
  local SE_MODE=""
  if [ "$g_secure" = "tlcp" ];then
    SE_MODE="--tlcp"
  fi
  if [ "$(install_type)" = "bare" ];then
    if [ -f /etc/systemd/system/kaiwudb.service ];then
      sudo sed -iq "s/--join $(local_addr):$(local_port)//" /etc/systemd/system/kaiwudb.service >/dev/null 2>&1
      sudo sed -iq "s/^ExecStart=.*/& --join $g_join_addr $SE_MODE/" /etc/systemd/system/kaiwudb.service >/dev/null 2>&1
    else
      echo "/etc/systemd/system/kaiwudb.service:No such file."
      return 1
    fi
  else
    if [ -f /etc/kaiwudb/script/docker-compose.yml ];then
      sudo sed -iq "s/--join $(local_addr):$(local_port)//" /etc/kaiwudb/script/docker-compose.yml >/dev/null 2>&1
      sudo sed -iq "s/.*\/kaiwudb\/bin\/kwbase.*/& --join $g_join_addr $SE_MODE/" /etc/kaiwudb/script/docker-compose.yml >/dev/null 2>&1
    else
      echo "/etc/kaiwudb/script/docker-compose.yml: No such file."
      return 1
    fi
  fi
  return 0
}

function modify_cache() {
  if [ "$g_secure" = "tlcp" ];then
    sudo sed -i '8c tlcp' /etc/kaiwudb/info/MODE
  elif [ "$g_secure" = "tls" ];then
    sudo sed -i '8c tls' /etc/kaiwudb/info/MODE
  else
    sudo sed -i '8c insecure' /etc/kaiwudb/info/MODE
  fi
}

function cluster_init() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  local ret=""
  if [ "$(install_type)" = "bare" ];then
    cd /usr/local/kaiwudb/bin
    ret=$(sudo -u $(user_name) bash -c "./kwbase init $(secure_opt) --host=127.0.0.1:$(local_port)" 2>&1)
    if [ $? -ne 0 ];then
      echo "$ret"
      return 1
    fi
  else
    ret=$(docker exec -it kaiwudb-container bash -c "./kwbase init $(secure_opt) --host=127.0.0.1:26257" 2>&1)
    if [ $? -ne 0 ];then
      echo "$ret"
      return 1
    fi
  fi
}

function parse_addr() {
  if [ ! -e /etc/kaiwudb/info/NODE ];then
    log_err "/etc/kaiwudb/info/NODE: No such file."
    exit 1
  fi
  ip_arr=($(sed -n "1p" /etc/kaiwudb/info/NODE))
  ssh_port=$(sed -n "2p" /etc/kaiwudb/info/NODE)
  ssh_user=$(sed -n "3p" /etc/kaiwudb/info/NODE)
  return 0
}

function rebuild_certs() {
  local ret=""
  local cmd=""
  local SE_MODE=""
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  if [ "$g_secure" = "tlcp" ];then
    SE_MODE="--tlcp"
  fi
  if [ "$g_secure" != "insecure" ];then
    read -t60 -p "Please input directory where the CA is stored :" -e ca_dir
    if [ ! -e $ca_dir/kaiwudb_certs.tar.gz ];then
      echo "$ca_dir/kaiwudb_certs.tar.gz: No such file."
      return 1
    fi
    eval $prefix rm -rf /etc/kaiwudb/certs/*
    ret=$(eval $prefix tar -xzf $ca_dir/kaiwudb_certs.tar.gz -C/etc/kaiwudb/certs 2>&1)
    if [ $? -ne 0 ];then
      echo "$ret"
      return 1
    fi
    local cluster_addr=${g_join_addr%:*}
    if [ "$(install_type)" = "bare" ];then
      cd /usr/local/kaiwudb/bin
      cmd="$prefix -u $(user_name) bash -c \"./kwbase cert create-node $(local_addr) $cluster_addr 127.0.0.1 0.0.0.0 localhost --certs-dir=/etc/kaiwudb/certs --ca-key=/etc/kaiwudb/certs/ca.key $SE_MODE\" 2>&1"
      ret=$(eval $cmd)
      if [ $? -ne 0 ]; then
        echo "$ret"
        return 1
      fi
      cmd="$prefix -u $(user_name) bash -c \"./kwbase cert create-client root --certs-dir=/etc/kaiwudb/certs --ca-key=/etc/kaiwudb/certs/ca.key $SE_MODE\" 2>&1"
      ret=$(eval $cmd)
      if [ $? -ne 0 ]; then
        echo "$ret"
        return 1
      fi
    else
      ret=$(docker run --rm -it --privileged --name init -w /kaiwudb/bin -v/etc/kaiwudb/certs:/kaiwudb/certs $(install_dir) bash -c "
      ./kwbase cert create-node $(local_addr) $cluster_addr 127.0.0.1 0.0.0.0 localhost --certs-dir=/kaiwudb/certs --ca-key=/kaiwudb/certs/ca.key $SE_MODE &&
      ./kwbase cert create-client root --certs-dir=/kaiwudb/certs --ca-key=/kaiwudb/certs/ca.key $SE_MODE" 2>&1)
      if [ $? -ne 0 ]; then
        echo "$ret"
        return 1
      fi
    fi
    eval $prefix chmod 700 /etc/kaiwudb/certs/*.key
    eval $prefix chmod 644 /etc/kaiwudb/certs/*.crt
  fi
}

# get all status in cluster
function cluster_status() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  if [ "$(install_type)" = "bare" ];then
    cd /usr/local/kaiwudb/bin
    local cmd="$prefix -u $(user_name) bash -c \"./kwbase node status --host=127.0.0.1:$(local_port) $(secure_opt)\""
    eval $cmd
  else
    docker exec -it kaiwudb-container bash -c "./kwbase node status $(secure_opt)"
  fi
}

function cluster_nodeid() {
  local ret=""
  local kw_node_id=""
  local cmd=""
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  if [ "$(install_type)" = "bare" ];then
    cd /usr/local/kaiwudb/bin
    cmd="$prefix -u $(user_name) bash -c \"./kwbase node status --host=127.0.0.1:$(local_port) $(secure_opt)\""
    ret=$(eval $cmd 2>&1)
    if [ $? -ne 0 ];then
      echo "$ret"
      return 1
    fi
    kw_node_id=$(eval $cmd | awk -F" " -v ip=$(local_addr) 'NR!=1{split($3,arr,":");if(arr[1]==ip){print $1}}')
  else
    ret=$(docker exec kaiwudb-container bash -c "./kwbase node status $(secure_opt)" 2>&1)
    if [ $? -ne 0 ];then
      echo "$ret"
      return 1
    fi
    kw_node_id=$(docker exec kaiwudb-container bash -c "./kwbase node status $(secure_opt)" | awk -F" " -v ip=$(local_addr) 'NR!=1{split($3,arr,":");if(arr[1]==ip)print $1}')
  fi
  echo $kw_node_id
  return 0
}

function decommission_from_cluster() {
  local ret=""
  local node_id=""
  node_id=$(cluster_nodeid)
  if [ "$(install_type)" = "bare" ];then
    if [ $? -ne 0 ];then
      log_err "Get node id failed: $node_id."
      exit 1
    fi
    cd /usr/local/kaiwudb/bin
    ret=$(sudo -u $(user_name) bash -c "./kwbase node decommission $node_id $(secure_opt) --wait=none --host=127.0.0.1:$(local_port)" 2>&1)
    if [ $? -ne 0 ];then
      log_err "Decommission from cluster failed: "$ret
      exit 1
    fi
  else
    ret=$(docker exec -it kaiwudb-container bash -c "./kwbase node decommission $node_id $(secure_opt) --wait=none" 2>&1)
    if [ $? -ne 0 ];then
      log_err "Decommission from cluster failed: "$ret
      exit 1
    fi
  fi
  wait_for_decommission $node_id &
}

function wait_for_decommission() {
  local ret=""
  while true
  do
    if [ "$(install_type)" = "bare" ];then
      ret=$(sudo -u $(user_name) bash -c "./kwbase node status $1 --decommission $(secure_opt) --host=127.0.0.1:$(local_port)" | awk -F" " 'NR!=1{print $13}')
    else
      ret=$(docker exec kaiwudb-container bash -c "./kwbase node status $1 --decommission $(secure_opt) --host=127.0.0.1:26257" | awk -F" " 'NR!=1{print $13}')
    fi
    if [ $ret -ne 0 ];then
      sudo bash -c "echo $ret >$g_deploy_path/decommission_progress"
    else
      sudo bash -c "echo 0 >$g_deploy_path/decommission_progress"
      break
    fi
    sleep 2
  done
}