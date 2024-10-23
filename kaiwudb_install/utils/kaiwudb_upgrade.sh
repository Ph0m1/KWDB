#! /bin/bash

# version compare function
function version_le() {
	test "$(echo "$@" | tr " " "\n" | sort -V | head -n 1)" = "$1"; 
}

function ext_compare() {
  local old_prefixn=$1
  local old_suffixn=$2
  local new_prefixn=$3
  local new_suffixn=$4
  local old_prefix=$(echo $1 | grep -oP '^[a-z]+(?=[0-9]*)')
  local old_suffix=$(echo $2 | grep -oP '[a-z]+(?=[0-9]*$)')
  local new_prefix=$(echo $3 | grep -oP '^[a-z]+(?=[0-9]*)')
  local new_suffix=$(echo $4 | grep -oP '[a-z]+(?=[0-9]*$)')
  # has same prefix,eg:
  # hotfix2 -> hotfix1 (failed)
  # hotfix1beta1 -> hotfix1alpha1 (failed)
  # hotfix1alpha2 -> hotfix1alpha1 (failed)
  if [ "$old_prefix" = "$new_prefix" ] && version_le $new_prefixn $old_prefixn;then
    if [ "$new_prefixn" = "$old_prefixn" ];then
      if [ "$old_prefix" != "$old_suffix" ] && [ "$new_prefix" != "$new_suffix" ];then
        if [[ "$old_suffix" =~ ^beta && "$new_suffix" =~ ^alpha ]];then
          echo "UPGRADE ERROR:The upgrade conditions are not met.($old_prefixn$old_suffixn --> $new_prefixn$new_suffixn)"
          return 1
        elif [ "$old_suffix" = "$new_suffix" ] && version_le $new_suffixn $old_suffixn;then
          echo "UPGRADE ERROR:The upgrade conditions are not met.($old_prefixn$old_suffixn --> $new_prefixn$new_suffixn)"
          return 1
        fi
      elif [ "$old_prefix" = "$old_suffix" ] && [ "$new_prefix" != "$new_suffix" ];then
        echo "UPGRADE ERROR:The upgrade conditions are not met.($old_prefixn --> $new_prefixn)"
        return 1
      elif [ "$old_prefix" = "$old_suffix" ] && [ "$new_prefix" = "$new_suffix" ];then
        echo "UPGRADE ERROR:The upgrade conditions are not met.($old_prefixn --> $new_prefixn)"
        return 1
      fi
    else
      echo "WARNING:This version upgrade is not supported, and if you continue to upgrade, you can use the "--bypass-version-check" option to re-execute the upgrade command to skip the version comparison check."
      return 1
    fi
  elif [ "$old_prefix" = "$new_prefix" ] && [[ "$old_prefix" =~ hotfix || "$old_prefix" =~ enhance ]];then
    echo "WARNING:This version upgrade is not supported, and if you continue to upgrade, you can use the "--bypass-version-check" option to re-execute the upgrade command to skip the version comparison check."
    return 1
  fi
}

function version_compare() {
  local base_dir=""
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
    base_dir=~/kaiwudb_files
  else
    prefix=$local_cmd_prefix
    base_dir="$g_deploy_path"
  fi
  local old_basic_version=
  local old_extend_version=
  local current_basic_version=
  local current_extend_version=
  # version file is exist
  if [ -f /etc/kaiwudb/info/.version ];then
    old_basic_version=$(sed -n '1p' /etc/kaiwudb/info/.version)
    old_extend_version=$(sed -n '2p' /etc/kaiwudb/info/.version)
  else
    # if version file is not exist, considered the version to be lower
    return 0
  fi
  current_basic_version=$(sed -n '1p' $base_dir/packages/.version)
  current_extend_version=$(sed -n '2p' $base_dir/packages/.version)
  # if version os equals current
  if [ "$current_basic_version" = "$old_basic_version" ];then
    if [ -z "$old_extend_version" ] && \
       [[ "$current_extend_version" =~ ^alpha || "$current_extend_version" =~ ^beta ]];then
      echo "UPGRADE ERROR:Official version can not upgrade to $current_extend_version."
      return 1
    fi
    if [[ "$old_extend_version" =~ ^beta && "$current_extend_version" =~ ^alpha ]];then
      echo "UPGRADE ERROR:The upgrade conditions are not met.($old_extend_version --> $current_extend_version)"
      return 1
    fi
    if [[ "$old_extend_version" =~ ^hotfix || "$old_extend_version" =~ ^enhance || "$old_extend_version" =~ ^patch ]] && \
       [[ "$current_extend_version" =~ ^alpha || "$current_extend_version" =~ ^beta ]];then
      echo "UPGRADE ERROR:The upgrade conditions are not met.($old_extend_version --> $current_extend_version)"
      return 1
    fi

    if [[ "$old_extend_version" =~ ^hotfix && "$current_extend_version" =~ ^enhance ]] \
       || [[ "$old_extend_version" =~ ^enhance && "$current_extend_version" =~ ^hotfix ]] \
       || [[ "$old_extend_version" =~ ^enhance && "$current_extend_version" =~ ^patch ]] \
       || [[ "$old_extend_version" =~ ^patch && "$current_extend_version" =~ ^enhance ]] \
       || [[ "$old_extend_version" =~ ^patch && "$current_extend_version" =~ ^hotfix ]] \
       || [[ "$old_extend_version" =~ ^hotfix && "$current_extend_version" =~ ^patch ]];then
      echo "WARNING:This version upgrade is not supported, and if you continue to upgrade, you can use the "--bypass-version-check" option to re-execute the upgrade command to skip the version comparison check."
      return 1
    fi
    ext_compare $(echo "$old_extend_version" | grep -oP '^[a-z]+([0-9]*)') \
                $(echo "$old_extend_version" | grep -oP '[a-z]+([0-9]?$)') \
                $(echo "$current_extend_version" | grep -oP '^[a-z]+([0-9]*)') \
                $(echo "$current_extend_version" | grep -oP '[a-z]+([0-9]?$)')
    if [ $? -ne 0 ];then
      return 1
    fi
    return 0
  fi
  # if new basic version is less or equal current
  if version_le $current_basic_version $old_basic_version;then
    echo "UPGRADE ERROR:The requested upgrade version is older than the current installed version.(current:$old_basic_version new_version:$current_basic_version)"
    return 1
  fi
  return 0
}

function upgrade() {
  local ret=""
  local base_dir=""
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
    base_dir=~/kaiwudb_files
  else
    prefix=$local_cmd_prefix
    base_dir="$g_deploy_path"
  fi
  cd $base_dir/packages
  if [ "$(install_type)" = "bare" ];then
    # get package name
    local kw_server=$(ls $base_dir/packages | grep "server")
    local kw_libcommon=$(ls $base_dir/packages | grep "libcommon")
    if [ "$g_package_tool" = "dpkg" ];then
      eval $prefix dpkg -r kaiwudb-server >/dev/null 2>&1
      eval $prefix dpkg -r kaiwudb-libcommon >/dev/null 2>&1
      eval $prefix dpkg -r kwdb-server >/dev/null 2>&1
      eval $prefix dpkg -r kwdb-libcommon >/dev/null 2>&1
      ret=$(eval $prefix dpkg -i ./$kw_libcommon 2>&1)
      if [ $? -ne 0 ];then
        echo "$ret"
        return 1
      fi
      ret=$(eval $prefix dpkg -i ./$kw_server 2>&1)
      if [ $? -ne 0 ];then
        echo "$ret"
        return 1
      fi
    elif [ "$g_package_tool" = "rpm" ];then
      eval $prefix rpm -e kaiwudb-server >/dev/null 2>&1
      eval $prefix rpm -e kaiwudb-libcommon >/dev/null 2>&1
      eval $prefix rpm -e kwdb-server >/dev/null 2>&1
      eval $prefix rpm -e kwdb-libcommon >/dev/null 2>&1
      ret=$(eval $prefix rpm -ivh ./$kw_libcommon 2>&1)
      if [ $? -ne 0 ];then
        echo "$ret"
        return 1
      fi
      ret=$(eval $prefix rpm -ivh ./$kw_server 2>&1)
      if [ $? -ne 0 ];then
        echo "$ret"
        return 1
      fi
    fi
    sudo chown -R $(user_name):$(user_name) /usr/local/kaiwudb 2>&1 > /dev/null
    if [ ! -f /etc/kaiwudb/script/kaiwudb_env ];then
      if [ -f /etc/kaiwudb/script/kw_env ];then
        sudo mv /etc/kaiwudb/script/kw_env /etc/kaiwudb/script/kaiwudb_env
      else
        sudo touch /etc/kaiwudb/script/kaiwudb_env
        sudo bash -c 'echo KAIWUDB_START_ARG=\"\" > /etc/kaiwudb/script/kaiwudb_env'
        sudo chown $(user_name):$(user_name) /etc/kaiwudb/script/kaiwudb_env
        sudo sed -iq 's/^ExecStart=.*/& \$KAIWUDB_START_ARG/' /etc/systemd/system/kaiwudb.service >/dev/null 2>&1
        ret=$(sed -n '/^EnvironmentFile/p' /etc/systemd/system/kaiwudb.service 2>/dev/null)
        if [ "$ret" = "" ];then
          sudo sed -i '/^Environment.*/i\EnvironmentFile=\/etc\/kaiwudb\/script\/kaiwudb_env' /etc/systemd/system/kaiwudb.service >/dev/null 2>&1
        fi
      fi
    fi
    sudo sed -iq "s/\/etc\/kwdb/\/etc\/kaiwudb/g" /etc/systemd/system/kaiwudb.service
    sudo sed -iq "s/\/usr\/local\/kwdb/\/usr\/local\/kaiwudb/g" /etc/systemd/system/kaiwudb.service
    # add option --upgrade-complete to start command
    sudo sed -i "s/KW_START_ARG/KAIWUDB_START_ARG/" /etc/kaiwudb/script/kaiwudb_env
    sudo sed -i "s/kw_env/kaiwudb_env/" /etc/systemd/system/kaiwudb.service
    sudo sed -i "s/KW_START_ARG/KAIWUDB_START_ARG/" /etc/systemd/system/kaiwudb.service
  else
    # load docker images
    ret=$(docker load < KaiwuDB.tar 2>&1)
    if [ $? -ne 0 ]; then
      echo "docker load failed: $ret"
      return 1
    fi
    # get the image name
    local new_image=$(echo "$ret" | awk -F": " '{print $2}')
    cd /etc/kaiwudb/script
    local img_name=$(docker ps -a --filter name=kaiwudb-container --format {{.Image}})
    if [ -n "$img_name" ];then
      docker-compose down >/dev/null 2>&1
    else
      local img_name=$(container_image)
    fi
    docker rmi $img_name > /dev/null 2>&1
    new_image=${new_image//\//\\\/}
    eval $prefix -s "exit" >/dev/null 2>&1
    sudo sed -i \"3s/.\*/$new_image/\" /etc/kaiwudb/info/MODE 2>&1 >/dev/null
    sudo sed -i "s/image:.*/image: $new_image/" /etc/kaiwudb/script/docker-compose.yml 2>&1 >/dev/null
    sudo sed -i "s/\/etc\/kwdb/\/etc\/kaiwudb/g" /etc/kaiwudb/script/docker-compose.yml 2>&1 >/dev/null
    # delete option --license-dir
    sudo sed -i "s/--license-dir=\/kaiwudb\/license //" /etc/kaiwudb/script/docker-compose.yml
    sudo sed -i "s/\/etc\/kwdb/\/etc\/kaiwudb/" /etc/kaiwudb/script/docker-compose.yml
    sudo sed -i "s/\/etc\/kwdb/\/etc\/kaiwudb/" /etc/systemd/system/kaiwudb.service
  fi
  if [ "$REMOTE" = "ON" ];then
    sudo cp -f ~/kaiwudb_files/packages/.version /etc/kaiwudb/info
  else
    sudo cp -f $base_dir/packages/.version /etc/kaiwudb/info
  fi
  return 0
}

function change_kwdb_status() {
  local ret=""
  local cmd=""
  local node_id=""
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  node_id=$(cluster_nodeid)
  if [ $? -ne 0 ];then
    echo "Get node id failed: $node_id."
    return 1
  fi
  if [ "$(install_type)" = "bare" ];then
    cd /usr/local/kaiwudb/bin
    cmd="$prefix -u $(user_name) bash -c \"./kwbase node upgrade $node_id --host=127.0.0.1:$(local_port) $(secure_opt)\""
    ret=$(eval $cmd 2>&1)
    if [ $? -ne 0 ];then
      echo "$ret"
      return 1
    fi
  else
    ret=$(docker exec kaiwudb-container bash -c "./kwbase node upgrade $node_id --host=127.0.0.1:26257 $(secure_opt)" 2>&1)
    if [ $? -ne 0 ];then
      echo "$ret"
      return 1
    fi
  fi
  return 0
}

function recover_node() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  eval $prefix -s "exit" >/dev/null 2>&1
  if [ "$(install_type)" = "bare" ];then
    sudo sed -i 's/KAIWUDB_START_ARG="/&--upgrade-complete /' /etc/kaiwudb/script/kaiwudb_env
    kw_restart
    if [ $? -ne 0 ];then
      echo "Restart KaiwuDB failed. For more information, use 'journalctl -u kaiwudb' to view the system logs."
      return 1
    fi
    # recover env file
    sudo sed -iq 's/--upgrade-complete//g' /etc/kaiwudb/script/kaiwudb_env
  else
    cd /etc/kaiwudb/script
    sudo sed -iq 's/.*\/kaiwudb\/bin\/kwbase.*/& --upgrade-complete/' /etc/kaiwudb/script/docker-compose.yml
    kw_start
    if [ $? -ne 0 ];then
      echo "Restart KaiwuDB failed. For more information, use 'journalctl -u kaiwudb' to view the system logs"
      return 1
    fi
    sudo rm -f /etc/kaiwudb/script/docker-compose.ymlq
  fi
  local secure=$(sed -n "8p" /etc/kaiwudb/info/MODE)
  if [ "$secure" != "tls" ] && [ "$secure" != "tlcp" ] && [ "$secure" != "insecure" ];then
    sudo sed -i "8 i $(secure_mode)" /etc/kaiwudb/info/MODE
  fi
}

function get_cluster_nodes() {
  local ret=""
  if [ "$(install_type)" = "bare" ];then
    cd /usr/local/kaiwudb/bin
    ret=$(sudo -u $(user_name) bash -c "./kwbase node status --host=127.0.0.1:$(local_port) $(secure_opt)" 2>&1)
    if [ $? -ne 0 ];then
      log_err "Node status failed: $ret"
      exit 1
    fi
    # get cluster nodes addr array
    addr_array=($(sudo -u $(user_name) bash -c "./kwbase node status --host=127.0.0.1:$(local_port) $(secure_opt)" | awk -F" " -v ip=$(local_addr) 'NR!=1{split($3,arr,":");if(arr[1]!=ip && arr[1]!="NULL" && $11=$12){print arr[1]}}'))
  else
    ret=$(docker exec kaiwudb-container bash -c "./kwbase node status $(secure_opt)" 2>&1)
    if [ $? -ne 0 ];then
      log_err "Node status failed: $ret"
      exit 1
    fi
    # get cluster nodes addr array
    addr_array=($(docker exec kaiwudb-container bash -c "./kwbase node status $(secure_opt) --host=127.0.0.1:26257" | awk -F " " -v ip=$(local_addr) 'NR!=1{split($3,arr,":");if(arr[1]!=ip && arr[1]!="NULL" && $11==$12)print arr[1]}'))
  fi
  # get all nodes ip
  for ((i=0; i<${#addr_array[@]}; i++))
  do
    addr_array[$i]=${addr_array[$i]%:*}
  done
  ssh_port=$(sed -n "2p" /etc/kaiwudb/info/NODE)
  ssh_user=$(sed -n "3p" /etc/kaiwudb/info/NODE)
}