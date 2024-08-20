#! /bin/bash

function rename_directory() {
  if [ -d /etc/kwdb ];then
    eval $kw_cmd_prefix mv /etc/kwdb /etc/kaiwudb
    config_dir="/etc/kaiwudb"
  fi
}

function upgrade_verify_files() {
  local packages_array=(server libcommon)
  local files=$(ls $g_deploy_path/packages)
  # determine the package manager tool
  if [ "$1" == "bare" ];then
    dpkg --help >/dev/null 2>&1
    if [ $? -ne 0 ];then
      g_package_tool="rpm"
    else
      g_package_tool="dpkg"
    fi

    for item in ${packages_array[@]}
    do
      ret=$(echo "$files" | grep -qo "$item" &&  echo "yes" || echo "no")
      if [ "$ret" == "no" ];then
        log_err "Package $item does not exist."
        exit 1
      fi
    done
  else
    local ret=$(echo $files | grep -wq "KaiwuDB.tar" && echo "yes" || echo "no")
    if [ "$ret" == "no" ];then
      log_err "docker image is not exist."
      exit 1
    fi
  fi
  # if [ ! -f "$g_deploy_path/license/custLicense.crt.sign" ];then
  #   log_err "License file does not exist."
  #   exit 1
  # fi
}

function rollback() {
  if [ "$g_package_tool" == "dpkg" ];then
    eval $kw_cmd_prefix dpkg -r kaiwudb-server >/dev/null 2>&1
    eval $kw_cmd_prefix dpkg -r kaiwudb-libcommon >/dev/null 2>&1
    eval $kw_cmd_prefix dpkg -r kwdb-server >/dev/null 2>&1
    eval $kw_cmd_prefix dpkg -r kwdb-libcommon >/dev/null 2>&1
    eval $kw_cmd_prefix dpkg -r libopentelemetry-kw >/dev/null 2>&1
    eval $kw_cmd_prefix dpkg -r libopentelemetry-kaiwudb >/dev/null 2>&1
  elif [ "$g_package_tool" == "rpm" ];then
    eval $kw_cmd_prefix rpm -e kaiwudb-server >/dev/null 2>&1
    eval $kw_cmd_prefix rpm -e kaiwudb-libcommon >/dev/null 2>&1
    eval $kw_cmd_prefix rpm -e kwdb-server >/dev/null 2>&1
    eval $kw_cmd_prefix rpm -e kwdb-libcommon >/dev/null 2>&1
    eval $kw_cmd_prefix rpm -e libopentelemetry-kw >/dev/null 2>&1
    eval $kw_cmd_prefix rpm -e libopentelemetry-kaiwudb >/dev/null 2>&1
  fi
  local kw_data_dir=$(sed -n "4p" /etc/kaiwudb/info/MODE)
  if [ -d /etc/kwdb ];then
    eval $kw_cmd_prefix tar -zcvf ~/kaiwudb_files.tar.gz  -C/etc kwdb
  elif [ -d /etc/kaiwudb ];then
    eval $kw_cmd_prefix tar -zcvf ~/kaiwudb_files.tar.gz  -C/etc kaiwudb
  fi
  eval $kw_cmd_prefix tar -zcvf ~/kw_data.tar.gz  $kw_data_dir
}

# version compare function
function version_le() {
	test "$(echo "$@" | tr " " "\n" | sort -V | head -n 1)" == "$1"; 
}

# check version whether satisfies
function version_compare() {
  if [ "$1" == "bare" ];then
    if [ -f /usr/local/kwdb/bin/kwbase ];then
      cd /usr/local/kwdb/bin
    else
      cd /usr/local/kaiwudb/bin
    fi
    old_version=`export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && kwbase version | awk -F": " '{if($1~/^KaiwuDB Version$/){gsub(/V/,"",$2);gsub(" ","",$2);print $2}}' 2>&1`
    if [ $? -ne 0 ];then
      log_err "Get local KaiwuDB version failed:$old_version"
      exit 1
    fi
    cd $g_deploy_path/packages
    local pac_server=`ls $g_deploy_path/packages | grep "server"`
    if [ "$g_package_tool" == "dpkg" ];then
      local new_version=`$g_package_tool --info ./$pac_server | awk -F": " '{if($1~/Version/){print $2}}' | awk -F"-" '{print $1}'`
      dpkg --compare-versions $new_version gt $old_version
      if [ $? -ne 0 ]; then
        log_err "UPGRADE ERROR:The requested upgrade version is older than the current installed version.(current:$old_version new_version:$new_version)"
        exit 1
      fi
    else
      local new_version=`$g_package_tool -qpi ./$pac_server | awk -F": " '{if($1~/Version/){print $2}}' | awk -F"-" '{print $1}'`
      local ret=`rpm --eval "%{lua:print(rpm.vercmp('$new_version', '$old_version'))}"`
      if [ $ret -lt 1 ]; then
        log_err "UPGRADE ERROR:The requested upgrade version is older than the current installed version.(current:$old_version new_version:$new_version)"
        exit 1
      fi
    fi
  else
    local image_name=$(install_dir)
    old_version=`docker image ls "--format={{.Tag}}" $image_name | awk -F"-" '{print $1}'`
    cd $g_deploy_path/packages
    image_name=`docker load < KaiwuDB.tar 2>/dev/null | awk -F": " '{print $2}'`
    local new_version=`docker image ls "--format={{.Tag}}" $image_name | awk -F"-" '{print $1}'`
    if version_le $new_version $old_version; then
      if [ "$new_version" != "$old_version" ];then
        docker rmi $image_name 2>&1 >/dev/null
      fi
      log_err "UPGRADE ERROR:The requested upgrade version is older than the current installed version.(current:$old_version new_version:$new_version)"
      exit 1
    fi
  fi
}

function upgrade() {
  local ret=""
  if [ "$ins_type" == "bare" ];then
    # get package name
    g_kw_user=$(user_name)
    local kw_server=`ls $g_deploy_path/packages | grep "server"`
    local kw_libcommon=`ls $g_deploy_path/packages | grep "libcommon"`
    if [ "$g_package_tool" == "dpkg" ];then
      cd $g_deploy_path/packages
			eval $kw_cmd_prefix dpkg -r kaiwudb-server >/dev/null 2>&1
			eval $kw_cmd_prefix dpkg -r kaiwudb-libcommon >/dev/null 2>&1
			eval $kw_cmd_prefix dpkg -r kwdb-server >/dev/null 2>&1
			eval $kw_cmd_prefix dpkg -r kwdb-libcommon >/dev/null 2>&1
      eval $kw_cmd_prefix dpkg -r libopentelemetry-kw >/dev/null 2>&1
      eval $kw_cmd_prefix dpkg -r libopentelemetry-kaiwudb >/dev/null 2>&1
      ret=`eval $kw_cmd_prefix dpkg -i ./$kw_libcommon 2>&1`
      if [ $? -ne 0 ];then
        log_err_without_console $ret
        log_err_only_console "Error occurred during $kw_libcommon installation. Please check log."
        exit 1
      fi
      ret=`eval $kw_cmd_prefix dpkg -i ./$kw_server 2>&1`
      if [ $? -ne 0 ];then
        log_err_without_console $ret
        log_err_only_console "Error occurred during $kw_server installation. Please check log."
        exit 1
      fi
    elif [ "$g_package_tool" == "rpm" ];then
      cd $g_deploy_path/packages
			eval $kw_cmd_prefix rpm -e kaiwudb-server >/dev/null 2>&1
			eval $kw_cmd_prefix rpm -e kaiwudb-libcommon >/dev/null 2>&1
			eval $kw_cmd_prefix rpm -e kwdb-server >/dev/null 2>&1
			eval $kw_cmd_prefix rpm -e kwdb-libcommon >/dev/null 2>&1
      eval $kw_cmd_prefix rpm -e libopentelemetry-kw >/dev/null 2>&1
      eval $kw_cmd_prefix rpm -e libopentelemetry-kaiwudb >/dev/null 2>&1
      ret=`eval $kw_cmd_prefix rpm -ivh ./$kw_libcommon ./$kw_server 2>&1`
      if [ $? -ne 0 ];then
        log_err_without_console $ret
        log_err_only_console "Error occurred during $kw_server installation. Please check log."
        exit 1
      fi
    fi
    eval $kw_cmd_prefix sed -i \"s/--license-dir=\\\/etc\\\/kwdb\\\/license //\" /etc/systemd/system/kaiwudb.service
    sudo chown -R $g_kw_user:$g_kw_user /usr/local/kaiwudb 2>&1 > /dev/null
    if [ ! -f /etc/kaiwudb/script/kaiwudb_env ];then
      if [ -f /etc/kaiwudb/script/kw_env ];then
        eval $kw_cmd_prefix mv /etc/kaiwudb/script/kw_env /etc/kaiwudb/script/kaiwudb_env
      else
        eval $kw_cmd_prefix touch /etc/kaiwudb/script/kaiwudb_env
        sudo bash -c "echo KAIWUDB_START_ARG=\\\"\\\" > /etc/kaiwudb/script/kaiwudb_env"
        sudo chown $kw_user:$kw_user /etc/kaiwudb/script/kaiwudb_env
        sudo sed -iq "s/^ExecStart=.*/& \\\$KAIWUDB_START_ARG/" /etc/systemd/system/kaiwudb.service >/dev/null 2>&1
        local service_check=$(sudo sed -n '/^EnvironmentFile/p' /etc/systemd/system/kaiwudb.service 2>/dev/null)
        if [ "$service_check" == "" ];then
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
    # check whether docker is installed
    docker --help >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      log_err "Please check if docker is installed."
      exit 1
    fi
    # check whether docker-compose is installed
    docker-compose --version >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      log_err "Please check if docker compose is installed."
      exit 1
    fi
    cd $g_deploy_path/packages
    # load docker images
    g_image_name=`docker load < KaiwuDB.tar 2>/dev/null | awk -F": " '{print $2}'`
    if [ $? -ne 0 ]; then
      log_err "Docker image load failed."
      exit 1
    fi
    cd /etc/kaiwudb/script
		local img_name=`docker ps -a --filter name=kaiwudb-container --format {{.Image}}`
		if [ -n "$img_name" ];then
			# docker rm kaiwudb-container > /dev/null 2>&1
      docker-compose down >/dev/null 2>&1
		else
			local img_name=$(container_image)
		fi
    docker rmi $img_name > /dev/null 2>&1
    g_image_name=${g_image_name//\//\\\/}
    eval $kw_cmd_prefix sed -i \"3s/.\*/$g_image_name/\" /etc/kaiwudb/info/MODE 2>&1 >/dev/null
    sudo sed -i "s/image:.*/image: $g_image_name/" /etc/kaiwudb/script/docker-compose.yml 2>&1 >/dev/null
    sudo sed -i "s/\/etc\/kwdb/\/etc\/kaiwudb/g" /etc/kaiwudb/script/docker-compose.yml 2>&1 >/dev/null
    # delete option --license-dir
    sudo sed -i "s/--license-dir=\/kaiwudb\/license //" /etc/kaiwudb/script/docker-compose.yml
    sudo sed -i "s/\/etc\/kwdb/\/etc\/kaiwudb/" /etc/kaiwudb/script/docker-compose.yml
    sudo sed -i "s/\/etc\/kwdb/\/etc\/kaiwudb/" /etc/systemd/system/kaiwudb.service
  fi
  return 0
}

function cluster_nodeid() {
  local kw_port=$(sed -n "5p" /etc/kaiwudb/info/MODE)
  if [ "`ls -A /etc/kaiwudb/certs`" == "" ];then
    local kw_secure="--insecure"
  else
    if [ "$ins_type" == "bare" ];then
      local kw_secure="--certs-dir=/etc/kaiwudb/certs"
    else
      local kw_secure="--certs-dir=/kaiwudb/certs"
    fi
  fi  
  local advertise_ip=$(sed -n "8p" /etc/kaiwudb/info/MODE)
  if [ "$ins_type" == "bare" ];then
    local kw_user=$(user_name)
    if [ -f /usr/local/kwdb/bin/kwbase ];then
      cd /usr/local/kwdb/bin
    else
      cd /usr/local/kaiwudb/bin
    fi
    kw_node_id=`sudo -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node status --host=127.0.0.1:$kw_port $kw_secure | awk -F\" \" -v ip=$advertise_ip '{split(\\\$3,arr,\":\");if(arr[1]==ip){print \\\$1}}'"`
    if [ $? -ne 0 ];then
      log_err "Failed to get node_id"
      exit 1
    fi
  else
    kw_node_id=`docker exec kaiwudb-container bash -c "./kwbase node status $kw_secure --host=127.0.0.1:26257" | awk -F" " -v ip=$advertise_ip '{split($3,arr,":");if(arr[1]==ip)print $1}'`
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

function change_kwdb_status() {
  local kw_port=$(sed -n "5p" /etc/kaiwudb/info/MODE)
  if [ "`ls -A /etc/kaiwudb/certs`" = "" ];then
    local kw_secure="--insecure"
  else
    if [ "$ins_type" == "bare" ];then
      local kw_secure="--certs-dir=/etc/kaiwudb/certs"
    else
      local kw_secure="--certs-dir=/kaiwudb/certs"
    fi
  fi
  if [ "$ins_type" == "bare" ];then
    local info=""
    local kw_user=$(user_name)
    if [ -f /usr/local/kwdb/bin/kwbase ];then
      cd /usr/local/kwdb/bin
    else
      cd /usr/local/kaiwudb/bin
    fi
    local cmd="$kw_cmd_prefix -u $kw_user bash -c \"export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node upgrade $kw_node_id --host=127.0.0.1:$kw_port $kw_secure\""
    info=$(eval $cmd 2>&1)
    if [ $? -ne 0 ];then
      log_err "Local change KaiwuDB status failed($info)"
      exit 1
    fi
  else
    info=$(docker exec kaiwudb-container bash -c "./kwbase node upgrade $kw_node_id --host=127.0.0.1:26257 $kw_secure" 2>&1)
    if [ $? -ne 0 ];then
      log_err "Change KaiwuDB status failed($info)"
      exit 1
    fi
  fi
}

function remote_upgrade() {
  # return value:
  # 0: success
  # 1: kwbase execute failed
  # 2: version compare failed
  # 3: kw-libcommon install failed
  # 4: kw-server install failed
  # 5: get node_id failed
  # 6: change KaiwuDB status failed
  # 7: KaiwuDB restart failed
  # 8: KaiwuDB is not running

  ssh -p $2 $3@$1 "
function version_le() {
	test \"\$(echo \"\$@\" | tr \" \" \"\n\" | sort -V | head -n 1)\" == \"\$1\"; 
}

function rename_directory() {
  if [ -d /etc/kwdb ];then
    eval $g_node_prefix mv /etc/kwdb /etc/kaiwudb
  fi
}

function rollback() {
  if [ \"$g_package_tool\" == \"dpkg\" ];then
    eval $g_node_prefix dpkg -r kaiwudb-server >/dev/null 2>&1
    eval $g_node_prefix dpkg -r kaiwudb-libcommon >/dev/null 2>&1
    eval $g_node_prefix dpkg -r kwdb-server >/dev/null 2>&1
    eval $g_node_prefix dpkg -r kwdb-libcommon >/dev/null 2>&1
    eval $g_node_prefix dpkg -r libopentelemetry-kw >/dev/null 2>&1
  elif [ \"$g_package_tool\" == \"rpm\" ];then
    eval $g_node_prefix rpm -e kaiwudb-server >/dev/null 2>&1
    eval $g_node_prefix rpm -e kaiwudb-libcommon >/dev/null 2>&1
    eval $g_node_prefix rpm -e kwdb-server >/dev/null 2>&1
    eval $g_node_prefix rpm -e kwdb-libcommon >/dev/null 2>&1
    eval $g_node_prefix rpm -e libopentelemetry-kw >/dev/null 2>&1
  fi
  local kw_data_dir=\$(sed -n \"4p\" /etc/kaiwudb/info/MODE)
  $g_node_prefix tar -zcvf ~/kaiwudb_files.tar.gz  -C/etc kaiwudb
  $g_node_prefix tar -zcvf ~/kw_data.tar.gz  \$kw_data_dir
}

function upgrade() {
  if [ \"$ins_type\" == \"bare\" ];then
    # get package name
    g_kw_user=\$(sed -n \"7p\" /etc/kaiwudb/info/MODE)
    cd ~/kaiwudb_files
    local kw_server=\`ls | grep \"server\"\`
    local kw_libcommon=\`ls | grep \"libcommon\"\`
    local kw_opentelemetry=\`ls $g_deploy_path/packages | grep \"libopentelemetry\"\`
    if [ \"$g_package_tool\" == \"dpkg\" ];then
			$g_node_prefix dpkg -r kaiwudb-server >/dev/null 2>&1
			$g_node_prefix dpkg -r kaiwudb-libcommon >/dev/null 2>&1
			$g_node_prefix dpkg -r kwdb-server >/dev/null 2>&1
			$g_node_prefix dpkg -r kwdb-libcommon >/dev/null 2>&1
      $g_node_prefix dpkg -r libopentelemetry-kw >/dev/null 2>&1
      $g_node_prefix dpkg -r libopentelemetry-kaiwudb >/dev/null 2>&1
      $g_node_prefix dpkg -i ./\$kw_libcommon 2>&1
      if [ \$? -ne 0 ];then
        rollback
        exit 3
      fi
      $g_node_prefix dpkg -i ./\$kw_server 2>&1
      if [ \$? -ne 0 ];then
        rollback
        exit 4
      fi
    elif [ \"$g_package_tool\" == \"rpm\" ];then
      cd ~/kaiwudb_files
			$g_node_prefix rpm -e kaiwudb-server >/dev/null 2>&1
			$g_node_prefix rpm -e kaiwudb-libcommon >/dev/null 2>&1
			$g_node_prefix rpm -e kwdb-server >/dev/null 2>&1
			$g_node_prefix rpm -e kwdb-libcommon >/dev/null 2>&1
      $g_node_prefix rpm -e libopentelemetry-kw >/dev/null 2>&1
      $g_node_prefix rpm -e libopentelemetry-kaiwudb >/dev/null 2>&1
      $g_node_prefix rpm -ivh  ./\$kw_libcommon ./\$kw_server 2>&1
      if [ \$? -ne 0 ];then
        rollback
        exit 4
      fi
    fi
    sudo sed -i \"s/--license-dir=\/etc\/kaiwudb\/license //\" /etc/systemd/system/kaiwudb.service
    sudo chown -R \$g_kw_user:\$g_kw_user /usr/local/kaiwudb >/dev/null 2>&1
    if [ ! -f /etc/kaiwudb/script/kaiwudb_env ];then
      if [ -f /etc/kaiwudb/script/kw_env ];then
        $g_node_prefix mv /etc/kaiwudb/script/kw_env /etc/kaiwudb/script/kaiwudb_env
      else
        $g_node_prefix touch /etc/kaiwudb/script/kaiwudb_env
        sudo bash -c \"echo KAIWUDB_START_ARG=\\\\\\\"\\\\\\\" > /etc/kaiwudb/script/kaiwudb_env\"
        sudo chown \$g_kw_user:\$g_kw_user /etc/kaiwudb/script/kaiwudb_env
        sudo sed -iq \"s/^ExecStart=.*/& \\\\\\\$KAIWUDB_START_ARG/\" /etc/systemd/system/kaiwudb.service >/dev/null 2>&1
        service_check=\$(sudo sed -n '/^EnvironmentFile/p' /etc/systemd/system/kaiwudb.service 2>/dev/null)
        if [ \"\$service_check\" == \"\" ];then
          sudo sed -i '/^Environment.*/i\EnvironmentFile=\/etc\/kaiwudb\/script\/kaiwudb_env' /etc/systemd/system/kaiwudb.service >/dev/null 2>&1
        fi
      fi
    fi
    sudo sed -i \"s/KW_START_ARG/KAIWUDB_START_ARG/\" /etc/kaiwudb/script/kaiwudb_env
    sudo sed -i \"s/KW_START_ARG/KAIWUDB_START_ARG/\" /etc/systemd/system/kaiwudb.service
    sudo sed -i \"s/kw_env/kaiwudb_env/\" /etc/systemd/system/kaiwudb.service
    sudo sed -iq \"s/\/etc\/kwdb/\/etc\/kaiwudb/g\" /etc/systemd/system/kaiwudb.service
    sudo sed -iq \"s/\/usr\/local\/kwdb/\/usr\/local\/kaiwudb/g\" /etc/systemd/system/kaiwudb.service
  else
    cd ~/kaiwudb_files
    # load docker images
    cd /etc/kaiwudb/script
    docker-compose down
    docker rmi \$old_img_name
    new_img_name=\${new_img_name//\//\\\\/}
    sudo sed -i \"3s/.*/\$new_img_name/\" /etc/kaiwudb/info/MODE
    sudo sed -i \"s/image:.*/image: \$new_img_name/\" /etc/kaiwudb/script/docker-compose.yml
    sudo sed -i \"s/\/etc\/kwdb/\/etc\/kaiwudb/g\" /etc/kaiwudb/script/docker-compose.yml
    # delete option --license-dir
    sudo sed -i \"s/--license-dir=\/kaiwudb\/license //\" /etc/kaiwudb/script/docker-compose.yml
    sudo sed -i \"s/\/etc\/kwdb/\/etc\/kaiwudb/\" /etc/kaiwudb/script/docker-compose.yml
    sudo sed -i \"s/\/etc\/kwdb/\/etc\/kaiwudb/\" /etc/systemd/system/kaiwudb.service
  fi
}

rename_directory

if [ \"$ins_type\" == \"bare\" ];then
  if [ -f /usr/local/kwdb/bin/kwbase ];then
    cd /usr/local/kwdb/bin
  else
    cd /usr/local/kaiwudb/bin
  fi
  old_version=\`export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && kwbase version | awk -F\": \" '{if(\\\$1~/^KaiwuDB Version/){gsub(/V/,\"\",\\\$2);gsub(\" \",\"\",\\\$2);print \\\$2}}' 2>&1\`
  if [ \$? -ne 0 ];then
    exit 1
  fi
  cd ~/kaiwudb_files
  pkg_server=\`ls | grep \"server\"\`
  if [ \"$g_package_tool\" == \"dpkg\" ];then
    new_version=\`dpkg --info ./\$pkg_server | awk -F\": \" '{if(\$1~/Version/){print \$2}}' | awk -F\"-\" '{print \$1}'\`
    dpkg --compare-versions \$new_version gt \$old_version
    if [ \$? -ne 0 ]; then
      exit 2
    fi
  else
    new_version=\`rpm -qpi ./\$pkg_server | awk -F\": \" '{if(\$1~/Version/){print \$2}}' | awk -F\"-\" '{print \$1}'\`
    ret=\`rpm --eval \"%{lua:print(rpm.vercmp('\$new_version', '\$old_version'))}\"\`
    if [ \$ret -lt 1 ]; then
      exit 2
    fi
  fi
else
  old_img_name=\$(sed -n \"3p\" /etc/kaiwudb/info/MODE)
  old_version=\`docker image ls \"--format={{.Tag}}\" \$old_img_name | awk -F\"-\" '{print \$1}'\`
  cd ~/kaiwudb_files
  new_img_name=\`docker load < KaiwuDB.tar 2>/dev/null | awk -F\": \" '{print \$2}'\`
  new_version=\`docker image ls \"--format={{.Tag}}\" \$new_img_name | awk -F\"-\" '{print \$1}'\`
  if version_le \$new_version \$old_version; then
    if [ \"\$new_version\" != \"\$new_img_name\" ];then
      docker rmi \$new_img_name >/dev/null 2>&1
    fi
    exit 2
  fi
fi

systemctl status kaiwudb >/dev/null 2>&1
if [ \$? -ne 0 ];then
  exit 8
fi

# online-upgrade
# get node_id
# echo on-line
kw_port=\$(sed -n \"5p\" /etc/kaiwudb/info/MODE)
if [ \"\`ls -A /etc/kaiwudb/certs\`\" == \"\" ];then
  kw_secure="--insecure"
else
  if [ \"$ins_type\" == \"bare\" ];then
    kw_secure=\"--certs-dir=/etc/kaiwudb/certs\"
  else
    kw_secure=\"--certs-dir=/kaiwudb/certs\"
  fi
fi
advertise_ip=\$(sed -n \"8p\" /etc/kaiwudb/info/MODE)
$g_node_prefix systemctl status kaiwudb >/dev/null 2>&1
if [ \"$ins_type\" == \"bare\" ];then
  g_kw_user=\$(sed -n \"7p\" /etc/kaiwudb/info/MODE)
  if [ -f /usr/local/kwdb/bin/kwbase ];then
    cd /usr/local/kwdb/bin
  else
    cd /usr/local/kaiwudb/bin
  fi
  kw_node_id=\$(sudo -u \$g_kw_user bash -c \"export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node status --host=127.0.0.1:\$kw_port \$kw_secure | awk -F\\\" \\\" -v ip=\$advertise_ip '{split(\\\$3,arr,\\\":\\\");if(arr[1]==ip){print \\\$1}}'\")
  if [ \$? -ne 0 ];then
    exit 5
  fi
else
  kw_node_id=\`docker exec kaiwudb-container bash -c \"./kwbase node status \$kw_secure --host=127.0.0.1:26257\" | awk -F\" \" -v ip=\$advertise_ip '{split(\$3,arr,\":\");if(arr[1]==ip)print \$1}'\`
  if [ \$? -ne 0 ];then
    exit 5
  fi
fi
echo \$kw_node_id
if [ \"\$kw_node_id\" != \"\" ];then
  if [[ \"\$kw_node_id\" =~ ^[0-9]+$ ]];then
    if [ \"$ins_type\" == \"bare\" ];then
      if [ -f /usr/local/kwdb/bin/kwbase ];then
        cd /usr/local/kwdb/bin
      else
        cd /usr/local/kaiwudb/bin
      fi
      sudo -u \$g_kw_user bash -c \"export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node upgrade \$kw_node_id --host=127.0.0.1:\$kw_port \$kw_secure\"
      if [ \$? -ne 0 ];then
        exit 6
      fi
    else
      docker exec kaiwudb-container bash -c \"./kwbase node upgrade \$kw_node_id --host=127.0.0.1:26257 \$kw_secure\"
      if [ \$? -ne 0 ];then
        exit 6
      fi
    fi
  else
    exit 5
  fi
fi
upgrade
if [ \"$ins_type\" == \"bare\" ];then
  # add option --upgrade-complete to start command
  sudo sed -i \"s/KAIWUDB_START_ARG=\\\"/&--upgrade-complete /\" /etc/kaiwudb/script/kaiwudb_env
  # reload service file
  sudo systemctl daemon-reload >/dev/null 2>&1
  # restart KaiwuDB
  sudo systemctl restart kaiwudb >/dev/null 2>&1
  if [ \$? -ne 0 ];then
    exit 7
  fi
  # wait KaiwuDB running
  sleep 2
  systemctl status kaiwudb >/dev/null 2>&1
  if [ \$? -ne 0 ];then
    exit 7
  fi
  # recover env file
  sudo sed -i \"s/--upgrade-complete //\" /etc/kaiwudb/script/kaiwudb_env >/dev/null 2>&1
else
  sudo sed -iq \"s/.*\/kaiwudb\/bin\/kwbase.*/& --upgrade-complete/\" /etc/kaiwudb/script/docker-compose.yml
  cd /etc/kaiwudb/script
  docker-compose up -d
  count=0
  until [ \$count -gt 60 ]
  do
    sleep 2
    stat=\$(docker ps -a --filter name=kaiwudb-container --format {{.Status}} | awk '{if(\$0~/^(Up).*/){print \"yes\";}else if(\$0~/^(Exited).*/){print \"failed\";}else{print \"no\";}}')
    if [ \"\$stat\" == \"yes\" ];then
      break
    elif [ \"\$stat\" == \"failed\" ];then
      exit 7
    fi
    ((count++));
  done
  if [ \$count -gt 60 ];then
    exit 7
  fi
  docker-compose down
  sudo sed -iq \"s/--upgrade-complete//\" /etc/kaiwudb/script/docker-compose.yml
  sudo systemctl daemon-reload
  sudo systemctl start kaiwudb
  sudo rm -f /etc/kaiwudb/script/docker-compose.ymlq
fi
exit 0
" >/dev/null 2>&1
  case $? in
  1)
    log_err "node[$1]Get KaiwuDB version failed."
    ;;
  2)
    log_err "node[$1]New version is lower than current version."
    ;;
  3)
    log_err "node[$1]kaiwudb-libcommon install failed."
    ;;
  4)
    log_err "node[$1]kaiwudb-server install failed."
    ;;
  5)
    log_err "node[$1]KaiwuDB get node_id failed."
    ;;
  6)
    log_err "node[$1]Change status failed."
    ;;
  7)
    log_err "node[$1]KaiwuDB restart failed."
    ;;
  8)
    log_err "node[$1]KaiwuDB is not running."
  esac
}

function recover_node() {
  # check kaiwudb_env whether exist
  local kw_user=$(user_name)

  if [ "$ins_type" == "bare" ];then
    sudo sed -i "s/KAIWUDB_START_ARG=\"/&--upgrade-complete /" /etc/kaiwudb/script/kaiwudb_env
    # reload service file
    sudo systemctl daemon-reload >/dev/null 2>&1
    # restart KaiwuDB
    sudo systemctl restart kaiwudb >/dev/null 2>&1
    if [ $? -ne 0 ];then
      log_err "Start KaiwuDB failed."
      exit 1
    fi
    # wait KaiwuDB running
    sleep 2
    systemctl status kaiwudb >/dev/null 2>&1
    if [ $? -ne 0 ];then
      log_err "Start KaiwuDB failed."
      exit 1
    fi
    # recover env file
    sudo sed -iq "s/--upgrade-complete//g" /etc/kaiwudb/script/kaiwudb_env
  else
    sudo sed -iq "s/.*\/kaiwudb\/bin\/kwbase.*/& --upgrade-complete/" /etc/kaiwudb/script/docker-compose.yml
    cd /etc/kaiwudb/script
    docker-compose up -d >/dev/null 2>&1
    local count=0
    until [ $count -gt 60 ]
    do
      sleep 2
      local stat=`docker ps -a --filter name=kaiwudb-container --format {{.Status}} | awk '{if($0~/^(Up).*/){print "yes";}else if($0~/^(Exited).*/){print "failed";}else{print "no";}}'`
      if [ "$stat" == "yes" ];then
        break
      elif [ "$stat" == "failed" ];then
        log_err "Restart KaiwuDB failed."
        exit 1
      fi
      ((count++));
    done
    if [ $count -gt 60 ];then
      log_err "Restart KaiwuDB failed."
      exit 1
    fi
    docker-compose down >/dev/null 2>&1
    sudo sed -iq "s/--upgrade-complete//" /etc/kaiwudb/script/docker-compose.yml
    sudo systemctl daemon-reload
    sudo systemctl start kaiwudb >/dev/null 2>&1
    sudo rm -f /etc/kaiwudb/script/docker-compose.ymlq
  fi
}

function get_cluster_nodes() {
  local kw_port=$(sed -n "5p" /etc/kaiwudb/info/MODE)
  if [ "`ls -A /etc/kaiwudb/certs`" == "" ];then
    local kw_secure="--insecure"
  else
    if [ "$ins_type" == "bare" ];then
      local kw_secure="--certs-dir=/etc/kaiwudb/certs"
    else
      local kw_secure="--certs-dir=/kaiwudb/certs"
    fi
  fi  
  local advertise_ip=$(sed -n "8p" /etc/kaiwudb/info/MODE)
  if [ "$ins_type" == "bare" ];then
    local kw_user=$(user_name)
    if [ -f /usr/local/kwdb/bin/kwbase ];then
      cd /usr/local/kwdb/bin
    else
      cd /usr/local/kaiwudb/bin
    fi
    # get cluster nodes addr array
    addr_array=($(sudo -u $kw_user bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase node status --host=127.0.0.1:$kw_port $kw_secure" | awk -F" " -v ip=$advertise_ip 'NR!=1{split($3,arr,":");if(arr[1]!=ip && arr[1]!="NULL" && $0~/.*healthy/){print arr[1]}}'))
    if [ $? -ne 0 ];then
      log_err "Failed to get nodes."
      exit 1
    fi
  else
    # get cluster nodes addr array
    addr_array=($(docker exec kaiwudb-container bash -c "./kwbase node status $kw_secure --host=127.0.0.1:26257" | awk -F " " -v ip=$advertise_ip 'NR!=1{split($3,arr,":");if(arr[1]!=ip && arr[1]!="NULL" && $0~/.*healthy/)print arr[1]}'))
    if [ $? -ne 0 ];then
      log_err "Failed to get nodes."
      exit 1
    fi
  fi
  # get all nodes ip
  for ((i=0; i<${#addr_array[@]}; i++))
  do
    addr_array[$i]=${addr_array[$i]%:*}
  done
}

function get_ssh_info() {
  if [ ! -e /etc/kaiwudb/info/NODE ];then
    return 1
  fi
  ssh_port=$(sed -n "2p" /etc/kaiwudb/info/NODE)
  ssh_user=$(sed -n "3p" /etc/kaiwudb/info/NODE)
  return 0
}