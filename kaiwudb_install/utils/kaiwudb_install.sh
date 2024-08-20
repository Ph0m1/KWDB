#! /bin/bash

# verify the installation package and get package manager tool
function verify_files() {
  local files=$(ls $g_deploy_path/packages)
  # determine the installation mode
  local ret=$(echo $files | grep -wq "KaiwuDB.tar" && echo "yes" || echo "no")
  if [ "$ret" != "yes" ];then
    g_deploy_type="bare"
  else
    g_deploy_type="container"
  fi
  local packages_array=(server libcommon)
  # determine the package manager tool
  if [ "$g_deploy_type" == "bare" ];then
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
  fi
}

# if installing failed, running this function to rollback
function rollback() {
  # remove packages
  if [ "$g_deploy_type" == "bare" ];then
    if [ "$g_package_tool" == "dpkg" ];then
      eval $kw_cmd_prefix dpkg -r kaiwudb-server >/dev/null 2>&1
      sudo dpkg -r kaiwudb-libcommon >/dev/null 2>&1
      eval $kw_cmd_prefix dpkg -r kwdb-server >/dev/null 2>&1
      sudo dpkg -r kwdb-libcommon >/dev/null 2>&1
    elif [ "$g_package_tool" == "rpm" ];then
      eval $kw_cmd_prefix rpm -e kaiwudb-server >/dev/null 2>&1
      sudo rpm -e kaiwudb-libcommon >/dev/null 2>&1
      eval $kw_cmd_prefix rpm -e kwdb-server >/dev/null 2>&1
      sudo rpm -e kwdb-libcommon >/dev/null 2>&1
    fi
    sudo rm -rf /etc/kaiwudb $g_data_root /etc/systemd/system/kaiwudb.service
    sudo userdel -r kaiwudb >/dev/null 2>&1
  else
    eval $kw_cmd_prefix rm -rf /etc/kaiwudb /etc/kwdb $g_data_root /etc/systemd/system/kaiwudb.service
  fi
  sudo rm $g_deploy_path/kw_pipe >/dev/null 2>&1
  # exit 1
}

# local init
function init_directory() {
  log_info_without_console "start init directory $config_dir $g_data_root"
  eval $kw_cmd_prefix rm -rf $config_dir $g_data_root
  sudo mkdir -p $config_dir/certs
  sudo mkdir -p $config_dir/script
  sudo mkdir -p $config_dir/info
  sudo mkdir -p $g_data_root
  log_info_without_console "init directory success"
}

function node_init_directory() {
  args=${1//:/ }
  eval set -- "${args}"
  ssh -t -p $2 $3@$1 >/dev/null 2>&1  << end
$g_node_prefix rm -rf $config_dir $g_data_root
sudo mkdir -p $config_dir/certs
sudo mkdir -p $config_dir/script
sudo mkdir -p $config_dir/info
sudo mkdir -p $g_data_root
end
  if [ $? -ne 0 ];then
    echo $1
    return
  fi
}

function parallel_init() {
  declare -a parallel_func
  local node_array=($1)
  local ssh_port=$2
  local ssh_user=$3
  if [ ${#node_array[@]} -ne 0 ];then
    log_info_without_console "start init directory in ${node_array[*]} $config_dir /$g_data_root"
    for ((i=0; i<${#node_array[@]}; i++))
    do
      tmp=(${node_array[$i]//:/ })
      parallel_func[$i]="${tmp[0]}:$ssh_port:$ssh_user"
    done
    # export function and variable
    export -f node_init_directory
    export g_node_prefix=$g_node_prefix
    export g_data_root=$g_data_root
    local arr=($(echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "node_init_directory args" 2>/dev/null))
    # echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "node_init_directory args"
    if [ ${#arr[@]} -ne 0 ];then
      log_err "Node [${arr[@]}] init directory failed."
      rollback
      node_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
      exit 1
    fi
    log_info_without_console "init directory in ${node_array[*]} success"
  fi
}

# if installing failed, running this function to rollback
function node_rollback() {
  args=${1//:/ }
  eval set -- "${args}"
  ssh -t -p $2 $3@$1 >/dev/null 2>&1 <<end
  # remove packages
  if [ "$g_deploy_type" == "bare" ];then
    if [ "$g_package_tool" == "dpkg" ];then
      $g_node_prefix dpkg -r kaiwudb-server >/dev/null 2>&1
      sudo dpkg -r kaiwudb-libcommon >/dev/null 2>&1
      $g_node_prefix dpkg -r kwdb-server >/dev/null 2>&1
      sudo dpkg -r kwdb-libcommon >/dev/null 2>&1
    elif [ "$g_package_tool" == "rpm" ];then
      $g_node_prefix rpm -e kaiwudb-server >/dev/null 2>&1
      sudo rpm -e kaiwudb-libcommon >/dev/null 2>&1
      $g_node_prefix rpm -e kwdb-server >/dev/null 2>&1
      sudo rpm -e kwdb-libcommon >/dev/null 2>&1
    fi
    sudo rm -rf /etc/kaiwudb $g_data_root ~/kaiwudb_files
    sudo userdel -r kaiwudb >/dev/null 2>&1
  else
    $g_node_prefix rm -rf /etc/kaiwudb $g_data_root
  fi
  $g_node_prefix rm -rf ~/kaiwudb_files /etc/systemd/system/kaiwudb.service
end
}

function parallel_rollback() {
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
    export -f node_rollback
    export g_deploy_type=$g_deploy_type
    export g_package_tool=$g_package_tool
    export g_node_prefix=$g_node_prefix
    export g_data_root=$g_data_root
    echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "node_rollback args" 2>/dev/null
  fi
  exit 1
}

function create_compose() {
  if [ "$g_secure_mode" == "n" ];then
    local secure_param="--insecure"
  else
    local secure_param="--certs-dir=/kaiwudb/certs"
  fi
  local start_type="start-single-node"
  local opt_join=""
  case $g_install_mode in
    single)
      start_type="start-single-node"
      ;;
    single-replication)
      start_type="start-single-replica"
      opt_join="--join $g_local_addr:$g_kwdb_port"
      ;;
    multi-replication)
      start_type="start"
      opt_join="--join $g_local_addr:$g_kwdb_port"
      ;;
  esac
  if [ "$g_cpu_usage" != "0" ];then
    local cores=`cat /proc/cpuinfo | grep "processor" | sort -u| wc -l `
    local cpu_usage=$(echo $g_cpu_usage $cores | awk '{printf("%.3f",$1*$2)}')
    sudo bash -c "echo \"version: '3.3'
services:
  kaiwudb-container:
    image: $g_image_name
    container_name: kaiwudb-container
    hostname: kaiwudb-container
    ports:
      - $g_rest_port:8080
      - $g_kwdb_port:26257
    ulimits:
      memlock: -1
    deploy:
      resources:
        limits:
          cpus: '$cpu_usage'
    volumes:
      - /etc/kaiwudb/certs:/kaiwudb/certs
      - $g_data_root:/kaiwudb/deploy/kaiwudb-container
      - /dev:/dev
    networks: 
      - default
    restart: on-failure
    ipc: shareable
    privileged: true
    environment:
      - LD_LIBRARY_PATH=/kaiwudb/lib
    tty: true
    working_dir: /kaiwudb/bin
    command: 
      - /bin/bash
      - -c
      - |
        /kaiwudb/bin/kwbase  $start_type $secure_param --listen-addr=0.0.0.0:26257 --advertise-addr=$g_local_addr:$g_kwdb_port --http-addr=0.0.0.0:8080 --store=/kaiwudb/deploy/kaiwudb-container $opt_join
\" >/etc/kaiwudb/script/docker-compose.yml"
  else
    sudo bash -c "echo \"version: '3.3'
services:
  kaiwudb-container:
    image: $g_image_name
    container_name: kaiwudb-container
    hostname: kaiwudb-container
    ports:
      - $g_rest_port:8080
      - $g_kwdb_port:26257
    ulimits:
      memlock: -1
    volumes:
      - /etc/kaiwudb/certs:/kaiwudb/certs
      - $g_data_root:/kaiwudb/deploy/kaiwudb-container
      - /dev:/dev
    networks: 
      - default
    restart: on-failure
    ipc: shareable
    privileged: true
    environment:
      - LD_LIBRARY_PATH=/kaiwudb/lib
    tty: true
    working_dir: /kaiwudb/bin
    command: 
      - /bin/bash
      - -c
      - |
        /kaiwudb/bin/kwbase  $start_type $secure_param --listen-addr=0.0.0.0:26257 --advertise-addr=$g_local_addr:$g_kwdb_port --http-addr=0.0.0.0:8080 --store=/kaiwudb/deploy/kaiwudb-container $opt_join
\" >/etc/kaiwudb/script/docker-compose.yml"
  fi
}

function install() {
  local ret=""
  if [ "$g_deploy_type" == "bare" ];then
    # get package name
    log_info_without_console "start install binaries and libraries to /usr/local/kaiwudb"
    local kw_server=`ls $g_deploy_path/packages | grep "server"`
    local kw_libcommon=`ls $g_deploy_path/packages | grep "libcommon"`
    if [ "$g_package_tool" == "dpkg" ];then
      cd $g_deploy_path/packages
      ret=`eval $kw_cmd_prefix dpkg -i ./$kw_libcommon 2>&1`
      if [ $? -ne 0 ];then
        log_err_without_console $ret
        log_err_only_console "Error occurred during $kw_libcommon installation. Please check log."
        return 1
      fi
      ret=`eval $kw_cmd_prefix dpkg -i ./$kw_server 2>&1`
      if [ $? -ne 0 ];then
        log_err_without_console $ret
        log_err_only_console "Error occurred during $kw_server installation. Please check log."
        return 1
      fi
    elif [ "$g_package_tool" == "rpm" ];then
      cd $g_deploy_path/packages
      ret=`eval $kw_cmd_prefix rpm -ivh --replacepkgs ./$kw_libcommon 2>&1`
      if [ $? -ne 0 ];then
        log_err_without_console $ret
        log_err_only_console "Error occurred during $kw_libcommon installation. Please check log."
        return 1
      fi
      ret=`eval $kw_cmd_prefix rpm -ivh --replacepkgs ./$kw_server 2>&1`
      if [ $? -ne 0 ];then
        log_err_without_console $ret
        log_err_only_console "Error occurred during $kw_server installation. Please check log."
        return 1
      fi
    fi
    log_info_without_console "install binaries and libraries success"
  else
    log_info_without_console "start load docker image and create compose file"
    # check whether docker is installed
    docker --help >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      log_err "Please check if docker is installed."
      return 1
    fi
    # check whether docker-compose is installed
    g_docker_compose_path=$(eval $kw_cmd_prefix which docker-compose)
    # docker-compose --version >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      log_err "Please check if docker compose is installed."
      return 1
    fi
    cd $g_deploy_path/packages
    # load docker images
    # g_image_name=`docker load < KaiwuDB.tar 2>/dev/null | awk -F": " '{print $2}'`
    g_image_name=`docker load < KaiwuDB.tar 2>&1`
    if [ $? -ne 0 ]; then
      log_err "Docker image load failed:$g_image_name"
      return 1
    fi
    # get the image name
    g_image_name=$(echo $g_image_name | awk -F": " '{print $2}')
    create_compose
    eval $kw_cmd_prefix chmod +x /etc/kaiwudb/script/docker-compose.yml
    log_info_without_console "load docker image and create compose file success"
  fi
  return 0
}

# remote node install
function node_install() {
  args=${1//:/ }
  eval set -- "${args}"
  log_info_without_console "start install KaiwuDB at node[$1]"
  ssh -t -p $2 $3@$1 >/dev/null 2>$g_deploy_path/log/err-$1 "
cd ~/kaiwudb_files
if [ \"$g_deploy_type\" == \"bare\" ];then
  kw_server=\`ls | grep \"server\"\`
  kw_libcommon=\`ls | grep \"libcommon\"\`
  if [ \"$g_package_tool\" == \"dpkg\" ];then
    $g_node_prefix dpkg -i ./\$kw_libcommon
    if [ \$? -ne 0 ];then
      exit 1
    fi
    $g_node_prefix dpkg -i ./\$kw_server
    if [ \$? -ne 0 ];then
      exit 1
    fi
  elif [ \"$g_package_tool\" == \"rpm\" ];then
    $g_node_prefix rpm -ivh --replacepkgs ./\$kw_libcommon
    if [ \$? -ne 0 ];then
      exit 1
    fi
    $g_node_prefix rpm -ivh --replacepkgs ./\$kw_server
    if [ \$? -ne 0 ];then
      exit 1
    fi
  fi
else
  docker --help >/dev/null 2>&1
  if [ \$? -ne 0 ]; then
    exit 1
  fi
  docker-compose --version >/dev/null 2>&1
  if [ \$? -ne 0 ]; then
    exit 1
  fi
  #image_name=\`docker load < KaiwuDB.tar 2>/dev/null | awk -F\": \" '{print \$2}'\`
  image_name=\`docker load < KaiwuDB.tar 2>&1\`
  if [ \$? -ne 0 ]; then
    exit 1
  fi
  image_name=\`echo \$image_name | awk -F\": \" '{print \$2}'\`
fi
$g_node_prefix rm -rf ~/kaiwudb_files/* >/dev/null 2>&1"
  if [ $? -ne 0 ];then
    local ins_err=`cat $g_deploy_path/log/err-$1`
    log_err_without_console "[$1]:$ins_err"
    echo $1
  else
    log_info_without_console "install KaiwuDB at node[$1] success"
  fi
  rm $g_deploy_path/log/err-$1
}

function parallel_install() {
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
    export -f node_install
    export -f log_err_without_console
    export -f log_info_without_console
    export g_deploy_type=$g_deploy_type
    export g_package_tool=$g_package_tool
    export g_node_prefix=$g_node_prefix
    export g_data_root=$g_data_root
    export g_deploy_path=$g_deploy_path
    export LOG_LEVEL=$LOG_LEVEL
    export LOG_FILE=$LOG_FILE
    local arr=($(echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "node_install args" 2>/dev/null))
    if [ ${#arr[@]} -ne 0 ];then
      log_err_only_console "Node [${arr[@]}] install failed. Please check the log to get moer information."
      return 1
    fi
  fi
  return 0
}

function remote_pre_check() {
  args=${1//:/ }
  eval set -- "${args}"
  ssh -t -p $2 $3@$1 >/dev/null 2>&1 "
cd ~/kaiwudb_files
source kaiwudb_hardware.sh
# cpu_info
# if [ \$? -eq 1 ];then
#   exit 1
# fi
# mem_info
# if [ \$? -eq 1 ];then
#   exit 2
# fi
if [ -e /etc/kaiwudb/info/MODE ];then
  exit 1
fi
rm ~/kaiwudb_files/kaiwudb_hardware.sh
exit 0
"
  if [ $? -eq 1 ];then
  #   log_warn_without_console "The number of CPU cores does not meet the requirement. KaiwuDB may running failed."
  # elif [ $? -eq 2 ];then
  #   log_warn_without_console "The memory does not meet the requirement. KaiwuDB may running failed."
  # elif [ $? -eq 3 ];then
    echo $1
  fi
}

function parallel_precheck() {
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
    export -f remote_pre_check
    export -f log_warn_without_console
    export LOG_LEVEL=$LOG_LEVEL
    export LOG_FILE=$LOG_FILE
    local arr=($(echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "remote_pre_check args" 2>/dev/null))
    if [ ${#arr[@]} -ne 0 ];then
      log_err "Node [${arr[@]}] has already installed KaiwuDB."
      return 1
    fi
  fi
  return 0
}

function create_info_files() {
  local node_array=$1
  local ssh_port=$2
  local ssh_user=$3
  if [ -e /etc/kaiwudb/info/MODE ];then
    eval $kw_cmd_prefix rm /etc/kaiwudb/info/*
  fi
  eval $kw_cmd_prefix touch /etc/kaiwudb/info/MODE
  sudo touch /etc/kaiwudb/info/NODE
  sudo touch /etc/kaiwudb/script/kaiwudb_env
  sudo bash -c "echo $g_deploy_type >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_install_mode >> /etc/kaiwudb/info/MODE"
  if [ "$g_deploy_type" == "bare" ];then
    sudo bash -c "echo /usr/local/kaiwudb >> /etc/kaiwudb/info/MODE"
  else
    sudo bash -c "echo $g_image_name >> /etc/kaiwudb/info/MODE"
  fi
  sudo bash -c "echo $g_data_root >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_kwdb_port >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_rest_port >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_user >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo \"${node_array// /,}\" >> /etc/kaiwudb/info/NODE"
  sudo bash -c "echo \"$ssh_port\" >> /etc/kaiwudb/info/NODE"
  sudo bash -c "echo \"$ssh_user\" >> /etc/kaiwudb/info/NODE"
  sudo bash -c "echo KAIWUDB_START_ARG=\\\"\\\" > /etc/kaiwudb/script/kaiwudb_env"
}

# Create certificate
function create_certificate() {
  log_info_without_console "start create certificate files in /etc/kaiwudb/certts"
  if [ "$g_deploy_type" == "bare" ];then
    local KWDB_CA_KEY_FILE=/etc/kaiwudb/certs/ca.key
    local KWDB_CA_CRT_FILE=/etc/kaiwudb/certs/ca.crt
    local KWDB_NODE_KEY_FILE=/etc/kaiwudb/certs/node.key
    local KWDB_NODE_CRT_FILE=/etc/kaiwudb/certs/node.crt
    local KWDB_ROOT_KEY_FILE=/etc/kaiwudb/certs/client.root.key
    local KWDB_ROOT_CRT_FILE=/etc/kaiwudb/certs/client.root.crt
    sudo ldconfig
    cd /usr/local/kaiwudb/bin
    if [ ! -f "${KWDB_CA_KEY_FILE}" -o ! -f "${KWDB_CA_CRT_FILE}" ]; then
      sudo bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase cert create-ca --certs-dir=/etc/kaiwudb/certs --ca-key=${KWDB_CA_KEY_FILE} 2>&1 >/dev/null"
      if [ $? -ne 0 ]; then
        log_err "CA certificate creation failed."
        rollback
        parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
        exit 1
      fi
    fi

    if [ ! -f "${KWDB_NODE_KEY_FILE}" -o ! -f "${KWDB_NODE_CRT_FILE}" ]; then
      sudo bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase cert create-node $g_local_addr ${g_cls_array[*]} 127.0.0.1 0.0.0.0 localhost --certs-dir=/etc/kaiwudb/certs --ca-key=${KWDB_CA_KEY_FILE} 2>&1 >/dev/null"
      if [ $? -ne 0 ]; then
        log_err "node certificate creation failed."
        rollback
        parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
        exit 1
      fi
    fi

    if [ ! -f "${KWDB_ROOT_KEY_FILE}" -o ! -f "${KWDB_ROOT_CRT_FILE}" ]; then
      sudo bash -c "export LD_LIBRARY_PATH=/usr/local/gcc/lib64 && ./kwbase cert create-client root --certs-dir=/etc/kaiwudb/certs --ca-key=${KWDB_CA_KEY_FILE} 2>&1 >/dev/null"
      if [ $? -ne 0 ]; then
        log_err "client certificate creation failed."
        rollback
        parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
        exit 1
      fi
      eval $kw_cmd_prefix openssl pkcs8 -topk8 -inform PEM -outform DER -in /etc/kaiwudb/certs/client.root.key -out /etc/kaiwudb/certs/client.root.pk8 -nocrypt
    fi
  else
    eval $kw_cmd_prefix rm -rf /etc/kaiwudb/certs/*
    local certs_ret=$(docker run --rm -it --privileged --name init -v/etc/kaiwudb/certs:/kaiwudb/certs -v$g_deploy_path/utils/container_shell.sh:/kaiwudb/container_shell.sh  -e LD_LIBRARY_PATH=/kaiwudb/lib ${g_image_name} bash -c "/kaiwudb/container_shell.sh $g_local_addr \"${g_cls_array[*]}\"" 2>&1)
    if [ $? -ne 0 ];then
        log_err_without_console "$certs_ret"
        log_err "client certificate creation failed. Please check the log to see more information."
        rollback
        parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
        exit 1
    fi
  fi
  log_info_without_console "create certificate files in /etc/kaiwudb/certts success"
}

# create user and group
function user_create() {
  # create group if not exists
  local group_info=$(egrep "^$g_user:" /etc/group 2>/dev/null)
  if [ "$group_info" = "" ]; then
    eval $kw_cmd_prefix groupadd $g_user
  fi

  # create user if not exists
  local user_info=$(egrep "^$g_user:" /etc/passwd 2>/dev/null)
  if [ "$user_info" = "" ]; then
    eval $kw_cmd_prefix useradd -g $g_user -m -s /bin/bash $g_user
    if [ $? -ne 0 ]; then
      log_err "Create user failed."
      rollback
      parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
      exit 1
    fi
    echo "pause" >&10
    read -e -s -t60 -p "Please input ${g_user}'s password: " management_passwd
    echo
    if [ -n "$management_passwd" ];then
      eval echo $g_user:$management_passwd | sudo chpasswd >/dev/null 2>&1
      if [ $? -ne 0 ]; then
        log_err "Create user failed."
        rollback
        parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
        exit 1
      fi
    fi
    sudo bash -c "echo \"$g_user ALL=(ALL)  NOPASSWD: ALL\" >> /etc/sudoers" >/dev/null 2>&1
    if [ $? -ne 0 ];then
      log_err "Append sudoers failed."
      rollback
      parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
      exit 1
    fi
  fi
  # change owner
  eval $kw_cmd_prefix chown -R $g_user:$g_user /usr/local/kaiwudb >/dev/null 2>&1
  sudo chown -R $g_user:$g_user $g_data_root >/dev/null 2>&1
  sudo chown -R $g_user:$g_user /etc/kaiwudb >/dev/null 2>&1

  local cgroup=`stat -fc %T /sys/fs/cgroup`

  if [ "$cgroup" == "tmpfs" ];then
    eval $kw_cmd_prefix mkdir -p /sys/fs/cgroup/cpu/$g_user
    if [ $? -ne 0 ];then
      log_err "Cgroup setup failed."
      rollback
      parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
      exit 1
    fi
    sudo chown $g_user:$g_user -R /sys/fs/cgroup/cpu/$g_user
  elif [ "$cgroup" == "cgroup2fs" ];then
    eval $kw_cmd_prefix mkdir -p /sys/fs/cgroup/$g_user
    if [ $? -ne 0 ];then
      log_err "Cgroup setup failed."
      rollback
      parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
      exit 1
    fi
    sudo chown $g_user:$g_user -R /sys/fs/cgroup/$g_user
  else
    log_err "Cgroup setup failed."
    rollback
    parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
}

function node_copy() {
  args=${1//:/ }
  eval set -- "${args}"
  ssh -t -p $2 $3@$1 "
cd ~/kaiwudb_files
$g_node_prefix mkdir -p /etc/kaiwudb
$g_node_prefix mv certs /etc/kaiwudb/
$g_node_prefix mv info /etc/kaiwudb/
$g_node_prefix mv script /etc/kaiwudb/
if [ \"$g_secure_mode\" == \"y\" ];then
  $g_node_prefix chmod 600 /etc/kaiwudb/certs/*.key /etc/kaiwudb/certs/*.pk8 >/dev/null 2>&1
fi
sudo bash -c \"echo $1 >> /etc/kaiwudb/info/MODE\"
start_type=\"start-single-node\"
case $g_install_mode in
  single)
    start_type=\"start-single-node\"
    ;;
  single-replication)
    start_type=\"start-single-replica\"
    join_addr=\"--join $g_local_addr:$g_kwdb_port\"
    ;;
  multi-replication)
    start_type=\"start\"
    join_addr=\"--join $g_local_addr:$g_kwdb_port\"
    ;;
esac

if [ \"$g_deploy_type\" == \"bare\" ];then
  group_info=\$(egrep \"^$g_user\:\" /etc/group)
  if [ \"\$group_info\" == \"\" ]; then
    $g_node_prefix groupadd $g_user
  fi

  user_info=\$(egrep \"^$g_user\:\" /etc/passwd)
  if [ \"\$user_info\" == \"\" ]; then
    $g_node_prefix useradd -g $g_user -m -s /bin/bash $g_user
    if [ \$? -ne 0 ];then
      exit 1
    fi
    if [ -n "$management_passwd" ];then
      echo $g_user:$management_passwd | sudo chpasswd
      if [ \$? -ne 0 ];then
        exit 1
      fi
    fi
    sudo bash -c \"echo \\\"$g_user ALL=(ALL)  NOPASSWD: ALL\\\" >> /etc/sudoers\"
  fi

  $g_node_prefix chown -R $g_user:$g_user /usr/local/kaiwudb
  $g_node_prefix chown -R $g_user:$g_user $g_data_root
  $g_node_prefix chown -R $g_user:$g_user /etc/kaiwudb
  
  cgroup=\$(stat -fc %T /sys/fs/cgroup)
  if [ "\$cgroup" == \"tmpfs\" ];then
    $g_node_prefix mkdir -p /sys/fs/cgroup/cpu/$g_user
    if [ \$? -ne 0 ];then
      exit 1
    fi
    sudo chown $g_user:$g_user -R /sys/fs/cgroup/cpu/$g_user
  elif [ "\$cgroup" == \"cgroup2fs\" ];then
    $g_node_prefix mkdir -p /sys/fs/cgroup/$g_user
    if [ \$? -ne 0 ];then
      exit 1
    fi
    sudo chown $g_user:$g_user -R /sys/fs/cgroup/$g_user
  else
    exit 1
  fi
  if [ \"$g_secure_mode\" == \"y\" ];then
    secure_param=\"--certs-dir=/etc/kaiwudb/certs\"
  else
    secure_param=\"--insecure\"
  fi
  if [ \"$g_cpu_usage\" != \"0\" ];then
    cores=\$(cat /proc/cpuinfo | grep \"processor\" | sort -u| wc -l )
    cpu_usage=\$(echo $g_cpu_usage \$cores | awk '{printf(\"%.1f\", \$1*\$2*100)}')
    sudo bash -c \"echo \\\"
[Unit]
Description=KaiwuDB Service
After=network.target
Wants=network.target
[Service]
User=$g_user
Group=$g_user
Type=simple
LimitMEMLOCK=infinity
CPUQuota=\$cpu_usage%
WorkingDirectory=/usr/local/kaiwudb/bin
EnvironmentFile=/etc/kaiwudb/script/kaiwudb_env
Environment=LD_LIBRARY_PATH=/usr/local/gcc/lib64
ExecStartPre=/usr/bin/sudo /usr/sbin/sysctl -w vm.max_map_count=10000000
ExecStart=/usr/local/kaiwudb/bin/kwbase \$start_type \\\\\\\$KAIWUDB_START_ARG \$secure_param --listen-addr=0.0.0.0:$g_kwdb_port --advertise-addr=$1:$g_kwdb_port --http-addr=0.0.0.0:$g_rest_port --store=$g_data_root \$join_addr
ExecStop=/bin/kill \\\\\\\$MAINPID
KillMode=control-group
# Restart=on-failure
# ExecStopPost=sudo umount -a -t squashfs
#RuntimeMaxSec=2592000
# Don't restart in the case of configuration error
RestartPreventExitStatus=INVALIDARGUMENT
[Install]
WantedBy=multi-user.target\\\" > /etc/systemd/system/kaiwudb.service\"
  else
    sudo bash -c \"echo \\\"
[Unit]
Description=KaiwuDB Service
After=network.target
Wants=network.target
[Service]
User=$g_user
Group=$g_user
Type=simple
LimitMEMLOCK=infinity
WorkingDirectory=/usr/local/kaiwudb/bin
EnvironmentFile=/etc/kaiwudb/script/kaiwudb_env
Environment=LD_LIBRARY_PATH=/usr/local/gcc/lib64
ExecStartPre=/usr/bin/sudo /usr/sbin/sysctl -w vm.max_map_count=10000000
ExecStart=/usr/local/kaiwudb/bin/kwbase \$start_type \\\\\\\$KAIWUDB_START_ARG \$secure_param --listen-addr=0.0.0.0:$g_kwdb_port --advertise-addr=$1:$g_kwdb_port --http-addr=0.0.0.0:$g_rest_port --store=$g_data_root \$join_addr
ExecStop=/bin/kill \\\\\\\$MAINPID
KillMode=control-group
# Restart=on-failure
# ExecStopPost=sudo umount -a -t squashfs
#RuntimeMaxSec=2592000
# Don't restart in the case of configuration error
RestartPreventExitStatus=INVALIDARGUMENT
[Install]
WantedBy=multi-user.target\\\" > /etc/systemd/system/kaiwudb.service\"
  fi
else
  if [ \"$g_secure_mode\" == \"y\" ];then
    secure_param=\"--certs-dir=/kaiwudb/certs\"
  else
    secure_param=\"--insecure\"
  fi
  docker_compose_path=\$($g_node_prefix which docker-compose)
  if [ \"$g_cpu_usage\" != \"0\" ];then
    cores=\$(cat /proc/cpuinfo | grep \"processor\" | sort -u| wc -l )
    cpu_usage=\$(echo $g_cpu_usage \$cores | awk '{printf(\"%.3f\",\$1*\$2)}')
    sudo bash -c \"echo \\\"version: '3.3'
services:
  kaiwudb-container:
    image: $g_image_name
    container_name: kaiwudb-container
    hostname: kaiwudb-container
    ports:
      - $g_rest_port:8080
      - $g_kwdb_port:26257
    ulimits:
      memlock: -1
    deploy:
      resources:
        limits:
          cpus: '\$cpu_usage'
    volumes:
      - /etc/kaiwudb/certs:/kaiwudb/certs
      - $g_data_root:/kaiwudb/deploy/kaiwudb-container
      - /dev:/dev
    networks: 
      - default
    restart: on-failure
    ipc: shareable
    privileged: true
    environment:
      - LD_LIBRARY_PATH=/kaiwudb/lib
    tty: true
    working_dir: /kaiwudb/bin
    command: 
      - /bin/bash
      - -c
      - |
        /kaiwudb/bin/kwbase \$start_type \$secure_param --listen-addr=0.0.0.0:26257 --advertise-addr=$1:$g_kwdb_port --http-addr=0.0.0.0:8080 --store=/kaiwudb/deploy/kaiwudb-container \$join_addr
  \\\" > /etc/kaiwudb/script/docker-compose.yml\"
  else
    sudo bash -c \"echo \\\"version: '3.3'
services:
  kaiwudb-container:
    image: $g_image_name
    container_name: kaiwudb-container
    hostname: kaiwudb-container
    ports:
      - $g_rest_port:8080
      - $g_kwdb_port:26257
    ulimits:
      memlock: -1
    volumes:
      - /etc/kaiwudb/certs:/kaiwudb/certs
      - $g_data_root:/kaiwudb/deploy/kaiwudb-container
      - /dev:/dev
    networks: 
      - default
    restart: on-failure
    ipc: shareable
    privileged: true
    environment:
      - LD_LIBRARY_PATH=/kaiwudb/lib
    tty: true
    working_dir: /kaiwudb/bin
    command: 
      - /bin/bash
      - -c
      - |
        /kaiwudb/bin/kwbase \$start_type \$secure_param --listen-addr=0.0.0.0:26257 --advertise-addr=$1:$g_kwdb_port --http-addr=0.0.0.0:8080 --store=/kaiwudb/deploy/kaiwudb-container \$join_addr
  \\\" > /etc/kaiwudb/script/docker-compose.yml\"
  fi

  sudo bash -c \"echo \\\"
[Unit]
Description=KaiwuDB Service
After=network.target
Wants=network.target
[Service]
User=root
Group=root
Type=simple
LimitMEMLOCK=infinity
WorkingDirectory=/etc/kaiwudb/script
ExecStartPre=/usr/sbin/sysctl -w vm.max_map_count=10000000
ExecStart=\$docker_compose_path --compatibility up
ExecStop=\$docker_compose_path stop
# Restart=on-failure
# ExecStopPost=sudo umount -a -t squashfs
#RuntimeMaxSec=2592000
# Don't restart in the case of configuration error
RestartPreventExitStatus=INVALIDARGUMENT
[Install]
WantedBy=multi-user.target\\\" > /etc/systemd/system/kaiwudb.service\"
fi
"
  if [ $? -ne 0 ]; then
    echo $1
  fi
}

function parallel_node_files() {
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
    export -f node_copy
    export g_node_prefix=$g_node_prefix
    export g_data_root=$g_data_root
    export g_user=$g_user
    export g_deploy_type=$g_deploy_type
    export g_install_mode=$g_install_mode
    export g_secure_mode=$g_secure_mode
    export g_kwdb_port=$g_kwdb_port
    export g_local_addr=$g_local_addr
    export g_image_name=$g_image_name
    export management_passwd=$management_passwd
    export g_rest_port=$g_rest_port
    export g_cpu_usage=$g_cpu_usage
    local arr=($(echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "node_copy args" 2>/dev/null))
    # echo ${parallel_func[@]} | xargs -d " " -n3 -P 5 -I args bash -c "node_copy args"
    if [ ${#arr[@]} -ne 0 ];then
      log_err "Node [${arr[@]}] create files failed."
      return 1
    fi
  fi
  return 0
}

function create_system_service() {
  log_info_without_console "start create system service in /etc/systemd/system/kaiwudb.service"
  cd /etc/kaiwudb/script
  eval $kw_cmd_prefix touch /etc/systemd/system/kaiwudb.service
  # eval $kw_cmd_prefix chmod 755 /etc/kaiwudb/script/*.sh

  local start_type="start-single-node"
  local opt_join=""
  case $g_install_mode in
    single)
      start_type="start-single-node"
      ;;
    single-replication)
      start_type="start-single-replica"
      opt_join="--join $g_local_addr:$g_kwdb_port"
      ;;
    multi-replication)
      start_type="start"
      opt_join="--join $g_local_addr:$g_kwdb_port"
      ;;
  esac
	if [ "$g_deploy_type" == "bare" ];then
    if [ "$g_secure_mode" == "y" ];then
      local secure_param="--certs-dir=/etc/kaiwudb/certs"
    else
      local secure_param="--insecure"
    fi
    if [ "$g_cpu_usage" != "0" ];then
      local cores=`cat /proc/cpuinfo | grep "processor" | sort -u| wc -l `
      local cpu_usage=$(echo $g_cpu_usage $cores | awk '{printf("%.1f", $1*$2*100)}')
      sudo bash -c "echo \"
[Unit]
Description=KaiwuDB Service
After=network.target
Wants=network.target
[Service]
User=$g_user
Group=$g_user
Type=simple
LimitMEMLOCK=infinity
CPUQuota=$cpu_usage%
WorkingDirectory=/usr/local/kaiwudb/bin
EnvironmentFile=/etc/kaiwudb/script/kaiwudb_env
Environment=LD_LIBRARY_PATH=/usr/local/gcc/lib64
ExecStartPre=/usr/bin/sudo /usr/sbin/sysctl -w vm.max_map_count=10000000
ExecStart=/usr/local/kaiwudb/bin/kwbase $start_type \\\$KAIWUDB_START_ARG $secure_param --listen-addr=0.0.0.0:$g_kwdb_port --advertise-addr=$g_local_addr:$g_kwdb_port --http-addr=0.0.0.0:$g_rest_port --store=$g_data_root $opt_join
ExecStop=/bin/kill \\\$MAINPID
KillMode=control-group
# Restart=on-failure
# ExecStopPost=sudo umount -a -t squashfs
#RuntimeMaxSec=2592000
# Don't restart in the case of configuration error
RestartPreventExitStatus=INVALIDARGUMENT
[Install]
WantedBy=multi-user.target\" > /etc/systemd/system/kaiwudb.service"
    else
      sudo bash -c "echo \"
[Unit]
Description=KaiwuDB Service
After=network.target
Wants=network.target
[Service]
User=$g_user
Group=$g_user
Type=simple
LimitMEMLOCK=infinity
WorkingDirectory=/usr/local/kaiwudb/bin
EnvironmentFile=/etc/kaiwudb/script/kaiwudb_env
Environment=LD_LIBRARY_PATH=/usr/local/gcc/lib64
ExecStartPre=/usr/bin/sudo /usr/sbin/sysctl -w vm.max_map_count=10000000
ExecStart=/usr/local/kaiwudb/bin/kwbase $start_type \\\$KAIWUDB_START_ARG $secure_param --listen-addr=0.0.0.0:$g_kwdb_port --advertise-addr=$g_local_addr:$g_kwdb_port --http-addr=0.0.0.0:$g_rest_port --store=$g_data_root $opt_join
ExecStop=/bin/kill \\\$MAINPID
KillMode=control-group
# Restart=on-failure
# ExecStopPost=sudo umount -a -t squashfs
#RuntimeMaxSec=2592000
# Don't restart in the case of configuration error
RestartPreventExitStatus=INVALIDARGUMENT
[Install]
WantedBy=multi-user.target\" > /etc/systemd/system/kaiwudb.service"
    fi
	else
		sudo bash -c "echo \"
[Unit]
Description=KaiwuDB Service
After=network.target
Wants=network.target
[Service]
User=root
Group=root
Type=simple
LimitMEMLOCK=infinity
WorkingDirectory=/etc/kaiwudb/script
ExecStartPre=/usr/sbin/sysctl -w vm.max_map_count=10000000
ExecStart=$g_docker_compose_path --compatibility up
ExecStop=$g_docker_compose_path stop
# Restart=on-failure
# ExecStopPost=sudo umount -a -t squashfs
#RuntimeMaxSec=2592000
# Don't restart in the case of configuration error
RestartPreventExitStatus=INVALIDARGUMENT
[Install]
WantedBy=multi-user.target\" > /etc/systemd/system/kaiwudb.service"
	fi
  log_info_without_console "create system service in /etc/systemd/system/kaiwudb.service success"
}

function compress_certs() {
  if [ "$g_secure_mode" == "y" ];then
    log_info_without_console "start compress certs to $g_deploy_path/kaiwudb_certs.tar.gz"
    eval $kw_cmd_prefix tar -zcvf $g_deploy_path/kaiwudb_certs.tar.gz -C/etc/kaiwudb/certs ca.key ca.crt >/dev/null 2>&1
    if [ $? -ne 0 ];then
      log_warn "Compress ca failed. Please manually compress the CA certificate."
    fi
    log_info_without_console "compress certs success"
  fi
}
