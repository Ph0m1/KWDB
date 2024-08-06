#! /bin/bash

# basic golbal variables
g_deploy_path=$(cd $(dirname $0);pwd)
g_cur_usr=`whoami`
g_install_mode="single"

source $g_deploy_path/utils/kaiwudb_common.sh
source $g_deploy_path/utils/utils.sh
source $g_deploy_path/utils/kaiwudb_log.sh

function global_usage() {
  echo "Usage:
  deploy.sh Commands [Options]

Commands:
  install                  Install KaiwuDB
  uninstall                Uninstall KaiwuDB in local node
  cluster                  Cluster operate
  start                    Start KaiwuDB in local node
  stop                     Stop KaiwuDB in local node
  restart                  Restart KaiwuDB in local node
  status                   Get local node KaiwuDB status
Options:
  -h, --help
"
  exit 0
}

function install_usage() {
  eval set -- "$@"
  if [ $# -eq 3 ];then
    case "$1" in
      --single)
        g_install_mode="single"
        return
        ;;
      --single-replica)
        g_install_mode="single-replication"
        return
        ;;
      --multi-replica)
        g_install_mode="multi-replication"
        return
        ;;
      -h|--help)
        ;;
    esac
  fi

  echo "Usage:
  deploy.sh install [Options]

Options:
  --single                  single mode(default)
  --single-replica          single replication mode
  --multi-replica           multi replication mode
  -h, --help                help message
"
  exit 0
}

function cluster_usage() {
  eval set -- "$@"
  if [ $# -eq 3 ];then
    case "$1" in
      -i|--init)
        g_cluster_opt="init"
        return
        ;;
      -s|--status)
        g_cluster_opt="status"
        return
        ;;
      -h|--help)
        ;;
    esac
  fi

  echo "Usage:
  deploy.sh cluster [Options]
Options:
  -i, --init                     Start and init all cluster
  -s, --status                   Show all nodes status
  -h, --help                     help message
"
  exit 0
}

function join_usage() {
  eval set -- "$@"
  if [ $# -eq 4 ];then
    case "$1" in
      --addr)
        if [ "$(addr_check $2)" == "no" ];then
          echo "Invalid parameter"
        else
          g_join_addr=$2
          return
        fi
        ;;
      -h|--help)
        ;;
    esac
  fi

  echo "Usage:
  deploy.sh join [Options]
Options:
  --addr [VALUE]             Target cluster addr. eg:127.0.0.1:26257
  -h, --help                 help message
"
  exit 0
}

function clsetting_usage() {
  eval set -- "$@"
  if [ $# -eq 4 ];then
    case "$1" in
      --dedup-rule)
        g_setting_opt=${1//-/}
        g_setting_val=$2
        return
        ;;
      --iot-interval)
        g_setting_opt=${1//-/}
        g_setting_val=$2
        return
        ;;
      --iot-disorder-interval)
        g_setting_opt=${1//-/}
        g_setting_val=$2
        return
        ;;
      --log-size)
        g_setting_opt=${1//-/}
        g_setting_val=$2
        return
        ;;
      --log-nums)
        g_setting_opt=${1//-/}
        g_setting_val=$2
        return
        ;;
      --log-mode)
        g_setting_opt=${1//-/}
        g_setting_val=$2
        return
        ;;
      --log-level)
        g_setting_opt=${1//-/}
        g_setting_val=$2
        return
        ;;
      --connecttion-nums)
        g_setting_opt=${1//-/}
        g_setting_val=$2
        return
        ;;
      -h|--help)
        ;;
    esac
  fi

  echo "Usage:
  deploy.sh clsetting [Options] [Value]
Options:
  --dedup-rule
  --iot-interval
  --iot-disorder-interval
  --log-size
  --log-nums
  --log-mode
  --log-level
  --connecttion-nums
  -h, --help                     help message
"
  exit 0
}

function limit_usage() {
  eval set -- "$@"
  if [ $# -eq 4 ];then
    case "$1" in
      --cpu)
        g_limit_value=$2
        if [ $(echo "$g_limit_value <= 0"|bc) == 1 ] || [ $(echo "$g_limit_value > 1"|bc) == 1 ];then
          echo "Invalid arguments"
        else
          return
        fi
        ;;
      -h|--help)
        ;;
    esac
  fi

  echo "Usage:
  deploy.sh limit [Options] [Values]

Options:
  --cpu               set cpu limit value(0-1]
  -h, --help          help message
"
  exit 0
}

function upgrade_usage() {
  eval set -- "$@"
  if [ $# -eq 3 ];then
    case "$1" in
      -l|--local)
        g_upgrade_opt=local
        return
        ;;
      -a|--all)
        g_upgrade_opt=all
        return
      ;;
      -h|--help)
        ;;
    esac
  fi

  echo "Usage:
  deploy.sh upgrade [Options]

Options:
  -l, --local          local node upgrade
  -a, --all            whole cluster upgrade
  -h, --help           help message
"
  exit 0
}

function cmd_check() {
  local commands=("install" "uninstall" "start" "stop" "restart" "cluster" "limit" "status" "upgrade" "join" "decommission")
  local flag=false
  g_kw_cmd=""
  local global_flag=false
  local short_cmd="hsial"
  local long_cmd="help,single,init,status,cpu:,addr:,single-replica,multi-replica,local,all"
  local options=$(getopt -a -o $short_cmd -l $long_cmd -n "deploy.sh" -- "$@" 2>/dev/null)
  if [ $? -ne 0 ];then
    global_usage
  fi
  eval set -- "$options"
  while true; do
    case $1 in
      --)
        g_kw_cmd=$2
        break
      ;;
      -h|--help)
        global_flag=true
        shift 1
      ;;
      *)
        shift 1
        ;;
    esac
  done
  for item in ${commands[@]}
  do
    if [ "$g_kw_cmd" == "$item" ]; then
      flag=true
      break
    fi
  done
  if [ $flag == "false" ] || [[ -z "$g_kw_cmd" && $global_flag == "true" ]];then
    global_usage
  fi

  case "$g_kw_cmd" in
    install)
      install_usage $options
    ;;
    cluster)
      cluster_usage $options
    ;;
    limit)
      limit_usage $options
    ;;
    join)
      join_usage $options
    ;;
    upgrade)
      upgrade_usage $options
    ;;
  esac
}

function cfg_check() {
  # secure_mode check
  g_secure_mode=$(read_config_value $g_deploy_path/deploy.cfg global secure_mode)
  if [ -z "$g_secure_mode" ];then
    log_err "secure_mode is missing in deploy.cfg."
    exit 1
  fi
  # kwdb_install_dir check
  # g_inst_dir=$(read_config_value $g_deploy_path/deploy.cfg global kwdb_install_dir)
  # if [ -z "$g_inst_dir" ];then
  #   log_err "kwdb_install_dir is missing in deploy.cfg."
  #   exit 1
  # fi
  # management_user check
  g_user=$(read_config_value $g_deploy_path/deploy.cfg global management_user)
  if [ -z "$g_user" ];then
    log_err "management_user is missing in deploy.cfg."
    exit 1
  fi
  # rest_port check
  g_rest_port=$(read_config_value $g_deploy_path/deploy.cfg global rest_port)
  if [ -z "$g_rest_port" ];then
    log_err "rest_port is missing in deploy.cfg."
    exit 1
  fi
  if [ $g_rest_port -le 0 -o $g_rest_port -gt 65535 ];then
    log_err "The rest_port value is incorrect, range [1-65535]."
    exit 1
  fi
  g_kwdb_port=$(read_config_value $g_deploy_path/deploy.cfg global kaiwudb_port)
  if [ -z "$g_kwdb_port" ];then
    log_err "kaiwudb_port is missing in deploy.cfg."
    exit 1
  fi
  if [ $g_kwdb_port -le 0 -o $g_kwdb_port -gt 65535 ];then
    log_err "The kaiwudb_port value is incorrect, range [1-65535]."
    exit 1
  fi
  g_cpu_usage=$(read_config_value $g_deploy_path/deploy.cfg global cpu)
  if [ -z "$g_cpu_usage" ];then
    g_cpu_usage=0
  fi
  # validate cpu usage
  echo $g_cpu_usage | awk '{if($1>1 || $1<0){exit 1}else{exit 0}}'
  if [ $? -ne 0 ];then
    log_err "cpu value is incorrect[0-1]."
    exit 1
  fi
  # data_root check
  g_data_root=$(read_config_value $g_deploy_path/deploy.cfg global data_root)
  if [ -z "$g_data_root" ];then
    log_err "data_root is missing in deploy.cfg."
    exit 1
  fi
  # local node addr check
  g_local_addr=$(read_config_value $g_deploy_path/deploy.cfg local node_addr)
  if [ -z "$g_local_addr" ];then
    log_err "[local]node_addr is missing in deploy.cfg."
    exit 1
  fi
  if [ "$(addr_check $g_local_addr)" == "no" ];then
    log_err "The [local]node_addr format is incorrect."
    exit 1
  fi
  # cluster node check(optional)
  if grep -Eq "^\[cluster\]$" $g_deploy_path/deploy.cfg;then
    # g_cluster_deploy=true
    local cls_addrs=$(read_config_value $g_deploy_path/deploy.cfg cluster node_addr)
    g_cls_array=(${cls_addrs//,/ })
    # g_node_num=${#g_cls_array[@]}
    if [ ${#g_cls_array[@]} -eq 0 ];then
      log_err "[cluster]node_addr is missing in deploy.cfg."
      exit 1
    fi
    for ((i=0; i<${#g_cls_array[@]}; i++))
    do
      if [ "$(addr_check ${g_cls_array[$i]})" == "no" ];then
        log_err "The [cluster]node_addr("${g_cls_array[$i]}") format is incorrect."
        exit 1
      fi
    done
    g_ssh_port=$(read_config_value $g_deploy_path/deploy.cfg cluster ssh_port)
    if [ -z "$g_ssh_port" ];then
      log_err "ssh_port is missing in deploy.cfg."
      exit 1
    fi
    if [ $g_ssh_port -le 0 -o $g_ssh_port -gt 65535 ];then
      log_err "The ssh_port value is incorrect, range [1-65535]."
      exit 1
    fi
    g_ssh_user=$(read_config_value $g_deploy_path/deploy.cfg cluster ssh_user)
    if [ -z "$g_ssh_user" ];then
      log_err "ssh_user is missing in deploy.cfg."
      exit 1
    fi
  fi
}

# cmd line parameter parsing
cmd_check $@

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

# init log
log_init $g_deploy_path $g_cur_usr

function get_config_dir() {
  config_dir="/etc/kaiwudb"
  if [ -d /etc/kwdb ];then
    config_dir="/etc/kwdb"
  fi
}

get_config_dir

if [ "$g_kw_cmd" == "install" ];then
  # pipe
  kw_pipe=$g_deploy_path/kw_pipe
  if [ ! -p $kw_pipe ]; then
      mkfifo $kw_pipe >/dev/null 2>&1
      if [ $? -ne 0 ];then
        log_err "Pipe create failed."
        exit 1
      fi
  fi
  exec 10<>$g_deploy_path/kw_pipe
  rm -rf $g_deploy_path/kw_pipe >/dev/null 2>&1
  source $g_deploy_path/utils/kaiwudb_install.sh
  source $g_deploy_path/utils/kaiwudb_hardware.sh
  source $g_deploy_path/utils/process_bar.sh
  process_bar $$ &
  process_pid=$!
  # whether installing KaiwuDB
  if $(install_check);then
    log_err "KaiwuDB is already installed."
    exit 1
  fi
  echo "install#check:5" >&10
  # hardware check
  cpu_info
  if [ $? -eq 1 ];then
    log_warn "The number of CPU cores does not meet the requirement. KaiwuDB may running failed."
  fi
  mem_info
  if [ $? -eq 2 ];then
    log_warn "The memory does not meet the requirement. KaiwuDB may running failed."
  fi
  echo "hardware#check:10" >&10
  # parsing config file
  cfg_check
  echo "config#check:15" >&10
  ssh_passwd_free_check "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
  echo "ssh#free#check:20" >&10
  remote_node_passwd "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
  echo "remote#node#passwd-free#check:25" >&10

  # clock_sync_check
  echo "clock#check:30" >&10
  verify_files
  echo "package#check:35" >&10
  
  # capture SIGINT signal
  trap "rollback && parallel_rollback ${g_cls_array[*]} $g_ssh_port $g_ssh_user && kill -9 $process_pid" INT

  parallel_copy_files "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "$g_deploy_path/utils/kaiwudb_hardware.sh"
  parallel_precheck "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
  if [ $? -ne 0 ];then
    rollback
    parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  echo "remote#node#check:40" >&10
  # local node init directory
  init_directory
  echo "init#directory:45" >&10
  # parallel init remote node directory
  parallel_init "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
  echo "remote#node#init:50" >&10
  
  # distribute files
  parallel_copy_files "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "$g_deploy_path/packages/*"
  if [ $? -ne 0 ];then
    rollback
    parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  echo "distribute#packages:55" >&10

  #install packages
  install
  if [ $? -ne 0 ];then
    rollback
    parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  echo "local#install:60" >&10
  parallel_install "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
  if [ $? -ne 0 ];then
    rollback
    parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  echo "remote#node#install:65" >&10
  create_info_files "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
  echo "create#config#cache:70" >&10
  create_system_service
  echo "create#service:75" >&10
  
  if [ "$g_secure_mode" == "y" ];then
    create_certificate
  fi
  echo "create#certs:80" >&10
  if [ "$g_deploy_type" == "bare" ];then
    user_create
  fi
  echo "create#system#user:85" >&10
  
  if [ "$g_secure_mode" == "y" ];then
    eval $kw_cmd_prefix chmod 777 $config_dir/certs/*.key $config_dir/certs/*.pk8 >/dev/null 2>&1
  fi
  parallel_copy_files "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "$config_dir/*"
  if [ $? -ne 0 ];then
    rollback
    parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  echo "distribute#cache#files:90" >&10
  if [ "$g_secure_mode" == "y" ];then
    eval $kw_cmd_prefix chmod 600 $config_dir/certs/*.key $config_dir/certs/*.pk8 >/dev/null 2>&1
  fi
  sudo bash -c "echo $g_local_addr >> $config_dir/info/MODE"
  echo "change#permissions:95" >&10

  parallel_node_files "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
  if [ $? -ne 0 ];then
    rollback
    parallel_rollback "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  compress_certs
  echo "install#complete:100" >&10
  wait
  exec 10>&-
  echo -e "\e[1;32m[INSTALL COMPLETED]:\e[0mKaiwuDB has been installed successfully! To start KaiwuDB, please execute the command '\e[1;34msystemctl daemon-reload\e[0m'."
  exit 0
fi

if [ "$g_kw_cmd" == "uninstall" ];then
  source $g_deploy_path/utils/kaiwudb_uninstall.sh
  if ! $(install_check);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  ins_type=$(install_type)
  kw_name=$(user_name)
  kw_status $ins_type
  if [ $? -eq 0 ];then
    log_err "Please stop KaiwuDB first."
    exit 1
  fi
  clear_user="n"
  read -t60 -p "When uninstalling KaiwuDB, you can either delete or keep all user data. Please confirm your choice: Do you want to delete the data? (y/N)" -e clear_user
  uninstall $ins_type
  uninstall_dir $(kw_data_dir)
  delete_user $ins_type $kw_name
  echo -e "\e[1;32m[UNINSTALL COMPLETED]:\e[0mKaiwuDB has been uninstalled successfully."
  exit 0
fi

if [ "$g_kw_cmd" == "join" ];then
  source $g_deploy_path/utils/kaiwudb_cluster.sh
  if ! $(install_check);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  ins_type=$(install_type)
  run_type=$(running_type)
  if [ "$run_type" == "single" ];then
    log_err "The current mode is not supported join a cluster."
    exit 1
  fi
  kw_status $ins_type
  if [ $? -eq 0 ];then
    log_err "KaiwuDB already running."
    exit 1
  fi

  rebuild_certs

  if ! $(check_start_type);then
    log_err "Joining a cluster is not supported in the current mode(single-node)."
    exit 1
  fi
  modify_start_cmd $g_join_addr
  if [ $? -ne 0 ];then
    log_err "Failed to modify the startup command."
    exit 1
  fi
  local_join $ins_type
  if [ $? -ne 0 ];then
    log_err "Failed to join the cluster. For more information, use 'journalctl -u kaiwudb' to view the system logs."
    exit 1
  fi
  join_check $ins_type
  echo -e "\e[1;32m[JOIN COMPLETED]:\e[0mNode has been joined to the cluster successfully."
  exit 0
fi

if [ "$g_kw_cmd" == "cluster" ];then
  source $g_deploy_path/utils/kaiwudb_cluster.sh
  if [ "$g_cluster_opt" == "init" ];then
    parse_addr
    if [ $? -ne 0 ];then
      log_err "Parse $config_dir/info/NODE failed."
      exit 1
    fi
    if ! $(install_check);then
      log_err "KaiwuDB does not exist. Please install KaiwuDB first."
      exit 1
    fi
    ins_type=$(install_type)
    run_type=$(running_type)
    if [ "$run_type" == "single" ];then
      log_err "The current mode is not supported cluster init."
      exit 1
    fi
    kw_status $ins_type
    if [ $? -eq 0 ];then
      log_err "KaiwuDB already running."
      exit 1
    fi
    # remote node passwd-free check
    ssh_passwd_free_check "${ip_arr[*]}" $ssh_port $ssh_user
    # remote node whether installed KaiwuDB
    parallel_install_check "${ip_arr[*]}" $ssh_port $ssh_user
    if [ $? -ne 0 ];then
      exit 1
    fi
    remote_node_passwd "${ip_arr[*]}" $ssh_port $ssh_user
    parallel_status
    local_join $ins_type
    if [ $? -ne 0 ];then
      log_err "Local start failed. For more information, use 'journalctl -u kaiwudb' to view the system logs."
      exit 1
    fi
    parallel_init "${ip_arr[*]}" $ssh_port $ssh_user
    local_init $ins_type "${ip_arr[*]}"
    echo -e "\e[1;32m[INIT COMPLETED]:\e[0mCluster init successfully."
    exit 0
  elif [ "$g_cluster_opt" == "status" ];then
    if ! $(install_check);then
      log_err "KaiwuDB does not exist. Please install KaiwuDB first."
      exit 1
    fi
    ins_type=$(install_type)
    kw_status $ins_type
    if [ $? -ne 0 ];then
      log_err "KaiwuDB is not running."
      exit 1
    fi
    cluster_status
  fi
fi

if [ "$g_kw_cmd" == "decommission" ];then
  source $g_deploy_path/utils/kaiwudb_cluster.sh
  kw_node_id=0
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
  cluster_nodeid
  decommission_from_cluster $ins_type

  echo -e "\e[1;32m[DECOMMISSION COMPLETED]:\e[0mDecommission complete. Please wait a few minutes to check whether decommission from cluster succeeded."
  exit 0
fi

if [ "$g_kw_cmd" == "status" ];then
  if ! $(install_check);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  ins_type=$(install_type) 
  kw_status $ins_type
  if [ $? -ne 0 ];then
    echo -e "\e[1;32m[STATUS COMPLETED]:\e[0mKaiwuDB is not runnning."
    exit 0
  else
    echo -e "\e[1;32m[STATUS COMPLETED]:\e[0mKaiwuDB is runnning now."
    exit 0
  fi
fi

if [ "$g_kw_cmd" == "start" ];then
  source $g_deploy_path/utils/kaiwudb_operate.sh
  if ! $(install_check);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  ins_type=$(install_type) 
  kw_status $ins_type
  if [ $? -eq 0 ];then
    log_err "KaiwuDB is runnning now."
    exit 1
  fi
  kw_start $ins_type
  if [ $? -ne 0 ];then
    log_err "Local start failed. For more information, use 'journalctl -u kaiwudb' to view the system logs."
    exit 1
  fi
  echo -e "\e[1;32m[START COMPLETED]:\e[0mKaiwuDB start successfully."
fi

if [ "$g_kw_cmd" == "stop" ];then
  source $g_deploy_path/utils/kaiwudb_operate.sh
  if ! $(install_check);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  ins_type=$(install_type)
  kw_status $ins_type
  if [ $? -ne 0 ];then
    log_err "KaiwuDB is not runnning now."
    exit 1
  fi
  kw_stop
  if [ $? -ne 0 ];then
    log_err "Local stop failed. For more information, use 'journalctl -u kaiwudb' to view the system logs."
    exit 1
  fi
  echo -e "\e[1;32m[STOP COMPLETED]:\e[0mKaiwuDB stop successfully."
fi

if [ "$g_kw_cmd" == "restart" ];then
  source $g_deploy_path/utils/kaiwudb_operate.sh
  if ! $(install_check);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  ins_type=$(install_type) 
  kw_status $ins_type
  if [ $? -ne 0 ];then
    log_err "KaiwuDB is not runnning now."
    exit 1
  fi
  kw_restart $ins_type
  if [ $? -ne 0 ];then
    log_err "Local restart failed. For more information, use 'journalctl -u kaiwudb' to view the system logs."
    exit 1
  fi
  echo -e "\e[1;32m[RESTART COMPLETED]:\e[0mKaiwuDB restart successfully."
fi

if [ "$g_kw_cmd" == "upgrade" ];then
  # ignore signal SIGINT
  trap "" INT
  source $g_deploy_path/utils/kaiwudb_install.sh
  source $g_deploy_path/utils/kaiwudb_upgrade.sh
  # load process_bar function
  source $g_deploy_path/utils/process_bar.sh
  # create pipe file, and associate file-descriptor(10)
  kw_pipe=$g_deploy_path/kw_pipe
  if [ ! -p $kw_pipe ]; then
      mkfifo $kw_pipe >/dev/null 2>&1
      if [ $? -ne 0 ];then
        log_err "Pipe create failed."
        exit 1
      fi
  fi
  exec 10<>$g_deploy_path/kw_pipe
  # delete pipe file
  rm -rf $g_deploy_path/kw_pipe >/dev/null 2>&1
  process_bar $$ &
  process_pid=$!
  rename_directory
  # whether install KaiwuDB
  if ! $(install_check);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  echo "install#check:10" >&10
  # Get KaiwuDB install type
  ins_type=$(install_type)
  echo "install#type:20" >&10
  upgrade_verify_files $ins_type
  echo "check#packages:30" >&10
  version_compare $ins_type
  echo "version#compare:40" >&10

  # Get KaiwuDB running type
  g_run_type=$(running_type)
  echo "get#running#type:50" >&10

  # local node upgrade
  if [ "$g_upgrade_opt" == "local" ];then
    # check kwbase whether running
    echo "running#check:60" >&10
    kw_status $ins_type
    if [ $? -ne 0 ];then
      # off-line upgrades
      upgrade
      echo "upgrade:70" >&10
    elif [ "$g_run_type" != "single" ];then
      # on-line upgrade
      kw_node_id=0
      echo "get#node#id:70" >&10
      cluster_nodeid
      echo "change#KaiwuDB#status:80" >&10
      change_kwdb_status
      echo "upgrade:90" >&10
      upgrade
      # modify start cmd and restart KaiwuDB
      echo "restart#KaiwuDB:95" >&10
      recover_node
    else
      log_err "Current local node is not support online upgrade! Please stop KaiwuDB then try again."
      exit 1
    fi
    echo "upgrade#complete:100" >&10
    # wait bar process exit
    wait
    # close file descriptor
    exec 10>&-
    echo -e "\e[1;32m[UPGRADE COMPLETED]:\e[0mLocal node upgrade successfully."
  elif [ "$g_upgrade_opt" == "all" ];then
    # local node upgrade first.
    # check kwbase whether running
    if [ "$g_run_type" == "single" ];then
      log_err "Current local node is not support online upgrade! Please stop KaiwuDB then try again."
      exit 1
    fi
    echo "get#Kaiwudb#status:55" >&10
    kw_status $ins_type
    if [ $? -ne 0 ];then
      # must be running
      log_err "KaiwuDB is not runnning, can not get cluster nodes status."
      exit 1
    else
      echo "get#all#nodes:60" >&10
      # local node upgrade first.
      get_cluster_nodes
      echo "get#local#node_id:65" >&10
      source $g_deploy_path/utils/kaiwudb_cluster.sh
      get_ssh_info
      ssh_passwd_free_check "${addr_array[*]}" $ssh_port $ssh_user
      remote_node_passwd "${addr_array[*]}" $ssh_port $ssh_user
      kw_node_id=0
      cluster_nodeid
      echo "change#local#node#status:70" >&10
      change_kwdb_status
      echo "local#upgrade:75" >&10
      upgrade
      # modify start cmd and restart KaiwuDB
      echo "local#restart:80" >&10
      recover_node
    fi

    # scp all packages
    echo "distribute#packages:85" >&10
    parallel_copy_files "${addr_array[*]}" $ssh_port $ssh_user "$g_deploy_path/packages/*"
    echo "remote#upgrade:90" >&10
    # remote node upgrade
    for node in ${addr_array[*]}
    do
      remote_upgrade $node $ssh_port $ssh_user
    done
    echo "upgrade#done:100" >&10
    wait
    exec 10>&-
    echo -e "\e[1;32m[UPGRADE COMPLETED]:\e[0mCluster upgrade completed."
  fi
fi