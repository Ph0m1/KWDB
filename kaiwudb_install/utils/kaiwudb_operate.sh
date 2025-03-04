#! /bin/bash

function kw_start() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  local count=0
  eval $prefix systemctl daemon-reload
  eval $prefix systemctl start kaiwudb >/dev/null 2>&1
  if [ $? -ne 0 ];then
    echo "Start KaiwuDB failed. For more information, check system log(journactl -u kaiwudb)."
    return 1
  fi
  sleep 3
  until [ $count -gt 300 ];do
    whether_running >/dev/null 2>&1
    if [ $? -eq 0 ];then
      return 0
    fi
    ((count++));
    sleep 2
  done
  echo "Start KaiwuDB failed. For more information, check kwbase's log."
  return 1
}

function kw_stop() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  eval $prefix systemctl daemon-reload
  eval $prefix systemctl stop kaiwudb >/dev/null 2>&1
  if [ $? -ne 0 ];then
    return 1
  fi
  return 0
}

function kw_restart() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  local count=0
  eval $prefix systemctl daemon-reload
  eval $prefix systemctl restart kaiwudb >/dev/null 2>&1
  if [ $? -ne 0 ];then
    return 1
  fi
  until [ $count -gt 300 ];do
    whether_running >/dev/null 2>&1
    if [ $? -eq 0 ];then
      return 0
    fi
    ((count++));
    sleep 2
  done
  return 1
}

function exec_start() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  local count=0
  eval $prefix systemctl daemon-reload
  eval $prefix systemctl start kaiwudb >/dev/null 2>&1
  if [ $? -ne 0 ];then
    echo "Start KaiwuDB failed. For more information, check system log(journactl -u kaiwudb)."
    return 1
  fi
  sleep 3
  until [ $count -gt 60 ];do
    kw_status >/dev/null 2>&1
    if [ $? -eq 0 ];then
      return 0
    fi
    ((count++));
    sleep 2
  done
  echo "Start KaiwuDB failed. For more information, check kwbase's log."
  return 1
}