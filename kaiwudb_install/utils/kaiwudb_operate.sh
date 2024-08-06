#! /bin/bash

function kw_start() {
  local count=0
  eval $kw_cmd_prefix systemctl daemon-reload
  eval $kw_cmd_prefix systemctl start kaiwudb >/dev/null 2>&1
  if [ $? -ne 0 ];then
    return 1
  fi
  if [ "$1" == "bare" ];then
    sudo systemctl status kaiwudb >/dev/null 2>&1
    if [ $? -eq 0 ];then
      return 0
    else
      return 1
    fi
  fi
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
  return 0
}

function kw_stop() {
  eval $kw_cmd_prefix systemctl daemon-reload
  eval $kw_cmd_prefix systemctl stop kaiwudb >/dev/null 2>&1
  if [ $? -ne 0 ];then
    return 1
  fi
  return 0
}

function kw_restart() {
  local count=0
  eval $kw_cmd_prefix systemctl daemon-reload
  eval $kw_cmd_prefix systemctl restart kaiwudb >/dev/null 2>&1
  if [ $? -ne 0 ];then
    return 1
  fi
  if [ "$1" == "bare" ];then
    sudo systemctl status kaiwudb >/dev/null 2>&1
    if [ $? -eq 0 ];then
      return 0
    else
      return 1
    fi
  fi
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
  return 0
}