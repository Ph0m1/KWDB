#!/bin/bash 
#cpu cores
function cpu_info() { 
  local cores=`cat /proc/cpuinfo | grep "processor" | sort -u| wc -l `
  if [ $cores -le 4 ];then
    return 1
  fi
}

#memory info 
function mem_info() { 
  local mem=`cat /proc/meminfo | grep MemTotal | grep -Eo "[0-9]+"`
  # if [ `echo "$mem/1024/1024 < 6" | bc` -eq 1 ];then
  if [ `expr $mem / 1024 / 1024` -lt 6 ];then
    return 1
  fi
}