#! /bin/bash

function process_bar() {
  local main_pid=$1
  while [ "$(ps -p ${main_pid} | wc -l)" -ne "1" ]; do
    b=""
    if read -u 10 line; then
      if [ "$line" == 'pause' ]; then
        printf "                                                                                                                    \r"
        continue
      fi
    fi
    array=(${line//:/ })
    local proportion=$((${array[1]}*50/100))
    for((i=0;i<$proportion;i++))
    do
      b+='#'
    done
    printf "%-35s: [%-50s] %d%%\r" "${array[0]//#/ }" $b ${array[1]}
    if [ ${array[1]} -eq 100 ];then
      break
    fi
  done
  printf "                                                                                                                    \r"
}