#!/bin/bash

# the script for automatically generating a flame graph, needs to copy a FlameGraph to this directory
is_perf=$1
if [ -z "$is_perf" ]; then is_perf=0; fi

if [ $is_perf -eq 1 ]; then
  mkdir perfdata
  svg_file=bench_$(date "+%Y%m%d_%H:%M:%S")
  rm -rf ts_data
fi

export KW_PARTITION_ROWS=10000000 #1KW
export KW_WAL_LEVEL=0
#export DATA_DIR=/wd/data/bench
export DATA_DIR=/data/bench
params="--exist_db 0 --error_exit 0 --benchmarks "savedata" --table_num 1 --column_num 10 \
            --entity_num 100 --data_type "double" --duration 180 --batch_num 1 --thread_num 4"
echo $params
./rel_bench ${params} &

sleep 5 # wait engine init and create table finish

if [ $is_perf -eq 1 ]; then
  bench_id=`pidof rel_bench`

  ./collect.sh $bench_id $svg_file
fi
