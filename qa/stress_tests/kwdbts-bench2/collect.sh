#!/bin/bash

if [ $# -lt 1 ]; then
    echo 'input pid'
    exit 1
fi

flamedir=$(cd $(dirname $0);pwd)
pushd ${flamedir}
cd ${flamedir}/perfdata
echo #####perf recording.....######
sudo rm perf.data
perf record -F 99 -p $1 --call-graph dwarf
perf script > out.perf

svg_file=$2
if [ -z "$svg_file" ]; then svg_file=$(date "+%Y%m%d%H%M%S"); fi
echo $svg_file

${flamedir}/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
${flamedir}/FlameGraph/flamegraph.pl --width 1920 out.folded > ${svg_file}.svg

popd
