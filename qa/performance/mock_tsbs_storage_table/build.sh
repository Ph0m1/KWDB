#!/usr/bin/env bash

if [ -d cmake_build ];then
  rm -rf cmake_build
fi

build_type=Release
mkdir -p cmake_build && cd cmake_build && cmake .. -DCMAKE_BUILD_TYPE=${build_type} && make -j`nproc`
