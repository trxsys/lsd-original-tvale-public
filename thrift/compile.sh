#!/bin/bash

files=("types" "txn_manager" "data_manager" "barrier")
gens=("py:new_style" "cpp:cob_style")
for file in "${files[@]}"; do
  printf "generating ${file}.thrift... "
  for gen in "${gens[@]}"; do
    printf "${gen} "
    lib/thrift-0.9.3/bin/thrift -o thrift --gen $gen thrift/$file.thrift
  done
  printf "done\n"
done
patch -p0 <thrift/types_types.cpp.patch
patch -p0 <thrift/data_manager.py.patch
patch -p0 <thrift/barrier.py.patch
