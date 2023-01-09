#!/bin/bash

cd protobuf
files=("tpcc")
for file in "${files[@]}"; do
  printf "generating ${file}.proto... "
  ../lib/protobuf-3.1.0/src/protoc --cpp_out=. --python_out=. $file.proto
  printf "done\n"
done
cd ..
