#!/bin/bash

make -j $3 $1 config=$2
./scripts/clean.sh
